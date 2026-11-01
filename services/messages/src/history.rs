// SPDX-License-Identifier: AGPL-3.0-only
//! History reads (plan §5.5, §6.6 lazy step): a channel's events after a client's last
//! `event_seq`, rebuilt as the `SequencedEvent`s that were delivered live, so clients
//! apply them with the same code. Members only.
//!
//! `channel_events` holds every event for 30 days; new messages and edits are stored
//! there as pointers (their `msg_seq`) and filled in from the current `messages` row, so
//! they show the latest body, and none once the message is deleted.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use xmsg_authn::{Error as AuthError, Verifier};
use xmsg_proto::Message;
use xmsg_proto::v1::{
    EventPage, MessageCreated, MessageEdited, SequencedEvent, sequenced_event::Payload,
};

use crate::EventLog;

/// Rows per `channel_events` / `messages` partition bucket (plan §7.2; same as the persister).
pub const BUCKET_SIZE: i64 = 10_000;
pub const DEFAULT_LIMIT: usize = 200;
pub const MAX_LIMIT: usize = 500;

pub fn bucket(seq: i64) -> i32 {
    (seq / BUCKET_SIZE) as i32
}

/// One `channel_events` row.
#[derive(Debug, Clone, PartialEq)]
pub struct EventRow {
    pub event_seq: i64,
    pub kind: String,
    pub payload: Vec<u8>,
}

/// One `thread_messages` row: a reply.
#[derive(Debug, Clone, PartialEq)]
pub struct ReplyRow {
    pub reply_seq: i64,
    pub message_id: i64,
    pub author_id: i64,
    pub body: String,
    /// `Attachments`, encoded (ADR-0024); empty without files.
    pub attachments: Vec<u8>,
}

/// One `messages` row.
#[derive(Debug, Clone, PartialEq)]
pub struct MessageRow {
    pub msg_seq: i64,
    pub message_id: i64,
    pub event_seq: i64,
    pub author_id: i64,
    pub client_msg_id: String,
    /// Empty once deleted.
    pub body: String,
    /// 0 if never edited.
    pub edited_at_ms: i64,
    pub deleted: bool,
    /// `Attachments`, encoded (ADR-0024); empty without files.
    pub attachments: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Head {
    pub event_seq: i64,
    pub msg_seq: i64,
    pub channel_type: i32,
}

pub trait Store: Send + Sync + 'static {
    fn is_member(
        &self,
        channel_id: i64,
        user_id: i64,
    ) -> impl Future<Output = Result<bool, String>> + Send;
    fn head(&self, channel_id: i64) -> impl Future<Output = Result<Option<Head>, String>> + Send;
    /// Up to `limit` rows of one bucket with `event_seq > after`, ascending.
    fn events(
        &self,
        channel_id: i64,
        bucket: i32,
        after: i64,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<EventRow>, String>> + Send;
    /// Up to `limit` rows of one bucket with `event_seq < before`, descending.
    fn events_before(
        &self,
        channel_id: i64,
        bucket: i32,
        before: i64,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<EventRow>, String>> + Send;
    /// The messages of one bucket with `from <= msg_seq <= to`.
    fn messages(
        &self,
        channel_id: i64,
        bucket: i32,
        from: i64,
        to: i64,
    ) -> impl Future<Output = Result<Vec<MessageRow>, String>> + Send;
    /// The channels `user_id` is in (at most 1,000), for scoping search.
    fn my_channels(&self, _user_id: i64) -> impl Future<Output = Result<Vec<i64>, String>> + Send {
        async { Ok(Vec::new()) }
    }
    /// A thread's replies after `after`, in reply_seq order, at most `limit`.
    fn thread(
        &self,
        _channel_id: i64,
        _root_msg_seq: i64,
        _after: i64,
        _limit: usize,
    ) -> impl Future<Output = Result<Vec<ReplyRow>, String>> + Send {
        async { Ok(Vec::new()) }
    }
}

/// Reads up to `limit` events after `after` and rebuilds them.
pub async fn page(
    store: &impl Store,
    channel_id: i64,
    after: i64,
    limit: usize,
) -> Result<EventPage, String> {
    let Some(head) = store.head(channel_id).await? else {
        return Ok(EventPage::default());
    };
    let mut rows = Vec::new();
    let mut b = bucket(after + 1);
    while rows.len() < limit && b <= bucket(head.event_seq) {
        let from = rows.last().map_or(after, |r: &EventRow| r.event_seq);
        rows.extend(
            store
                .events(channel_id, b, from, limit - rows.len())
                .await?,
        );
        b += 1;
    }
    // The log keeps 30 days: if the first event after `after` is gone, there's a gap.
    let truncated = after < head.event_seq && rows.first().is_none_or(|r| r.event_seq > after + 1);

    let events = rebuild(store, channel_id, head, &rows).await?;
    let has_more = rows.last().is_some_and(|r| r.event_seq < head.event_seq);
    Ok(EventPage {
        events,
        has_more,
        head_event_seq: head.event_seq,
        truncated,
    })
}

/// Reads up to `limit` events before `before` (older pages, scroll-up) and rebuilds
/// them, oldest first in the answer. `has_more` stays true while older events may
/// exist; a page short of `limit` reached the retained log's oldest row, so when that
/// row isn't event 1 the true beginning is gone (`truncated`).
pub async fn page_before(
    store: &impl Store,
    channel_id: i64,
    before: i64,
    limit: usize,
) -> Result<EventPage, String> {
    let Some(head) = store.head(channel_id).await? else {
        return Ok(EventPage::default());
    };
    let mut rows: Vec<EventRow> = Vec::new();
    let mut b = bucket(before - 1);
    while rows.len() < limit && b >= 0 {
        let up_to = rows.last().map_or(before, |r: &EventRow| r.event_seq);
        rows.extend(
            store
                .events_before(channel_id, b, up_to, limit - rows.len())
                .await?,
        );
        b -= 1;
    }
    rows.reverse(); // collected newest-first across descending buckets

    let oldest = rows.last().map(|r| r.event_seq);
    let has_more = rows.len() == limit && oldest.is_some_and(|seq| seq > 1);
    let truncated = if rows.is_empty() {
        before > 1
    } else {
        !has_more && oldest.is_some_and(|seq| seq > 1)
    };

    let events = rebuild(store, channel_id, head, &rows).await?;
    Ok(EventPage {
        events,
        has_more,
        head_event_seq: head.event_seq,
        truncated,
    })
}

/// Fills in pointer rows from `messages` and rebuilds the sequenced events, in row order.
async fn rebuild(
    store: &impl Store,
    channel_id: i64,
    head: Head,
    rows: &[EventRow],
) -> Result<Vec<SequencedEvent>, String> {
    let pointers: Vec<i64> = rows
        .iter()
        .filter(|r| is_pointer(&r.kind))
        .filter_map(pointer)
        .collect();
    let mut bodies: HashMap<i64, MessageRow> = HashMap::new();
    let mut by_bucket: HashMap<i32, (i64, i64)> = HashMap::new();
    for seq in &pointers {
        let range = by_bucket.entry(bucket(*seq)).or_insert((*seq, *seq));
        *range = (range.0.min(*seq), range.1.max(*seq));
    }
    for (b, (from, to)) in by_bucket {
        for m in store.messages(channel_id, b, from, to).await? {
            bodies.insert(m.msg_seq, m);
        }
    }

    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        if let Some(event) = rebuild_row(channel_id, head, row, &bodies) {
            events.push(event);
        }
    }
    Ok(events)
}

fn is_pointer(kind: &str) -> bool {
    kind == "message.created" || kind == "message.edited"
}

fn pointer(row: &EventRow) -> Option<i64> {
    <[u8; 8]>::try_from(row.payload.as_slice())
        .ok()
        .map(i64::from_be_bytes)
}

fn rebuild_row(
    channel_id: i64,
    head: Head,
    row: &EventRow,
    bodies: &HashMap<i64, MessageRow>,
) -> Option<SequencedEvent> {
    if !is_pointer(&row.kind) {
        return SequencedEvent::decode(row.payload.as_slice()).ok();
    }
    let m = bodies.get(&pointer(row)?)?;
    let base = SequencedEvent {
        channel_id,
        actor_id: m.author_id,
        event_seq: row.event_seq,
        channel_type: head.channel_type,
        ..Default::default()
    };
    Some(if row.kind == "message.edited" {
        SequencedEvent {
            payload: Some(Payload::MessageEdited(MessageEdited {
                msg_seq: m.msg_seq,
                message_id: m.message_id,
                body: m.body.clone(),
                edited_at_ms: m.edited_at_ms,
            })),
            ..base
        }
    } else {
        SequencedEvent {
            msg_seq: m.msg_seq,
            dedupe_id: m.client_msg_id.clone(),
            payload: Some(Payload::MessageCreated(MessageCreated {
                message_id: m.message_id,
                body: m.body.clone(),
                // A deleted message's files go with its body.
                attachments: if m.deleted || m.attachments.is_empty() {
                    Vec::new()
                } else {
                    xmsg_proto::v1::Attachments::decode(m.attachments.as_slice())
                        .map(|a| a.items)
                        .unwrap_or_default()
                },
                ..Default::default()
            })),
            ..base
        }
    })
}

pub struct App<S, L> {
    pub store: S,
    /// Where edits and deletes go (`messages.in`).
    pub log: L,
    /// Where reactions go (`reactions.in`, ADR-0015).
    pub reactions: L,
    /// OpenSearch, when search is on (it's optional on the Small tier).
    pub search: Option<crate::search::Search>,
    pub verifier: Verifier,
}

pub fn router<S: Store, L: EventLog + Send + Sync + 'static>(app: Arc<App<S, L>>) -> Router {
    Router::new()
        .route("/v1/channels/{channel_id}/events", get(events::<S, L>))
        .route("/v1/search", get(crate::search::search::<S, L>))
        .route(
            "/v1/channels/{channel_id}/threads/{root_msg_seq}",
            get(crate::threads::replies::<S, L>),
        )
        .route(
            "/v1/channels/{channel_id}/messages/{msg_seq}",
            axum::routing::patch(crate::edits::edit::<S, L>).delete(crate::edits::delete::<S, L>),
        )
        .route(
            "/v1/channels/{channel_id}/messages/{msg_seq}/reactions/{emoji}",
            axum::routing::put(crate::reactions::add::<S, L>)
                .delete(crate::reactions::remove::<S, L>),
        )
        .route("/healthz", get(|| async { "ok" }))
        .with_state(app)
}

#[derive(Deserialize)]
pub struct EventsQuery {
    #[serde(default)]
    after: i64,
    /// When set, page backward instead: up to `limit` events with `event_seq < before`,
    /// oldest first in the answer (scrolling up toward older history).
    before: Option<i64>,
    limit: Option<usize>,
}

pub(crate) fn error(status: StatusCode, message: &str) -> Response {
    (status, Json(serde_json::json!({ "error": message }))).into_response()
}

/// The caller's user ID from the bearer token, or the status and message to answer.
pub(crate) async fn caller(
    verifier: &Verifier,
    headers: &HeaderMap,
) -> Result<i64, (StatusCode, &'static str)> {
    const INVALID: (StatusCode, &str) =
        (StatusCode::UNAUTHORIZED, "invalid or missing access token");
    let auth = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or(INVALID)?;
    match verifier.verify(auth).await {
        Ok(p) => Ok(p.user_id),
        Err(AuthError::KeysUnavailable) => {
            Err((StatusCode::SERVICE_UNAVAILABLE, "try again later"))
        }
        Err(_) => Err(INVALID),
    }
}

async fn events<S: Store, L>(
    State(app): State<Arc<App<S, L>>>,
    headers: HeaderMap,
    Path(channel_id): Path<i64>,
    Query(q): Query<EventsQuery>,
) -> Response {
    let user = match caller(&app.verifier, &headers).await {
        Ok(u) => u,
        Err((status, message)) => return error(status, message),
    };
    if q.after < 0 {
        return error(StatusCode::BAD_REQUEST, "after must be >= 0");
    }
    if let Some(before) = q.before
        && before < 1
    {
        return error(StatusCode::BAD_REQUEST, "before must be >= 1");
    }
    // Same answer for "no such channel" and "not a member": channel IDs don't leak.
    match app.store.is_member(channel_id, user).await {
        Ok(true) => {}
        Ok(false) => return error(StatusCode::NOT_FOUND, "unknown channel"),
        Err(e) => {
            tracing::error!(error = %e, "membership read failed");
            return error(StatusCode::SERVICE_UNAVAILABLE, "try again later");
        }
    }
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let read = match q.before {
        Some(before) => page_before(&app.store, channel_id, before, limit).await,
        None => page(&app.store, channel_id, q.after, limit).await,
    };
    match read {
        Ok(page) => (
            [(header::CONTENT_TYPE, "application/x-protobuf")],
            page.encode_to_vec(),
        )
            .into_response(),
        Err(e) => {
            tracing::error!(error = %e, channel_id, "history read failed");
            error(StatusCode::SERVICE_UNAVAILABLE, "try again later")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xmsg_proto::v1::{ChannelType, MemberJoined, MemberRole};

    #[derive(Default)]
    struct Fake {
        head: Option<Head>,
        events: Vec<EventRow>,
        messages: Vec<MessageRow>,
    }

    impl Store for Fake {
        async fn is_member(&self, _: i64, _: i64) -> Result<bool, String> {
            Ok(true)
        }
        async fn head(&self, _: i64) -> Result<Option<Head>, String> {
            Ok(self.head)
        }
        async fn events(
            &self,
            _: i64,
            b: i32,
            after: i64,
            limit: usize,
        ) -> Result<Vec<EventRow>, String> {
            Ok(self
                .events
                .iter()
                .filter(|r| bucket(r.event_seq) == b && r.event_seq > after)
                .take(limit)
                .cloned()
                .collect())
        }
        async fn events_before(
            &self,
            _: i64,
            b: i32,
            before: i64,
            limit: usize,
        ) -> Result<Vec<EventRow>, String> {
            let mut rows: Vec<EventRow> = self
                .events
                .iter()
                .filter(|r| bucket(r.event_seq) == b && r.event_seq < before)
                .cloned()
                .collect();
            rows.reverse(); // descending, per the trait contract
            rows.truncate(limit);
            Ok(rows)
        }
        async fn messages(
            &self,
            _: i64,
            b: i32,
            from: i64,
            to: i64,
        ) -> Result<Vec<MessageRow>, String> {
            Ok(self
                .messages
                .iter()
                .filter(|m| bucket(m.msg_seq) == b && (from..=to).contains(&m.msg_seq))
                .cloned()
                .collect())
        }
    }

    fn joined(event_seq: i64, user: i64) -> EventRow {
        let event = SequencedEvent {
            channel_id: 7,
            event_seq,
            payload: Some(Payload::MemberJoined(MemberJoined {
                user_id: user,
                role: MemberRole::Member as i32,
            })),
            ..Default::default()
        };
        EventRow {
            event_seq,
            kind: "member.joined".into(),
            payload: event.encode_to_vec(),
        }
    }

    fn message(event_seq: i64, msg_seq: i64) -> (EventRow, MessageRow) {
        (
            EventRow {
                event_seq,
                kind: "message.created".into(),
                payload: msg_seq.to_be_bytes().to_vec(),
            },
            MessageRow {
                msg_seq,
                message_id: 1000 + msg_seq,
                event_seq,
                author_id: 5,
                client_msg_id: format!("c-{msg_seq}"),
                body: format!("body {msg_seq}"),
                edited_at_ms: 0,
                deleted: false,
                attachments: Vec::new(),
            },
        )
    }

    fn channel(n: i64) -> Fake {
        // Event 1 is a join, then n messages (event_seq k+1 holds msg_seq k).
        let mut fake = Fake {
            head: Some(Head {
                event_seq: n + 1,
                msg_seq: n,
                channel_type: ChannelType::Private as i32,
            }),
            ..Default::default()
        };
        fake.events.push(joined(1, 5));
        for k in 1..=n {
            let (e, m) = message(k + 1, k);
            fake.events.push(e);
            fake.messages.push(m);
        }
        fake
    }

    #[tokio::test]
    async fn events_are_rebuilt_exactly_as_delivered() {
        let page = page(&channel(2), 7, 0, 10).await.unwrap();
        assert_eq!(
            page.events.iter().map(|e| e.event_seq).collect::<Vec<_>>(),
            [1, 2, 3]
        );
        assert!(matches!(
            page.events[0].payload,
            Some(Payload::MemberJoined(_))
        ));
        let m = &page.events[2];
        assert_eq!(
            (m.msg_seq, m.actor_id, m.dedupe_id.as_str(), m.channel_type),
            (2, 5, "c-2", ChannelType::Private as i32)
        );
        assert!(
            matches!(&m.payload, Some(Payload::MessageCreated(c)) if c.body == "body 2" && c.message_id == 1002)
        );
        assert!(!page.has_more && !page.truncated);
        assert_eq!(page.head_event_seq, 3);
    }

    #[tokio::test]
    async fn pages_continue_from_the_last_event_and_across_buckets() {
        let fake = channel(BUCKET_SIZE + 5); // spans buckets 0 and 1
        let first = page(&fake, 7, BUCKET_SIZE - 3, 5).await.unwrap();
        let seqs: Vec<i64> = first.events.iter().map(|e| e.event_seq).collect();
        assert_eq!(
            seqs,
            (BUCKET_SIZE - 2..=BUCKET_SIZE + 2).collect::<Vec<_>>()
        );
        assert!(first.has_more);
        let rest = page(&fake, 7, *seqs.last().unwrap(), 500).await.unwrap();
        assert_eq!(rest.events.first().unwrap().event_seq, BUCKET_SIZE + 3);
        assert_eq!(rest.events.last().unwrap().event_seq, BUCKET_SIZE + 6);
        assert!(!rest.has_more);
    }

    #[tokio::test]
    async fn a_position_older_than_the_log_is_reported_as_truncated() {
        let mut fake = channel(10);
        fake.events.retain(|e| e.event_seq > 6); // expired by TTL
        let page = page(&fake, 7, 2, 50).await.unwrap();
        assert!(page.truncated);
        assert_eq!(page.events.first().unwrap().event_seq, 7);
        let current = super::page(&fake, 7, 11, 50).await.unwrap();
        assert!(
            !current.truncated && current.events.is_empty(),
            "caught up is not a gap"
        );
    }

    #[tokio::test]
    async fn attachments_come_back_and_go_with_a_deleted_message() {
        let file = xmsg_proto::v1::Attachment {
            file_id: 42,
            name: "cat.png".into(),
            content_type: "image/png".into(),
            size: 10,
            ..Default::default()
        };
        let stored = xmsg_proto::v1::Attachments {
            items: vec![file.clone()],
        }
        .encode_to_vec();
        let mut fake = channel(2);
        fake.messages[0].attachments = stored.clone();
        fake.messages[1].attachments = stored;
        fake.messages[1].deleted = true;
        let events = page(&fake, 7, 0, 10).await.unwrap().events;
        let attachments = |i: usize| match &events[i].payload {
            Some(Payload::MessageCreated(m)) => m.attachments.clone(),
            other => panic!("{other:?}"),
        };
        assert_eq!(attachments(1), vec![file]);
        assert!(attachments(2).is_empty(), "deleted");
    }

    #[tokio::test]
    async fn edits_rebuild_from_the_current_row_and_deletes_leave_no_body() {
        let mut fake = channel(2);
        // Message 1 was edited (event 4), message 2 deleted (event 5).
        fake.messages[0].body = "edited 1".into();
        fake.messages[0].edited_at_ms = 42;
        fake.messages[1].body = String::new();
        fake.messages[1].deleted = true;
        fake.events.push(EventRow {
            event_seq: 4,
            kind: "message.edited".into(),
            payload: 1i64.to_be_bytes().to_vec(),
        });
        let deleted = SequencedEvent {
            channel_id: 7,
            event_seq: 5,
            payload: Some(Payload::MessageDeleted(xmsg_proto::v1::MessageDeleted {
                msg_seq: 2,
                message_id: 1002,
            })),
            ..Default::default()
        };
        fake.events.push(EventRow {
            event_seq: 5,
            kind: "message.deleted".into(),
            payload: deleted.encode_to_vec(),
        });
        fake.head = Some(Head {
            event_seq: 5,
            msg_seq: 2,
            channel_type: ChannelType::Private as i32,
        });

        let events = page(&fake, 7, 0, 10).await.unwrap().events;
        assert_eq!(events.len(), 5);
        let Some(Payload::MessageCreated(first)) = &events[1].payload else {
            panic!()
        };
        assert_eq!(first.body, "edited 1", "history shows the latest body");
        let Some(Payload::MessageCreated(second)) = &events[2].payload else {
            panic!()
        };
        assert_eq!(second.body, "", "a deleted message's body is gone");
        let edited = &events[3];
        assert_eq!(
            (edited.event_seq, edited.msg_seq, edited.actor_id),
            (4, 0, 5)
        );
        let Some(Payload::MessageEdited(e)) = &edited.payload else {
            panic!()
        };
        assert_eq!(
            (e.msg_seq, e.message_id, e.body.as_str(), e.edited_at_ms),
            (1, 1001, "edited 1", 42)
        );
        assert_eq!(events[4], deleted);
    }

    #[tokio::test]
    async fn an_unknown_channel_is_an_empty_page() {
        assert_eq!(
            page(&Fake::default(), 7, 0, 10).await.unwrap(),
            EventPage::default()
        );
    }

    #[tokio::test]
    async fn before_pages_walk_back_oldest_first_with_has_more() {
        let fake = channel(10); // events 1 (join) ..= 11
        let page = page_before(&fake, 7, 12, 5).await.unwrap();
        assert_eq!(
            page.events.iter().map(|e| e.event_seq).collect::<Vec<_>>(),
            [7, 8, 9, 10, 11]
        );
        assert!(page.has_more, "event 1 still exists below");
        assert!(!page.truncated);
        assert_eq!(page.head_event_seq, 11);

        let next = page_before(&fake, 7, 7, 5).await.unwrap();
        assert_eq!(
            next.events.iter().map(|e| e.event_seq).collect::<Vec<_>>(),
            [2, 3, 4, 5, 6]
        );
        assert!(next.has_more);

        let start = page_before(&fake, 7, 2, 5).await.unwrap();
        assert_eq!(
            start.events.iter().map(|e| e.event_seq).collect::<Vec<_>>(),
            [1]
        );
        assert!(
            !start.has_more && !start.truncated,
            "reached the real start"
        );
    }

    #[tokio::test]
    async fn before_pages_continue_across_buckets_and_rebuild_bodies() {
        let fake = channel(BUCKET_SIZE + 5); // events 1 ..= BUCKET_SIZE + 6
        let page = page_before(&fake, 7, BUCKET_SIZE + 4, 5).await.unwrap();
        let seqs: Vec<i64> = page.events.iter().map(|e| e.event_seq).collect();
        assert_eq!(
            seqs,
            (BUCKET_SIZE - 1..=BUCKET_SIZE + 3).collect::<Vec<_>>()
        );
        assert!(page.has_more);
        let m = page.events.last().unwrap();
        assert!(
            matches!(&m.payload, Some(Payload::MessageCreated(c)) if c.body == format!("body {}", BUCKET_SIZE + 2))
        );
        let earlier = page_before(&fake, 7, BUCKET_SIZE, 5).await.unwrap();
        assert_eq!(
            earlier.events.first().unwrap().event_seq,
            BUCKET_SIZE - 5,
            "the previous bucket picks up right below"
        );
    }

    #[tokio::test]
    async fn a_before_page_short_of_the_oldest_row_is_truncated() {
        let mut fake = channel(10);
        fake.events.retain(|e| e.event_seq > 6); // the beginning expired by TTL
        let page = page_before(&fake, 7, 12, 50).await.unwrap();
        assert_eq!(
            page.events.iter().map(|e| e.event_seq).collect::<Vec<_>>(),
            [7, 8, 9, 10, 11]
        );
        assert!(!page.has_more);
        assert!(page.truncated, "the true beginning is older than the log");
    }

    #[tokio::test]
    async fn a_before_page_past_everything_is_empty_and_not_truncated_at_the_start() {
        let fake = channel(3);
        let page = page_before(&fake, 7, 1, 50).await.unwrap();
        assert!(page.events.is_empty() && !page.has_more && !page.truncated);
    }
}
