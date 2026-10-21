// SPDX-License-Identifier: AGPL-3.0-only
//! History reads (plan §5.5, §6.6 lazy step): a channel's events after a client's last
//! `event_seq`, rebuilt as the `SequencedEvent`s that were delivered live, so clients
//! apply them with the same code. Members only.
//!
//! `channel_events` holds every event for 30 days; new messages are stored there as
//! pointers (their `msg_seq`) and filled in from `messages`.

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
use xmsg_proto::v1::{EventPage, MessageCreated, SequencedEvent, sequenced_event::Payload};

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

/// One `messages` row.
#[derive(Debug, Clone, PartialEq)]
pub struct MessageRow {
    pub msg_seq: i64,
    pub message_id: i64,
    pub event_seq: i64,
    pub author_id: i64,
    pub client_msg_id: String,
    pub body: String,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Head {
    pub event_seq: i64,
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
    /// The messages of one bucket with `from <= msg_seq <= to`.
    fn messages(
        &self,
        channel_id: i64,
        bucket: i32,
        from: i64,
        to: i64,
    ) -> impl Future<Output = Result<Vec<MessageRow>, String>> + Send;
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

    let pointers: Vec<i64> = rows
        .iter()
        .filter(|r| r.kind == "message.created")
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
    for row in &rows {
        if let Some(event) = rebuild(channel_id, head, row, &bodies) {
            events.push(event);
        }
    }
    let has_more = rows.last().is_some_and(|r| r.event_seq < head.event_seq);
    Ok(EventPage {
        events,
        has_more,
        head_event_seq: head.event_seq,
        truncated,
    })
}

fn pointer(row: &EventRow) -> Option<i64> {
    <[u8; 8]>::try_from(row.payload.as_slice())
        .ok()
        .map(i64::from_be_bytes)
}

fn rebuild(
    channel_id: i64,
    head: Head,
    row: &EventRow,
    bodies: &HashMap<i64, MessageRow>,
) -> Option<SequencedEvent> {
    if row.kind != "message.created" {
        return SequencedEvent::decode(row.payload.as_slice()).ok();
    }
    let m = bodies.get(&pointer(row)?)?;
    Some(SequencedEvent {
        channel_id,
        actor_id: m.author_id,
        event_seq: row.event_seq,
        msg_seq: m.msg_seq,
        dedupe_id: m.client_msg_id.clone(),
        channel_type: head.channel_type,
        payload: Some(Payload::MessageCreated(MessageCreated {
            message_id: m.message_id,
            body: m.body.clone(),
        })),
        ..Default::default()
    })
}

pub struct App<S> {
    pub store: S,
    pub verifier: Verifier,
}

pub fn router<S: Store>(app: Arc<App<S>>) -> Router {
    Router::new()
        .route("/v1/channels/{channel_id}/events", get(events::<S>))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(app)
}

#[derive(Deserialize)]
pub struct EventsQuery {
    #[serde(default)]
    after: i64,
    limit: Option<usize>,
}

fn error(status: StatusCode, message: &str) -> Response {
    (status, Json(serde_json::json!({ "error": message }))).into_response()
}

async fn events<S: Store>(
    State(app): State<Arc<App<S>>>,
    headers: HeaderMap,
    Path(channel_id): Path<i64>,
    Query(q): Query<EventsQuery>,
) -> Response {
    let Some(auth) = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    else {
        return error(StatusCode::UNAUTHORIZED, "invalid or missing access token");
    };
    let user = match app.verifier.verify(auth).await {
        Ok(p) => p.user_id,
        Err(AuthError::KeysUnavailable) => {
            return error(StatusCode::SERVICE_UNAVAILABLE, "try again later");
        }
        Err(_) => return error(StatusCode::UNAUTHORIZED, "invalid or missing access token"),
    };
    if q.after < 0 {
        return error(StatusCode::BAD_REQUEST, "after must be >= 0");
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
    match page(&app.store, channel_id, q.after, limit).await {
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
            },
        )
    }

    fn channel(n: i64) -> Fake {
        // Event 1 is a join, then n messages (event_seq k+1 holds msg_seq k).
        let mut fake = Fake {
            head: Some(Head {
                event_seq: n + 1,
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
    async fn an_unknown_channel_is_an_empty_page() {
        assert_eq!(
            page(&Fake::default(), 7, 0, 10).await.unwrap(),
            EventPage::default()
        );
    }
}
