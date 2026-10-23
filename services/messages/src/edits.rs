// SPDX-License-Identifier: AGPL-3.0-only
//! Edits and deletes (plan §5.5).
//!
//! - Only the author edits. The author deletes their own message; a delete of anyone
//!   else's goes out with `as_admin`, and the sequencer checks the actor's role.
//! - The target is read at LOCAL_QUORUM. If it isn't there yet but its `msg_seq` is at or
//!   below the channel head (the persister is a moment behind), the read is retried for
//!   up to ~200 ms.
//! - The answer is 202: the change is sequenced like any event and reaches every member,
//!   the caller included, through fan-out.

use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use xmsg_proto::Message;
use xmsg_proto::v1::{DeleteMessage, EditMessage, InboundEvent, inbound_event::Payload};

use crate::history::{App, MessageRow, Store, bucket, caller, error};
use crate::{EventLog, MAX_BODY_CHARS};

/// How long a target that the head says exists may take to appear.
pub const TARGET_WAIT: Duration = Duration::from_millis(200);
pub const MAX_EDIT_ID_LEN: usize = 64;

#[derive(Deserialize)]
pub struct EditBody {
    body: String,
    /// Client-chosen ID of this edit, so a retried request is sequenced once.
    edit_id: String,
}

/// Why a target can't be edited or deleted, as the HTTP answer.
#[derive(Debug, PartialEq)]
pub enum Refused {
    NotFound,
    Forbidden,
    Invalid(&'static str),
    Unavailable,
}

impl IntoResponse for Refused {
    fn into_response(self) -> Response {
        match self {
            Refused::NotFound => error(StatusCode::NOT_FOUND, "no such message"),
            Refused::Forbidden => {
                error(StatusCode::FORBIDDEN, "only the author can edit a message")
            }
            Refused::Invalid(why) => error(StatusCode::BAD_REQUEST, why),
            Refused::Unavailable => error(StatusCode::SERVICE_UNAVAILABLE, "try again later"),
        }
    }
}

/// The message at `msg_seq`, waiting briefly if the head says it exists but the row
/// isn't visible yet. `None`: there is no such message.
pub async fn target(
    store: &impl Store,
    channel_id: i64,
    msg_seq: i64,
) -> Result<Option<MessageRow>, String> {
    let deadline = tokio::time::Instant::now() + TARGET_WAIT;
    loop {
        let rows = store
            .messages(channel_id, bucket(msg_seq), msg_seq, msg_seq)
            .await?;
        if let Some(row) = rows.into_iter().next() {
            return Ok(Some(row));
        }
        let head = store.head(channel_id).await?.map_or(0, |h| h.msg_seq);
        if msg_seq > head || tokio::time::Instant::now() >= deadline {
            return Ok(None);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Checks an edit and builds its event.
pub async fn plan_edit(
    store: &impl Store,
    user: i64,
    channel_id: i64,
    msg_seq: i64,
    body: &EditBody,
) -> Result<InboundEvent, Refused> {
    if body.body.trim().is_empty() {
        return Err(Refused::Invalid("message body is empty"));
    }
    if body.body.chars().count() > MAX_BODY_CHARS {
        return Err(Refused::Invalid(
            "message body is longer than 40,000 characters",
        ));
    }
    if body.edit_id.is_empty() || body.edit_id.len() > MAX_EDIT_ID_LEN {
        return Err(Refused::Invalid("edit_id must be 1-64 bytes"));
    }
    let row = member_target(store, user, channel_id, msg_seq).await?;
    if row.deleted {
        return Err(Refused::NotFound);
    }
    if row.author_id != user {
        return Err(Refused::Forbidden);
    }
    Ok(InboundEvent {
        channel_id,
        actor_id: user,
        dedupe_id: format!("edit:{msg_seq}:{}", body.edit_id),
        payload: Some(Payload::EditMessage(EditMessage {
            msg_seq,
            message_id: row.message_id,
            body: body.body.clone(),
        })),
        ..Default::default()
    })
}

/// Checks a delete and builds its event; `None` if it's already deleted (nothing to do).
pub async fn plan_delete(
    store: &impl Store,
    user: i64,
    channel_id: i64,
    msg_seq: i64,
) -> Result<Option<InboundEvent>, Refused> {
    let row = member_target(store, user, channel_id, msg_seq).await?;
    if row.deleted {
        return Ok(None);
    }
    Ok(Some(InboundEvent {
        channel_id,
        actor_id: user,
        // One delete per message and actor is enough: repeats are dropped.
        dedupe_id: format!("delete:{msg_seq}:{user}"),
        payload: Some(Payload::DeleteMessage(DeleteMessage {
            msg_seq,
            message_id: row.message_id,
            as_admin: row.author_id != user,
        })),
        ..Default::default()
    }))
}

async fn member_target(
    store: &impl Store,
    user: i64,
    channel_id: i64,
    msg_seq: i64,
) -> Result<MessageRow, Refused> {
    if msg_seq <= 0 {
        return Err(Refused::NotFound);
    }
    let unavailable = |e: String| {
        tracing::error!(error = %e, channel_id, msg_seq, "reading an edit or delete target failed");
        Refused::Unavailable
    };
    // Same answer for "no such channel" and "not a member": channel IDs don't leak.
    if !store
        .is_member(channel_id, user)
        .await
        .map_err(unavailable)?
    {
        return Err(Refused::NotFound);
    }
    target(store, channel_id, msg_seq)
        .await
        .map_err(unavailable)?
        .ok_or(Refused::NotFound)
}

async fn append(log: &impl EventLog, event: InboundEvent) -> Response {
    match log.append(event.channel_id, event.encode_to_vec()).await {
        Ok(()) => (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({ "status": "accepted" })),
        )
            .into_response(),
        Err(e) => {
            tracing::warn!(error = %e, channel_id = event.channel_id, "append to messages.in failed");
            Refused::Unavailable.into_response()
        }
    }
}

pub async fn edit<S: Store, L: EventLog>(
    State(app): State<Arc<App<S, L>>>,
    headers: HeaderMap,
    Path((channel_id, msg_seq)): Path<(i64, i64)>,
    Json(body): Json<EditBody>,
) -> Response {
    let user = match caller(&app.verifier, &headers).await {
        Ok(u) => u,
        Err((status, message)) => return error(status, message),
    };
    match plan_edit(&app.store, user, channel_id, msg_seq, &body).await {
        Ok(event) => append(&app.log, event).await,
        Err(refused) => refused.into_response(),
    }
}

pub async fn delete<S: Store, L: EventLog>(
    State(app): State<Arc<App<S, L>>>,
    headers: HeaderMap,
    Path((channel_id, msg_seq)): Path<(i64, i64)>,
) -> Response {
    let user = match caller(&app.verifier, &headers).await {
        Ok(u) => u,
        Err((status, message)) => return error(status, message),
    };
    match plan_delete(&app.store, user, channel_id, msg_seq).await {
        Ok(Some(event)) => append(&app.log, event).await,
        Ok(None) => (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({ "status": "already_deleted" })),
        )
            .into_response(),
        Err(refused) => refused.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::{EventRow, Head};

    struct Fake {
        member: bool,
        head_msg_seq: i64,
        rows: Vec<MessageRow>,
    }

    impl Store for Fake {
        async fn is_member(&self, _: i64, _: i64) -> Result<bool, String> {
            Ok(self.member)
        }
        async fn head(&self, _: i64) -> Result<Option<Head>, String> {
            Ok(Some(Head {
                event_seq: 100,
                msg_seq: self.head_msg_seq,
                channel_type: 0,
            }))
        }
        async fn events(&self, _: i64, _: i32, _: i64, _: usize) -> Result<Vec<EventRow>, String> {
            Ok(Vec::new())
        }
        async fn messages(
            &self,
            _: i64,
            _: i32,
            from: i64,
            to: i64,
        ) -> Result<Vec<MessageRow>, String> {
            Ok(self
                .rows
                .iter()
                .filter(|m| (from..=to).contains(&m.msg_seq))
                .cloned()
                .collect())
        }
    }

    fn row(msg_seq: i64, author_id: i64, deleted: bool) -> MessageRow {
        MessageRow {
            msg_seq,
            message_id: 1000 + msg_seq,
            event_seq: msg_seq + 1,
            author_id,
            client_msg_id: String::new(),
            body: if deleted { String::new() } else { "hi".into() },
            edited_at_ms: 0,
            deleted,
        }
    }

    fn store() -> Fake {
        Fake {
            member: true,
            head_msg_seq: 3,
            rows: vec![row(1, 5, false), row(2, 6, false), row(3, 5, true)],
        }
    }

    fn body(text: &str) -> EditBody {
        EditBody {
            body: text.into(),
            edit_id: "e1".into(),
        }
    }

    #[tokio::test]
    async fn only_the_author_edits_a_live_message() {
        let event = plan_edit(&store(), 5, 9, 1, &body("fixed")).await.unwrap();
        assert_eq!(event.dedupe_id, "edit:1:e1");
        assert_eq!(
            event.payload,
            Some(Payload::EditMessage(EditMessage {
                msg_seq: 1,
                message_id: 1001,
                body: "fixed".into()
            }))
        );
        assert_eq!(
            plan_edit(&store(), 5, 9, 2, &body("x")).await,
            Err(Refused::Forbidden)
        );
        assert_eq!(
            plan_edit(&store(), 5, 9, 3, &body("x")).await,
            Err(Refused::NotFound),
            "deleted"
        );
        assert_eq!(
            plan_edit(&store(), 5, 9, 9, &body("x")).await,
            Err(Refused::NotFound),
            "past the head"
        );
        assert!(matches!(
            plan_edit(&store(), 5, 9, 1, &body("  ")).await,
            Err(Refused::Invalid(_))
        ));
        let outsider = Fake {
            member: false,
            ..store()
        };
        assert_eq!(
            plan_edit(&outsider, 5, 9, 1, &body("x")).await,
            Err(Refused::NotFound)
        );
    }

    #[tokio::test]
    async fn deleting_someone_elses_message_asks_the_sequencer_for_admin_rights() {
        let own = plan_delete(&store(), 5, 9, 1).await.unwrap().unwrap();
        let Some(Payload::DeleteMessage(d)) = own.payload else {
            panic!()
        };
        assert!(!d.as_admin);
        let other = plan_delete(&store(), 5, 9, 2).await.unwrap().unwrap();
        let Some(Payload::DeleteMessage(d)) = other.payload else {
            panic!()
        };
        assert!(d.as_admin);
        assert_eq!(
            plan_delete(&store(), 5, 9, 3).await,
            Ok(None),
            "already deleted"
        );
    }

    #[tokio::test]
    async fn a_target_the_head_promises_is_waited_for_briefly() {
        // msg_seq 4 is within the head but its row never shows up: gives up after ~200 ms.
        let mut s = store();
        s.head_msg_seq = 4;
        let started = std::time::Instant::now();
        assert_eq!(target(&s, 9, 4).await, Ok(None));
        assert!(started.elapsed() >= TARGET_WAIT);
        // Past the head: no wait.
        let started = std::time::Instant::now();
        assert_eq!(target(&s, 9, 8).await, Ok(None));
        assert!(started.elapsed() < Duration::from_millis(50));
    }
}
