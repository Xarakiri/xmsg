// SPDX-License-Identifier: AGPL-3.0-only
//! Search indexing (plan §5.12): `messages.persisted` → OpenSearch.
//!
//! - Messages and thread replies become documents keyed by `message_id`, carrying
//!   `workspace_id`, `channel_id`, `msg_seq` (the reply_seq for replies), the author and
//!   the body. Edits replace the body; deletes remove the document.
//! - The search API filters by the caller's channels, so documents need no access fields
//!   beyond `channel_id` and `workspace_id`.
//! - Bulk requests are built here (pure, tested); the binary sends them and commits the
//!   Kafka offsets only after OpenSearch accepted the whole batch. A replay re-applies
//!   events in channel order, so it ends in the same state.

use serde_json::{Value, json};
use xmsg_proto::v1::{SequencedEvent, sequenced_event::Payload};

/// One index for now; monthly indices per workspace (plan §5.12) come with the Large tier.
pub const INDEX: &str = "xmsg-messages-v1";

/// The index's mapping: ids as longs (filters), the body as text.
pub fn mapping() -> Value {
    json!({
        "settings": { "index": { "refresh_interval": "1s" } },
        "mappings": {
            "dynamic": "strict",
            "properties": {
                "workspace_id": { "type": "long" },
                "channel_id": { "type": "long" },
                "msg_seq": { "type": "long" },
                "message_id": { "type": "long" },
                "author_id": { "type": "long" },
                "kind": { "type": "keyword" },
                "root_msg_seq": { "type": "long" },
                "body": { "type": "text" },
                "at_ms": { "type": "date", "format": "epoch_millis" }
            }
        }
    })
}

/// The bulk-API lines (NDJSON: action, then source where needed) for one event; empty
/// for events search doesn't keep.
pub fn bulk_lines(event: &SequencedEvent, at_ms: i64) -> Vec<Value> {
    if event.event_seq <= 0 {
        return Vec::new();
    }
    let doc = |id: i64, msg_seq: i64, kind: &str, root: i64, body: &str| {
        vec![
            json!({ "index": { "_index": INDEX, "_id": id.to_string() } }),
            json!({
                "workspace_id": event.workspace_id,
                "channel_id": event.channel_id,
                "msg_seq": msg_seq,
                "message_id": id,
                "author_id": event.actor_id,
                "kind": kind,
                "root_msg_seq": root,
                "body": body,
                "at_ms": at_ms,
            }),
        ]
    };
    match &event.payload {
        Some(Payload::MessageCreated(m)) => doc(m.message_id, event.msg_seq, "message", 0, &m.body),
        Some(Payload::ThreadReply(r)) => {
            doc(r.message_id, r.reply_seq, "reply", r.root_msg_seq, &r.body)
        }
        Some(Payload::MessageEdited(e)) => vec![
            json!({ "update": { "_index": INDEX, "_id": e.message_id.to_string() } }),
            json!({ "doc": { "body": e.body } }),
        ],
        Some(Payload::MessageDeleted(d)) => {
            vec![json!({ "delete": { "_index": INDEX, "_id": d.message_id.to_string() } })]
        }
        _ => Vec::new(),
    }
}

/// Whether a bulk response says every item went through. A missing document is fine for
/// updates and deletes (an edit or delete of a message indexed before search existed).
pub fn bulk_ok(response: &Value) -> Result<(), String> {
    if response["errors"] != json!(true) {
        return Ok(());
    }
    let items = response["items"].as_array().cloned().unwrap_or_default();
    for item in items {
        let Some((op, result)) = item.as_object().and_then(|o| o.iter().next()) else {
            continue;
        };
        let status = result["status"].as_u64().unwrap_or(0);
        let tolerated = status == 404 && (op == "update" || op == "delete");
        if status >= 300 && !tolerated {
            return Err(format!("{op} {}: {}", result["_id"], result["error"]));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use xmsg_proto::v1::{
        MemberJoined, MessageCreated, MessageDeleted, MessageEdited, ThreadReply,
    };

    fn event(payload: Payload) -> SequencedEvent {
        SequencedEvent {
            channel_id: 7,
            workspace_id: 1,
            actor_id: 42,
            event_seq: 9,
            msg_seq: 3,
            payload: Some(payload),
            ..Default::default()
        }
    }

    #[test]
    fn messages_and_replies_are_documents_edits_update_deletes_remove() {
        let created = bulk_lines(
            &event(Payload::MessageCreated(MessageCreated {
                message_id: 500,
                body: "lunch?".into(),
                ..Default::default()
            })),
            1_000,
        );
        assert_eq!(created[0]["index"]["_id"], "500");
        assert_eq!(created[1]["channel_id"], 7);
        assert_eq!(created[1]["msg_seq"], 3);
        assert_eq!(created[1]["kind"], "message");
        let reply = bulk_lines(
            &event(Payload::ThreadReply(ThreadReply {
                message_id: 501,
                body: "yes".into(),
                root_msg_seq: 3,
                reply_seq: 2,
                ..Default::default()
            })),
            1_000,
        );
        assert_eq!(
            (
                reply[1]["kind"].clone(),
                reply[1]["msg_seq"].clone(),
                reply[1]["root_msg_seq"].clone()
            ),
            (json!("reply"), json!(2), json!(3))
        );
        let edited = bulk_lines(
            &event(Payload::MessageEdited(MessageEdited {
                message_id: 500,
                body: "dinner?".into(),
                ..Default::default()
            })),
            0,
        );
        assert_eq!(
            edited,
            vec![
                json!({"update": {"_index": INDEX, "_id": "500"}}),
                json!({"doc": {"body": "dinner?"}})
            ]
        );
        let deleted = bulk_lines(
            &event(Payload::MessageDeleted(MessageDeleted {
                message_id: 500,
                msg_seq: 3,
            })),
            0,
        );
        assert_eq!(
            deleted,
            vec![json!({"delete": {"_index": INDEX, "_id": "500"}})]
        );
        assert!(bulk_lines(&event(Payload::MemberJoined(MemberJoined::default())), 0).is_empty());
    }

    #[test]
    fn a_missing_document_is_fine_for_updates_and_deletes_only() {
        let ok = json!({"errors": true, "items": [{"delete": {"_id": "1", "status": 404}}, {"update": {"_id": "2", "status": 404}}]});
        assert!(bulk_ok(&ok).is_ok());
        let bad = json!({"errors": true, "items": [{"index": {"_id": "3", "status": 429, "error": "too many"}}]});
        assert!(bulk_ok(&bad).is_err());
        assert!(bulk_ok(&json!({"errors": false})).is_ok());
    }
}
