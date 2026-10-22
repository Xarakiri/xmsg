// SPDX-License-Identifier: AGPL-3.0-only
//! The outbox path for the Small and Medium tiers (ADR-0010, ADR-0020): the built-in Rust
//! poller in place of Debezium, plus `meta-relay`.
//!
//! - **Poller** (`xmsg-outbox-relay`): publishes committed `outbox` rows, in `id` order, to
//!   the outbox topic of their aggregate, then marks them published. A crash in between
//!   republishes them (at least once); every consumer dedupes by the outbox event.
//! - **meta-relay** (`xmsg-meta-relay`): copies channel events from `channels.meta` into
//!   `messages.in`, where the sequencer orders them with the channel's messages. The fast
//!   path already sent most of them; the sequencer drops the repeat by its dedupe ID.

pub mod postgres;

use xmsg_proto::Message;
use xmsg_proto::v1::InboundEvent;

pub const TOPIC_CHANNELS_META: &str = "channels.meta";
pub const TOPIC_USERS_EVENTS: &str = "users.events";
pub const TOPIC_AUTH_EVENTS: &str = "auth.events";
pub const TOPIC_MESSAGES_IN: &str = "messages.in";

/// One committed outbox row.
#[derive(Debug, Clone, PartialEq)]
pub struct OutboxRow {
    pub id: i64,
    pub aggregate_type: String,
    pub aggregate_id: i64,
    pub event_type: String,
    pub payload: Vec<u8>,
}

/// The outbox topic for an aggregate type (plan §8). Unknown types aren't published: they
/// stay in the table until a relay that knows them runs, rather than going somewhere wrong.
pub fn topic_for(aggregate_type: &str) -> Option<&'static str> {
    match aggregate_type {
        "channel" => Some(TOPIC_CHANNELS_META),
        "user" | "workspace" => Some(TOPIC_USERS_EVENTS),
        "session" | "auth" => Some(TOPIC_AUTH_EVENTS),
        _ => None,
    }
}

pub fn known_aggregate_types() -> Vec<String> {
    ["channel", "user", "workspace", "session", "auth"]
        .map(String::from)
        .to_vec()
}

/// Records keyed by aggregate, so one aggregate's events stay in order. Channel events use
/// the channel key of the partition function (ADR-0004), like `messages.in`.
pub fn key_for(row: &OutboxRow) -> Vec<u8> {
    xmsg_partition::channel_key(row.aggregate_id).to_vec()
}

/// What `meta-relay` sends to `messages.in` for a `channels.meta` record: the unchanged
/// `InboundEvent`, keyed by its channel. Records that don't decode, or whose key names
/// another channel, are refused (logged and skipped) instead of being misrouted.
pub fn relay_channel_event(key: Option<&[u8]>, payload: &[u8]) -> Result<(i64, Vec<u8>), String> {
    let event = InboundEvent::decode(payload).map_err(|e| format!("not an InboundEvent: {e}"))?;
    let expected = xmsg_partition::channel_key(event.channel_id);
    if key != Some(&expected[..]) {
        return Err(format!("key does not match channel {}", event.channel_id));
    }
    if event.dedupe_id.is_empty() {
        // Without a dedupe ID the sequencer couldn't drop the fast-path copy.
        return Err(format!(
            "event for channel {} has no dedupe_id",
            event.channel_id
        ));
    }
    Ok((event.channel_id, payload.to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use xmsg_proto::v1::{ChannelCreated, inbound_event};

    fn created(channel_id: i64, dedupe_id: &str) -> Vec<u8> {
        InboundEvent {
            channel_id,
            dedupe_id: dedupe_id.into(),
            payload: Some(inbound_event::Payload::ChannelCreated(
                ChannelCreated::default(),
            )),
            ..Default::default()
        }
        .encode_to_vec()
    }

    #[test]
    fn aggregates_route_to_their_outbox_topics() {
        assert_eq!(topic_for("channel"), Some("channels.meta"));
        assert_eq!(topic_for("workspace"), Some("users.events"));
        assert_eq!(topic_for("session"), Some("auth.events"));
        assert_eq!(topic_for("something-new"), None);
        assert!(
            known_aggregate_types()
                .iter()
                .all(|t| topic_for(t).is_some())
        );
    }

    #[test]
    fn channel_events_are_keyed_like_messages_in() {
        let row = OutboxRow {
            id: 1,
            aggregate_type: "channel".into(),
            aggregate_id: 42,
            event_type: "channel.created".into(),
            payload: vec![],
        };
        assert_eq!(key_for(&row), xmsg_partition::channel_key(42).to_vec());
    }

    #[test]
    fn meta_relay_copies_valid_events_unchanged() {
        let payload = created(42, "create:42");
        let key = xmsg_partition::channel_key(42);
        assert_eq!(
            relay_channel_event(Some(&key), &payload),
            Ok((42, payload.clone()))
        );
    }

    #[test]
    fn meta_relay_refuses_what_it_cannot_route_safely() {
        let payload = created(42, "create:42");
        assert!(
            relay_channel_event(Some(&xmsg_partition::channel_key(43)), &payload).is_err(),
            "wrong key"
        );
        assert!(relay_channel_event(None, &payload).is_err(), "no key");
        assert!(
            relay_channel_event(Some(&xmsg_partition::channel_key(42)), &created(42, "")).is_err(),
            "no dedupe id"
        );
        assert!(
            relay_channel_event(Some(&xmsg_partition::channel_key(42)), b"\xff\xff").is_err(),
            "garbage"
        );
    }
}
