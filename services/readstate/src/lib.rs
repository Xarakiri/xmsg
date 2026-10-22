// SPDX-License-Identifier: AGPL-3.0-only
//! Read positions (ADR-0012, plan §5.10). Unread = the channel's `msg_seq` minus the
//! user's `last_read_msg_seq`: no counters, so nothing can be double-counted.
//!
//! **One writer per user.** Every change to a read position (mark-read from any device,
//! an own send, a join, mark-unread) is a `ReadStateEvent` on `readstate.events`, keyed by
//! user. The consumer that owns a user's partition is the only writer of their
//! `read_state` rows and applies the events in order: positions merge with max, and
//! "unread from here" is the one explicit step back. The feeder, which reads
//! `messages.persisted` (partitioned by channel), only produces events.

pub mod api;
pub mod cassandra;

use xmsg_proto::v1::{
    ReadStateEvent, SequencedEvent, read_state_event::Change, sequenced_event::Payload,
};

pub const TOPIC: &str = "readstate.events";

/// The position after applying `change` to `current`.
pub fn apply(current: i64, change: &Change) -> i64 {
    match change {
        Change::ReadUpTo(seq) => current.max(*seq),
        Change::UnreadFrom(seq) => (seq - 1).max(0),
    }
}

pub fn read_up_to(user_id: i64, channel_id: i64, msg_seq: i64, at_ms: i64) -> ReadStateEvent {
    ReadStateEvent {
        user_id,
        channel_id,
        at_ms,
        change: Some(Change::ReadUpTo(msg_seq)),
    }
}

/// What the feeder produces for one sequenced event. `channel_msg_seq` is the channel's
/// latest `msg_seq` before this event (for joins: a new member has read nothing older).
pub fn feed(event: &SequencedEvent, channel_msg_seq: i64, at_ms: i64) -> Vec<ReadStateEvent> {
    if event.event_seq <= 0 {
        return Vec::new(); // rejections aren't channel events
    }
    match &event.payload {
        // Sending a message marks the channel read up to it (as Slack does).
        Some(Payload::MessageCreated(_)) => vec![read_up_to(
            event.actor_id,
            event.channel_id,
            event.msg_seq,
            at_ms,
        )],
        // A new member starts at the channel's current message: history isn't unread.
        Some(Payload::MemberJoined(j)) if channel_msg_seq > 0 => {
            vec![read_up_to(
                j.user_id,
                event.channel_id,
                channel_msg_seq,
                at_ms,
            )]
        }
        // channel.created: no messages yet, and no row means position 0.
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xmsg_proto::v1::{MemberJoined, MessageCreated, Rejected};

    #[test]
    fn positions_only_move_forward_except_on_an_explicit_unread() {
        assert_eq!(apply(5, &Change::ReadUpTo(9)), 9);
        assert_eq!(
            apply(9, &Change::ReadUpTo(5)),
            9,
            "a late update from another device can't undo progress"
        );
        assert_eq!(
            apply(9, &Change::UnreadFrom(4)),
            3,
            "mark unread from message 4"
        );
        assert_eq!(apply(9, &Change::UnreadFrom(0)), 0);
    }

    #[test]
    fn replaying_the_same_events_in_order_gives_the_same_position() {
        let events = [
            Change::ReadUpTo(3),
            Change::ReadUpTo(8),
            Change::UnreadFrom(5),
            Change::ReadUpTo(6),
        ];
        let once = events.iter().fold(0, apply);
        let twice = events.iter().chain(events.iter()).fold(0, apply);
        assert_eq!(once, 6);
        // At-least-once delivery re-applies a suffix in order, never reorders.
        let resumed = events[1..].iter().fold(once, apply);
        assert_eq!((once, resumed), (6, 6));
        assert_eq!(twice, 6);
    }

    fn event(event_seq: i64, msg_seq: i64, actor: i64, payload: Payload) -> SequencedEvent {
        SequencedEvent {
            channel_id: 7,
            event_seq,
            msg_seq,
            actor_id: actor,
            payload: Some(payload),
            ..Default::default()
        }
    }

    #[test]
    fn own_sends_mark_read_and_joins_start_at_the_current_message() {
        let sent = feed(
            &event(
                10,
                4,
                42,
                Payload::MessageCreated(MessageCreated::default()),
            ),
            3,
            1,
        );
        assert_eq!(sent, vec![read_up_to(42, 7, 4, 1)]);
        let joined = feed(
            &event(
                11,
                0,
                1,
                Payload::MemberJoined(MemberJoined {
                    user_id: 99,
                    role: 1,
                }),
            ),
            4,
            1,
        );
        assert_eq!(joined, vec![read_up_to(99, 7, 4, 1)]);
        assert!(
            feed(
                &event(
                    2,
                    0,
                    1,
                    Payload::MemberJoined(MemberJoined {
                        user_id: 99,
                        role: 1
                    })
                ),
                0,
                1
            )
            .is_empty()
        );
        assert!(
            feed(
                &event(0, 0, 1, Payload::Rejected(Rejected::default())),
                4,
                1
            )
            .is_empty()
        );
    }
}
