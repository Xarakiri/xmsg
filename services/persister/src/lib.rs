// SPDX-License-Identifier: AGPL-3.0-only
//! Persister (plan §5.7): the only writer of the wide-column tables it owns.
//!
//! Reads `messages.persisted` in order and turns each sequenced event into
//! idempotent writes. Values that change over time are written as the absolute
//! values carried by the event, never incremented, so a replay after a crash
//! ends in the same state (ADR-0002). Membership rows are written before the
//! `membership_version` that announces them (ADR-0006, review #4 F3).

pub mod cassandra;
pub mod config;

use xmsg_proto::v1::{
    ChannelType, FanoutMode, MemberRole, SequencedEvent, sequenced_event::Payload,
};

/// Rows per partition bucket for `messages` and `channel_events` (plan §7.2).
pub const BUCKET_SIZE: i64 = 10_000;

/// One write to the store. `apply` executes them strictly in order.
#[derive(Debug, Clone, PartialEq)]
pub enum Write {
    Message {
        channel_id: i64,
        bucket: i32,
        msg_seq: i64,
        message_id: i64,
        event_seq: i64,
        author_id: i64,
        /// The sender's client_msg_id (the event's dedupe_id).
        client_msg_id: String,
        body: String,
    },
    /// Sets the new body unless the message was deleted meanwhile: a delete and an edit
    /// can both pass the messages service's check and be sequenced delete first.
    MessageEdited {
        channel_id: i64,
        bucket: i32,
        msg_seq: i64,
        message_id: i64,
        body: String,
        edited_at_ms: i64,
    },
    /// Clears the body and marks the row deleted.
    MessageDeleted {
        channel_id: i64,
        bucket: i32,
        msg_seq: i64,
        message_id: i64,
    },
    /// 30-day event log for sync. New messages and edits are stored as pointers (their
    /// msg_seq), not bodies, so a delete removes every version from what sync returns.
    ChannelEvent {
        channel_id: i64,
        bucket: i32,
        event_seq: i64,
        kind: &'static str,
        payload: Vec<u8>,
    },
    MemberAdded {
        channel_id: i64,
        user_id: i64,
        workspace_id: i64,
        role: &'static str,
        channel_type: &'static str,
        fanout_mode: &'static str,
        joined_at_ms: i64,
    },
    MemberRemoved {
        channel_id: i64,
        user_id: i64,
        workspace_id: i64,
    },
    /// Head fields every event updates; `msg_seq` only for new messages.
    Head {
        channel_id: i64,
        event_seq: i64,
        msg_seq: Option<i64>,
        membership_version: i64,
        fanout_mode: &'static str,
        channel_type: &'static str,
    },
    /// Rewrites `channels_by_user.fanout_mode` for every member after a mode switch.
    FanoutModeForMembers {
        channel_id: i64,
        workspace_id: i64,
        fanout_mode: &'static str,
    },
}

pub fn bucket(seq: i64) -> i32 {
    (seq / BUCKET_SIZE) as i32
}

pub fn channel_type_name(t: i32) -> &'static str {
    match ChannelType::try_from(t) {
        Ok(ChannelType::Public) => "public",
        Ok(ChannelType::Private) => "private",
        Ok(ChannelType::Dm) => "dm",
        Ok(ChannelType::GroupDm) => "group_dm",
        Ok(ChannelType::Broadcast) => "broadcast",
        _ => "unspecified",
    }
}

pub fn fanout_mode_name(m: i32) -> &'static str {
    match FanoutMode::try_from(m) {
        Ok(FanoutMode::Pull) => "pull",
        _ => "push",
    }
}

pub fn role_name(r: i32) -> &'static str {
    match MemberRole::try_from(r) {
        Ok(MemberRole::Admin) => "admin",
        Ok(MemberRole::Poster) => "poster",
        _ => "member",
    }
}

/// The writes for one sequenced event, in the order they must be applied.
/// `event_time_ms` is the Kafka record timestamp (used for `joined_at`).
pub fn writes(event: &SequencedEvent, event_time_ms: i64) -> Vec<Write> {
    // Rejections aren't channel events (event_seq 0): nothing to store.
    if event.event_seq <= 0 {
        return Vec::new();
    }
    let channel_type = channel_type_name(event.channel_type);
    let fanout_mode = fanout_mode_name(event.fanout_mode);
    let event_bucket = bucket(event.event_seq);
    let mut out = Vec::new();
    let mut msg_seq = None;
    let full_payload = || xmsg_proto::Message::encode_to_vec(event);

    let member = |user_id: i64, role: i32| Write::MemberAdded {
        channel_id: event.channel_id,
        user_id,
        workspace_id: event.workspace_id,
        role: role_name(role),
        channel_type,
        fanout_mode,
        joined_at_ms: event_time_ms,
    };

    match &event.payload {
        Some(Payload::MessageCreated(created)) => {
            msg_seq = Some(event.msg_seq);
            out.push(Write::Message {
                channel_id: event.channel_id,
                bucket: bucket(event.msg_seq),
                msg_seq: event.msg_seq,
                message_id: created.message_id,
                event_seq: event.event_seq,
                author_id: event.actor_id,
                client_msg_id: event.dedupe_id.clone(),
                body: created.body.clone(),
            });
            out.push(Write::ChannelEvent {
                channel_id: event.channel_id,
                bucket: event_bucket,
                event_seq: event.event_seq,
                kind: "message.created",
                payload: event.msg_seq.to_be_bytes().to_vec(),
            });
        }
        Some(Payload::MessageEdited(edited)) => {
            out.push(Write::MessageEdited {
                channel_id: event.channel_id,
                bucket: bucket(edited.msg_seq),
                msg_seq: edited.msg_seq,
                message_id: edited.message_id,
                body: edited.body.clone(),
                edited_at_ms: edited.edited_at_ms,
            });
            out.push(channel_event(
                event,
                "message.edited",
                edited.msg_seq.to_be_bytes().to_vec(),
            ));
        }
        Some(Payload::MessageDeleted(deleted)) => {
            out.push(Write::MessageDeleted {
                channel_id: event.channel_id,
                bucket: bucket(deleted.msg_seq),
                msg_seq: deleted.msg_seq,
                message_id: deleted.message_id,
            });
            out.push(channel_event(event, "message.deleted", full_payload()));
        }
        Some(Payload::ChannelCreated(created)) => {
            out.extend(created.members.iter().map(|m| member(m.user_id, m.role)));
            out.push(channel_event(event, "channel.created", full_payload()));
        }
        Some(Payload::MemberJoined(joined)) => {
            out.push(member(joined.user_id, joined.role));
            out.push(channel_event(event, "member.joined", full_payload()));
        }
        Some(Payload::MemberLeft(left)) => {
            out.push(Write::MemberRemoved {
                channel_id: event.channel_id,
                user_id: left.user_id,
                workspace_id: event.workspace_id,
            });
            out.push(channel_event(event, "member.left", full_payload()));
        }
        Some(Payload::ChannelModeChanged(_)) => {
            out.push(Write::FanoutModeForMembers {
                channel_id: event.channel_id,
                workspace_id: event.workspace_id,
                fanout_mode,
            });
            out.push(channel_event(event, "channel.mode_changed", full_payload()));
        }
        Some(Payload::Rejected(_)) | None => return Vec::new(),
    }

    // Last: the head, including membership_version, after the rows it describes.
    out.push(Write::Head {
        channel_id: event.channel_id,
        event_seq: event.event_seq,
        msg_seq,
        membership_version: event.membership_version,
        fanout_mode,
        channel_type,
    });
    out
}

fn channel_event(event: &SequencedEvent, kind: &'static str, payload: Vec<u8>) -> Write {
    Write::ChannelEvent {
        channel_id: event.channel_id,
        bucket: bucket(event.event_seq),
        event_seq: event.event_seq,
        kind,
        payload,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xmsg_proto::v1::{
        ChannelCreated, MemberJoined, MemberLeft, MessageCreated, MessageDeleted, MessageEdited,
        Rejected,
    };

    fn event(event_seq: i64, payload: Payload) -> SequencedEvent {
        SequencedEvent {
            channel_id: 100,
            workspace_id: 10,
            actor_id: 1,
            event_seq,
            membership_version: 1,
            fanout_mode: FanoutMode::Push as i32,
            channel_type: ChannelType::Private as i32,
            payload: Some(payload),
            ..Default::default()
        }
    }

    #[test]
    fn a_message_writes_the_row_a_pointer_event_and_the_head_last() {
        let mut e = event(
            12_345,
            Payload::MessageCreated(MessageCreated {
                message_id: 99,
                body: "hi".into(),
            }),
        );
        e.msg_seq = 20_001;
        let w = writes(&e, 0);
        assert_eq!(w.len(), 3);
        assert!(matches!(
            &w[0],
            Write::Message {
                bucket: 2,
                msg_seq: 20_001,
                message_id: 99,
                author_id: 1,
                ..
            }
        ));
        match &w[1] {
            Write::ChannelEvent {
                bucket,
                kind,
                payload,
                ..
            } => {
                assert_eq!((*bucket, *kind), (1, "message.created"));
                assert_eq!(
                    payload,
                    &20_001_i64.to_be_bytes().to_vec(),
                    "a pointer, not the body"
                );
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            &w[2],
            Write::Head {
                msg_seq: Some(20_001),
                event_seq: 12_345,
                ..
            }
        ));
    }

    #[test]
    fn membership_rows_come_before_the_version_that_announces_them() {
        let created = event(
            1,
            Payload::ChannelCreated(ChannelCreated {
                r#type: ChannelType::Private as i32,
                name: String::new(),
                initial_member_ids: vec![1, 2],
                members: vec![
                    MemberJoined {
                        user_id: 1,
                        role: MemberRole::Admin as i32,
                    },
                    MemberJoined {
                        user_id: 2,
                        role: MemberRole::Member as i32,
                    },
                ],
            }),
        );
        let w = writes(&created, 1_000);
        assert!(matches!(
            &w[0],
            Write::MemberAdded {
                user_id: 1,
                role: "admin",
                channel_type: "private",
                joined_at_ms: 1_000,
                ..
            }
        ));
        assert!(matches!(
            &w[1],
            Write::MemberAdded {
                user_id: 2,
                role: "member",
                ..
            }
        ));
        assert!(matches!(
            w.last(),
            Some(Write::Head {
                msg_seq: None,
                membership_version: 1,
                ..
            })
        ));

        let mut left = event(5, Payload::MemberLeft(MemberLeft { user_id: 2 }));
        left.membership_version = 5;
        let w = writes(&left, 0);
        assert!(matches!(
            &w[0],
            Write::MemberRemoved {
                user_id: 2,
                workspace_id: 10,
                ..
            }
        ));
        assert!(matches!(
            w.last(),
            Some(Write::Head {
                membership_version: 5,
                ..
            })
        ));
    }

    #[test]
    fn edits_and_deletes_update_the_row_and_log_without_the_body() {
        let edited = event(
            8,
            Payload::MessageEdited(MessageEdited {
                msg_seq: 3,
                message_id: 77,
                body: "fixed".into(),
                edited_at_ms: 1_700_000_000_000,
            }),
        );
        let w = writes(&edited, 0);
        assert_eq!(
            w[0],
            Write::MessageEdited {
                channel_id: 100,
                bucket: 0,
                msg_seq: 3,
                message_id: 77,
                body: "fixed".into(),
                edited_at_ms: 1_700_000_000_000,
            }
        );
        // The log keeps a pointer, never the edited body.
        assert!(
            matches!(&w[1], Write::ChannelEvent { kind: "message.edited", payload, event_seq: 8, .. } if payload == &3i64.to_be_bytes().to_vec())
        );
        assert!(matches!(
            w[2],
            Write::Head {
                msg_seq: None,
                event_seq: 8,
                ..
            }
        ));

        let deleted = event(
            9,
            Payload::MessageDeleted(MessageDeleted {
                msg_seq: 3,
                message_id: 77,
            }),
        );
        let w = writes(&deleted, 0);
        assert_eq!(
            w[0],
            Write::MessageDeleted {
                channel_id: 100,
                bucket: 0,
                msg_seq: 3,
                message_id: 77,
            }
        );
        assert!(matches!(
            &w[1],
            Write::ChannelEvent {
                kind: "message.deleted",
                event_seq: 9,
                ..
            }
        ));
        assert!(matches!(
            w[2],
            Write::Head {
                msg_seq: None,
                event_seq: 9,
                ..
            }
        ));
    }

    #[test]
    fn rejections_write_nothing() {
        let mut e = event(0, Payload::Rejected(Rejected::default()));
        e.event_seq = 0;
        assert!(writes(&e, 0).is_empty());
    }

    #[test]
    fn writes_are_deterministic_so_replays_converge() {
        let mut e = event(
            7,
            Payload::MessageCreated(MessageCreated {
                message_id: 5,
                body: "x".into(),
            }),
        );
        e.msg_seq = 3;
        assert_eq!(writes(&e, 42), writes(&e, 42));
    }
}
