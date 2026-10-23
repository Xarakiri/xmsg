// SPDX-License-Identifier: AGPL-3.0-only
//! Reaction batching (ADR-0015, plan §6.8).
//!
//! A popular message can get thousands of reactions a second, all on one channel
//! partition. One event per reaction would flood the sequencer task and push every
//! viewer's `event_seq`. The batcher owns `reactions.in` partitions (keyed by channel, so
//! each channel's commands arrive in order) and every second emits one `ReactionBatch`
//! per (message, emoji) to `messages.in`, with at most one operation per user: the one
//! with the highest counter. The sequencer applies an operation only if its counter is
//! above the last applied one, so batches may repeat after a crash without harm.

use std::collections::{BTreeMap, HashMap};

use xmsg_proto::v1::{
    InboundEvent, ReactionBatch, ReactionCommand, ReactionOp, inbound_event::Payload,
};

/// What one batch is about.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Target {
    pub channel_id: i64,
    pub message_id: i64,
    pub msg_seq: i64,
    pub emoji: String,
}

/// Commands collected since the last flush.
#[derive(Default)]
pub struct Pending {
    // BTreeMap per target: users in a stable order, so a batch encodes the same way twice.
    by_target: HashMap<Target, BTreeMap<i64, (bool, i64)>>,
    ops: usize,
}

impl Pending {
    pub fn add(&mut self, c: ReactionCommand) {
        let target = Target {
            channel_id: c.channel_id,
            message_id: c.message_id,
            msg_seq: c.msg_seq,
            emoji: c.emoji,
        };
        let users = self.by_target.entry(target).or_default();
        // A user's later operation (higher counter) replaces their earlier one.
        let keep = users
            .get(&c.user_id)
            .is_none_or(|&(_, counter)| c.counter > counter);
        if keep && users.insert(c.user_id, (c.add, c.counter)).is_none() {
            self.ops += 1;
        }
    }

    pub fn ops(&self) -> usize {
        self.ops
    }

    pub fn is_empty(&self) -> bool {
        self.by_target.is_empty()
    }

    /// One `InboundEvent` per (message, emoji), ready for `messages.in`, and empties itself.
    pub fn drain(&mut self) -> Vec<InboundEvent> {
        self.ops = 0;
        let mut targets: Vec<_> = self.by_target.drain().collect();
        targets.sort_by(|a, b| a.0.cmp(&b.0));
        targets
            .into_iter()
            .map(|(t, users)| InboundEvent {
                channel_id: t.channel_id,
                // Several users at once: the actor is nobody in particular.
                actor_id: 0,
                payload: Some(Payload::ReactionBatch(ReactionBatch {
                    message_id: t.message_id,
                    msg_seq: t.msg_seq,
                    emoji: t.emoji,
                    ops: users
                        .into_iter()
                        .map(|(user_id, (add, counter))| ReactionOp {
                            user_id,
                            add,
                            counter,
                        })
                        .collect(),
                })),
                ..Default::default()
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmd(user: i64, emoji: &str, add: bool, counter: i64) -> ReactionCommand {
        ReactionCommand {
            channel_id: 7,
            message_id: 70,
            msg_seq: 3,
            user_id: user,
            emoji: emoji.into(),
            add,
            counter,
        }
    }

    #[test]
    fn one_batch_per_message_and_emoji_with_each_users_latest_operation() {
        let mut p = Pending::default();
        p.add(cmd(1, "👍", true, 10));
        p.add(cmd(1, "👍", false, 12)); // changed their mind
        p.add(cmd(1, "👍", true, 11)); // arrived late: older than the removal
        p.add(cmd(2, "👍", true, 5));
        p.add(cmd(2, "🎉", true, 6));
        assert_eq!(p.ops(), 3);
        let batches = p.drain();
        assert!(p.is_empty());
        assert_eq!(batches.len(), 2);
        let ops = |e: &InboundEvent| match &e.payload {
            Some(Payload::ReactionBatch(b)) => (b.emoji.clone(), b.ops.clone()),
            _ => panic!(),
        };
        let thumbs = batches.iter().map(ops).find(|(e, _)| e == "👍").unwrap().1;
        assert_eq!(
            thumbs,
            vec![
                ReactionOp {
                    user_id: 1,
                    add: false,
                    counter: 12
                },
                ReactionOp {
                    user_id: 2,
                    add: true,
                    counter: 5
                },
            ]
        );
        assert!(
            batches
                .iter()
                .all(|b| b.channel_id == 7 && b.dedupe_id.is_empty())
        );
    }
}
