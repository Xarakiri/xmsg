// SPDX-License-Identifier: AGPL-3.0-only
//! Channel membership in memory, applied in channel order (ADR-0006).

use std::collections::{HashMap, HashSet};

use xmsg_proto::v1::{SequencedEvent, sequenced_event::Payload};

/// A membership snapshot from the store, with the version it reflects.
#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    pub version: i64,
    pub members: HashSet<i64>,
}

/// Where cold channels are loaded from (the wide-column store in production).
pub trait SnapshotSource {
    fn load(
        &self,
        channel_id: i64,
    ) -> impl Future<Output = Result<Option<Snapshot>, String>> + Send;
}

#[derive(Default)]
pub struct Membership {
    channels: HashMap<i64, HashSet<i64>>,
}

/// Why a channel's members couldn't be determined for an event.
#[derive(Debug, PartialEq)]
pub enum Unknown {
    /// The store hasn't caught up with this event's membership version yet: wait and retry.
    SnapshotBehind { have: i64, need: i64 },
    /// No snapshot at all yet (channel created moments ago and the persister lags): retry.
    NoSnapshot,
}

impl Membership {
    /// Applies membership changes carried by `event` to a channel already in memory.
    /// `channel.created` also brings a new channel into memory.
    pub fn apply(&mut self, event: &SequencedEvent) {
        match &event.payload {
            Some(Payload::ChannelCreated(created)) => {
                self.channels.insert(
                    event.channel_id,
                    created.members.iter().map(|m| m.user_id).collect(),
                );
            }
            Some(Payload::MemberJoined(joined)) => {
                if let Some(m) = self.channels.get_mut(&event.channel_id) {
                    m.insert(joined.user_id);
                }
            }
            Some(Payload::MemberLeft(left)) => {
                if let Some(m) = self.channels.get_mut(&event.channel_id) {
                    m.remove(&left.user_id);
                }
            }
            _ => {}
        }
    }

    pub fn get(&self, channel_id: i64) -> Option<&HashSet<i64>> {
        self.channels.get(&channel_id)
    }

    /// Ensures `event`'s channel is in memory. A snapshot is used only if its
    /// version is at least the event's `membership_version` (the snapshot rule).
    pub async fn ensure_loaded(
        &mut self,
        event: &SequencedEvent,
        source: &impl SnapshotSource,
    ) -> Result<(), Unknown> {
        // Rejections (event_seq 0) go only to the actor and never need membership; they
        // may even concern a channel that doesn't exist, which will never have a snapshot.
        if event.event_seq <= 0
            || matches!(
                event.payload,
                Some(Payload::Rejected(_)) | Some(Payload::ChannelCreated(_))
            )
            || self.channels.contains_key(&event.channel_id)
        {
            return Ok(());
        }
        match source.load(event.channel_id).await {
            Ok(Some(snapshot)) if snapshot.version >= event.membership_version => {
                self.channels.insert(event.channel_id, snapshot.members);
                Ok(())
            }
            Ok(Some(snapshot)) => Err(Unknown::SnapshotBehind {
                have: snapshot.version,
                need: event.membership_version,
            }),
            Ok(None) | Err(_) => Err(Unknown::NoSnapshot),
        }
    }

    pub fn channel_count(&self) -> usize {
        self.channels.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xmsg_proto::v1::{ChannelCreated, MemberJoined, MemberLeft, MessageCreated};

    struct Fixed(Option<Snapshot>);
    impl SnapshotSource for Fixed {
        async fn load(&self, _channel_id: i64) -> Result<Option<Snapshot>, String> {
            Ok(self.0.clone())
        }
    }

    fn msg(channel: i64, membership_version: i64) -> SequencedEvent {
        SequencedEvent {
            channel_id: channel,
            event_seq: 10,
            membership_version,
            payload: Some(Payload::MessageCreated(MessageCreated::default())),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn stale_snapshots_are_never_used() {
        let mut m = Membership::default();
        let stale = Fixed(Some(Snapshot {
            version: 4,
            members: HashSet::from([1, 2]),
        }));
        assert_eq!(
            m.ensure_loaded(&msg(100, 5), &stale).await,
            Err(Unknown::SnapshotBehind { have: 4, need: 5 })
        );
        assert!(m.get(100).is_none());

        let fresh = Fixed(Some(Snapshot {
            version: 5,
            members: HashSet::from([1]),
        }));
        assert_eq!(m.ensure_loaded(&msg(100, 5), &fresh).await, Ok(()));
        assert_eq!(m.get(100), Some(&HashSet::from([1])));
    }

    #[tokio::test]
    async fn changes_are_applied_in_order_to_channels_in_memory() {
        let mut m = Membership::default();
        let created = SequencedEvent {
            channel_id: 7,
            event_seq: 1,
            payload: Some(Payload::ChannelCreated(ChannelCreated {
                members: vec![
                    MemberJoined {
                        user_id: 1,
                        role: 2,
                    },
                    MemberJoined {
                        user_id: 2,
                        role: 1,
                    },
                ],
                ..Default::default()
            })),
            ..Default::default()
        };
        m.ensure_loaded(&created, &Fixed(None)).await.unwrap();
        m.apply(&created);
        m.apply(&SequencedEvent {
            channel_id: 7,
            payload: Some(Payload::MemberJoined(MemberJoined {
                user_id: 3,
                role: 1,
            })),
            ..Default::default()
        });
        m.apply(&SequencedEvent {
            channel_id: 7,
            payload: Some(Payload::MemberLeft(MemberLeft { user_id: 1 })),
            ..Default::default()
        });
        assert_eq!(m.get(7), Some(&HashSet::from([2, 3])));
    }

    #[tokio::test]
    async fn rejections_never_wait_for_membership() {
        // Regression: a send to a channel that doesn't exist is rejected by the sequencer;
        // waiting for that channel's snapshot blocked the whole partition forever.
        use xmsg_proto::v1::Rejected;
        let mut m = Membership::default();
        let rejected = SequencedEvent {
            channel_id: 4242,
            event_seq: 0,
            payload: Some(Payload::Rejected(Rejected::default())),
            ..Default::default()
        };
        assert_eq!(m.ensure_loaded(&rejected, &Fixed(None)).await, Ok(()));
        assert!(m.get(4242).is_none());
    }

    #[tokio::test]
    async fn a_channel_with_no_snapshot_yet_is_retried() {
        let mut m = Membership::default();
        assert_eq!(
            m.ensure_loaded(&msg(5, 1), &Fixed(None)).await,
            Err(Unknown::NoSnapshot)
        );
    }
}
