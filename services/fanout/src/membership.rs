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

/// Where cold channels are loaded from (the wide-column store, then the sequencer).
/// `at_least` is the membership version the event needs: a source that knows it can't
/// be that current may hand over to another (see `Fallback`).
pub trait SnapshotSource {
    fn load(
        &self,
        channel_id: i64,
        at_least: i64,
    ) -> impl Future<Output = Result<Option<Snapshot>, String>> + Send;
}

/// The primary source, and the secondary only when the primary has nothing or is behind
/// (plan §5.6: the wide-column store first, the sequencer's membership endpoint when the
/// persister hasn't caught up). The newer of the two answers wins.
pub struct Fallback<A, B> {
    pub primary: A,
    pub secondary: Option<B>,
}

impl<A: SnapshotSource + Sync, B: SnapshotSource + Sync> SnapshotSource for Fallback<A, B> {
    async fn load(&self, channel_id: i64, at_least: i64) -> Result<Option<Snapshot>, String> {
        let primary = self.primary.load(channel_id, at_least).await;
        let current = matches!(&primary, Ok(Some(s)) if s.version >= at_least);
        let Some(secondary) = self.secondary.as_ref().filter(|_| !current) else {
            return primary;
        };
        match (primary, secondary.load(channel_id, at_least).await) {
            (Ok(Some(a)), Ok(Some(b))) => Ok(Some(if b.version > a.version { b } else { a })),
            (Ok(Some(a)), _) => Ok(Some(a)),
            (_, Ok(b)) => Ok(b),
            (Err(e), Err(_)) => Err(e),
            (Ok(None), Err(e)) => Err(e),
        }
    }
}

#[derive(Default)]
pub struct Membership {
    channels: HashMap<i64, HashSet<i64>>,
    /// Each channel's type, from the events applied (for DM-only features: receipts).
    types: HashMap<i64, i32>,
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
        if event.event_seq > 0 {
            self.types.insert(event.channel_id, event.channel_type);
        }
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

    /// Whether the channel is a DM or group DM, as far as this instance has seen.
    pub fn is_dm(&self, channel_id: i64) -> bool {
        use xmsg_proto::v1::ChannelType;
        matches!(
            self.types
                .get(&channel_id)
                .copied()
                .map(ChannelType::try_from),
            Some(Ok(ChannelType::Dm | ChannelType::GroupDm))
        )
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
        match source
            .load(event.channel_id, event.membership_version)
            .await
        {
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

    #[test]
    fn receipts_are_for_channels_seen_as_dms() {
        use xmsg_proto::v1::ChannelType;
        let mut m = Membership::default();
        for (channel, kind) in [
            (1, ChannelType::Dm),
            (2, ChannelType::GroupDm),
            (3, ChannelType::Private),
        ] {
            let mut e = msg(channel, 1);
            e.channel_type = kind as i32;
            m.apply(&e);
        }
        assert!(m.is_dm(1) && m.is_dm(2));
        assert!(!m.is_dm(3), "a channel isn't a DM");
        assert!(!m.is_dm(4), "unknown: dropped rather than guessed");
    }

    struct Fixed(Option<Snapshot>);
    impl SnapshotSource for Fixed {
        async fn load(&self, _channel_id: i64, _at_least: i64) -> Result<Option<Snapshot>, String> {
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

    #[tokio::test]
    async fn the_sequencer_is_asked_only_when_the_store_is_behind() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Counted(Option<Snapshot>, AtomicUsize);
        impl SnapshotSource for Counted {
            async fn load(&self, _: i64, _: i64) -> Result<Option<Snapshot>, String> {
                self.1.fetch_add(1, Ordering::SeqCst);
                Ok(self.0.clone())
            }
        }
        let snap = |version, members: &[i64]| {
            Some(Snapshot {
                version,
                members: members.iter().copied().collect(),
            })
        };
        let source = Fallback {
            primary: Fixed(snap(5, &[1])),
            secondary: Some(Counted(snap(9, &[1, 2]), AtomicUsize::new(0))),
        };

        // Current enough in the store: the sequencer isn't bothered.
        assert_eq!(source.load(7, 5).await.unwrap().unwrap().version, 5);
        assert_eq!(
            source.secondary.as_ref().unwrap().1.load(Ordering::SeqCst),
            0
        );
        // Behind: the sequencer's newer answer is used.
        assert_eq!(source.load(7, 8).await.unwrap(), snap(9, &[1, 2]));
        // Nothing in the store yet (channel created moments ago): the sequencer answers.
        let cold = Fallback {
            primary: Fixed(None),
            secondary: Some(Fixed(snap(1, &[1]))),
        };
        assert_eq!(cold.load(7, 1).await.unwrap().unwrap().version, 1);
        // No sequencer configured: the store's answer as before.
        let alone: Fallback<Fixed, Fixed> = Fallback {
            primary: Fixed(snap(5, &[1])),
            secondary: None,
        };
        assert_eq!(alone.load(7, 8).await.unwrap().unwrap().version, 5);
    }
}
