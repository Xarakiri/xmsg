// SPDX-License-Identifier: AGPL-3.0-only
//! Fan-out (plan §5.8): delivers sequenced events to the gateways where the
//! channel's members are connected.
//!
//! - **Sessions** come from the compacted `sessions.events` topic (one entry per
//!   connection; a tombstone removes it), kept in memory.
//! - **Membership** is applied from the `member.*` events in the same ordered
//!   stream as messages. A channel not in memory is loaded from the wide-column
//!   store only if its `membership_version` is at least the event's (ADR-0006),
//!   so a stale snapshot can never send a private message to a removed member.
//! - **Delivery:** each event is encoded once into a `ServerFrame` and sent to
//!   each gateway as a `GatewayBatch` on its stream `(partition, owner_epoch)`
//!   with consecutive sequence numbers, plus `StreamHeads` every second (ADR-0009).

pub mod config;
pub mod denylist;
pub mod membership;
pub mod sequencer;
pub mod sessions;
pub mod store;
pub mod streams;

use std::collections::{BTreeMap, HashSet};

use xmsg_proto::v1::{FanoutMode, SequencedEvent, sequenced_event::Payload};

/// Which users should receive `event`, given the channel's members.
/// Rejections (event_seq 0) go only to the actor.
pub fn recipients(event: &SequencedEvent, members: &HashSet<i64>) -> HashSet<i64> {
    if event.event_seq <= 0 || matches!(event.payload, Some(Payload::Rejected(_))) {
        return HashSet::from([event.actor_id]);
    }
    let mut out = members.clone();
    // A removed member still learns about their own removal.
    if let Some(Payload::MemberLeft(left)) = &event.payload {
        out.insert(left.user_id);
    }
    out
}

/// How one event travels (ADR-0008).
#[derive(Debug, Default, PartialEq)]
pub struct Plan {
    /// Users whose gateways get it on the sequenced stream (`gw.{id}`).
    pub push: HashSet<i64>,
    /// Published once on `ch.{channel_id}` for gateways with local members.
    pub publish_channel: bool,
    /// `(user, subscribe)`: tell the user's gateways to start or stop `ch.{channel_id}`.
    pub controls: Vec<(i64, bool)>,
}

/// Plans `event`'s delivery. `dual`: the channel switched modes moments ago, so both
/// paths are used while gateways catch up (ADR-0008; clients drop duplicates by
/// `event_seq`).
///
/// - Push mode: pushed to every recipient, as always.
/// - Pull mode: published once on `ch.{id}`; a member who joins or leaves also gets the
///   event pushed (their gateway isn't subscribed yet, or no longer), with a control
///   message; a channel created in pull mode is pushed to everyone once, with controls.
/// - A mode switch goes both ways, with a control message for every member.
/// - Rejections are pushed to the actor only.
pub fn plan(event: &SequencedEvent, members: &HashSet<i64>, dual: bool) -> Plan {
    let to = recipients(event, members);
    if event.event_seq <= 0 || matches!(event.payload, Some(Payload::Rejected(_))) {
        return Plan {
            push: to,
            ..Plan::default()
        };
    }
    let pull = event.fanout_mode == FanoutMode::Pull as i32;
    let everyone = |subscribe: bool| members.iter().map(|&u| (u, subscribe)).collect();
    match &event.payload {
        Some(Payload::ChannelModeChanged(changed)) => Plan {
            push: to,
            publish_channel: true,
            controls: everyone(changed.mode == FanoutMode::Pull as i32),
        },
        _ if !pull => Plan {
            push: to,
            publish_channel: dual,
            controls: Vec::new(),
        },
        Some(Payload::ChannelCreated(_)) => Plan {
            push: to,
            publish_channel: true,
            controls: everyone(true),
        },
        Some(Payload::MemberJoined(joined)) => Plan {
            push: if dual {
                to
            } else {
                HashSet::from([joined.user_id])
            },
            publish_channel: true,
            controls: vec![(joined.user_id, true)],
        },
        Some(Payload::MemberLeft(left)) => {
            let mut push = if dual { to } else { HashSet::new() };
            push.insert(left.user_id);
            Plan {
                push,
                publish_channel: true,
                controls: vec![(left.user_id, false)],
            }
        }
        _ => Plan {
            push: if dual { to } else { HashSet::new() },
            publish_channel: true,
            controls: Vec::new(),
        },
    }
}

/// Users subscribed to a pull-mode channel moments ago (ADR-0008). Their gateway may not
/// be listening on `ch.{id}` yet, so for `FRESH_FOR` their events are also pushed (the same
/// overlap as a mode switch; clients drop duplicates by `event_seq`). Without it, a message
/// sent right after someone joins, or right after a channel is created, can miss them
/// until their next sync.
#[derive(Default)]
pub struct Fresh {
    by_channel: std::collections::HashMap<i64, Vec<(i64, std::time::Instant)>>,
}

pub const FRESH_FOR: std::time::Duration = std::time::Duration::from_secs(10);

impl Fresh {
    pub fn note(&mut self, channel_id: i64, user_id: i64, now: std::time::Instant) {
        // Channels that went quiet never get `users` called again: sweep them now and then.
        if self.by_channel.len() > 10_000 {
            self.by_channel.retain(|_, users| {
                users.retain(|(_, at)| now.duration_since(*at) < FRESH_FOR);
                !users.is_empty()
            });
        }
        let users = self.by_channel.entry(channel_id).or_default();
        users.retain(|(u, at)| *u != user_id && now.duration_since(*at) < FRESH_FOR);
        users.push((user_id, now));
    }

    pub fn forget(&mut self, channel_id: i64, user_id: i64) {
        if let Some(users) = self.by_channel.get_mut(&channel_id) {
            users.retain(|(u, _)| *u != user_id);
        }
    }

    /// Those still within `FRESH_FOR`; expired entries (and empty channels) are dropped.
    pub fn users(&mut self, channel_id: i64, now: std::time::Instant) -> Vec<i64> {
        let Some(users) = self.by_channel.get_mut(&channel_id) else {
            return Vec::new();
        };
        users.retain(|(_, at)| now.duration_since(*at) < FRESH_FOR);
        let out = users.iter().map(|(u, _)| *u).collect();
        if users.is_empty() {
            self.by_channel.remove(&channel_id);
        }
        out
    }
}

/// Who sees `user` typing in a channel with these members: everyone else, and only if
/// `user` is a member (best-effort, plan §5.9). `None`: drop the notice.
pub fn typing_recipients(members: &HashSet<i64>, user: i64) -> Option<HashSet<i64>> {
    if !members.contains(&user) {
        return None;
    }
    let others: HashSet<i64> = members.iter().copied().filter(|&m| m != user).collect();
    (!others.is_empty()).then_some(others)
}

/// Groups recipients by the gateways their connections are on.
pub fn by_gateway(
    recipients: &HashSet<i64>,
    sessions: &sessions::SessionMap,
) -> BTreeMap<String, Vec<i64>> {
    let mut out: BTreeMap<String, Vec<i64>> = BTreeMap::new();
    for &user in recipients {
        for gateway in sessions.gateways_of(user) {
            out.entry(gateway.to_string()).or_default().push(user);
        }
    }
    for users in out.values_mut() {
        users.sort_unstable();
        users.dedup();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use xmsg_proto::v1::{MemberLeft, MessageCreated, Rejected, SessionEvent};

    fn session(conn: &str, user: i64, gateway: &str) -> (String, Option<SessionEvent>) {
        (
            conn.to_string(),
            Some(SessionEvent {
                conn_id: conn.into(),
                user_id: user,
                gateway_id: gateway.into(),
                ..Default::default()
            }),
        )
    }

    #[test]
    fn messages_go_to_members_and_rejections_only_to_the_actor() {
        let members = HashSet::from([1, 2]);
        let msg = SequencedEvent {
            event_seq: 5,
            actor_id: 1,
            payload: Some(Payload::MessageCreated(MessageCreated::default())),
            ..Default::default()
        };
        assert_eq!(recipients(&msg, &members), members);

        let rejected = SequencedEvent {
            actor_id: 9,
            payload: Some(Payload::Rejected(Rejected::default())),
            ..Default::default()
        };
        assert_eq!(recipients(&rejected, &members), HashSet::from([9]));

        let left = SequencedEvent {
            event_seq: 6,
            payload: Some(Payload::MemberLeft(MemberLeft { user_id: 3 })),
            ..Default::default()
        };
        assert_eq!(recipients(&left, &members), HashSet::from([1, 2, 3]));
    }

    #[test]
    fn typing_goes_to_the_other_members_and_only_from_a_member() {
        let members = HashSet::from([1, 2, 3]);
        assert_eq!(typing_recipients(&members, 2), Some(HashSet::from([1, 3])));
        assert_eq!(typing_recipients(&members, 9), None, "not a member");
        assert_eq!(
            typing_recipients(&HashSet::from([4]), 4),
            None,
            "nobody to tell"
        );
    }

    #[test]
    fn recipients_are_grouped_by_gateway_including_several_devices() {
        let mut s = sessions::SessionMap::default();
        for (k, v) in [
            session("c1", 1, "gw-a"),
            session("c2", 1, "gw-b"),
            session("c3", 2, "gw-a"),
        ] {
            s.apply(&k, v);
        }
        let grouped = by_gateway(&HashSet::from([1, 2, 3]), &s);
        assert_eq!(grouped.get("gw-a"), Some(&vec![1, 2]));
        assert_eq!(grouped.get("gw-b"), Some(&vec![1]));
        assert_eq!(grouped.len(), 2, "user 3 has no connection");
    }

    mod pull_mode {
        use super::super::*;
        use xmsg_proto::v1::{
            ChannelModeChanged, MemberJoined, MemberLeft, MessageCreated, Rejected,
        };

        fn event(mode: FanoutMode, payload: Payload) -> SequencedEvent {
            SequencedEvent {
                channel_id: 7,
                actor_id: 1,
                event_seq: 10,
                fanout_mode: mode as i32,
                payload: Some(payload),
                ..Default::default()
            }
        }

        fn members() -> HashSet<i64> {
            HashSet::from([1, 2, 3])
        }

        fn message() -> Payload {
            Payload::MessageCreated(MessageCreated::default())
        }

        #[test]
        fn push_mode_pushes_to_everyone_and_publishes_only_around_a_switch() {
            let p = plan(&event(FanoutMode::Push, message()), &members(), false);
            assert_eq!(
                p,
                Plan {
                    push: members(),
                    ..Plan::default()
                }
            );
            assert!(plan(&event(FanoutMode::Push, message()), &members(), true).publish_channel);
        }

        #[test]
        fn pull_mode_publishes_once_and_pushes_nothing() {
            let p = plan(&event(FanoutMode::Pull, message()), &members(), false);
            assert_eq!(
                p,
                Plan {
                    publish_channel: true,
                    ..Plan::default()
                }
            );
            // Around a switch both paths are used.
            let p = plan(&event(FanoutMode::Pull, message()), &members(), true);
            assert_eq!((p.push, p.publish_channel), (members(), true));
        }

        #[test]
        fn joiners_and_leavers_get_their_event_pushed_with_a_control_message() {
            let joined = Payload::MemberJoined(MemberJoined {
                user_id: 4,
                ..Default::default()
            });
            let p = plan(
                &event(FanoutMode::Pull, joined),
                &HashSet::from([1, 2, 3, 4]),
                false,
            );
            assert_eq!(p.push, HashSet::from([4]));
            assert!(p.publish_channel);
            assert_eq!(p.controls, vec![(4, true)]);

            let left = Payload::MemberLeft(MemberLeft { user_id: 3 });
            let p = plan(
                &event(FanoutMode::Pull, left),
                &HashSet::from([1, 2]),
                false,
            );
            assert_eq!(p.push, HashSet::from([3]));
            assert_eq!(p.controls, vec![(3, false)]);
        }

        #[test]
        fn a_switch_goes_both_ways_and_tells_every_member() {
            let to_pull = Payload::ChannelModeChanged(ChannelModeChanged {
                mode: FanoutMode::Pull as i32,
            });
            let p = plan(&event(FanoutMode::Pull, to_pull), &members(), false);
            assert_eq!((p.push.clone(), p.publish_channel), (members(), true));
            let mut controls = p.controls;
            controls.sort();
            assert_eq!(controls, vec![(1, true), (2, true), (3, true)]);

            let to_push = Payload::ChannelModeChanged(ChannelModeChanged {
                mode: FanoutMode::Push as i32,
            });
            let p = plan(&event(FanoutMode::Push, to_push), &members(), false);
            assert!(p.controls.iter().all(|(_, subscribe)| !subscribe));
        }

        #[test]
        fn rejections_only_reach_the_actor() {
            let rejected = Payload::Rejected(Rejected::default());
            let mut e = event(FanoutMode::Pull, rejected);
            e.event_seq = 0;
            assert_eq!(
                plan(&e, &members(), true),
                Plan {
                    push: HashSet::from([1]),
                    ..Plan::default()
                }
            );
        }
    }

    #[test]
    fn fresh_subscribers_last_ten_seconds() {
        let t0 = std::time::Instant::now();
        let mut fresh = Fresh::default();
        fresh.note(7, 1, t0);
        fresh.note(7, 2, t0 + std::time::Duration::from_secs(5));
        assert_eq!(
            fresh.users(7, t0 + std::time::Duration::from_secs(6)),
            vec![1, 2]
        );
        // 1 expires first; noting again renews.
        assert_eq!(
            fresh.users(7, t0 + std::time::Duration::from_secs(11)),
            vec![2]
        );
        fresh.note(7, 2, t0 + std::time::Duration::from_secs(12));
        assert_eq!(
            fresh.users(7, t0 + std::time::Duration::from_secs(20)),
            vec![2]
        );
        fresh.forget(7, 2);
        assert!(
            fresh
                .users(7, t0 + std::time::Duration::from_secs(20))
                .is_empty()
        );
        assert!(fresh.users(8, t0).is_empty());
    }
}
