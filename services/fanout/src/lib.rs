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
pub mod sessions;
pub mod store;
pub mod streams;

use std::collections::{BTreeMap, HashSet};

use xmsg_proto::v1::{SequencedEvent, sequenced_event::Payload};

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
}
