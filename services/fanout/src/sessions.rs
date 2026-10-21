// SPDX-License-Identifier: AGPL-3.0-only
//! Per-connection session map built from `sessions.events` (plan §5.8).

use std::collections::{HashMap, HashSet};

use xmsg_proto::v1::SessionEvent;

#[derive(Default, Debug)]
pub struct SessionMap {
    /// conn_id → (user_id, gateway_id)
    conns: HashMap<String, (i64, String)>,
    /// user_id → conn_ids
    by_user: HashMap<i64, HashSet<String>>,
}

impl SessionMap {
    /// Applies one record: `Some` adds or replaces a connection, `None` (tombstone) removes it.
    /// Entries are per connection, so events from different gateways in any order can't
    /// remove a live connection.
    pub fn apply(&mut self, conn_id: &str, event: Option<SessionEvent>) {
        if let Some((user, _)) = self.conns.remove(conn_id)
            && let Some(set) = self.by_user.get_mut(&user)
        {
            set.remove(conn_id);
            if set.is_empty() {
                self.by_user.remove(&user);
            }
        }
        if let Some(e) = event {
            self.conns
                .insert(conn_id.to_string(), (e.user_id, e.gateway_id));
            self.by_user
                .entry(e.user_id)
                .or_default()
                .insert(conn_id.to_string());
        }
    }

    /// Distinct gateways where `user` has a connection.
    pub fn gateways_of(&self, user: i64) -> impl Iterator<Item = &str> {
        let mut seen: Vec<&str> = self
            .by_user
            .get(&user)
            .into_iter()
            .flatten()
            .filter_map(|c| self.conns.get(c).map(|(_, g)| g.as_str()))
            .collect();
        seen.sort_unstable();
        seen.dedup();
        seen.into_iter()
    }

    pub fn connection_count(&self) -> usize {
        self.conns.len()
    }

    /// Every gateway that currently has at least one connection.
    pub fn gateways(&self) -> HashSet<String> {
        self.conns.values().map(|(_, g)| g.clone()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(user: i64, gw: &str) -> Option<SessionEvent> {
        Some(SessionEvent {
            user_id: user,
            gateway_id: gw.into(),
            ..Default::default()
        })
    }

    #[test]
    fn tombstones_remove_only_their_connection() {
        let mut s = SessionMap::default();
        s.apply("c1", ev(1, "gw-a"));
        s.apply("c2", ev(1, "gw-b"));
        s.apply("c1", None);
        assert_eq!(s.gateways_of(1).collect::<Vec<_>>(), vec!["gw-b"]);
        s.apply("c2", None);
        assert_eq!(s.gateways_of(1).count(), 0);
        assert_eq!(s.connection_count(), 0);
    }

    #[test]
    fn a_reused_conn_id_moves_the_connection() {
        let mut s = SessionMap::default();
        s.apply("c1", ev(1, "gw-a"));
        s.apply("c1", ev(2, "gw-b"));
        assert_eq!(s.gateways_of(1).count(), 0);
        assert_eq!(s.gateways_of(2).collect::<Vec<_>>(), vec!["gw-b"]);
    }
}
