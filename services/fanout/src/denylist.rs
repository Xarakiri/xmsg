// SPDX-License-Identifier: AGPL-3.0-only
//! Workspace removals (plan §6.5): from the moment a user is removed from a workspace,
//! nothing of that workspace is delivered to them, even before the removal job has taken
//! them out of its channels one by one. Entries come from NATS `auth.revocations` (the
//! users service's fast path) and Kafka `users.events` (the outbox copy, read from the
//! start after a restart); `removal_completed` ends them.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use xmsg_proto::Message;
use xmsg_proto::v1::{AuthEvent, UsersEvent, auth_event, users_event};

#[derive(Default)]
pub struct DenyList {
    /// (user, workspace) -> removed_at_ms of the removal that put it here.
    entries: RwLock<HashMap<(i64, i64), i64>>,
}

impl DenyList {
    pub fn denied(&self, user_id: i64, workspace_id: i64) -> bool {
        self.entries
            .read()
            .unwrap()
            .contains_key(&(user_id, workspace_id))
    }

    /// Drops the denied users of `workspace_id` from `recipients`.
    pub fn filter(&self, workspace_id: i64, recipients: &mut HashSet<i64>) {
        let entries = self.entries.read().unwrap();
        if !entries.is_empty() {
            recipients.retain(|u| !entries.contains_key(&(*u, workspace_id)));
        }
    }

    fn removed(&self, user_id: i64, workspace_id: i64, at_ms: i64) {
        let mut entries = self.entries.write().unwrap();
        let at = entries.entry((user_id, workspace_id)).or_insert(at_ms);
        *at = (*at).max(at_ms);
    }

    /// A completion ends the entry, unless a newer removal (after a re-invite) put it back.
    fn completed(&self, user_id: i64, workspace_id: i64, completed_at_ms: i64) {
        let mut entries = self.entries.write().unwrap();
        if entries
            .get(&(user_id, workspace_id))
            .is_some_and(|at| *at <= completed_at_ms)
        {
            entries.remove(&(user_id, workspace_id));
        }
    }

    /// An encoded `UsersEvent` (Kafka `users.events`).
    pub fn apply_users_event(&self, bytes: &[u8]) {
        match UsersEvent::decode(bytes).map(|e| e.event) {
            Ok(Some(users_event::Event::MemberRemoved(r))) => {
                self.removed(r.user_id, r.workspace_id, r.removed_at_ms)
            }
            Ok(Some(users_event::Event::RemovalCompleted(c))) => {
                self.completed(c.user_id, c.workspace_id, c.completed_at_ms)
            }
            _ => {}
        }
    }

    /// An encoded `AuthEvent` (NATS `auth.revocations`); session revocations are the gateways' business.
    pub fn apply_auth_event(&self, bytes: &[u8]) {
        if let Ok(AuthEvent {
            event: Some(auth_event::Event::WorkspaceMemberRemoved(r)),
        }) = AuthEvent::decode(bytes)
        {
            self.removed(r.user_id, r.workspace_id, r.removed_at_ms);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xmsg_proto::v1::{WorkspaceMemberRemovalCompleted, WorkspaceMemberRemoved};

    fn removed(user_id: i64, workspace_id: i64, at: i64) -> WorkspaceMemberRemoved {
        WorkspaceMemberRemoved {
            workspace_id,
            user_id,
            removed_by: user_id,
            removed_at_ms: at,
        }
    }

    fn users(event: users_event::Event) -> Vec<u8> {
        UsersEvent { event: Some(event) }.encode_to_vec()
    }

    #[test]
    fn a_removed_user_gets_nothing_of_that_workspace_until_completion() {
        let d = DenyList::default();
        d.apply_auth_event(
            &AuthEvent {
                event: Some(auth_event::Event::WorkspaceMemberRemoved(removed(
                    7, 2, 100,
                ))),
            }
            .encode_to_vec(),
        );
        let mut to = HashSet::from([7, 8]);
        d.filter(2, &mut to);
        assert_eq!(to, HashSet::from([8]));
        let mut other_workspace = HashSet::from([7, 8]);
        d.filter(1, &mut other_workspace);
        assert_eq!(
            other_workspace,
            HashSet::from([7, 8]),
            "their other workspaces are unaffected"
        );

        d.apply_users_event(&users(users_event::Event::MemberRemoved(removed(
            7, 2, 100,
        )))); // outbox copy: no change
        d.apply_users_event(&users(users_event::Event::RemovalCompleted(
            WorkspaceMemberRemovalCompleted {
                workspace_id: 2,
                user_id: 7,
                channels: 3,
                completed_at_ms: 200,
            },
        )));
        assert!(!d.denied(7, 2));
    }

    #[test]
    fn an_older_completion_does_not_end_a_newer_removal() {
        let d = DenyList::default();
        d.apply_users_event(&users(users_event::Event::MemberRemoved(removed(
            7, 2, 500,
        ))));
        d.apply_users_event(&users(users_event::Event::RemovalCompleted(
            WorkspaceMemberRemovalCompleted {
                workspace_id: 2,
                user_id: 7,
                channels: 0,
                completed_at_ms: 300,
            },
        )));
        assert!(d.denied(7, 2));
    }

    #[test]
    fn revocations_and_garbage_are_ignored() {
        let d = DenyList::default();
        d.apply_users_event(b"\xff\xff");
        d.apply_auth_event(b"\xff\xff");
        d.apply_auth_event(
            &AuthEvent {
                event: Some(auth_event::Event::SessionRevoked(Default::default())),
            }
            .encode_to_vec(),
        );
        assert!(d.entries.read().unwrap().is_empty());
    }
}
