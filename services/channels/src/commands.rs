// SPDX-License-Identifier: AGPL-3.0-only
//! Waiting for the sequencer's verdict on a membership command. Every instance reads
//! all of `commands.results` (small, 1-hour retention) and completes the waiters it
//! has; results for other instances' requests are ignored.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use tokio::sync::oneshot;
use xmsg_proto::v1::CommandResult;

#[derive(Default)]
pub struct Pending {
    waiters: Mutex<HashMap<String, oneshot::Sender<CommandResult>>>,
}

/// A registered request; dropping it without waiting unregisters it.
pub struct Waiter<'a> {
    pending: &'a Pending,
    request_id: String,
    rx: oneshot::Receiver<CommandResult>,
}

impl Pending {
    /// Register before producing the command, so a fast result can't be missed.
    pub fn register(&self, request_id: &str) -> Waiter<'_> {
        let (tx, rx) = oneshot::channel();
        self.waiters
            .lock()
            .unwrap()
            .insert(request_id.to_string(), tx);
        Waiter {
            pending: self,
            request_id: request_id.to_string(),
            rx,
        }
    }

    /// Called for every record on `commands.results`.
    pub fn complete(&self, result: CommandResult) {
        if let Some(tx) = self.waiters.lock().unwrap().remove(&result.request_id) {
            let _ = tx.send(result);
        }
    }

    pub fn len(&self) -> usize {
        self.waiters.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Waiter<'_> {
    /// The result, or `None` after `timeout` (the command may still be applied later).
    pub async fn wait(mut self, timeout: Duration) -> Option<CommandResult> {
        tokio::time::timeout(timeout, &mut self.rx)
            .await
            .ok()
            .and_then(Result::ok)
    }
}

impl Drop for Waiter<'_> {
    fn drop(&mut self) {
        self.pending
            .waiters
            .lock()
            .unwrap()
            .remove(&self.request_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn results_reach_their_waiter_and_others_are_ignored() {
        let pending = Pending::default();
        let waiter = pending.register("r1");
        pending.complete(CommandResult {
            request_id: "other".into(),
            ..Default::default()
        });
        pending.complete(CommandResult {
            request_id: "r1".into(),
            accepted: true,
            event_seq: 4,
            ..Default::default()
        });
        let result = waiter.wait(Duration::from_millis(100)).await.unwrap();
        assert!(result.accepted);
        assert_eq!(result.event_seq, 4);
        assert!(pending.is_empty());
    }

    #[tokio::test]
    async fn a_timeout_unregisters_the_waiter() {
        let pending = Pending::default();
        assert_eq!(
            pending
                .register("slow")
                .wait(Duration::from_millis(10))
                .await,
            None
        );
        assert!(pending.is_empty());
    }
}
