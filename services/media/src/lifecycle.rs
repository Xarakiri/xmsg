// SPDX-License-Identifier: AGPL-3.0-only
//! What happens to files after upload (ADR-0024), driven by `messages.persisted`:
//!
//! - **Sent:** a message or reply carrying a file claims it (`files.message_id`), so the
//!   messages service refuses to attach it again: a file belongs to one message.
//! - **Deleted:** when that message is deleted, its files are marked deleted (gone for
//!   everyone at once) and queued in `file_purges`, by day.
//! - **Purged:** after the grace period (24 h by default) the sweeper removes the objects
//!   and the thumbnail from storage, and the file stops counting against its workspace's
//!   quota. The `files` row stays, marked deleted.

use xmsg_proto::v1::{Attachment, SequencedEvent, sequenced_event::Payload};

use crate::Blobs;

pub const DAY_MS: i64 = 86_400_000;
/// How far back the sweeper looks for queued purges (a service down longer than this
/// leaves objects behind; `xmsg-media purge --days N` would catch up).
pub const SWEEP_DAYS: i64 = 30;

/// A file waiting for its bytes to be removed.
#[derive(Clone, Debug, PartialEq)]
pub struct Purge {
    pub file_id: i64,
    pub channel_id: i64,
    pub deleted_at_ms: i64,
    /// Whose quota to release, and by how much.
    pub workspace_id: Option<i64>,
    pub size: i64,
}

/// What the lifecycle reads and writes (Cassandra in production).
pub trait Lifecycle: Send + Sync + 'static {
    /// The files attached to a channel message, from its `messages` row.
    fn message_files(
        &self,
        channel_id: i64,
        msg_seq: i64,
    ) -> impl Future<Output = Result<Vec<i64>, String>> + Send;
    /// The file's workspace and size (for its quota), if it exists.
    fn usage_of(
        &self,
        file_id: i64,
    ) -> impl Future<Output = Result<Option<(Option<i64>, i64)>, String>> + Send;
    /// Stops counting bytes against a workspace's quota.
    fn release(
        &self,
        workspace_id: i64,
        bytes: i64,
    ) -> impl Future<Output = Result<(), String>> + Send;
    /// Records which message a file was sent with.
    fn claim(
        &self,
        file_id: i64,
        message_id: i64,
    ) -> impl Future<Output = Result<(), String>> + Send;
    /// Marks the file deleted and queues its bytes for removal.
    fn mark_deleted(&self, purge: &Purge) -> impl Future<Output = Result<(), String>> + Send;
    /// The purges queued on a day (days since the Unix epoch).
    fn queued(&self, day: i64) -> impl Future<Output = Result<Vec<Purge>, String>> + Send;
    fn forget(&self, day: i64, file_id: i64) -> impl Future<Output = Result<(), String>> + Send;
}

fn file_ids(attachments: &[Attachment]) -> impl Iterator<Item = i64> + '_ {
    attachments.iter().map(|a| a.file_id)
}

/// Applies one sequenced event. Returns how many files it touched.
pub async fn on_event(
    store: &impl Lifecycle,
    event: &SequencedEvent,
    event_time_ms: i64,
) -> Result<usize, String> {
    match &event.payload {
        Some(Payload::MessageCreated(m)) => {
            for id in file_ids(&m.attachments) {
                store.claim(id, m.message_id).await?;
            }
            Ok(m.attachments.len())
        }
        Some(Payload::ThreadReply(r)) => {
            for id in file_ids(&r.attachments) {
                store.claim(id, r.message_id).await?;
            }
            Ok(r.attachments.len())
        }
        Some(Payload::MessageDeleted(d)) => {
            let files = store.message_files(event.channel_id, d.msg_seq).await?;
            for file_id in &files {
                let (workspace_id, size) = store.usage_of(*file_id).await?.unwrap_or((None, 0));
                store
                    .mark_deleted(&Purge {
                        file_id: *file_id,
                        channel_id: event.channel_id,
                        deleted_at_ms: event_time_ms,
                        workspace_id,
                        size,
                    })
                    .await?;
            }
            Ok(files.len())
        }
        _ => Ok(0),
    }
}

/// Removes the bytes of every queued file deleted more than `grace_ms` ago. Returns how
/// many were removed; a failure leaves that one queued for the next sweep.
pub async fn sweep(
    store: &impl Lifecycle,
    blobs: &impl Blobs,
    now_ms: i64,
    grace_ms: i64,
) -> Result<usize, String> {
    let today = now_ms.div_euclid(DAY_MS);
    let mut removed = 0;
    for day in (today - SWEEP_DAYS)..=today {
        for p in store.queued(day).await? {
            if p.deleted_at_ms + grace_ms > now_ms {
                continue;
            }
            let key = format!("{}/{}", p.channel_id, p.file_id);
            let removal = async {
                blobs.delete(&format!("{key}.thumb")).await?;
                blobs.delete(&key).await?;
                // Released before forgetting: a crash in between releases twice on the
                // next sweep, which a counter-based quota tolerates (it's approximate).
                if let Some(ws) = p.workspace_id {
                    store.release(ws, p.size).await?;
                }
                store.forget(day, p.file_id).await
            };
            match removal.await {
                Ok(()) => removed += 1,
                Err(e) => {
                    tracing::warn!(error = %e, file_id = p.file_id, "purge failed; retrying next sweep")
                }
            }
        }
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::sync::Mutex;

    use axum::body::Body;
    use bytes::Bytes;
    use xmsg_proto::v1::{MessageCreated, MessageDeleted, ThreadReply};

    use super::*;

    #[derive(Default)]
    struct Fake {
        /// (channel, msg_seq) -> files
        messages: HashMap<(i64, i64), Vec<i64>>,
        claims: Mutex<HashMap<i64, i64>>,
        queue: Mutex<BTreeMap<(i64, i64), Purge>>,
        objects: Mutex<HashSet<String>>,
        /// workspace -> bytes counted
        usage: Mutex<HashMap<i64, i64>>,
    }

    impl Lifecycle for Fake {
        async fn message_files(&self, channel_id: i64, msg_seq: i64) -> Result<Vec<i64>, String> {
            Ok(self
                .messages
                .get(&(channel_id, msg_seq))
                .cloned()
                .unwrap_or_default())
        }
        async fn usage_of(&self, file_id: i64) -> Result<Option<(Option<i64>, i64)>, String> {
            Ok(Some((Some(70), file_id * 100)))
        }
        async fn release(&self, workspace_id: i64, bytes: i64) -> Result<(), String> {
            *self.usage.lock().unwrap().entry(workspace_id).or_default() -= bytes;
            Ok(())
        }
        async fn claim(&self, file_id: i64, message_id: i64) -> Result<(), String> {
            self.claims.lock().unwrap().insert(file_id, message_id);
            Ok(())
        }
        async fn mark_deleted(&self, p: &Purge) -> Result<(), String> {
            self.queue
                .lock()
                .unwrap()
                .insert((p.deleted_at_ms.div_euclid(DAY_MS), p.file_id), p.clone());
            Ok(())
        }
        async fn queued(&self, day: i64) -> Result<Vec<Purge>, String> {
            Ok(self
                .queue
                .lock()
                .unwrap()
                .iter()
                .filter(|((d, _), _)| *d == day)
                .map(|(_, p)| p.clone())
                .collect())
        }
        async fn forget(&self, day: i64, file_id: i64) -> Result<(), String> {
            self.queue.lock().unwrap().remove(&(day, file_id));
            Ok(())
        }
    }

    impl Blobs for Fake {
        async fn put(&self, key: &str, _: Bytes, _: &str) -> Result<(), String> {
            self.objects.lock().unwrap().insert(key.into());
            Ok(())
        }
        async fn get(&self, _: &str) -> Result<Option<Body>, String> {
            Ok(None)
        }
        async fn delete(&self, key: &str) -> Result<(), String> {
            self.objects.lock().unwrap().remove(key);
            Ok(())
        }
    }

    fn event(channel_id: i64, payload: Payload) -> SequencedEvent {
        SequencedEvent {
            channel_id,
            payload: Some(payload),
            ..Default::default()
        }
    }

    fn file(id: i64) -> Attachment {
        Attachment {
            file_id: id,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn sent_files_are_claimed_by_their_message() {
        let f = Fake::default();
        let created = event(
            7,
            Payload::MessageCreated(MessageCreated {
                message_id: 100,
                attachments: vec![file(1), file(2)],
                ..Default::default()
            }),
        );
        let reply = event(
            7,
            Payload::ThreadReply(ThreadReply {
                message_id: 200,
                attachments: vec![file(3)],
                ..Default::default()
            }),
        );
        assert_eq!(on_event(&f, &created, 0).await.unwrap(), 2);
        assert_eq!(on_event(&f, &reply, 0).await.unwrap(), 1);
        assert_eq!(
            *f.claims.lock().unwrap(),
            HashMap::from([(1, 100), (2, 100), (3, 200)])
        );
    }

    #[tokio::test]
    async fn deleting_a_message_queues_its_files_and_the_sweep_waits_out_the_grace() {
        let f = Fake {
            messages: HashMap::from([((7, 5), vec![1, 2])]),
            ..Default::default()
        };
        for key in ["7/1", "7/1.thumb", "7/2", "7/9"] {
            f.objects.lock().unwrap().insert(key.into());
        }
        let deleted_at = 1_800_000_000_000;
        let delete = event(
            7,
            Payload::MessageDeleted(MessageDeleted {
                msg_seq: 5,
                message_id: 105,
            }),
        );
        assert_eq!(on_event(&f, &delete, deleted_at).await.unwrap(), 2);

        // Within the grace period: nothing goes.
        assert_eq!(
            sweep(&f, &f, deleted_at + DAY_MS - 1, DAY_MS)
                .await
                .unwrap(),
            0
        );
        assert_eq!(f.objects.lock().unwrap().len(), 4);
        // After it (and across midnight): both files, the thumbnail too; others stay.
        assert_eq!(
            sweep(&f, &f, deleted_at + DAY_MS + 1, DAY_MS)
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            *f.objects.lock().unwrap(),
            HashSet::from(["7/9".to_string()])
        );
        assert!(f.queue.lock().unwrap().is_empty());
        // Files 1 and 2 (100 and 200 bytes in this fake) no longer count.
        assert_eq!(*f.usage.lock().unwrap(), HashMap::from([(70, -300)]));
        // A second sweep has nothing left to do.
        assert_eq!(
            sweep(&f, &f, deleted_at + 2 * DAY_MS, DAY_MS)
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn a_message_without_files_changes_nothing() {
        let f = Fake::default();
        let delete = event(
            7,
            Payload::MessageDeleted(MessageDeleted {
                msg_seq: 5,
                message_id: 105,
            }),
        );
        assert_eq!(on_event(&f, &delete, 0).await.unwrap(), 0);
        assert!(f.queue.lock().unwrap().is_empty());
    }
}
