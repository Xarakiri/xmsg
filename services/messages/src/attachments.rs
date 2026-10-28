// SPDX-License-Identifier: AGPL-3.0-only
//! Files in messages (ADR-0024). The client sends only file IDs; this service looks them
//! up in the media service's `files` table and writes what the message shows (name, type,
//! size, image size) into the event itself, replacing anything the client put there. A
//! sender can attach only files they uploaded to the same channel.

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::Consistency;
use scylla::statement::prepared::PreparedStatement;
use xmsg_proto::v1::{Attachment, InboundEvent, RejectReason, inbound_event::Payload};

/// Per message.
pub const MAX_ATTACHMENTS: usize = 10;

/// A `files` row, as far as messages need it.
#[derive(Clone, Debug, PartialEq)]
pub struct FileRecord {
    pub file_id: i64,
    pub channel_id: i64,
    pub uploader_id: i64,
    pub name: String,
    pub content_type: String,
    pub size: i64,
    pub width: Option<i32>,
    pub height: Option<i32>,
    pub has_thumbnail: bool,
    /// Already sent with a message (a file belongs to one), or that message was deleted.
    pub used: bool,
}

/// Where file records are read.
pub trait Files: Send + Sync {
    /// The records that exist among `ids` (any order).
    fn files(&self, ids: &[i64]) -> impl Future<Output = Result<Vec<FileRecord>, String>> + Send;
}

/// The lookup this instance has: none without Cassandra (history off), so sends with files
/// are refused rather than passed on unchecked.
pub enum Lookup {
    Off,
    Cassandra(Box<CassandraFiles>),
}

impl Files for Lookup {
    async fn files(&self, ids: &[i64]) -> Result<Vec<FileRecord>, String> {
        match self {
            Lookup::Off => Err("file attachments need XMSG_AUTH_JWKS_URL (Cassandra reads)".into()),
            Lookup::Cassandra(c) => c.files(ids).await,
        }
    }
}

/// Fills in the send's attachments from our records, or says why it's refused.
pub async fn resolve(
    files: &impl Files,
    event: &mut InboundEvent,
) -> Result<(), (RejectReason, String)> {
    let invalid = |detail: String| Err((RejectReason::Invalid, detail));
    let (channel_id, actor_id) = (event.channel_id, event.actor_id);
    let Some(Payload::SendMessage(send)) = &mut event.payload else {
        return Ok(());
    };
    // Never what the client sent.
    send.attachments.clear();
    if send.file_ids.is_empty() {
        return Ok(());
    }
    if send.file_ids.len() > MAX_ATTACHMENTS {
        return invalid(format!("at most {MAX_ATTACHMENTS} files per message"));
    }
    let mut seen = std::collections::HashSet::new();
    if !send.file_ids.iter().all(|id| seen.insert(*id)) {
        return invalid("the same file twice".into());
    }
    let records = files.files(&send.file_ids).await.map_err(|e| {
        tracing::warn!(error = %e, "file lookup failed");
        (
            RejectReason::Unavailable,
            "temporarily unavailable, retry".to_string(),
        )
    })?;
    for id in &send.file_ids {
        // Unknown, another channel's, or someone else's upload: the same answer.
        let Some(f) = records
            .iter()
            .find(|f| f.file_id == *id && f.channel_id == channel_id && f.uploader_id == actor_id)
        else {
            return invalid(format!("unknown file {id}"));
        };
        // Best effort: two sends in the same moment can both pass, before the media service
        // records the first (ADR-0024).
        if f.used {
            return invalid(format!("file {id} was already sent; upload it again"));
        }
        send.attachments.push(Attachment {
            file_id: f.file_id,
            name: f.name.clone(),
            content_type: f.content_type.clone(),
            size: f.size,
            width: f.width.unwrap_or(0),
            height: f.height.unwrap_or(0),
            has_thumbnail: f.has_thumbnail,
        });
    }
    Ok(())
}

pub struct CassandraFiles {
    session: Session,
    get: PreparedStatement,
}

impl CassandraFiles {
    pub async fn connect(nodes: &[String], keyspace: &str) -> Result<Self, String> {
        let session = SessionBuilder::new()
            .known_nodes(nodes)
            .build()
            .await
            .map_err(|e| format!("connecting to {nodes:?}: {e}"))?;
        let cql = format!(
            "SELECT file_id, channel_id, uploader_id, name, content_type, size, width, height, thumbnail_type, message_id, deleted_at FROM {keyspace}.files WHERE file_id = ?"
        );
        let mut get = session
            .prepare(cql.clone())
            .await
            .map_err(|e| format!("preparing `{cql}`: {e}"))?;
        get.set_consistency(Consistency::LocalQuorum);
        Ok(CassandraFiles { session, get })
    }

    async fn files(&self, ids: &[i64]) -> Result<Vec<FileRecord>, String> {
        type Row = (
            i64,
            i64,
            i64,
            String,
            String,
            i64,
            Option<i32>,
            Option<i32>,
            Option<String>,
            Option<i64>,
            Option<scylla::value::CqlTimestamp>,
        );
        let mut out = Vec::new();
        for id in ids {
            let rows = self
                .session
                .execute_unpaged(&self.get, (*id,))
                .await
                .map_err(|e| e.to_string())?
                .into_rows_result()
                .map_err(|e| e.to_string())?;
            if let Some(r) = rows.maybe_first_row::<Row>().map_err(|e| e.to_string())? {
                out.push(FileRecord {
                    file_id: r.0,
                    channel_id: r.1,
                    uploader_id: r.2,
                    name: r.3,
                    content_type: r.4,
                    size: r.5,
                    width: r.6,
                    height: r.7,
                    has_thumbnail: r.8.is_some(),
                    used: r.9.is_some() || r.10.is_some(),
                });
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xmsg_proto::v1::{SendMessage, ThreadRoot};

    struct Fake(Vec<FileRecord>);
    impl Files for Fake {
        async fn files(&self, ids: &[i64]) -> Result<Vec<FileRecord>, String> {
            Ok(self
                .0
                .iter()
                .filter(|f| ids.contains(&f.file_id))
                .cloned()
                .collect())
        }
    }

    fn file(file_id: i64, channel_id: i64, uploader_id: i64) -> FileRecord {
        FileRecord {
            file_id,
            channel_id,
            uploader_id,
            name: format!("f{file_id}.png"),
            content_type: "image/png".into(),
            size: 10,
            width: Some(4),
            height: Some(3),
            has_thumbnail: true,
            used: false,
        }
    }

    fn send(file_ids: Vec<i64>) -> InboundEvent {
        InboundEvent {
            channel_id: 7,
            actor_id: 1,
            dedupe_id: "c-1".into(),
            payload: Some(Payload::SendMessage(SendMessage {
                body: "look".into(),
                file_ids,
                // A client trying to describe its own attachments.
                attachments: vec![Attachment {
                    file_id: 999,
                    name: "evil".into(),
                    ..Default::default()
                }],
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    fn attachments(e: &InboundEvent) -> Vec<Attachment> {
        match &e.payload {
            Some(Payload::SendMessage(s)) => s.attachments.clone(),
            _ => vec![],
        }
    }

    fn fake() -> Fake {
        Fake(vec![
            file(1, 7, 1),
            file(2, 7, 1),
            file(3, 8, 1),
            file(4, 7, 2),
        ])
    }

    #[tokio::test]
    async fn own_files_in_this_channel_are_attached_in_order() {
        let mut e = send(vec![2, 1]);
        resolve(&fake(), &mut e).await.unwrap();
        let a = attachments(&e);
        assert_eq!(a.iter().map(|a| a.file_id).collect::<Vec<_>>(), [2, 1]);
        assert_eq!(
            (
                a[0].name.as_str(),
                a[0].width,
                a[0].height,
                a[0].has_thumbnail
            ),
            ("f2.png", 4, 3, true)
        );
    }

    #[tokio::test]
    async fn client_supplied_attachments_are_dropped() {
        let mut e = send(vec![]);
        resolve(&fake(), &mut e).await.unwrap();
        assert!(attachments(&e).is_empty());
    }

    #[tokio::test]
    async fn other_channels_others_uploads_and_unknown_files_are_refused() {
        for ids in [vec![3], vec![4], vec![42], vec![1, 3]] {
            let mut e = send(ids.clone());
            let (reason, _) = resolve(&fake(), &mut e).await.unwrap_err();
            assert_eq!(reason, RejectReason::Invalid, "{ids:?}");
        }
    }

    #[tokio::test]
    async fn limits() {
        let mut e = send((1..=11).collect());
        assert!(resolve(&fake(), &mut e).await.is_err());
        let mut e = send(vec![1, 1]);
        assert!(resolve(&fake(), &mut e).await.is_err());
    }

    #[tokio::test]
    async fn a_file_already_sent_is_refused() {
        let mut sent = file(1, 7, 1);
        sent.used = true;
        let mut e = send(vec![1]);
        let (reason, detail) = resolve(&Fake(vec![sent]), &mut e).await.unwrap_err();
        assert_eq!(reason, RejectReason::Invalid);
        assert!(detail.contains("already sent"), "{detail}");
    }

    #[tokio::test]
    async fn thread_replies_carry_files_too() {
        let mut e = send(vec![1]);
        if let Some(Payload::SendMessage(s)) = &mut e.payload {
            s.thread_root = Some(ThreadRoot {
                msg_seq: 1,
                message_id: 1,
            });
        }
        resolve(&fake(), &mut e).await.unwrap();
        assert_eq!(attachments(&e).len(), 1);
    }

    #[tokio::test]
    async fn without_a_lookup_files_are_refused_as_unavailable() {
        let mut e = send(vec![1]);
        let (reason, _) = resolve(&Lookup::Off, &mut e).await.unwrap_err();
        assert_eq!(reason, RejectReason::Unavailable);
        let mut plain = send(vec![]);
        assert!(resolve(&Lookup::Off, &mut plain).await.is_ok());
    }
}
