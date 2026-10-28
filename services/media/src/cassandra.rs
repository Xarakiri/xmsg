// SPDX-License-Identifier: AGPL-3.0-only
//! File metadata (`files`, written once per upload) and membership reads
//! (`members_by_channel`, written by the persister), at LOCAL_QUORUM.

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::Consistency;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::CqlTimestamp;

use crate::lifecycle::{DAY_MS, Lifecycle, Purge};
use crate::{FileMeta, Store};

pub struct Cassandra {
    session: Session,
    member: PreparedStatement,
    insert: PreparedStatement,
    get: PreparedStatement,
    message_files: PreparedStatement,
    claim: PreparedStatement,
    mark_deleted: PreparedStatement,
    queue_purge: PreparedStatement,
    queued: PreparedStatement,
    forget: PreparedStatement,
}

fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

impl Cassandra {
    pub async fn connect(nodes: &[String], keyspace: &str) -> Result<Self, String> {
        let session = SessionBuilder::new()
            .known_nodes(nodes)
            .build()
            .await
            .map_err(|e| format!("connecting to {nodes:?}: {e}"))?;
        let prepare = async |cql: String| -> Result<PreparedStatement, String> {
            let mut p = session
                .prepare(cql.clone())
                .await
                .map_err(|e| format!("preparing `{cql}`: {e}"))?;
            p.set_consistency(Consistency::LocalQuorum);
            Ok(p)
        };
        let k = keyspace;
        Ok(Cassandra {
            member: prepare(format!("SELECT user_id FROM {k}.members_by_channel WHERE channel_id = ? AND user_id = ?")).await?,
            insert: prepare(format!("INSERT INTO {k}.files (file_id, channel_id, uploader_id, name, content_type, size, sha256, created_at, width, height, thumbnail_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")).await?,
            get: prepare(format!("SELECT file_id, channel_id, uploader_id, name, content_type, size, sha256, created_at, width, height, thumbnail_type, deleted_at FROM {k}.files WHERE file_id = ?")).await?,
            message_files: prepare(format!("SELECT attachments FROM {k}.messages WHERE channel_id = ? AND bucket = ? AND msg_seq = ?")).await?,
            claim: prepare(format!("UPDATE {k}.files SET message_id = ? WHERE file_id = ?")).await?,
            mark_deleted: prepare(format!("UPDATE {k}.files SET deleted_at = ? WHERE file_id = ?")).await?,
            queue_purge: prepare(format!("INSERT INTO {k}.file_purges (day, file_id, channel_id, deleted_at) VALUES (?, ?, ?, ?)")).await?,
            queued: prepare(format!("SELECT file_id, channel_id, deleted_at FROM {k}.file_purges WHERE day = ?")).await?,
            forget: prepare(format!("DELETE FROM {k}.file_purges WHERE day = ? AND file_id = ?")).await?,
            session,
        })
    }
}

type Row = (
    i64,
    i64,
    i64,
    String,
    String,
    i64,
    String,
    CqlTimestamp,
    Option<i32>,
    Option<i32>,
    Option<String>,
    Option<CqlTimestamp>,
);

impl Store for Cassandra {
    async fn is_member(&self, channel_id: i64, user_id: i64) -> Result<bool, String> {
        let rows = self
            .session
            .execute_unpaged(&self.member, (channel_id, user_id))
            .await
            .map_err(err)?;
        Ok(rows.into_rows_result().map_err(err)?.rows_num() > 0)
    }

    async fn insert(&self, f: &FileMeta) -> Result<(), String> {
        self.session
            .execute_unpaged(
                &self.insert,
                (
                    f.file_id,
                    f.channel_id,
                    f.uploader_id,
                    &f.name,
                    &f.content_type,
                    f.size,
                    &f.sha256,
                    CqlTimestamp(f.created_at_ms),
                    f.width,
                    f.height,
                    &f.thumbnail_type,
                ),
            )
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn get(&self, file_id: i64) -> Result<Option<FileMeta>, String> {
        let rows = self
            .session
            .execute_unpaged(&self.get, (file_id,))
            .await
            .map_err(err)?
            .into_rows_result()
            .map_err(err)?;
        let row = rows.maybe_first_row::<Row>().map_err(err)?;
        Ok(row.map(|r| FileMeta {
            file_id: r.0,
            channel_id: r.1,
            uploader_id: r.2,
            name: r.3,
            content_type: r.4,
            size: r.5,
            sha256: r.6,
            created_at_ms: r.7.0,
            width: r.8,
            height: r.9,
            thumbnail_type: r.10,
            deleted: r.11.is_some(),
        }))
    }
}

/// The `messages` table's partition bucket for a msg_seq (as in the messages service).
fn bucket(msg_seq: i64) -> i32 {
    (msg_seq / 10_000) as i32
}

impl Lifecycle for Cassandra {
    async fn message_files(&self, channel_id: i64, msg_seq: i64) -> Result<Vec<i64>, String> {
        use xmsg_proto::Message as _;
        let rows = self
            .session
            .execute_unpaged(&self.message_files, (channel_id, bucket(msg_seq), msg_seq))
            .await
            .map_err(err)?
            .into_rows_result()
            .map_err(err)?;
        let mut files = Vec::new();
        for row in rows.rows::<(Option<Vec<u8>>,)>().map_err(err)? {
            let (stored,) = row.map_err(err)?;
            if let Some(stored) = stored.filter(|s| !s.is_empty()) {
                let decoded =
                    xmsg_proto::v1::Attachments::decode(stored.as_slice()).map_err(err)?;
                files.extend(decoded.items.iter().map(|a| a.file_id));
            }
        }
        Ok(files)
    }

    async fn claim(&self, file_id: i64, message_id: i64) -> Result<(), String> {
        self.session
            .execute_unpaged(&self.claim, (message_id, file_id))
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn mark_deleted(&self, p: &Purge) -> Result<(), String> {
        let at = CqlTimestamp(p.deleted_at_ms);
        self.session
            .execute_unpaged(&self.mark_deleted, (at, p.file_id))
            .await
            .map_err(err)?;
        let day = p.deleted_at_ms.div_euclid(DAY_MS) as i32;
        self.session
            .execute_unpaged(&self.queue_purge, (day, p.file_id, p.channel_id, at))
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn queued(&self, day: i64) -> Result<Vec<Purge>, String> {
        let rows = self
            .session
            .execute_unpaged(&self.queued, (day as i32,))
            .await
            .map_err(err)?
            .into_rows_result()
            .map_err(err)?;
        rows.rows::<(i64, i64, CqlTimestamp)>()
            .map_err(err)?
            .map(|r| {
                r.map(|(file_id, channel_id, at)| Purge {
                    file_id,
                    channel_id,
                    deleted_at_ms: at.0,
                })
                .map_err(err)
            })
            .collect()
    }

    async fn forget(&self, day: i64, file_id: i64) -> Result<(), String> {
        self.session
            .execute_unpaged(&self.forget, (day as i32, file_id))
            .await
            .map_err(err)?;
        Ok(())
    }
}
