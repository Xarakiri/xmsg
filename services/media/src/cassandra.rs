// SPDX-License-Identifier: AGPL-3.0-only
//! File metadata (`files`, written once per upload) and membership reads
//! (`members_by_channel`, written by the persister), at LOCAL_QUORUM. Storage quotas
//! (ADR-0024): usage in a `workspace_storage` counter, the channel's workspace from
//! `channels_by_user`, and from Postgres (when configured) the operator's overrides and
//! workspace roles, cached for a minute.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::Consistency;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::CqlTimestamp;

use crate::lifecycle::{DAY_MS, Lifecycle, Purge};
use crate::{FileMeta, Store};

pub struct Cassandra {
    session: Session,
    /// Workspaces and roles (the users service's tables); without it, every workspace
    /// uses the default quota and nobody can read usage.
    pg: Option<sqlx::PgPool>,
    overrides: Mutex<HashMap<i64, (Option<i64>, Instant)>>,
    workspace_of: PreparedStatement,
    used: PreparedStatement,
    add_usage: PreparedStatement,
    file_usage: PreparedStatement,
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
    /// Reads quota overrides and roles from the users service's Postgres.
    pub fn with_postgres(mut self, pool: sqlx::PgPool) -> Self {
        self.pg = Some(pool);
        self
    }

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
            insert: prepare(format!("INSERT INTO {k}.files (file_id, channel_id, uploader_id, name, content_type, size, sha256, created_at, width, height, thumbnail_type, workspace_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")).await?,
            get: prepare(format!("SELECT file_id, channel_id, uploader_id, name, content_type, size, sha256, created_at, width, height, thumbnail_type, deleted_at, workspace_id FROM {k}.files WHERE file_id = ?")).await?,
            // One user's partition: small, so filtering on the channel is cheap.
            workspace_of: prepare(format!("SELECT workspace_id FROM {k}.channels_by_user WHERE user_id = ? AND channel_id = ? ALLOW FILTERING")).await?,
            used: prepare(format!("SELECT bytes FROM {k}.workspace_storage WHERE workspace_id = ?")).await?,
            add_usage: prepare(format!("UPDATE {k}.workspace_storage SET bytes = bytes + ? WHERE workspace_id = ?")).await?,
            file_usage: prepare(format!("SELECT workspace_id, size FROM {k}.files WHERE file_id = ?")).await?,
            pg: None,
            overrides: Mutex::new(HashMap::new()),
            message_files: prepare(format!("SELECT attachments FROM {k}.messages WHERE channel_id = ? AND bucket = ? AND msg_seq = ?")).await?,
            claim: prepare(format!("UPDATE {k}.files SET message_id = ? WHERE file_id = ?")).await?,
            mark_deleted: prepare(format!("UPDATE {k}.files SET deleted_at = ? WHERE file_id = ?")).await?,
            queue_purge: prepare(format!("INSERT INTO {k}.file_purges (day, file_id, channel_id, deleted_at, workspace_id, size) VALUES (?, ?, ?, ?, ?, ?)")).await?,
            queued: prepare(format!("SELECT file_id, channel_id, deleted_at, workspace_id, size FROM {k}.file_purges WHERE day = ?")).await?,
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
    Option<i64>,
);

impl Store for Cassandra {
    async fn workspace_of(&self, user_id: i64, channel_id: i64) -> Result<Option<i64>, String> {
        let rows = self
            .session
            .execute_unpaged(&self.workspace_of, (user_id, channel_id))
            .await
            .map_err(err)?
            .into_rows_result()
            .map_err(err)?;
        Ok(rows.maybe_first_row::<(i64,)>().map_err(err)?.map(|r| r.0))
    }

    async fn used(&self, workspace_id: i64) -> Result<i64, String> {
        let rows = self
            .session
            .execute_unpaged(&self.used, (workspace_id,))
            .await
            .map_err(err)?
            .into_rows_result()
            .map_err(err)?;
        let row = rows
            .maybe_first_row::<(Option<scylla::value::Counter>,)>()
            .map_err(err)?;
        Ok(row.and_then(|r| r.0).map_or(0, |c| c.0.max(0)))
    }

    async fn add_usage(&self, workspace_id: i64, delta: i64) -> Result<(), String> {
        self.session
            .execute_unpaged(
                &self.add_usage,
                (scylla::value::Counter(delta), workspace_id),
            )
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn quota_override(&self, workspace_id: i64) -> Result<Option<i64>, String> {
        let Some(pg) = &self.pg else { return Ok(None) };
        if let Some((value, at)) = self.overrides.lock().unwrap().get(&workspace_id)
            && at.elapsed() < Duration::from_secs(60)
        {
            return Ok(*value);
        }
        let value: Option<i64> =
            sqlx::query_scalar("SELECT storage_quota_bytes FROM workspaces WHERE id = $1")
                .bind(workspace_id)
                .fetch_optional(pg)
                .await
                .map_err(err)?
                .flatten();
        self.overrides
            .lock()
            .unwrap()
            .insert(workspace_id, (value, Instant::now()));
        Ok(value)
    }

    async fn workspace_role(
        &self,
        workspace_id: i64,
        user_id: i64,
    ) -> Result<Option<String>, String> {
        let Some(pg) = &self.pg else { return Ok(None) };
        sqlx::query_scalar(
            "SELECT role FROM workspace_members WHERE workspace_id = $1 AND user_id = $2",
        )
        .bind(workspace_id)
        .bind(user_id)
        .fetch_optional(pg)
        .await
        .map_err(err)
    }

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
                    f.workspace_id,
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
            workspace_id: r.12,
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

    async fn usage_of(&self, file_id: i64) -> Result<Option<(Option<i64>, i64)>, String> {
        let rows = self
            .session
            .execute_unpaged(&self.file_usage, (file_id,))
            .await
            .map_err(err)?
            .into_rows_result()
            .map_err(err)?;
        Ok(rows
            .maybe_first_row::<(Option<i64>, Option<i64>)>()
            .map_err(err)?
            .map(|(ws, size)| (ws, size.unwrap_or(0))))
    }

    async fn release(&self, workspace_id: i64, bytes: i64) -> Result<(), String> {
        Store::add_usage(self, workspace_id, -bytes).await
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
            .execute_unpaged(
                &self.queue_purge,
                (day, p.file_id, p.channel_id, at, p.workspace_id, p.size),
            )
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
        rows.rows::<(i64, i64, CqlTimestamp, Option<i64>, Option<i64>)>()
            .map_err(err)?
            .map(|r| {
                r.map(|(file_id, channel_id, at, workspace_id, size)| Purge {
                    file_id,
                    channel_id,
                    deleted_at_ms: at.0,
                    workspace_id,
                    size: size.unwrap_or(0),
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
