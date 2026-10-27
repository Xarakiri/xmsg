// SPDX-License-Identifier: AGPL-3.0-only
//! File metadata (`files`, written once per upload) and membership reads
//! (`members_by_channel`, written by the persister), at LOCAL_QUORUM.

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::Consistency;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::CqlTimestamp;

use crate::{FileMeta, Store};

pub struct Cassandra {
    session: Session,
    member: PreparedStatement,
    insert: PreparedStatement,
    get: PreparedStatement,
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
            get: prepare(format!("SELECT file_id, channel_id, uploader_id, name, content_type, size, sha256, created_at, width, height, thumbnail_type FROM {k}.files WHERE file_id = ?")).await?,
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
        }))
    }
}
