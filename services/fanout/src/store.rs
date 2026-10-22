// SPDX-License-Identifier: AGPL-3.0-only
//! Membership snapshots from the wide-column store (written by the persister).

use std::collections::HashSet;

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::Consistency;
use scylla::statement::prepared::PreparedStatement;

use crate::membership::{Snapshot, SnapshotSource};

pub struct Store {
    session: Session,
    version: PreparedStatement,
    members: PreparedStatement,
}

impl Store {
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
            // Same level as the persister's writes, so a version we see has its rows visible (ADR-0006).
            p.set_consistency(Consistency::LocalQuorum);
            Ok(p)
        };
        Ok(Store {
            version: prepare(format!(
                "SELECT membership_version FROM {keyspace}.channel_heads WHERE channel_id = ?"
            ))
            .await?,
            members: prepare(format!(
                "SELECT user_id FROM {keyspace}.members_by_channel WHERE channel_id = ?"
            ))
            .await?,
            session,
        })
    }
}

impl SnapshotSource for Store {
    async fn load(&self, channel_id: i64, _at_least: i64) -> Result<Option<Snapshot>, String> {
        // Read the version first: rows are written before the version, so rows read
        // afterwards are at least as new as the version says.
        let version = self
            .session
            .execute_unpaged(&self.version, (channel_id,))
            .await
            .map_err(|e| e.to_string())?
            .into_rows_result()
            .map_err(|e| e.to_string())?
            .maybe_first_row::<(Option<i64>,)>()
            .map_err(|e| e.to_string())?;
        let Some((Some(version),)) = version else {
            return Ok(None);
        };
        let members: HashSet<i64> = self
            .session
            .execute_unpaged(&self.members, (channel_id,))
            .await
            .map_err(|e| e.to_string())?
            .into_rows_result()
            .map_err(|e| e.to_string())?
            .rows::<(i64,)>()
            .map_err(|e| e.to_string())?
            .map(|r| r.map(|(u,)| u))
            .collect::<Result<_, _>>()
            .map_err(|e| e.to_string())?;
        Ok(Some(Snapshot { version, members }))
    }
}
