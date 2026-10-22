// SPDX-License-Identifier: AGPL-3.0-only
//! `read_state` (written only by the owner) and `channel_heads` (read for joins), at
//! LOCAL_QUORUM like the persister's writes.

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::Consistency;
use scylla::statement::prepared::PreparedStatement;

pub struct Cassandra {
    session: Session,
    get: PreparedStatement,
    set: PreparedStatement,
    list: PreparedStatement,
    head: PreparedStatement,
}

fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

/// What the HTTP API reads and the owner writes; a trait so the API can be tested without a database.
pub trait Positions: Send + Sync + 'static {
    fn positions(
        &self,
        user_id: i64,
    ) -> impl Future<Output = Result<Vec<(i64, i64)>, String>> + Send;
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
            get: prepare(format!("SELECT last_read_msg_seq FROM {k}.read_state WHERE user_id = ? AND channel_id = ?")).await?,
            set: prepare(format!("UPDATE {k}.read_state SET last_read_msg_seq = ? WHERE user_id = ? AND channel_id = ?"))
                .await?,
            list: prepare(format!("SELECT channel_id, last_read_msg_seq FROM {k}.read_state WHERE user_id = ?")).await?,
            head: prepare(format!("SELECT msg_seq FROM {k}.channel_heads WHERE channel_id = ?")).await?,
            session,
        })
    }

    pub async fn get(&self, user_id: i64, channel_id: i64) -> Result<i64, String> {
        let row = self
            .session
            .execute_unpaged(&self.get, (user_id, channel_id))
            .await
            .map_err(err)?
            .into_rows_result()
            .map_err(err)?
            .maybe_first_row::<(Option<i64>,)>()
            .map_err(err)?;
        Ok(row.and_then(|(s,)| s).unwrap_or(0))
    }

    pub async fn set(&self, user_id: i64, channel_id: i64, msg_seq: i64) -> Result<(), String> {
        self.session
            .execute_unpaged(&self.set, (msg_seq, user_id, channel_id))
            .await
            .map_err(err)?;
        Ok(())
    }

    /// The channel's latest `msg_seq` as persisted (for a join seen after a feeder restart).
    pub async fn head_msg_seq(&self, channel_id: i64) -> Result<i64, String> {
        let row = self
            .session
            .execute_unpaged(&self.head, (channel_id,))
            .await
            .map_err(err)?
            .into_rows_result()
            .map_err(err)?
            .maybe_first_row::<(Option<i64>,)>()
            .map_err(err)?;
        Ok(row.and_then(|(s,)| s).unwrap_or(0))
    }
}

impl Positions for Cassandra {
    async fn positions(&self, user_id: i64) -> Result<Vec<(i64, i64)>, String> {
        self.session
            .execute_unpaged(&self.list, (user_id,))
            .await
            .map_err(err)?
            .into_rows_result()
            .map_err(err)?
            .rows::<(i64, Option<i64>)>()
            .map_err(err)?
            .map(|r| r.map(|(c, s)| (c, s.unwrap_or(0))).map_err(err))
            .collect()
    }
}
