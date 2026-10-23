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
    insert_mention: PreparedStatement,
    my_channels: PreparedStatement,
    head_mention: PreparedStatement,
    user_mentions: PreparedStatement,
    channel_mentions: PreparedStatement,
}

/// Most unread mentions listed per channel; the client shows "N+" beyond it.
pub const MENTIONS_PER_CHANNEL: i32 = 100;

fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

/// What the HTTP API reads and the owner writes; a trait so the API can be tested without a database.
pub trait Positions: Send + Sync + 'static {
    fn positions(
        &self,
        user_id: i64,
    ) -> impl Future<Output = Result<Vec<(i64, i64)>, String>> + Send;
    /// The user's channels with unread mentions: `(channel_id, msg_seqs)`.
    fn mentions(
        &self,
        user_id: i64,
    ) -> impl Future<Output = Result<Vec<(i64, Vec<i64>)>, String>> + Send;
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
            insert_mention: prepare(format!("INSERT INTO {k}.user_mentions (user_id, channel_id, msg_seq) VALUES (?, ?, ?)"))
                .await?,
            my_channels: prepare(format!("SELECT channel_id FROM {k}.channels_by_user WHERE user_id = ?")).await?,
            head_mention: prepare(format!("SELECT last_mention_seq FROM {k}.channel_heads WHERE channel_id = ?")).await?,
            user_mentions: prepare(format!(
                "SELECT msg_seq FROM {k}.user_mentions WHERE user_id = ? AND channel_id = ? AND msg_seq > ? LIMIT {MENTIONS_PER_CHANNEL}"
            ))
            .await?,
            channel_mentions: prepare(format!(
                "SELECT msg_seq FROM {k}.channel_mentions WHERE channel_id = ? AND msg_seq > ? LIMIT {MENTIONS_PER_CHANNEL}"
            ))
            .await?,
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

    /// The feeder's `user_mentions` row (an idempotent insert).
    pub async fn insert_mention(
        &self,
        user_id: i64,
        channel_id: i64,
        msg_seq: i64,
    ) -> Result<(), String> {
        self.session
            .execute_unpaged(&self.insert_mention, (user_id, channel_id, msg_seq))
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn seqs(
        &self,
        statement: &PreparedStatement,
        values: impl scylla::serialize::row::SerializeRow,
    ) -> Result<Vec<i64>, String> {
        self.session
            .execute_unpaged(statement, values)
            .await
            .map_err(err)?
            .into_rows_result()
            .map_err(err)?
            .rows::<(i64,)>()
            .map_err(err)?
            .map(|r| r.map(|(s,)| s).map_err(err))
            .collect()
    }

    /// One channel's unread mentions. The `@channel` rows are read only when the head
    /// says there is one after the read position.
    async fn channel_unread_mentions(
        &self,
        user_id: i64,
        channel_id: i64,
        last_read: i64,
    ) -> Result<Vec<i64>, String> {
        let own = self
            .seqs(&self.user_mentions, (user_id, channel_id, last_read))
            .await?;
        let latest = self
            .session
            .execute_unpaged(&self.head_mention, (channel_id,))
            .await
            .map_err(err)?
            .into_rows_result()
            .map_err(err)?
            .maybe_first_row::<(Option<i64>,)>()
            .map_err(err)?
            .and_then(|(s,)| s)
            .unwrap_or(0);
        let channel = if latest > last_read {
            self.seqs(&self.channel_mentions, (channel_id, last_read))
                .await?
        } else {
            Vec::new()
        };
        Ok(crate::unread_mentions(last_read, own, channel))
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
    async fn mentions(&self, user_id: i64) -> Result<Vec<(i64, Vec<i64>)>, String> {
        use futures_util::{StreamExt, TryStreamExt};
        let read: std::collections::HashMap<i64, i64> =
            self.positions(user_id).await?.into_iter().collect();
        let channels = self.seqs(&self.my_channels, (user_id,)).await?;
        let found: Vec<(i64, Vec<i64>)> = futures_util::stream::iter(channels)
            .map(|channel_id| {
                let last_read = read.get(&channel_id).copied().unwrap_or(0);
                async move {
                    self.channel_unread_mentions(user_id, channel_id, last_read)
                        .await
                        .map(|seqs| (channel_id, seqs))
                }
            })
            .buffer_unordered(16)
            .try_collect()
            .await?;
        Ok(found
            .into_iter()
            .filter(|(_, seqs)| !seqs.is_empty())
            .collect())
    }

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
