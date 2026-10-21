// SPDX-License-Identifier: AGPL-3.0-only
//! A user's channels and their heads, from the wide-column store (written by the
//! persister, ADR-0006). The cheap step of sync (plan §6.6) until heads move to Redis.

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::Consistency;
use scylla::statement::prepared::PreparedStatement;

#[derive(Debug, Clone, PartialEq)]
pub struct MyChannel {
    pub channel_id: i64,
    pub workspace_id: i64,
    /// `public`, `private`, `dm`, `group_dm` or `broadcast`.
    pub kind: String,
    pub event_seq: i64,
    pub msg_seq: i64,
}

pub trait Memberships: Send + Sync + 'static {
    fn channels_of(
        &self,
        user_id: i64,
    ) -> impl Future<Output = Result<Vec<MyChannel>, String>> + Send;
}

pub struct Cassandra {
    session: Session,
    by_user: PreparedStatement,
    head: PreparedStatement,
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
            by_user: prepare(format!("SELECT workspace_id, channel_id, channel_type FROM {k}.channels_by_user WHERE user_id = ?"))
                .await?,
            head: prepare(format!("SELECT event_seq, msg_seq FROM {k}.channel_heads WHERE channel_id = ?")).await?,
            session,
        })
    }
}

impl Memberships for Cassandra {
    async fn channels_of(&self, user_id: i64) -> Result<Vec<MyChannel>, String> {
        let rows: Vec<(i64, i64, Option<String>)> = self
            .session
            .execute_unpaged(&self.by_user, (user_id,))
            .await
            .map_err(err)?
            .into_rows_result()
            .map_err(err)?
            .rows::<(i64, i64, Option<String>)>()
            .map_err(err)?
            .collect::<Result<_, _>>()
            .map_err(err)?;
        // One head read per channel, concurrently (a few hundred channels at most per user here).
        let heads = rows.iter().map(|(_, channel_id, _)| async move {
            self.session
                .execute_unpaged(&self.head, (*channel_id,))
                .await
                .map_err(err)?
                .into_rows_result()
                .map_err(err)?
                .maybe_first_row::<(Option<i64>, Option<i64>)>()
                .map_err(err)
        });
        let heads = futures_util::future::try_join_all(heads).await?;
        Ok(rows
            .into_iter()
            .zip(heads)
            .map(|((workspace_id, channel_id, kind), head)| {
                let (event_seq, msg_seq) = head.unwrap_or_default();
                MyChannel {
                    channel_id,
                    workspace_id,
                    kind: kind.unwrap_or_default(),
                    event_seq: event_seq.unwrap_or_default(),
                    msg_seq: msg_seq.unwrap_or_default(),
                }
            })
            .collect())
    }
}
