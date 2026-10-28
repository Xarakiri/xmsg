// SPDX-License-Identifier: AGPL-3.0-only
//! Active calls (`active_calls`, one row per channel while its call lasts), with
//! lightweight transactions so concurrent starts agree on one room and a late webhook for
//! an earlier call can't touch the current one. Membership and channel types are the
//! persister's tables.

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::Consistency;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::{CqlTimestamp, CqlValue, Row};

use crate::{ActiveCall, Calls};

pub struct Cassandra {
    session: Session,
    member: PreparedStatement,
    channel_type: PreparedStatement,
    start: PreparedStatement,
    active: PreparedStatement,
    join: PreparedStatement,
    leave: PreparedStatement,
    announce: PreparedStatement,
    finish: PreparedStatement,
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
            channel_type: prepare(format!("SELECT channel_type FROM {k}.channel_heads WHERE channel_id = ?")).await?,
            start: prepare(format!("INSERT INTO {k}.active_calls (channel_id, call_id, huddle, started_at, announced) VALUES (?, ?, ?, ?, false) IF NOT EXISTS")).await?,
            active: prepare(format!("SELECT call_id, huddle, started_at, participants, announced FROM {k}.active_calls WHERE channel_id = ?")).await?,
            join: prepare(format!("UPDATE {k}.active_calls SET participants = participants + ? WHERE channel_id = ? IF call_id = ?")).await?,
            leave: prepare(format!("UPDATE {k}.active_calls SET participants = participants - ? WHERE channel_id = ? IF call_id = ?")).await?,
            announce: prepare(format!("UPDATE {k}.active_calls SET announced = true WHERE channel_id = ? IF call_id = ? AND announced = false")).await?,
            finish: prepare(format!("DELETE FROM {k}.active_calls WHERE channel_id = ? IF call_id = ?")).await?,
            session,
        })
    }

    /// Whether a lightweight transaction applied (its first column, `[applied]`).
    async fn applied(
        &self,
        statement: &PreparedStatement,
        values: impl scylla::serialize::row::SerializeRow,
    ) -> Result<bool, String> {
        let rows = self
            .session
            .execute_unpaged(statement, values)
            .await
            .map_err(err)?
            .into_rows_result()
            .map_err(err)?;
        let row = rows.maybe_first_row::<Row>().map_err(err)?;
        Ok(matches!(
            row.and_then(|r| r.columns.into_iter().next().flatten()),
            Some(CqlValue::Boolean(true))
        ))
    }
}

type ActiveRow = (
    Option<String>,
    Option<bool>,
    Option<CqlTimestamp>,
    Option<Vec<i64>>,
    Option<bool>,
);

impl Calls for Cassandra {
    async fn is_member(&self, channel_id: i64, user_id: i64) -> Result<bool, String> {
        let rows = self
            .session
            .execute_unpaged(&self.member, (channel_id, user_id))
            .await
            .map_err(err)?;
        Ok(rows.into_rows_result().map_err(err)?.rows_num() > 0)
    }

    async fn channel_type(&self, channel_id: i64) -> Result<Option<String>, String> {
        let rows = self
            .session
            .execute_unpaged(&self.channel_type, (channel_id,))
            .await
            .map_err(err)?
            .into_rows_result()
            .map_err(err)?;
        Ok(rows
            .maybe_first_row::<(Option<String>,)>()
            .map_err(err)?
            .and_then(|r| r.0))
    }

    async fn start(&self, channel_id: i64, call: &ActiveCall) -> Result<ActiveCall, String> {
        self.applied(
            &self.start,
            (
                channel_id,
                &call.call_id,
                call.huddle,
                CqlTimestamp(call.started_at_ms),
            ),
        )
        .await?;
        // Ours, or the one someone else started a moment earlier.
        self.active(channel_id)
            .await?
            .ok_or_else(|| "the call vanished as it started".into())
    }

    async fn active(&self, channel_id: i64) -> Result<Option<ActiveCall>, String> {
        let rows = self
            .session
            .execute_unpaged(&self.active, (channel_id,))
            .await
            .map_err(err)?
            .into_rows_result()
            .map_err(err)?;
        Ok(rows.maybe_first_row::<ActiveRow>().map_err(err)?.and_then(
            |(call_id, huddle, started, participants, announced)| {
                Some(ActiveCall {
                    call_id: call_id?,
                    huddle: huddle.unwrap_or(false),
                    started_at_ms: started.map_or(0, |t| t.0),
                    participants: participants.unwrap_or_default(),
                    announced: announced.unwrap_or(false),
                })
            },
        ))
    }

    async fn set_participant(
        &self,
        channel_id: i64,
        call_id: &str,
        user_id: i64,
        present: bool,
    ) -> Result<Option<ActiveCall>, String> {
        let statement = if present { &self.join } else { &self.leave };
        if !self
            .applied(statement, (vec![user_id], channel_id, call_id))
            .await?
        {
            return Ok(None);
        }
        Ok(self
            .active(channel_id)
            .await?
            .filter(|c| c.call_id == call_id))
    }

    async fn announce(&self, channel_id: i64, call_id: &str) -> Result<bool, String> {
        self.applied(&self.announce, (channel_id, call_id)).await
    }

    async fn finish(&self, channel_id: i64, call_id: &str) -> Result<Option<ActiveCall>, String> {
        let Some(call) = self
            .active(channel_id)
            .await?
            .filter(|c| c.call_id == call_id)
        else {
            return Ok(None);
        };
        Ok(self
            .applied(&self.finish, (channel_id, call_id))
            .await?
            .then_some(call))
    }
}
