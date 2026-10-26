// SPDX-License-Identifier: AGPL-3.0-only
//! A user's channels and their heads, from the wide-column store (written by the
//! persister, ADR-0006). The cheap step of sync (plan §6.6) until heads move to Redis.

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::Consistency;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::CqlTimestamp;
use xmsg_proto::v1::{MemberSettings, NotifyLevel};

#[derive(Debug, Clone, PartialEq)]
pub struct MyChannel {
    pub channel_id: i64,
    pub workspace_id: i64,
    /// `public`, `private`, `dm`, `group_dm` or `broadcast`.
    pub kind: String,
    pub event_seq: i64,
    pub msg_seq: i64,
    /// `push` or `pull` (ADR-0008): pull-mode channels reach gateways on `ch.{id}`.
    pub fanout_mode: String,
}

pub trait Memberships: Send + Sync + 'static {
    fn channels_of(
        &self,
        user_id: i64,
    ) -> impl Future<Output = Result<Vec<MyChannel>, String>> + Send;

    /// The user's `member_settings` rows (including orphans; callers filter by membership).
    fn settings_of(
        &self,
        user_id: i64,
    ) -> impl Future<Output = Result<Vec<MemberSettings>, String>> + Send;

    /// Only the settings-writer calls this (ADR-0016: one writer).
    fn save_settings(
        &self,
        settings: &MemberSettings,
    ) -> impl Future<Output = Result<(), String>> + Send;
}

pub struct Cassandra {
    session: Session,
    by_user: PreparedStatement,
    head: PreparedStatement,
    settings_by_user: PreparedStatement,
    save_settings: PreparedStatement,
}

fn notify_name(level: i32) -> &'static str {
    match NotifyLevel::try_from(level) {
        Ok(NotifyLevel::Mentions) => "mentions",
        Ok(NotifyLevel::Nothing) => "nothing",
        _ => "all",
    }
}

fn notify_level(name: &str) -> NotifyLevel {
    match name {
        "mentions" => NotifyLevel::Mentions,
        "nothing" => NotifyLevel::Nothing,
        _ => NotifyLevel::All,
    }
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
            by_user: prepare(format!("SELECT workspace_id, channel_id, channel_type, fanout_mode FROM {k}.channels_by_user WHERE user_id = ?"))
                .await?,
            head: prepare(format!("SELECT event_seq, msg_seq FROM {k}.channel_heads WHERE channel_id = ?")).await?,
            settings_by_user: prepare(format!(
                "SELECT channel_id, notify_level, muted, updated_at FROM {k}.member_settings WHERE user_id = ?"
            ))
            .await?,
            save_settings: prepare(format!(
                "INSERT INTO {k}.member_settings (user_id, channel_id, notify_level, muted, updated_at) VALUES (?, ?, ?, ?, ?)"
            ))
            .await?,
            session,
        })
    }
}

impl Memberships for Cassandra {
    async fn settings_of(&self, user_id: i64) -> Result<Vec<MemberSettings>, String> {
        self.session
            .execute_unpaged(&self.settings_by_user, (user_id,))
            .await
            .map_err(err)?
            .into_rows_result()
            .map_err(err)?
            .rows::<(i64, Option<String>, Option<bool>, Option<CqlTimestamp>)>()
            .map_err(err)?
            .map(|r| {
                r.map(|(channel_id, level, muted, at)| MemberSettings {
                    user_id,
                    channel_id,
                    notify_level: notify_level(level.as_deref().unwrap_or("all")) as i32,
                    muted: muted.unwrap_or(false),
                    updated_at_ms: at.map(|t| t.0).unwrap_or(0),
                })
                .map_err(err)
            })
            .collect()
    }

    async fn save_settings(&self, s: &MemberSettings) -> Result<(), String> {
        self.session
            .execute_unpaged(
                &self.save_settings,
                (
                    s.user_id,
                    s.channel_id,
                    notify_name(s.notify_level),
                    s.muted,
                    CqlTimestamp(s.updated_at_ms),
                ),
            )
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn channels_of(&self, user_id: i64) -> Result<Vec<MyChannel>, String> {
        let rows: Vec<(i64, i64, Option<String>, Option<String>)> = self
            .session
            .execute_unpaged(&self.by_user, (user_id,))
            .await
            .map_err(err)?
            .into_rows_result()
            .map_err(err)?
            .rows::<(i64, i64, Option<String>, Option<String>)>()
            .map_err(err)?
            .collect::<Result<_, _>>()
            .map_err(err)?;
        // One head read per channel, concurrently (a few hundred channels at most per user here).
        let heads = rows.iter().map(|(_, channel_id, _, _)| async move {
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
            .map(|((workspace_id, channel_id, kind, mode), head)| {
                let (event_seq, msg_seq) = head.unwrap_or_default();
                MyChannel {
                    channel_id,
                    workspace_id,
                    kind: kind.unwrap_or_default(),
                    event_seq: event_seq.unwrap_or_default(),
                    msg_seq: msg_seq.unwrap_or_default(),
                    fanout_mode: mode.unwrap_or_else(|| "push".into()),
                }
            })
            .collect())
    }
}
