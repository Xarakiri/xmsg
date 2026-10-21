// SPDX-License-Identifier: AGPL-3.0-only
//! Channel metadata and DM pairs in Postgres (plan §7.1), written together with the
//! outbox row in one transaction (ADR-0010).

use sqlx::PgPool;
use xmsg_proto::Message;
use xmsg_proto::v1::InboundEvent;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum StoreError {
    #[error("unknown workspace")]
    UnknownWorkspace,
    #[error("unknown user")]
    UnknownUser,
    #[error("database: {0}")]
    Db(String),
}

impl From<sqlx::Error> for StoreError {
    fn from(e: sqlx::Error) -> Self {
        match &e {
            sqlx::Error::Database(db) if db.is_foreign_key_violation() => match db.constraint() {
                Some(c) if c.contains("workspace") => StoreError::UnknownWorkspace,
                _ => StoreError::UnknownUser,
            },
            _ => StoreError::Db(e.to_string()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewChannel {
    pub id: i64,
    pub workspace_id: i64,
    /// `public`, `private`, `broadcast`, `dm` or `group_dm` (plan §7.1).
    pub kind: &'static str,
    pub name: Option<String>,
    pub topic: Option<String>,
    pub created_by: i64,
}

#[derive(Debug, PartialEq)]
pub enum Dm {
    Created,
    /// The pair already has a DM; its channel.
    Existing(i64),
}

pub trait Store: Send + Sync + 'static {
    /// Inserts the channel and its outbox `channel.created`. Every initial member must exist.
    fn create_channel(
        &self,
        channel: &NewChannel,
        members: &[i64],
        event: &InboundEvent,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Like `create_channel` for a 1:1 DM, unless the pair already has one.
    fn create_dm(
        &self,
        channel: &NewChannel,
        other: i64,
        event: &InboundEvent,
    ) -> impl Future<Output = Result<Dm, StoreError>> + Send;
}

pub struct Postgres {
    pool: PgPool,
}

impl Postgres {
    pub async fn connect(url: &str) -> Result<Self, StoreError> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(16)
            .connect(url)
            .await?;
        Ok(Postgres { pool })
    }
}

type Tx<'a> = sqlx::Transaction<'a, sqlx::Postgres>;

async fn insert_channel(
    tx: &mut Tx<'_>,
    c: &NewChannel,
    members: &[i64],
    event: &InboundEvent,
) -> Result<(), StoreError> {
    let known: i64 = sqlx::query_scalar("SELECT count(*) FROM users WHERE id = ANY($1)")
        .bind(members)
        .fetch_one(&mut **tx)
        .await?;
    let mut distinct = members.to_vec();
    distinct.sort_unstable();
    distinct.dedup();
    if known != distinct.len() as i64 {
        return Err(StoreError::UnknownUser);
    }
    sqlx::query(
        "INSERT INTO channels (id, workspace_id, type, name, topic, created_by) VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(c.id)
    .bind(c.workspace_id)
    .bind(c.kind)
    .bind(&c.name)
    .bind(&c.topic)
    .bind(c.created_by)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload) \
         VALUES ('channel', $1, 'channel.created', $2)",
    )
    .bind(c.id)
    .bind(event.encode_to_vec())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

impl Store for Postgres {
    async fn create_channel(
        &self,
        channel: &NewChannel,
        members: &[i64],
        event: &InboundEvent,
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        insert_channel(&mut tx, channel, members, event).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn create_dm(
        &self,
        channel: &NewChannel,
        other: i64,
        event: &InboundEvent,
    ) -> Result<Dm, StoreError> {
        let (low, high) = (channel.created_by.min(other), channel.created_by.max(other));
        let existing = || {
            sqlx::query_scalar::<_, i64>(
                "SELECT channel_id FROM dm_pairs WHERE user_low = $1 AND user_high = $2",
            )
            .bind(low)
            .bind(high)
            .fetch_optional(&self.pool)
        };
        if let Some(id) = existing().await? {
            return Ok(Dm::Existing(id));
        }
        let mut tx = self.pool.begin().await?;
        insert_channel(&mut tx, channel, &[channel.created_by, other], event).await?;
        let inserted = sqlx::query(
            "INSERT INTO dm_pairs (user_low, user_high, channel_id) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
        )
        .bind(low)
        .bind(high)
        .bind(channel.id)
        .execute(&mut *tx)
        .await?;
        if inserted.rows_affected() == 0 {
            // Both users opened the DM at the same moment: the other transaction won.
            tx.rollback().await?;
            return existing()
                .await?
                .map(Dm::Existing)
                .ok_or(StoreError::Db("DM pair vanished".into()));
        }
        tx.commit().await?;
        Ok(Dm::Created)
    }
}
