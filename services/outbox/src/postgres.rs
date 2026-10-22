// SPDX-License-Identifier: AGPL-3.0-only
//! The poller's side of Postgres. Everything runs on one connection that holds a session
//! advisory lock, so exactly one poller publishes at a time: standbys wait for the lock,
//! and if the active one dies its connection closes and the lock passes on. One publisher
//! keeps each aggregate's events in commit (`id`) order.

use sqlx::{Connection, PgConnection};

use crate::OutboxRow;

/// "xmsgoutb": the advisory lock key every poller of an installation agrees on.
pub const LEADER_LOCK: i64 = 0x786d_7367_6f75_7462;

pub struct Leader {
    conn: PgConnection,
}

impl Leader {
    /// Connects and takes the lock if it is free; `None` means another poller is active.
    pub async fn try_acquire(url: &str) -> Result<Option<Leader>, sqlx::Error> {
        let mut conn = PgConnection::connect(url).await?;
        let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(LEADER_LOCK)
            .fetch_one(&mut conn)
            .await?;
        Ok(locked.then_some(Leader { conn }))
    }

    /// The oldest unpublished rows of known aggregate types, in commit order.
    pub async fn unpublished(
        &mut self,
        known: &[String],
        limit: i64,
    ) -> Result<Vec<OutboxRow>, sqlx::Error> {
        let rows: Vec<(i64, String, i64, String, Vec<u8>)> = sqlx::query_as(
            "SELECT id, aggregate_type, aggregate_id, event_type, payload FROM outbox \
             WHERE published_at IS NULL AND aggregate_type = ANY($1) ORDER BY id LIMIT $2",
        )
        .bind(known)
        .bind(limit)
        .fetch_all(&mut self.conn)
        .await?;
        Ok(rows
            .into_iter()
            .map(
                |(id, aggregate_type, aggregate_id, event_type, payload)| OutboxRow {
                    id,
                    aggregate_type,
                    aggregate_id,
                    event_type,
                    payload,
                },
            )
            .collect())
    }

    pub async fn mark_published(&mut self, ids: &[i64]) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE outbox SET published_at = now() WHERE id = ANY($1)")
            .bind(ids)
            .execute(&mut self.conn)
            .await?;
        Ok(())
    }

    /// Rows are kept for `days` after publishing (replays after a failover), then deleted.
    pub async fn delete_published_before(&mut self, days: i32) -> Result<u64, sqlx::Error> {
        let done = sqlx::query(
            "DELETE FROM outbox WHERE published_at < now() - make_interval(days => $1)",
        )
        .bind(days)
        .execute(&mut self.conn)
        .await?;
        Ok(done.rows_affected())
    }

    /// Debezium relays rows without marking them: they are deleted `days` after being written.
    pub async fn delete_written_before(&mut self, days: i32) -> Result<u64, sqlx::Error> {
        let done =
            sqlx::query("DELETE FROM outbox WHERE created_at < now() - make_interval(days => $1)")
                .bind(days)
                .execute(&mut self.conn)
                .await?;
        Ok(done.rows_affected())
    }

    /// Seconds since the oldest unpublished row was written (0 when caught up): the lag.
    pub async fn lag_secs(&mut self) -> Result<f64, sqlx::Error> {
        sqlx::query_scalar(
            "SELECT COALESCE(EXTRACT(EPOCH FROM now() - min(created_at))::float8, 0) FROM outbox WHERE published_at IS NULL",
        )
        .fetch_one(&mut self.conn)
        .await
    }
}
