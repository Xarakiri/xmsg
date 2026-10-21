// SPDX-License-Identifier: AGPL-3.0-only
//! Users, devices and refresh tokens (plan §7.1). Postgres in production; the trait
//! lets the HTTP layer be tested without a database.

use chrono::{DateTime, Utc};
use sqlx::PgPool;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("email already registered")]
    EmailTaken,
    #[error("database: {0}")]
    Db(String),
}

impl From<sqlx::Error> for StoreError {
    fn from(e: sqlx::Error) -> Self {
        match &e {
            sqlx::Error::Database(db) if db.is_unique_violation() => StoreError::EmailTaken,
            _ => StoreError::Db(e.to_string()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Credentials {
    pub user_id: i64,
    pub password_hash: Option<String>,
}

/// A new refresh token to store (only its hash is kept).
#[derive(Debug, Clone)]
pub struct NewRefresh {
    pub id: i64,
    pub token_hash: String,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, PartialEq)]
pub enum Rotation {
    /// The old token was valid; it's revoked and the new one stored.
    Rotated { user_id: i64, device_id: i64 },
    /// No such token, or it has expired.
    Invalid,
    /// The token was already revoked: someone replayed it. Every token of that device
    /// is revoked, so a stolen refresh token dies at the latest when the owner refreshes.
    Reused { device_id: i64 },
}

pub trait Store: Send + Sync + 'static {
    fn create_user(
        &self,
        id: i64,
        email: &str,
        password_hash: &str,
        name: &str,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    fn credentials(
        &self,
        email: &str,
    ) -> impl Future<Output = Result<Option<Credentials>, StoreError>> + Send;

    /// Registers a device (a login) and its first refresh token.
    fn start_session(
        &self,
        user_id: i64,
        device_id: i64,
        platform: &str,
        refresh: NewRefresh,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    fn rotate(
        &self,
        old_hash: &str,
        new: NewRefresh,
        now: DateTime<Utc>,
    ) -> impl Future<Output = Result<Rotation, StoreError>> + Send;

    /// Revokes a refresh token; unknown or already revoked tokens are fine.
    fn revoke(
        &self,
        token_hash: &str,
        now: DateTime<Utc>,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
}

/// user_id, device_id, expires_at, revoked_at
type TokenRow = (i64, i64, DateTime<Utc>, Option<DateTime<Utc>>);

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

impl Store for Postgres {
    async fn create_user(
        &self,
        id: i64,
        email: &str,
        password_hash: &str,
        name: &str,
    ) -> Result<(), StoreError> {
        sqlx::query("INSERT INTO users (id, email, password_hash, name) VALUES ($1, $2, $3, $4)")
            .bind(id)
            .bind(email)
            .bind(password_hash)
            .bind(name)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn credentials(&self, email: &str) -> Result<Option<Credentials>, StoreError> {
        let row: Option<(i64, Option<String>)> =
            sqlx::query_as("SELECT id, password_hash FROM users WHERE email = $1")
                .bind(email)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|(user_id, password_hash)| Credentials {
            user_id,
            password_hash,
        }))
    }

    async fn start_session(
        &self,
        user_id: i64,
        device_id: i64,
        platform: &str,
        refresh: NewRefresh,
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO devices (id, user_id, platform, last_seen_at) VALUES ($1, $2, $3, now())",
        )
        .bind(device_id)
        .bind(user_id)
        .bind(platform)
        .execute(&mut *tx)
        .await?;
        insert_refresh(&mut tx, user_id, device_id, &refresh).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn rotate(
        &self,
        old_hash: &str,
        new: NewRefresh,
        now: DateTime<Utc>,
    ) -> Result<Rotation, StoreError> {
        let mut tx = self.pool.begin().await?;
        let row: Option<TokenRow> = sqlx::query_as(
            "SELECT user_id, device_id, expires_at, revoked_at FROM refresh_tokens \
             WHERE token_hash = $1 FOR UPDATE",
        )
        .bind(old_hash)
        .fetch_optional(&mut *tx)
        .await?;
        let rotation = match row {
            None => Rotation::Invalid,
            Some((_, device_id, _, Some(_))) => {
                sqlx::query("UPDATE refresh_tokens SET revoked_at = $2 WHERE device_id = $1 AND revoked_at IS NULL")
                    .bind(device_id)
                    .bind(now)
                    .execute(&mut *tx)
                    .await?;
                Rotation::Reused { device_id }
            }
            Some((_, _, expires_at, None)) if expires_at <= now => Rotation::Invalid,
            Some((user_id, device_id, _, None)) => {
                sqlx::query("UPDATE refresh_tokens SET revoked_at = $2 WHERE token_hash = $1")
                    .bind(old_hash)
                    .bind(now)
                    .execute(&mut *tx)
                    .await?;
                insert_refresh(&mut tx, user_id, device_id, &new).await?;
                sqlx::query("UPDATE devices SET last_seen_at = $2 WHERE id = $1")
                    .bind(device_id)
                    .bind(now)
                    .execute(&mut *tx)
                    .await?;
                Rotation::Rotated { user_id, device_id }
            }
        };
        tx.commit().await?;
        Ok(rotation)
    }

    async fn revoke(&self, token_hash: &str, now: DateTime<Utc>) -> Result<(), StoreError> {
        sqlx::query("UPDATE refresh_tokens SET revoked_at = $2 WHERE token_hash = $1 AND revoked_at IS NULL")
            .bind(token_hash)
            .bind(now)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

async fn insert_refresh(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    user_id: i64,
    device_id: i64,
    refresh: &NewRefresh,
) -> Result<(), StoreError> {
    sqlx::query(
        "INSERT INTO refresh_tokens (id, user_id, device_id, token_hash, expires_at) VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(refresh.id)
    .bind(user_id)
    .bind(device_id)
    .bind(&refresh.token_hash)
    .bind(refresh.expires_at)
    .execute(&mut **tx)
    .await?;
    Ok(())
}
