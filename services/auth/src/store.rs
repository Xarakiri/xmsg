// SPDX-License-Identifier: AGPL-3.0-only
//! Users, devices and refresh tokens (plan §7.1). Postgres in production; the trait
//! lets the HTTP layer be tested without a database.

use chrono::{DateTime, Duration, Utc};
use sqlx::PgPool;

use crate::token_hash;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("email already registered")]
    EmailTaken,
    #[error("sign-up is not allowed")]
    SignupForbidden,
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
    /// The token was rotated less than the reuse grace ago and the session is still live:
    /// most likely the client never got the reply (or two tabs raced), so a new token is
    /// issued and the family stays intact.
    Reissued { user_id: i64, device_id: i64 },
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
        // An invite code, when the workspace's sign-up policy demands one (ADR-0026).
        invite: Option<&str>,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Whether any user exists yet, and whether workspace 1's sign-up policy is
    /// `invite_only` (the default; `settings_json.signup` may say `open`).
    fn signup_policy(&self) -> impl Future<Output = Result<(bool, bool), StoreError>> + Send;

    /// Replaces any earlier code: one live reset code per user (ADR-0026).
    fn save_password_reset(
        &self,
        user_id: i64,
        code_hash: &str,
        expires_at: DateTime<Utc>,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Applies a reset code: checks it (in one transaction with the password change),
    /// consumes it and revokes every live session of the user, writing `revoked(user,
    /// device)` to the outbox per device. `Invalid` is the one answer for an unknown
    /// email, a wrong code or an expired code.
    fn reset_password(
        &self,
        email: &str,
        code_hash: &str,
        password_hash: &str,
        now: DateTime<Utc>,
        revoked: &Revoked<'_>,
    ) -> impl Future<Output = Result<PasswordReset, StoreError>> + Send;

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

    /// Replaces a refresh token with `new`. A token revoked by rotation (not by logout)
    /// within `reuse_grace` of `now` is `Reissued` rather than treated as theft.
    /// A reuse that revokes the session also writes `revoked(user_id, device_id)` to the
    /// outbox, in the same transaction.
    fn rotate(
        &self,
        old_hash: &str,
        new: NewRefresh,
        now: DateTime<Utc>,
        reuse_grace: Duration,
        revoked: &Revoked<'_>,
    ) -> impl Future<Output = Result<Rotation, StoreError>> + Send;

    /// Ends the token's session (logout): revokes every live token of its device and
    /// writes `revoked(user_id, device_id)` to the outbox, in one transaction. Unknown or
    /// already revoked tokens are fine and write nothing.
    fn revoke(
        &self,
        token_hash: &str,
        now: DateTime<Utc>,
        revoked: &Revoked<'_>,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Remembers a sign-in started with an external identity provider.
    fn save_sso_state(
        &self,
        state: &str,
        s: &SsoState,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Takes (and deletes) a sign-in started less than `max_age` ago: each state is used once.
    fn take_sso_state(
        &self,
        state: &str,
        max_age: Duration,
    ) -> impl Future<Output = Result<Option<SsoState>, StoreError>> + Send;

    /// The user behind an external identity: the one already linked to it, else the one
    /// with that (verified) email, which gets linked, else a new user without a password,
    /// in the default workspace like any sign-up. One transaction.
    fn external_user(
        &self,
        provider: &str,
        subject: &str,
        email: &str,
        name: &str,
        new_id: i64,
    ) -> impl Future<Output = Result<i64, StoreError>> + Send;

    /// Whether any workspace the user belongs to enforces single sign-on.
    fn sso_required(&self, user_id: i64) -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// The user's role in the workspace, if they're a member.
    fn role(
        &self,
        workspace_id: i64,
        user_id: i64,
    ) -> impl Future<Output = Result<Option<String>, StoreError>> + Send;

    fn sso_enforced(
        &self,
        workspace_id: i64,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;

    fn set_sso_enforced(
        &self,
        workspace_id: i64,
        enforced: bool,
        by: i64,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Whether the user has ever signed in through an identity provider.
    fn has_external_identity(
        &self,
        user_id: i64,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// Replaces the user's recovery codes for the workspace (hashes) with these.
    fn replace_recovery_codes(
        &self,
        workspace_id: i64,
        user_id: i64,
        code_hashes: &[String],
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Uses up one of the user's recovery codes: true if it was unused and its holder is
    /// still an owner or admin of the workspace it was made for.
    fn use_recovery_code(
        &self,
        user_id: i64,
        code_hash: &str,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;
}

#[derive(Debug, PartialEq)]
pub enum PasswordReset {
    /// The password changed; these device IDs had live sessions, now revoked.
    Applied { user_id: i64, devices: Vec<i64> },
    /// Unknown email, wrong code or expired code: indistinguishable on purpose.
    Invalid,
}

/// A sign-in in progress with an external identity provider (see `sso`).
#[derive(Debug, Clone, PartialEq)]
pub struct SsoState {
    pub provider: String,
    pub nonce: String,
    pub code_verifier: String,
    pub return_to: String,
}

/// Encodes the outbox event for a revoked session: `(user_id, device_id) -> payload`.
pub type Revoked<'a> = dyn Fn(i64, i64) -> Vec<u8> + Send + Sync + 'a;

/// user_id, device_id, expires_at, revoked_at, replaced_by
type TokenRow = (i64, i64, DateTime<Utc>, Option<DateTime<Utc>>, Option<i64>);

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

    /// The connection pool, for integration tests that read the database directly.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

impl Store for Postgres {
    async fn sso_required(&self, user_id: i64) -> Result<bool, StoreError> {
        let required: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM workspace_members m \
             JOIN workspace_sso s ON s.workspace_id = m.workspace_id \
             WHERE m.user_id = $1 AND s.enforced)",
        )
        .bind(user_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(required)
    }

    async fn role(&self, workspace_id: i64, user_id: i64) -> Result<Option<String>, StoreError> {
        Ok(sqlx::query_scalar(
            "SELECT role FROM workspace_members WHERE workspace_id = $1 AND user_id = $2",
        )
        .bind(workspace_id)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?)
    }

    async fn sso_enforced(&self, workspace_id: i64) -> Result<bool, StoreError> {
        let enforced: Option<bool> =
            sqlx::query_scalar("SELECT enforced FROM workspace_sso WHERE workspace_id = $1")
                .bind(workspace_id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(enforced.unwrap_or(false))
    }

    async fn set_sso_enforced(
        &self,
        workspace_id: i64,
        enforced: bool,
        by: i64,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO workspace_sso (workspace_id, enforced, updated_by) VALUES ($1, $2, $3) \
             ON CONFLICT (workspace_id) DO UPDATE \
             SET enforced = EXCLUDED.enforced, updated_by = EXCLUDED.updated_by, updated_at = now()",
        )
        .bind(workspace_id)
        .bind(enforced)
        .bind(by)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn has_external_identity(&self, user_id: i64) -> Result<bool, StoreError> {
        let linked: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM external_identities WHERE user_id = $1)",
        )
        .bind(user_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(linked)
    }

    async fn replace_recovery_codes(
        &self,
        workspace_id: i64,
        user_id: i64,
        code_hashes: &[String],
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM recovery_codes WHERE workspace_id = $1 AND user_id = $2")
            .bind(workspace_id)
            .bind(user_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "INSERT INTO recovery_codes (code_hash, user_id, workspace_id) \
             SELECT h, $2, $3 FROM unnest($1::text[]) AS h",
        )
        .bind(code_hashes)
        .bind(user_id)
        .bind(workspace_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn use_recovery_code(&self, user_id: i64, code_hash: &str) -> Result<bool, StoreError> {
        let used = sqlx::query(
            "UPDATE recovery_codes r SET used_at = now() \
             WHERE r.code_hash = $1 AND r.user_id = $2 AND r.used_at IS NULL \
             AND EXISTS (SELECT 1 FROM workspace_members m WHERE m.workspace_id = r.workspace_id \
                         AND m.user_id = r.user_id AND m.role IN ('owner', 'admin'))",
        )
        .bind(code_hash)
        .bind(user_id)
        .execute(&self.pool)
        .await?;
        Ok(used.rows_affected() == 1)
    }

    async fn save_sso_state(&self, state: &str, s: &SsoState) -> Result<(), StoreError> {
        // Old, abandoned sign-ins are cleared on the way.
        sqlx::query("DELETE FROM sso_states WHERE created_at < now() - interval '1 hour'")
            .execute(&self.pool)
            .await?;
        sqlx::query(
            "INSERT INTO sso_states (state, provider, nonce, code_verifier, return_to) VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(state)
        .bind(&s.provider)
        .bind(&s.nonce)
        .bind(&s.code_verifier)
        .bind(&s.return_to)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn take_sso_state(
        &self,
        state: &str,
        max_age: Duration,
    ) -> Result<Option<SsoState>, StoreError> {
        let row: Option<(String, String, String, String)> = sqlx::query_as(
            "DELETE FROM sso_states WHERE state = $1 AND created_at > now() - make_interval(secs => $2) \
             RETURNING provider, nonce, code_verifier, return_to",
        )
        .bind(state)
        .bind(max_age.num_seconds() as f64)
        .fetch_optional(&self.pool)
        .await?;
        Ok(
            row.map(|(provider, nonce, code_verifier, return_to)| SsoState {
                provider,
                nonce,
                code_verifier,
                return_to,
            }),
        )
    }

    async fn external_user(
        &self,
        provider: &str,
        subject: &str,
        email: &str,
        name: &str,
        new_id: i64,
    ) -> Result<i64, StoreError> {
        let mut tx = self.pool.begin().await?;
        let linked: Option<i64> = sqlx::query_scalar(
            "SELECT user_id FROM external_identities WHERE provider = $1 AND subject = $2",
        )
        .bind(provider)
        .bind(subject)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(user_id) = linked {
            tx.commit().await?;
            return Ok(user_id);
        }
        let existing: Option<i64> = sqlx::query_scalar("SELECT id FROM users WHERE email = $1")
            .bind(email)
            .fetch_optional(&mut *tx)
            .await?;
        let user_id = match existing {
            Some(id) => id,
            None => {
                sqlx::query(
                    "INSERT INTO users (id, email, password_hash, name) VALUES ($1, $2, NULL, $3)",
                )
                .bind(new_id)
                .bind(email)
                .bind(name)
                .execute(&mut *tx)
                .await?;
                // The first account owns the default workspace (ADR-0026); same lock
                // and rule as `create_user`.
                sqlx::query("SELECT pg_advisory_xact_lock(726026)")
                    .execute(&mut *tx)
                    .await?;
                let has_owner: bool = sqlx::query_scalar(
                    "SELECT EXISTS (SELECT 1 FROM workspace_members \
                     WHERE workspace_id = 1 AND role = 'owner')",
                )
                .fetch_one(&mut *tx)
                .await?;
                let role = if has_owner { "member" } else { "owner" };
                sqlx::query(
                    "INSERT INTO workspace_members (workspace_id, user_id, role) VALUES (1, $1, $2)",
                )
                .bind(new_id)
                .bind(role)
                .execute(&mut *tx)
                .await?;
                new_id
            }
        };
        sqlx::query(
            "INSERT INTO external_identities (provider, subject, user_id) VALUES ($1, $2, $3)",
        )
        .bind(provider)
        .bind(subject)
        .bind(user_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(user_id)
    }

    async fn create_user(
        &self,
        id: i64,
        email: &str,
        password_hash: &str,
        name: &str,
        invite: Option<&str>,
    ) -> Result<(), StoreError> {
        // Joining the default workspace commits with the user, so no account exists
        // outside every workspace (where nobody could see or message it). The first
        // account owns the workspace (ADR-0026): the transaction-scoped advisory lock
        // serializes concurrent first sign-ups, so exactly one of them sees no owner.
        // The same lock covers the sign-up policy: the first account always gets in,
        // later ones need an `open` policy or a valid invite code, checked here so an
        // invite can't race a racing sign-up. The code is verified, not consumed.
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(726026)")
            .execute(&mut *tx)
            .await?;
        let first: bool = sqlx::query_scalar("SELECT count(*) = 0 FROM users")
            .fetch_one(&mut *tx)
            .await?;
        if !first {
            // `fetch_optional`'s outer Option is row presence; the value itself is
            // NULL when the policy was never set, which then means `invite_only`.
            let policy: Option<Option<String>> =
                sqlx::query_scalar("SELECT settings_json->>'signup' FROM workspaces WHERE id = 1")
                    .fetch_optional(&mut *tx)
                    .await?;
            if policy.flatten().as_deref() != Some("open") {
                let invited = match invite {
                    None => false,
                    Some(code) => {
                        sqlx::query_scalar(
                            "SELECT EXISTS (SELECT 1 FROM workspace_invites \
                         WHERE code_hash = $1 AND expires_at > now())",
                        )
                        .bind(token_hash(code))
                        .fetch_one(&mut *tx)
                        .await?
                    }
                };
                if !invited {
                    return Err(StoreError::SignupForbidden);
                }
            }
        }
        let has_owner: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM workspace_members \
             WHERE workspace_id = 1 AND role = 'owner')",
        )
        .fetch_one(&mut *tx)
        .await?;
        sqlx::query("INSERT INTO users (id, email, password_hash, name) VALUES ($1, $2, $3, $4)")
            .bind(id)
            .bind(email)
            .bind(password_hash)
            .bind(name)
            .execute(&mut *tx)
            .await?;
        let role = if has_owner { "member" } else { "owner" };
        sqlx::query(
            "INSERT INTO workspace_members (workspace_id, user_id, role) VALUES (1, $1, $2)",
        )
        .bind(id)
        .bind(role)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn signup_policy(&self) -> Result<(bool, bool), StoreError> {
        let row: (bool, Option<String>) = sqlx::query_as(
            "SELECT (SELECT count(*) = 0 FROM users), \
             (SELECT settings_json->>'signup' FROM workspaces WHERE id = 1)",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok((row.0, row.1.as_deref() != Some("open")))
    }

    async fn save_password_reset(
        &self,
        user_id: i64,
        code_hash: &str,
        expires_at: DateTime<Utc>,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO password_reset_codes (user_id, code_hash, expires_at) VALUES ($1, $2, $3) \
             ON CONFLICT (user_id) DO UPDATE \
             SET code_hash = EXCLUDED.code_hash, expires_at = EXCLUDED.expires_at, created_at = now()",
        )
        .bind(user_id)
        .bind(code_hash)
        .bind(expires_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn reset_password(
        &self,
        email: &str,
        code_hash: &str,
        password_hash: &str,
        now: DateTime<Utc>,
        revoked: &Revoked<'_>,
    ) -> Result<PasswordReset, StoreError> {
        let mut tx = self.pool.begin().await?;
        let user: Option<i64> = sqlx::query_scalar("SELECT id FROM users WHERE email = $1")
            .bind(email)
            .fetch_optional(&mut *tx)
            .await?;
        let Some(user_id) = user else {
            return Ok(PasswordReset::Invalid);
        };
        let code_ok: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM password_reset_codes \
             WHERE user_id = $1 AND code_hash = $2 AND expires_at > $3)",
        )
        .bind(user_id)
        .bind(code_hash)
        .bind(now)
        .fetch_one(&mut *tx)
        .await?;
        if !code_ok {
            return Ok(PasswordReset::Invalid);
        }
        sqlx::query("UPDATE users SET password_hash = $2 WHERE id = $1")
            .bind(user_id)
            .bind(password_hash)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM password_reset_codes WHERE user_id = $1")
            .bind(user_id)
            .execute(&mut *tx)
            .await?;
        let devices: Vec<i64> = sqlx::query_scalar(
            "SELECT DISTINCT device_id FROM refresh_tokens \
             WHERE user_id = $1 AND revoked_at IS NULL",
        )
        .bind(user_id)
        .fetch_all(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE refresh_tokens SET revoked_at = $2 WHERE user_id = $1 AND revoked_at IS NULL",
        )
        .bind(user_id)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        for device_id in &devices {
            outbox_revocation(&mut tx, *device_id, revoked(user_id, *device_id)).await?;
        }
        tx.commit().await?;
        Ok(PasswordReset::Applied { user_id, devices })
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
        reuse_grace: Duration,
        revoked: &Revoked<'_>,
    ) -> Result<Rotation, StoreError> {
        let mut tx = self.pool.begin().await?;
        let row: Option<TokenRow> = sqlx::query_as(
            "SELECT user_id, device_id, expires_at, revoked_at, replaced_by FROM refresh_tokens \
             WHERE token_hash = $1 FOR UPDATE",
        )
        .bind(old_hash)
        .fetch_optional(&mut *tx)
        .await?;
        let rotation = match row {
            None => Rotation::Invalid,
            Some((user_id, device_id, expires_at, Some(revoked_at), replaced_by)) => {
                let in_grace =
                    replaced_by.is_some() && now < revoked_at + reuse_grace && expires_at > now;
                if in_grace && live_session(&mut tx, device_id, now).await? {
                    issue(&mut tx, user_id, device_id, &new, now).await?;
                    Rotation::Reissued { user_id, device_id }
                } else {
                    let ended = sqlx::query("UPDATE refresh_tokens SET revoked_at = $2 WHERE device_id = $1 AND revoked_at IS NULL")
                        .bind(device_id)
                        .bind(now)
                        .execute(&mut *tx)
                        .await?;
                    // Only when this ended a live session: a later replay changes nothing.
                    if ended.rows_affected() > 0 {
                        outbox_revocation(&mut tx, device_id, revoked(user_id, device_id)).await?;
                    }
                    Rotation::Reused { device_id }
                }
            }
            Some((_, _, expires_at, None, _)) if expires_at <= now => Rotation::Invalid,
            Some((user_id, device_id, _, None, _)) => {
                sqlx::query(
                    "UPDATE refresh_tokens SET revoked_at = $2, replaced_by = $3 WHERE token_hash = $1",
                )
                .bind(old_hash)
                .bind(now)
                .bind(new.id)
                .execute(&mut *tx)
                .await?;
                issue(&mut tx, user_id, device_id, &new, now).await?;
                Rotation::Rotated { user_id, device_id }
            }
        };
        tx.commit().await?;
        Ok(rotation)
    }

    async fn revoke(
        &self,
        token_hash: &str,
        now: DateTime<Utc>,
        revoked: &Revoked<'_>,
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        let session: Option<(i64, i64)> = sqlx::query_as(
            "UPDATE refresh_tokens SET revoked_at = $2 WHERE token_hash = $1 AND revoked_at IS NULL \
             RETURNING user_id, device_id",
        )
        .bind(token_hash)
        .bind(now)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some((user_id, device_id)) = session {
            // Logout ends the session (device), including tokens a grace reissue left live.
            sqlx::query("UPDATE refresh_tokens SET revoked_at = $2 WHERE device_id = $1 AND revoked_at IS NULL")
                .bind(device_id)
                .bind(now)
                .execute(&mut *tx)
                .await?;
            outbox_revocation(&mut tx, device_id, revoked(user_id, device_id)).await?;
        }
        tx.commit().await?;
        Ok(())
    }
}

async fn outbox_revocation(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    device_id: i64,
    payload: Vec<u8>,
) -> Result<(), StoreError> {
    sqlx::query(
        "INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload) VALUES ('session', $1, 'session.revoked', $2)",
    )
    .bind(device_id)
    .bind(payload)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Stores the next token of a session and marks the device as seen.
async fn issue(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    user_id: i64,
    device_id: i64,
    refresh: &NewRefresh,
    now: DateTime<Utc>,
) -> Result<(), StoreError> {
    insert_refresh(tx, user_id, device_id, refresh).await?;
    sqlx::query("UPDATE devices SET last_seen_at = $2 WHERE id = $1")
        .bind(device_id)
        .bind(now)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Whether the device still has a usable token, so a grace reissue can't revive a session
/// that was logged out or revoked for reuse. The row lock makes a concurrent logout wait.
async fn live_session(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    device_id: i64,
    now: DateTime<Utc>,
) -> Result<bool, StoreError> {
    let live: Option<(i64,)> = sqlx::query_as(
        "SELECT id FROM refresh_tokens WHERE device_id = $1 AND revoked_at IS NULL \
         AND expires_at > $2 LIMIT 1 FOR UPDATE",
    )
    .bind(device_id)
    .bind(now)
    .fetch_optional(&mut **tx)
    .await?;
    Ok(live.is_some())
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
