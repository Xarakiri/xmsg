// SPDX-License-Identifier: AGPL-3.0-only
//! Profiles, workspaces, roles and invites in Postgres (plan §7.1). The trait lets the
//! HTTP layer be tested without a database.

use chrono::{DateTime, Utc};
use sqlx::PgPool;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum StoreError {
    #[error("slug already taken")]
    SlugTaken,
    #[error("database: {0}")]
    Db(String),
}

impl From<sqlx::Error> for StoreError {
    fn from(e: sqlx::Error) -> Self {
        match &e {
            sqlx::Error::Database(db) if db.is_unique_violation() => StoreError::SlugTaken,
            _ => StoreError::Db(e.to_string()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct Profile {
    pub user_id: i64,
    pub name: String,
    pub avatar_url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct Workspace {
    pub id: i64,
    pub name: String,
    pub slug: String,
    /// The caller's role in it: `owner`, `admin` or `member`.
    pub role: String,
}

#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct Member {
    pub user_id: i64,
    pub name: String,
    pub avatar_url: Option<String>,
    pub role: String,
}

/// A user as provisioning sees them (an identity provider's SCIM, ADR-0022).
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct Provisioned {
    pub user_id: i64,
    pub email: String,
    pub name: String,
    /// Whether they're in the workspace now (`false`: deactivated, or never added).
    pub member: bool,
}

/// A profile change; `None` leaves the field as it is.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ProfileChange {
    pub name: Option<String>,
    /// `Some(None)` clears the avatar.
    pub avatar_url: Option<Option<String>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewInvite {
    pub code_hash: String,
    pub workspace_id: i64,
    pub created_by: i64,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, PartialEq)]
pub enum Accepted {
    Joined {
        workspace_id: i64,
    },
    AlreadyMember {
        workspace_id: i64,
    },
    /// No such code, or it has expired.
    Invalid,
}

pub trait Store: Send + Sync + 'static {
    /// The user's email and profile (absent: the user was deleted).
    fn me(
        &self,
        user_id: i64,
    ) -> impl Future<Output = Result<Option<(String, Profile)>, StoreError>> + Send;

    fn update_profile(
        &self,
        user_id: i64,
        change: &ProfileChange,
    ) -> impl Future<Output = Result<Option<Profile>, StoreError>> + Send;

    /// Profiles of `ids` that share at least one workspace with `viewer` (and the
    /// viewer's own); everyone else is left out.
    fn visible_profiles(
        &self,
        viewer: i64,
        ids: &[i64],
    ) -> impl Future<Output = Result<Vec<Profile>, StoreError>> + Send;

    /// Creates the workspace with `owner` as its first member.
    fn create_workspace(
        &self,
        id: i64,
        name: &str,
        slug: &str,
        owner: i64,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    fn workspaces_of(
        &self,
        user_id: i64,
    ) -> impl Future<Output = Result<Vec<Workspace>, StoreError>> + Send;

    /// The user's role in the workspace, if they belong to it.
    fn role(
        &self,
        workspace_id: i64,
        user_id: i64,
    ) -> impl Future<Output = Result<Option<String>, StoreError>> + Send;

    /// Members by ascending user ID, starting after `after`.
    fn members(
        &self,
        workspace_id: i64,
        after: i64,
        limit: i64,
    ) -> impl Future<Output = Result<Vec<Member>, StoreError>> + Send;

    fn create_invite(
        &self,
        invite: &NewInvite,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Removes the user from the workspace and writes `event` (an encoded
    /// `UsersEvent{member_removed}`) to the outbox, in one transaction. `false`: they
    /// weren't a member (nothing written).
    fn remove_member(
        &self,
        workspace_id: i64,
        user_id: i64,
        event: &[u8],
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// Provisioning: the user with `email`, created (without a password: they sign in
    /// through the identity provider) if there's none, and made a `member` of the
    /// workspace if they aren't one. `true` if the user was created.
    fn provision_user(
        &self,
        _workspace_id: i64,
        _email: &str,
        _name: &str,
        _new_id: i64,
    ) -> impl Future<Output = Result<(i64, bool), StoreError>> + Send {
        async {
            Err(StoreError::Db(
                "provisioning isn't supported by this store".into(),
            ))
        }
    }

    /// Provisioning: a user by email, and whether they're in the workspace.
    fn provisioned_by_email(
        &self,
        _workspace_id: i64,
        _email: &str,
    ) -> impl Future<Output = Result<Option<Provisioned>, StoreError>> + Send {
        async {
            Err(StoreError::Db(
                "provisioning isn't supported by this store".into(),
            ))
        }
    }

    /// Provisioning: one user by ID, and whether they're in the workspace.
    fn provisioned(
        &self,
        _workspace_id: i64,
        _user_id: i64,
    ) -> impl Future<Output = Result<Option<Provisioned>, StoreError>> + Send {
        async {
            Err(StoreError::Db(
                "provisioning isn't supported by this store".into(),
            ))
        }
    }

    /// Provisioning: the workspace's members after `after`, by ID.
    fn provisioned_members(
        &self,
        _workspace_id: i64,
        _after: i64,
        _limit: i64,
    ) -> impl Future<Output = Result<Vec<Provisioned>, StoreError>> + Send {
        async {
            Err(StoreError::Db(
                "provisioning isn't supported by this store".into(),
            ))
        }
    }

    /// Adds the user to the invite's workspace as a `member`.
    fn accept_invite(
        &self,
        code_hash: &str,
        user_id: i64,
        now: DateTime<Utc>,
    ) -> impl Future<Output = Result<Accepted, StoreError>> + Send;
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

impl Store for Postgres {
    async fn provision_user(
        &self,
        workspace_id: i64,
        email: &str,
        name: &str,
        new_id: i64,
    ) -> Result<(i64, bool), StoreError> {
        let mut tx = self.pool.begin().await?;
        let existing: Option<i64> = sqlx::query_scalar("SELECT id FROM users WHERE email = $1")
            .bind(email)
            .fetch_optional(&mut *tx)
            .await?;
        let (user_id, created) = match existing {
            Some(id) => (id, false),
            None => {
                sqlx::query(
                    "INSERT INTO users (id, email, password_hash, name) VALUES ($1, $2, NULL, $3)",
                )
                .bind(new_id)
                .bind(email)
                .bind(name)
                .execute(&mut *tx)
                .await?;
                (new_id, true)
            }
        };
        sqlx::query(
            "INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, 'member') \
             ON CONFLICT DO NOTHING",
        )
        .bind(workspace_id)
        .bind(user_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok((user_id, created))
    }

    async fn provisioned_by_email(
        &self,
        workspace_id: i64,
        email: &str,
    ) -> Result<Option<Provisioned>, StoreError> {
        Ok(sqlx::query_as(
            "SELECT u.id AS user_id, u.email, u.name, (m.user_id IS NOT NULL) AS member FROM users u \
             LEFT JOIN workspace_members m ON m.user_id = u.id AND m.workspace_id = $1 WHERE u.email = $2",
        )
        .bind(workspace_id)
        .bind(email)
        .fetch_optional(&self.pool)
        .await?)
    }

    async fn provisioned(
        &self,
        workspace_id: i64,
        user_id: i64,
    ) -> Result<Option<Provisioned>, StoreError> {
        Ok(sqlx::query_as(
            "SELECT u.id AS user_id, u.email, u.name, (m.user_id IS NOT NULL) AS member FROM users u \
             LEFT JOIN workspace_members m ON m.user_id = u.id AND m.workspace_id = $1 WHERE u.id = $2",
        )
        .bind(workspace_id)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?)
    }

    async fn provisioned_members(
        &self,
        workspace_id: i64,
        after: i64,
        limit: i64,
    ) -> Result<Vec<Provisioned>, StoreError> {
        Ok(sqlx::query_as(
            "SELECT u.id AS user_id, u.email, u.name, true AS member FROM workspace_members m \
             JOIN users u ON u.id = m.user_id WHERE m.workspace_id = $1 AND u.id > $2 ORDER BY u.id LIMIT $3",
        )
        .bind(workspace_id)
        .bind(after)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?)
    }

    async fn me(&self, user_id: i64) -> Result<Option<(String, Profile)>, StoreError> {
        let row: Option<(String, String, Option<String>)> =
            sqlx::query_as("SELECT email, name, avatar_url FROM users WHERE id = $1")
                .bind(user_id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|(email, name, avatar_url)| {
            (
                email,
                Profile {
                    user_id,
                    name,
                    avatar_url,
                },
            )
        }))
    }

    async fn update_profile(
        &self,
        user_id: i64,
        change: &ProfileChange,
    ) -> Result<Option<Profile>, StoreError> {
        Ok(sqlx::query_as(
            "UPDATE users SET name = COALESCE($2, name), \
             avatar_url = CASE WHEN $3 THEN $4 ELSE avatar_url END \
             WHERE id = $1 RETURNING id AS user_id, name, avatar_url",
        )
        .bind(user_id)
        .bind(&change.name)
        .bind(change.avatar_url.is_some())
        .bind(change.avatar_url.clone().flatten())
        .fetch_optional(&self.pool)
        .await?)
    }

    async fn visible_profiles(&self, viewer: i64, ids: &[i64]) -> Result<Vec<Profile>, StoreError> {
        Ok(sqlx::query_as(
            "SELECT u.id AS user_id, u.name, u.avatar_url FROM users u \
             WHERE u.id = ANY($2) AND (u.id = $1 OR EXISTS ( \
                SELECT 1 FROM workspace_members theirs \
                JOIN workspace_members mine \
                  ON mine.workspace_id = theirs.workspace_id AND mine.user_id = $1 \
                WHERE theirs.user_id = u.id))",
        )
        .bind(viewer)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?)
    }

    async fn create_workspace(
        &self,
        id: i64,
        name: &str,
        slug: &str,
        owner: i64,
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("INSERT INTO workspaces (id, name, slug) VALUES ($1, $2, $3)")
            .bind(id)
            .bind(name)
            .bind(slug)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, 'owner')",
        )
        .bind(id)
        .bind(owner)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn workspaces_of(&self, user_id: i64) -> Result<Vec<Workspace>, StoreError> {
        Ok(sqlx::query_as(
            "SELECT w.id, w.name, w.slug, m.role FROM workspace_members m \
             JOIN workspaces w ON w.id = m.workspace_id \
             WHERE m.user_id = $1 ORDER BY w.id",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?)
    }

    async fn remove_member(
        &self,
        workspace_id: i64,
        user_id: i64,
        event: &[u8],
    ) -> Result<bool, StoreError> {
        let mut tx = self.pool.begin().await?;
        let removed =
            sqlx::query("DELETE FROM workspace_members WHERE workspace_id = $1 AND user_id = $2")
                .bind(workspace_id)
                .bind(user_id)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                > 0;
        if removed {
            sqlx::query(
                "INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload) \
                 VALUES ('workspace', $1, 'workspace.member_removed', $2)",
            )
            .bind(workspace_id)
            .bind(event)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(removed)
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

    async fn members(
        &self,
        workspace_id: i64,
        after: i64,
        limit: i64,
    ) -> Result<Vec<Member>, StoreError> {
        Ok(sqlx::query_as(
            "SELECT u.id AS user_id, u.name, u.avatar_url, m.role FROM workspace_members m \
             JOIN users u ON u.id = m.user_id \
             WHERE m.workspace_id = $1 AND m.user_id > $2 ORDER BY m.user_id LIMIT $3",
        )
        .bind(workspace_id)
        .bind(after)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?)
    }

    async fn create_invite(&self, invite: &NewInvite) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO workspace_invites (code_hash, workspace_id, created_by, expires_at) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(&invite.code_hash)
        .bind(invite.workspace_id)
        .bind(invite.created_by)
        .bind(invite.expires_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn accept_invite(
        &self,
        code_hash: &str,
        user_id: i64,
        now: DateTime<Utc>,
    ) -> Result<Accepted, StoreError> {
        let workspace_id: Option<i64> = sqlx::query_scalar(
            "SELECT workspace_id FROM workspace_invites WHERE code_hash = $1 AND expires_at > $2",
        )
        .bind(code_hash)
        .bind(now)
        .fetch_optional(&self.pool)
        .await?;
        let Some(workspace_id) = workspace_id else {
            return Ok(Accepted::Invalid);
        };
        let inserted = sqlx::query(
            "INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, 'member') \
             ON CONFLICT DO NOTHING",
        )
        .bind(workspace_id)
        .bind(user_id)
        .execute(&self.pool)
        .await?;
        Ok(if inserted.rows_affected() == 1 {
            Accepted::Joined { workspace_id }
        } else {
            Accepted::AlreadyMember { workspace_id }
        })
    }
}
