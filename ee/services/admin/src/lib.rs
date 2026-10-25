// SPDX-License-Identifier: LicenseRef-xmsg-EE
//! The admin console's API (ADR-0022): what the Community Edition's own APIs don't show.
//! Read-only, so it keeps working after the licence expires.
//!
//! | Method | Path | |
//! |---|---|---|
//! | GET | `/admin/api/v1/licence` | the licence: customer, dates, status, features, seats licensed and used |
//! | GET | `/admin/api/v1/workspaces/{ws}/usage` | members by role, recent joins, channels, sessions, SSO |
//!
//! Callers need an access token of a workspace owner or admin (the licence: of any
//! workspace; usage: of that one). Usage needs a licence with `admin`.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use xmsg_ee_license::{Action, Feature, Licence, Status};

/// One workspace's numbers.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Usage {
    pub members_by_role: BTreeMap<String, i64>,
    pub joined_last_30_days: i64,
    pub channels_by_type: BTreeMap<String, i64>,
    pub archived_channels: i64,
    /// Members' sessions with a live refresh token.
    pub active_sessions: i64,
    /// Members who have signed in through an identity provider or directory.
    pub sso_linked_members: i64,
    pub sso_enforced: bool,
}

pub trait Store: Send + Sync + 'static {
    fn role(
        &self,
        workspace_id: i64,
        user_id: i64,
    ) -> impl Future<Output = Result<Option<String>, String>> + Send;
    /// Whether the user is an owner or admin of any workspace.
    fn admin_anywhere(&self, user_id: i64) -> impl Future<Output = Result<bool, String>> + Send;
    /// People in at least one workspace: what the licence's seat count is compared with.
    fn seats_used(&self) -> impl Future<Output = Result<i64, String>> + Send;
    fn usage(&self, workspace_id: i64) -> impl Future<Output = Result<Usage, String>> + Send;
}

pub struct App<S> {
    pub store: S,
    pub verifier: xmsg_authn::Verifier,
    pub licence: Option<Licence>,
}

pub fn router<S: Store>(app: Arc<App<S>>) -> Router {
    Router::new()
        .route("/admin/api/v1/licence", get(licence::<S>))
        .route("/admin/api/v1/workspaces/{ws}/usage", get(usage::<S>))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(app)
}

pub enum ApiError {
    BadRequest(&'static str),
    Unauthorized,
    Forbidden(&'static str),
    Unavailable,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            ApiError::Unauthorized => (StatusCode::UNAUTHORIZED, "invalid or missing access token"),
            ApiError::Forbidden(m) => (StatusCode::FORBIDDEN, m),
            ApiError::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "try again later"),
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}

fn store_error(e: String) -> ApiError {
    tracing::error!(error = %e, "store failed");
    ApiError::Unavailable
}

async fn caller<S>(app: &App<S>, headers: &HeaderMap) -> Result<i64, ApiError> {
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or(ApiError::Unauthorized)?;
    match app.verifier.verify(token).await {
        Ok(principal) => Ok(principal.user_id),
        Err(xmsg_authn::Error::KeysUnavailable) => Err(ApiError::Unavailable),
        Err(_) => Err(ApiError::Unauthorized),
    }
}

fn status_name(status: Status) -> &'static str {
    match status {
        Status::Active => "active",
        Status::Grace => "grace",
        Status::Expired => "expired",
    }
}

/// The licence as the console shows it; `{"licensed": false}` without one.
pub fn licence_json(licence: Option<&Licence>, seats_used: i64, now: DateTime<Utc>) -> Value {
    let Some(l) = licence else {
        return json!({ "licensed": false, "seats_used": seats_used });
    };
    json!({
        "licensed": true,
        "licence_id": l.licence_id,
        "customer": l.customer,
        "issued_at": l.issued_at.to_rfc3339(),
        "expires_at": l.expires_at.to_rfc3339(),
        "status": status_name(l.status(now)),
        "features": l.features,
        "seats": l.users,
        "seats_used": seats_used,
    })
}

async fn licence<S: Store>(
    State(app): State<Arc<App<S>>>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let me = caller(&app, &headers).await?;
    if !app.store.admin_anywhere(me).await.map_err(store_error)? {
        return Err(ApiError::Forbidden("only workspace owners and admins"));
    }
    let seats_used = app.store.seats_used().await.map_err(store_error)?;
    Ok(Json(licence_json(
        app.licence.as_ref(),
        seats_used,
        Utc::now(),
    )))
}

async fn usage<S: Store>(
    State(app): State<Arc<App<S>>>,
    headers: HeaderMap,
    Path(ws): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let me = caller(&app, &headers).await?;
    let workspace_id: i64 = ws
        .parse()
        .map_err(|_| ApiError::BadRequest("invalid workspace"))?;
    match app
        .store
        .role(workspace_id, me)
        .await
        .map_err(store_error)?
        .as_deref()
    {
        Some("owner" | "admin") => {}
        _ => {
            return Err(ApiError::Forbidden(
                "only the workspace's owners and admins",
            ));
        }
    }
    // Reading analytics is "use": it keeps working after expiry (ADR-0022).
    let licensed = app
        .licence
        .as_ref()
        .is_some_and(|l| l.allowed(Feature::Admin, Action::Use, Utc::now()));
    if !licensed {
        return Err(ApiError::Forbidden(
            "usage analytics need a licence with admin",
        ));
    }
    let u = app.store.usage(workspace_id).await.map_err(store_error)?;
    Ok(Json(json!({
        "members_by_role": u.members_by_role,
        "members": u.members_by_role.values().sum::<i64>(),
        "joined_last_30_days": u.joined_last_30_days,
        "channels_by_type": u.channels_by_type,
        "archived_channels": u.archived_channels,
        "active_sessions": u.active_sessions,
        "sso_linked_members": u.sso_linked_members,
        "sso_enforced": u.sso_enforced,
    })))
}

pub struct Postgres {
    pool: sqlx::PgPool,
}

impl Postgres {
    pub async fn connect(url: &str) -> Result<Self, String> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(url)
            .await
            .map_err(|e| e.to_string())?;
        Ok(Postgres { pool })
    }
}

fn db(e: sqlx::Error) -> String {
    e.to_string()
}

impl Store for Postgres {
    async fn role(&self, workspace_id: i64, user_id: i64) -> Result<Option<String>, String> {
        sqlx::query_scalar(
            "SELECT role FROM workspace_members WHERE workspace_id = $1 AND user_id = $2",
        )
        .bind(workspace_id)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db)
    }

    async fn admin_anywhere(&self, user_id: i64) -> Result<bool, String> {
        sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM workspace_members \
             WHERE user_id = $1 AND role IN ('owner', 'admin'))",
        )
        .bind(user_id)
        .fetch_one(&self.pool)
        .await
        .map_err(db)
    }

    async fn seats_used(&self) -> Result<i64, String> {
        sqlx::query_scalar("SELECT count(DISTINCT user_id) FROM workspace_members")
            .fetch_one(&self.pool)
            .await
            .map_err(db)
    }

    async fn usage(&self, workspace_id: i64) -> Result<Usage, String> {
        let roles: Vec<(String, i64)> = sqlx::query_as(
            "SELECT role, count(*) FROM workspace_members WHERE workspace_id = $1 GROUP BY role",
        )
        .bind(workspace_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;
        let channels: Vec<(String, i64)> = sqlx::query_as(
            "SELECT type, count(*) FROM channels \
             WHERE workspace_id = $1 AND NOT is_archived GROUP BY type",
        )
        .bind(workspace_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;
        let (joined, archived, sessions, linked, enforced): (i64, i64, i64, i64, bool) =
            sqlx::query_as(
                "SELECT \
                   (SELECT count(*) FROM workspace_members \
                     WHERE workspace_id = $1 AND joined_at > now() - interval '30 days'), \
                   (SELECT count(*) FROM channels WHERE workspace_id = $1 AND is_archived), \
                   (SELECT count(DISTINCT t.device_id) FROM refresh_tokens t \
                     JOIN workspace_members m ON m.user_id = t.user_id AND m.workspace_id = $1 \
                     WHERE t.revoked_at IS NULL AND t.expires_at > now()), \
                   (SELECT count(DISTINCT e.user_id) FROM external_identities e \
                     JOIN workspace_members m ON m.user_id = e.user_id AND m.workspace_id = $1), \
                   COALESCE((SELECT enforced FROM workspace_sso WHERE workspace_id = $1), false)",
            )
            .bind(workspace_id)
            .fetch_one(&self.pool)
            .await
            .map_err(db)?;
        Ok(Usage {
            members_by_role: roles.into_iter().collect(),
            joined_last_30_days: joined,
            channels_by_type: channels.into_iter().collect(),
            archived_channels: archived,
            active_sessions: sessions,
            sso_linked_members: linked,
            sso_enforced: enforced,
        })
    }
}
