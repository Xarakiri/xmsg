// SPDX-License-Identifier: AGPL-3.0-only
//! HTTP API. Every request carries `Authorization: Bearer <access token>`; the caller
//! is the token's user. IDs are JSON strings. Errors are `{"error": "..."}`.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, patch, post};
use axum::{Json, Router};
use chrono::{Duration, SecondsFormat, Utc};
use serde::{Deserialize, Deserializer};
use serde_json::{Value, json};
use xmsg_authn::{Error as AuthError, Verifier};

use crate::store::{Accepted, Member, NewInvite, Profile, ProfileChange, Store, StoreError};
use crate::{code_hash, invite_code, new_id};

/// Most profiles one batch read returns.
pub const MAX_BATCH: usize = 200;
pub const MAX_MEMBERS_PAGE: i64 = 1_000;
const DEFAULT_INVITE_HOURS: i64 = 7 * 24;
const MAX_INVITE_HOURS: i64 = 30 * 24;

pub struct App<S> {
    pub store: S,
    pub verifier: Verifier,
    /// Fast path for workspace removals (NATS `auth.revocations`, plan §6.5), fed after
    /// commit with encoded `AuthEvent`s; `None` leaves them to the outbox.
    pub removals: Option<tokio::sync::mpsc::UnboundedSender<Vec<u8>>>,
    /// The service token for the provisioning API (`XMSG_PROVISIONING_TOKEN`); `None`
    /// turns it off.
    pub provisioning_token: Option<String>,
}

pub fn router<S: Store>(app: Arc<App<S>>) -> Router {
    Router::new()
        .route("/v1/me", get(me::<S>).patch(update_me::<S>))
        .route("/v1/users", get(users::<S>))
        .route("/v1/workspaces", post(create_workspace::<S>))
        .route("/v1/workspaces/{id}", patch(update_workspace::<S>))
        .route("/v1/me/workspaces", get(my_workspaces::<S>))
        .route("/v1/workspaces/{id}/members", get(members::<S>))
        .route("/v1/workspaces/{id}/invites", post(create_invite::<S>))
        .route(
            "/v1/workspaces/{id}/members/{user_id}",
            delete(remove_member::<S>).patch(set_member_role::<S>),
        )
        .route("/v1/invites/{code}/accept", post(accept_invite::<S>))
        .route("/healthz", get(|| async { "ok" }))
        .merge(crate::provisioning::routes::<S>())
        .with_state(app)
}

pub enum ApiError {
    BadRequest(&'static str),
    Unauthorized,
    Forbidden(&'static str),
    NotFound(&'static str),
    Conflict(&'static str),
    Unavailable,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            ApiError::Unauthorized => (StatusCode::UNAUTHORIZED, "invalid or missing access token"),
            ApiError::Forbidden(m) => (StatusCode::FORBIDDEN, m),
            ApiError::NotFound(m) => (StatusCode::NOT_FOUND, m),
            ApiError::Conflict(m) => (StatusCode::CONFLICT, m),
            ApiError::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "try again later"),
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}

impl From<StoreError> for ApiError {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::SlugTaken => ApiError::Conflict("slug already taken"),
            StoreError::Db(e) => {
                tracing::error!(error = %e, "store failed");
                ApiError::Unavailable
            }
        }
    }
}

async fn caller<S>(app: &App<S>, headers: &HeaderMap) -> Result<i64, ApiError> {
    let header = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or(ApiError::Unauthorized)?;
    match app.verifier.verify(header).await {
        Ok(principal) => Ok(principal.user_id),
        Err(AuthError::KeysUnavailable) => Err(ApiError::Unavailable),
        Err(_) => Err(ApiError::Unauthorized),
    }
}

pub(crate) fn parse_id(s: &str) -> Result<i64, ApiError> {
    s.trim()
        .parse::<i64>()
        .ok()
        .filter(|id| *id > 0)
        .ok_or(ApiError::BadRequest("invalid id"))
}

fn profile_json(p: &Profile) -> Value {
    json!({ "user_id": p.user_id.to_string(), "name": p.name, "avatar_url": p.avatar_url })
}

fn member_json(m: &Member) -> Value {
    json!({
        "user_id": m.user_id.to_string(),
        "name": m.name,
        "avatar_url": m.avatar_url,
        "role": m.role,
    })
}

fn valid_name(name: &str) -> Result<String, ApiError> {
    let name = name.trim();
    if name.is_empty() || name.chars().count() > 80 {
        return Err(ApiError::BadRequest("name must be 1 to 80 characters"));
    }
    Ok(name.to_string())
}

async fn me<S: Store>(
    State(app): State<Arc<App<S>>>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let me = caller(&app, &headers).await?;
    let (email, profile) = app
        .store
        .me(me)
        .await?
        .ok_or(ApiError::NotFound("unknown user"))?;
    let mut reply = profile_json(&profile);
    reply["email"] = json!(email);
    Ok(Json(reply))
}

/// Tells an absent field (leave it) from an explicit `null` (clear it).
fn present<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Option<String>>, D::Error> {
    Option::<String>::deserialize(d).map(Some)
}

#[derive(Deserialize)]
pub struct UpdateMe {
    name: Option<String>,
    #[serde(default, deserialize_with = "present")]
    avatar_url: Option<Option<String>>,
}

async fn update_me<S: Store>(
    State(app): State<Arc<App<S>>>,
    headers: HeaderMap,
    Json(req): Json<UpdateMe>,
) -> Result<Json<Value>, ApiError> {
    let me = caller(&app, &headers).await?;
    let name = req.name.as_deref().map(valid_name).transpose()?;
    let avatar_url = match req.avatar_url {
        Some(Some(url)) if url.trim().is_empty() => Some(None),
        Some(Some(url)) => {
            let url = url.trim();
            // Clients render it as an image source: plain web URLs only.
            if url.len() > 2048 || !(url.starts_with("https://") || url.starts_with("http://")) {
                return Err(ApiError::BadRequest(
                    "avatar_url must be an http(s) URL of at most 2048 characters",
                ));
            }
            Some(Some(url.to_string()))
        }
        other => other,
    };
    let change = ProfileChange { name, avatar_url };
    let profile = app
        .store
        .update_profile(me, &change)
        .await?
        .ok_or(ApiError::NotFound("unknown user"))?;
    Ok(Json(profile_json(&profile)))
}

#[derive(Deserialize)]
pub struct Ids {
    #[serde(default)]
    ids: String,
}

/// Batch profile read. Users who share no workspace with the caller are left out
/// rather than refused, so a client can't tell them from unknown IDs.
async fn users<S: Store>(
    State(app): State<Arc<App<S>>>,
    headers: HeaderMap,
    Query(query): Query<Ids>,
) -> Result<Json<Value>, ApiError> {
    let me = caller(&app, &headers).await?;
    let mut ids = query
        .ids
        .split(',')
        .filter(|s| !s.trim().is_empty())
        .map(parse_id)
        .collect::<Result<Vec<_>, _>>()?;
    ids.sort_unstable();
    ids.dedup();
    if ids.len() > MAX_BATCH {
        return Err(ApiError::BadRequest("at most 200 ids per request"));
    }
    let profiles = if ids.is_empty() {
        Vec::new()
    } else {
        app.store.visible_profiles(me, &ids).await?
    };
    let users: Vec<Value> = profiles.iter().map(profile_json).collect();
    Ok(Json(json!({ "users": users })))
}

#[derive(Deserialize)]
pub struct CreateWorkspace {
    name: String,
    slug: String,
}

/// Lower-case letters, digits and inner hyphens, 2 to 40 characters.
fn valid_slug(slug: &str) -> Result<String, ApiError> {
    let slug = slug.trim().to_lowercase();
    let ok = (2..=40).contains(&slug.len())
        && slug
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && !slug.starts_with('-')
        && !slug.ends_with('-');
    if ok {
        Ok(slug)
    } else {
        Err(ApiError::BadRequest(
            "slug must be 2 to 40 lower-case letters, digits or inner hyphens",
        ))
    }
}

async fn create_workspace<S: Store>(
    State(app): State<Arc<App<S>>>,
    headers: HeaderMap,
    Json(req): Json<CreateWorkspace>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let owner = caller(&app, &headers).await?;
    let name = valid_name(&req.name)?;
    let slug = valid_slug(&req.slug)?;
    let id = new_id();
    app.store.create_workspace(id, &name, &slug, owner).await?;
    tracing::info!(workspace_id = id, owner, "workspace created");
    Ok((
        StatusCode::CREATED,
        Json(
            json!({ "workspace_id": id.to_string(), "name": name, "slug": slug, "role": "owner" }),
        ),
    ))
}

async fn my_workspaces<S: Store>(
    State(app): State<Arc<App<S>>>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let me = caller(&app, &headers).await?;
    let list: Vec<Value> = app
        .store
        .workspaces_of(me)
        .await?
        .iter()
        .map(|w| {
            json!({
                "workspace_id": w.id.to_string(),
                "name": w.name,
                "slug": w.slug,
                "role": w.role,
                "setup_completed": w.setup_completed,
            })
        })
        .collect();
    Ok(Json(json!({ "workspaces": list })))
}

/// The caller's role, or 404 for non-members so outsiders can't probe which workspaces exist.
async fn require_member<S: Store>(
    app: &App<S>,
    workspace_id: i64,
    user_id: i64,
) -> Result<String, ApiError> {
    app.store
        .role(workspace_id, user_id)
        .await?
        .ok_or(ApiError::NotFound("unknown workspace"))
}

#[derive(Deserialize)]
pub struct UpdateWorkspace {
    name: Option<String>,
    setup_completed: Option<bool>,
    /// `invite_only` (default) or `open` (ADR-0026).
    signup: Option<String>,
}

/// First-run setup, renaming and the sign-up policy (ADR-0026): owners and admins.
/// `setup_completed` only ever turns true; completing it again is a no-op.
async fn update_workspace<S: Store>(
    State(app): State<Arc<App<S>>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(req): Json<UpdateWorkspace>,
) -> Result<Json<Value>, ApiError> {
    let me = caller(&app, &headers).await?;
    let workspace_id = parse_id(&id)?;
    let role = require_member(&app, workspace_id, me).await?;
    if role != "owner" && role != "admin" {
        return Err(ApiError::Forbidden(
            "only owners and admins change the workspace",
        ));
    }
    if req.setup_completed == Some(false) {
        return Err(ApiError::BadRequest(
            "setup_completed can only be set to true",
        ));
    }
    if let Some(signup) = &req.signup
        && signup != "invite_only"
        && signup != "open"
    {
        return Err(ApiError::BadRequest("signup must be invite_only or open"));
    }
    let name = req.name.as_deref().map(valid_name).transpose()?;
    let Some((name, setup_completed, signup)) = app
        .store
        .update_workspace(
            workspace_id,
            name.as_deref(),
            req.setup_completed.is_some(),
            req.signup.as_deref(),
        )
        .await?
    else {
        return Err(ApiError::NotFound("unknown workspace"));
    };
    tracing::info!(
        workspace_id,
        user_id = me,
        setup_completed,
        signup,
        "workspace updated"
    );
    Ok(Json(
        json!({ "workspace_id": workspace_id.to_string(), "name": name, "setup_completed": setup_completed, "signup": signup }),
    ))
}

#[derive(Deserialize)]
pub struct Page {
    after: Option<String>,
    limit: Option<i64>,
}

async fn members<S: Store>(
    State(app): State<Arc<App<S>>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(page): Query<Page>,
) -> Result<Json<Value>, ApiError> {
    let me = caller(&app, &headers).await?;
    let workspace_id = parse_id(&id)?;
    require_member(&app, workspace_id, me).await?;
    let after = page
        .after
        .as_deref()
        .map(parse_id)
        .transpose()?
        .unwrap_or(0);
    let limit = page.limit.unwrap_or(200).clamp(1, MAX_MEMBERS_PAGE);
    let members = app.store.members(workspace_id, after, limit).await?;
    // A full page may have more behind it; the client continues from the last ID.
    let next = (members.len() as i64 == limit)
        .then(|| members.last().map(|m| m.user_id.to_string()))
        .flatten();
    let list: Vec<Value> = members.iter().map(member_json).collect();
    Ok(Json(json!({ "members": list, "next": next })))
}

#[derive(Deserialize, Default)]
pub struct CreateInvite {
    ttl_hours: Option<i64>,
}

/// Removes `user_id` from the workspace (plan §6.5): owners remove anyone but the owner,
/// admins remove members, and anyone but the owner can leave. The removal commits with a
/// `workspace.member_removed` outbox event; fan-out stops delivering the workspace to
/// them at once, and a job takes them out of its channels.
async fn remove_member<S: Store>(
    State(app): State<Arc<App<S>>>,
    headers: HeaderMap,
    Path((id, user_id)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let me = caller(&app, &headers).await?;
    let (workspace_id, user_id) = (parse_id(&id)?, parse_id(&user_id)?);
    let mine = require_member(&app, workspace_id, me).await?;
    let theirs = app
        .store
        .role(workspace_id, user_id)
        .await?
        .ok_or(ApiError::NotFound("not a member of this workspace"))?;
    let allowed = match (mine.as_str(), theirs.as_str()) {
        // The owner can't leave (nor be removed): the workspace would have nobody in charge.
        (_, "owner") => false,
        _ if user_id == me => true,
        ("owner", _) => true,
        ("admin", "member") => true,
        _ => false,
    };
    if !allowed {
        return Err(ApiError::Forbidden("not allowed to remove this member"));
    }
    remove_from_workspace(&app, workspace_id, user_id, me).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
pub struct SetRole {
    role: String,
}

/// Changes a member's role: only the owner, between `admin` and `member` (ownership isn't
/// handed out here, and the owner's own role never changes).
async fn set_member_role<S: Store>(
    State(app): State<Arc<App<S>>>,
    headers: HeaderMap,
    Path((id, user_id)): Path<(String, String)>,
    Json(body): Json<SetRole>,
) -> Result<StatusCode, ApiError> {
    let me = caller(&app, &headers).await?;
    let (workspace_id, user_id) = (parse_id(&id)?, parse_id(&user_id)?);
    if require_member(&app, workspace_id, me).await? != "owner" {
        return Err(ApiError::Forbidden("only the owner changes roles"));
    }
    if body.role != "admin" && body.role != "member" {
        return Err(ApiError::BadRequest("role must be admin or member"));
    }
    if user_id == me {
        return Err(ApiError::Forbidden("the owner's role doesn't change"));
    }
    if !app
        .store
        .set_role(workspace_id, user_id, &body.role)
        .await?
    {
        return Err(ApiError::NotFound("not a member of this workspace"));
    }
    tracing::info!(
        workspace_id,
        user_id,
        role = body.role,
        by = me,
        "member role changed"
    );
    Ok(StatusCode::NO_CONTENT)
}

/// Removes a member (plan §6.5): the outbox event that drives the channel removal job and
/// fan-out's deny list, then the same removal on NATS as the fast path. `by` is 0 for the
/// system (provisioning). `false`: they weren't a member.
pub(crate) async fn remove_from_workspace<S: Store>(
    app: &App<S>,
    workspace_id: i64,
    user_id: i64,
    by: i64,
) -> Result<bool, ApiError> {
    use xmsg_proto::Message;
    use xmsg_proto::v1::{AuthEvent, UsersEvent, WorkspaceMemberRemoved, auth_event, users_event};

    let removed = WorkspaceMemberRemoved {
        workspace_id,
        user_id,
        removed_by: by,
        removed_at_ms: Utc::now().timestamp_millis(),
    };
    let event = UsersEvent {
        event: Some(users_event::Event::MemberRemoved(removed)),
    };
    let was_member = app
        .store
        .remove_member(workspace_id, user_id, &event.encode_to_vec())
        .await?;
    if was_member && let Some(tx) = &app.removals {
        let fast = AuthEvent {
            event: Some(auth_event::Event::WorkspaceMemberRemoved(removed)),
        };
        let _ = tx.send(fast.encode_to_vec());
    }
    tracing::info!(workspace_id, user_id, by, "workspace member removed");
    Ok(was_member)
}

async fn create_invite<S: Store>(
    State(app): State<Arc<App<S>>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    body: Option<Json<CreateInvite>>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let me = caller(&app, &headers).await?;
    let workspace_id = parse_id(&id)?;
    let role = require_member(&app, workspace_id, me).await?;
    if role != "owner" && role != "admin" {
        return Err(ApiError::Forbidden("only owners and admins invite"));
    }
    let Json(req) = body.unwrap_or_default();
    let hours = req.ttl_hours.unwrap_or(DEFAULT_INVITE_HOURS);
    if !(1..=MAX_INVITE_HOURS).contains(&hours) {
        return Err(ApiError::BadRequest("ttl_hours must be 1 to 720"));
    }
    let code = invite_code();
    let expires_at = Utc::now() + Duration::hours(hours);
    app.store
        .create_invite(&NewInvite {
            code_hash: code_hash(&code),
            workspace_id,
            created_by: me,
            expires_at,
        })
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(json!({
            "code": code,
            "workspace_id": workspace_id.to_string(),
            "expires_at": expires_at.to_rfc3339_opts(SecondsFormat::Secs, true),
        })),
    ))
}

async fn accept_invite<S: Store>(
    State(app): State<Arc<App<S>>>,
    headers: HeaderMap,
    Path(code): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let me = caller(&app, &headers).await?;
    let (workspace_id, status) = match app
        .store
        .accept_invite(&code_hash(code.trim()), me, Utc::now())
        .await?
    {
        Accepted::Joined { workspace_id } => (workspace_id, "joined"),
        Accepted::AlreadyMember { workspace_id } => (workspace_id, "already_member"),
        Accepted::Invalid => return Err(ApiError::NotFound("invalid or expired invite")),
    };
    if status == "joined" {
        tracing::info!(workspace_id, user_id = me, "joined workspace by invite");
    }
    Ok(Json(
        json!({ "workspace_id": workspace_id.to_string(), "status": status }),
    ))
}
