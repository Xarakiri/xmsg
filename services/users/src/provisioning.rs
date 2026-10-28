// SPDX-License-Identifier: AGPL-3.0-only
//! The provisioning API (ADR-0022): what an identity provider's provisioning (SCIM, an
//! Enterprise Edition service) calls to create, find, rename and deactivate users. Core,
//! so provisioning goes through the same paths as everything else: a deactivation is the
//! ordinary workspace removal (outbox event, channel removal job, fan-out deny list).
//!
//! Service-to-service only: every request needs `Authorization: Bearer
//! <XMSG_PROVISIONING_TOKEN>`; without that variable the API doesn't exist (404).
//!
//! | Method | Path | |
//! |---|---|---|
//! | POST | `/internal/v1/workspaces/{ws}/users` `{email, name}` | create or (re)activate: 201 created, 200 existed |
//! | GET | `/internal/v1/workspaces/{ws}/users?email=` | one user (404 unknown) |
//! | GET | `/internal/v1/workspaces/{ws}/users?after=&limit=` | the workspace's members |
//! | GET | `/internal/v1/workspaces/{ws}/users/{id}` | one user |
//! | PATCH | `/internal/v1/workspaces/{ws}/users/{id}` `{name}` | rename (members only) |
//! | DELETE | `/internal/v1/workspaces/{ws}/users/{id}` | deactivate: removed from the workspace |
//! | GET | `/internal/v1/workspaces/{ws}/storage-quota` | `{bytes}`: the override, `null` for the default |
//! | PUT | `/internal/v1/workspaces/{ws}/storage-quota` `{bytes}` | set it (bytes, 0 = unlimited) or `null` to use the default (ADR-0024) |

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::routing::{get, put};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::api::{ApiError, App, parse_id, remove_from_workspace};
use crate::new_id;
use crate::store::{ProfileChange, Provisioned, Store};

pub fn routes<S: Store>() -> Router<Arc<App<S>>> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{ws}/users",
            get(list::<S>).post(provision::<S>),
        )
        .route(
            "/internal/v1/workspaces/{ws}/users/{id}",
            get(one::<S>).patch(rename::<S>).delete(deactivate::<S>),
        )
        .route(
            "/internal/v1/workspaces/{ws}/storage-quota",
            put(set_quota::<S>).get(quota::<S>),
        )
}

#[derive(Deserialize)]
pub struct Quota {
    bytes: Option<i64>,
}

/// The instance operator's override of a workspace's storage quota (ADR-0024).
async fn quota<S: Store>(
    State(app): State<Arc<App<S>>>,
    headers: HeaderMap,
    Path(ws): Path<String>,
) -> Result<Json<Value>, ApiError> {
    authorize(&app, &headers)?;
    match app.store.storage_quota(parse_id(&ws)?).await? {
        Some(bytes) => Ok(Json(json!({ "bytes": bytes }))),
        None => Err(ApiError::NotFound("unknown workspace")),
    }
}

async fn set_quota<S: Store>(
    State(app): State<Arc<App<S>>>,
    headers: HeaderMap,
    Path(ws): Path<String>,
    Json(body): Json<Quota>,
) -> Result<Json<Value>, ApiError> {
    authorize(&app, &headers)?;
    let workspace_id = parse_id(&ws)?;
    if body.bytes.is_some_and(|b| b < 0) {
        return Err(ApiError::BadRequest(
            "bytes must be 0 (unlimited) or more, or null",
        ));
    }
    if !app
        .store
        .set_storage_quota(workspace_id, body.bytes)
        .await?
    {
        return Err(ApiError::NotFound("unknown workspace"));
    }
    tracing::info!(workspace_id, bytes = ?body.bytes, "storage quota set");
    Ok(Json(json!({ "bytes": body.bytes })))
}

/// Equal without leaking where they differ through timing.
fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn authorize<S>(app: &App<S>, headers: &HeaderMap) -> Result<(), ApiError> {
    let Some(token) = &app.provisioning_token else {
        return Err(ApiError::NotFound("not found"));
    };
    let given = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    if same(given.as_bytes(), token.as_bytes()) {
        Ok(())
    } else {
        Err(ApiError::Unauthorized)
    }
}

fn json_user(p: &Provisioned) -> Value {
    json!({ "user_id": p.user_id.to_string(), "email": p.email, "name": p.name, "active": p.member })
}

fn normalize_email(email: &str) -> Result<String, ApiError> {
    let email = email.trim().to_lowercase();
    let valid = email.len() <= 254
        && email
            .split_once('@')
            .is_some_and(|(local, domain)| !local.is_empty() && domain.contains('.'));
    if valid {
        Ok(email)
    } else {
        Err(ApiError::BadRequest("invalid email"))
    }
}

#[derive(Deserialize)]
pub struct NewUser {
    email: String,
    #[serde(default)]
    name: String,
}

async fn provision<S: Store>(
    State(app): State<Arc<App<S>>>,
    headers: HeaderMap,
    Path(ws): Path<String>,
    Json(body): Json<NewUser>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    authorize(&app, &headers)?;
    let workspace_id = parse_id(&ws)?;
    let email = normalize_email(&body.email)?;
    let name: String = match body.name.trim() {
        "" => email.split('@').next().unwrap_or("").to_string(),
        n => n.chars().take(80).collect(),
    };
    let (user_id, created) = app
        .store
        .provision_user(workspace_id, &email, &name, new_id())
        .await?;
    tracing::info!(workspace_id, user_id, created, "user provisioned");
    let user = app
        .store
        .provisioned(workspace_id, user_id)
        .await?
        .ok_or(ApiError::Unavailable)?;
    let status = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(json_user(&user))))
}

#[derive(Deserialize)]
pub struct ListQuery {
    email: Option<String>,
    #[serde(default)]
    after: Option<String>,
    limit: Option<i64>,
}

async fn list<S: Store>(
    State(app): State<Arc<App<S>>>,
    headers: HeaderMap,
    Path(ws): Path<String>,
    Query(q): Query<ListQuery>,
) -> Result<Json<Value>, ApiError> {
    authorize(&app, &headers)?;
    let workspace_id = parse_id(&ws)?;
    if let Some(email) = q.email {
        let user = app
            .store
            .provisioned_by_email(workspace_id, &normalize_email(&email)?)
            .await?
            .ok_or(ApiError::NotFound("no such user"))?;
        return Ok(Json(json!({ "users": [json_user(&user)] })));
    }
    let after = match q.after.as_deref() {
        Some(a) if !a.is_empty() => parse_id(a)?,
        _ => 0,
    };
    let limit = q.limit.unwrap_or(100).clamp(1, 500);
    let users = app
        .store
        .provisioned_members(workspace_id, after, limit)
        .await?;
    let next = (users.len() as i64 == limit)
        .then(|| users.last().map(|u| u.user_id.to_string()))
        .flatten();
    Ok(Json(
        json!({ "users": users.iter().map(json_user).collect::<Vec<_>>(), "next": next }),
    ))
}

async fn one<S: Store>(
    State(app): State<Arc<App<S>>>,
    headers: HeaderMap,
    Path((ws, id)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    authorize(&app, &headers)?;
    let user = app
        .store
        .provisioned(parse_id(&ws)?, parse_id(&id)?)
        .await?
        .ok_or(ApiError::NotFound("no such user"))?;
    Ok(Json(json_user(&user)))
}

#[derive(Deserialize)]
pub struct Rename {
    name: String,
}

async fn rename<S: Store>(
    State(app): State<Arc<App<S>>>,
    headers: HeaderMap,
    Path((ws, id)): Path<(String, String)>,
    Json(body): Json<Rename>,
) -> Result<StatusCode, ApiError> {
    authorize(&app, &headers)?;
    let (workspace_id, user_id) = (parse_id(&ws)?, parse_id(&id)?);
    // Only the workspace's own members: a name is global, and provisioning is per workspace.
    if app.store.role(workspace_id, user_id).await?.is_none() {
        return Err(ApiError::NotFound("no such member"));
    }
    let name = body.name.trim();
    if name.is_empty() || name.chars().count() > 80 {
        return Err(ApiError::BadRequest("name must be 1 to 80 characters"));
    }
    let change = ProfileChange {
        name: Some(name.to_string()),
        avatar_url: None,
    };
    app.store
        .update_profile(user_id, &change)
        .await?
        .ok_or(ApiError::NotFound("no such user"))?;
    Ok(StatusCode::NO_CONTENT)
}

/// Deactivation is the ordinary workspace removal: the person stops receiving anything of
/// the workspace at once, and the removal job takes them out of its channels.
async fn deactivate<S: Store>(
    State(app): State<Arc<App<S>>>,
    headers: HeaderMap,
    Path((ws, id)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    authorize(&app, &headers)?;
    let (workspace_id, user_id) = (parse_id(&ws)?, parse_id(&id)?);
    match app.store.role(workspace_id, user_id).await? {
        // The owner is never deactivated by provisioning: the workspace would have nobody in charge.
        Some(role) if role == "owner" => {
            return Err(ApiError::Conflict(
                "the workspace owner can't be deactivated",
            ));
        }
        None => return Ok(StatusCode::NO_CONTENT), // already out: deactivation is idempotent
        Some(_) => {}
    }
    remove_from_workspace(&app, workspace_id, user_id, 0).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_compare_whole() {
        assert!(same(b"secret", b"secret"));
        assert!(!same(b"secret", b"secreT"));
        assert!(!same(b"secret", b"secret-longer"));
        assert!(!same(b"", b"secret"));
    }
}
