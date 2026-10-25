// SPDX-License-Identifier: AGPL-3.0-only
//! Enforced single sign-on per workspace, and break-glass recovery codes (ADR-0022).
//!
//! A workspace that enforces SSO refuses password sign-in for its members, but only
//! while an identity provider is configured: back on the Community Edition (no
//! providers) passwords work again, so nobody is ever locked out. Changing the policy
//! needs a licence that allows it (the Enterprise Edition says so through `Sso`);
//! recovery codes always work, whatever the licence.
//!
//! | Method | Path | |
//! |---|---|---|
//! | GET | `/v1/workspaces/{ws}/sso` | the policy: `{enforced, available, editable}` (members) |
//! | PUT | `/v1/workspaces/{ws}/sso` `{enforced}` | change it (owners and admins); enabling returns the caller's recovery codes |
//! | POST | `/v1/workspaces/{ws}/sso/recovery-codes` | new recovery codes for the caller (owners and admins), replacing their old ones |
//! | POST | `/v1/login/recovery` `{email, code}` | sign in with a recovery code, once |

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::api::{ApiError, Auth, Tokens, normalize_email};
use crate::store::Store;
use crate::token_hash;

/// How many recovery codes an admin gets at a time.
pub const RECOVERY_CODES: usize = 10;

pub fn routes<S: Store>() -> Router<Arc<Auth<S>>> {
    Router::new()
        .route(
            "/v1/workspaces/{ws}/sso",
            get(policy::<S>).put(set_policy::<S>),
        )
        .route(
            "/v1/workspaces/{ws}/sso/recovery-codes",
            post(new_codes::<S>),
        )
        .route("/v1/login/recovery", post(recover::<S>))
}

/// A fresh code, `xxxx-xxxx-xxxx-xxxx` (64 random bits, hex).
fn recovery_code() -> String {
    let mut bytes = [0u8; 8];
    getrandom::fill(&mut bytes).expect("OS random");
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    hex.as_bytes()
        .chunks(4)
        .map(|c| std::str::from_utf8(c).unwrap())
        .collect::<Vec<_>>()
        .join("-")
}

/// What's hashed: the code without dashes, spaces or case, as people retype it.
fn code_hash(code: &str) -> String {
    let normalized: String = code
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_lowercase())
        .collect();
    token_hash(&normalized)
}

/// The caller's user ID, from their access token.
fn caller<S>(auth: &Auth<S>, headers: &HeaderMap) -> Result<i64, ApiError> {
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(ApiError::Unauthorized)?;
    let claims = auth
        .keys
        .verify(token)
        .map_err(|_| ApiError::Unauthorized)?;
    claims.sub.parse().map_err(|_| ApiError::Unauthorized)
}

fn workspace(ws: &str) -> Result<i64, ApiError> {
    ws.parse()
        .map_err(|_| ApiError::BadRequest("invalid workspace"))
}

async fn admin<S: Store>(auth: &Auth<S>, workspace_id: i64, user_id: i64) -> Result<(), ApiError> {
    match auth.store.role(workspace_id, user_id).await?.as_deref() {
        Some("owner" | "admin") => Ok(()),
        _ => Err(ApiError::Forbidden(
            "only the workspace's owners and admins",
        )),
    }
}

/// New codes for the caller, stored hashed; the plain codes are shown once.
async fn issue_codes<S: Store>(
    auth: &Auth<S>,
    workspace_id: i64,
    user_id: i64,
) -> Result<Vec<String>, ApiError> {
    let codes: Vec<String> = (0..RECOVERY_CODES).map(|_| recovery_code()).collect();
    let hashes: Vec<String> = codes.iter().map(|c| code_hash(c)).collect();
    auth.store
        .replace_recovery_codes(workspace_id, user_id, &hashes)
        .await?;
    tracing::info!(workspace_id, user_id, "recovery codes issued");
    Ok(codes)
}

async fn policy<S: Store>(
    State(auth): State<Arc<Auth<S>>>,
    headers: HeaderMap,
    Path(ws): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let user_id = caller(&auth, &headers)?;
    let workspace_id = workspace(&ws)?;
    if auth.store.role(workspace_id, user_id).await?.is_none() {
        return Err(ApiError::Forbidden("not a member of this workspace"));
    }
    Ok(Json(json!({
        "enforced": auth.store.sso_enforced(workspace_id).await?,
        "available": auth.sso.is_some(),
        "editable": auth.sso.as_ref().is_some_and(|s| s.policy_editable),
    })))
}

#[derive(Deserialize)]
pub struct SetPolicy {
    enforced: bool,
}

async fn set_policy<S: Store>(
    State(auth): State<Arc<Auth<S>>>,
    headers: HeaderMap,
    Path(ws): Path<String>,
    Json(body): Json<SetPolicy>,
) -> Result<Json<Value>, ApiError> {
    let user_id = caller(&auth, &headers)?;
    let workspace_id = workspace(&ws)?;
    admin(&auth, workspace_id, user_id).await?;
    let Some(sso) = &auth.sso else {
        return Err(ApiError::Conflict("single sign-on isn't set up"));
    };
    if !sso.policy_editable {
        return Err(ApiError::Forbidden(
            "the licence doesn't allow changing single sign-on",
        ));
    }
    let mut reply = json!({ "enforced": body.enforced });
    if body.enforced {
        // Whoever turns it on must have proven SSO works for them, and gets a way back in.
        if !auth.store.has_external_identity(user_id).await? {
            return Err(ApiError::Conflict(
                "sign in with single sign-on once before enforcing it",
            ));
        }
        reply["recovery_codes"] = json!(issue_codes(&auth, workspace_id, user_id).await?);
    }
    auth.store
        .set_sso_enforced(workspace_id, body.enforced, user_id)
        .await?;
    tracing::info!(
        workspace_id,
        user_id,
        enforced = body.enforced,
        "SSO policy changed"
    );
    Ok(Json(reply))
}

async fn new_codes<S: Store>(
    State(auth): State<Arc<Auth<S>>>,
    headers: HeaderMap,
    Path(ws): Path<String>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let user_id = caller(&auth, &headers)?;
    let workspace_id = workspace(&ws)?;
    admin(&auth, workspace_id, user_id).await?;
    // No licence check: break-glass access must always be available.
    let codes = issue_codes(&auth, workspace_id, user_id).await?;
    Ok((
        StatusCode::CREATED,
        Json(json!({ "recovery_codes": codes })),
    ))
}

#[derive(Deserialize)]
pub struct Recover {
    email: String,
    code: String,
    #[serde(default = "crate::api::default_platform")]
    platform: String,
}

async fn recover<S: Store>(
    State(auth): State<Arc<Auth<S>>>,
    Json(req): Json<Recover>,
) -> Result<Json<Tokens>, ApiError> {
    let email = normalize_email(&req.email).map_err(|_| ApiError::Unauthorized)?;
    let Some(credentials) = auth.store.credentials(&email).await? else {
        return Err(ApiError::Unauthorized);
    };
    let user_id = credentials.user_id;
    if !auth
        .store
        .use_recovery_code(user_id, &code_hash(&req.code))
        .await?
    {
        tracing::warn!(user_id, "recovery code refused");
        return Err(ApiError::Unauthorized);
    }
    let tokens = auth.start_session(user_id, &req.platform).await?;
    tracing::warn!(
        user_id,
        session_id = tokens.session_id,
        "signed in with a recovery code"
    );
    Ok(Json(tokens))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_are_random_and_forgiving_to_retype() {
        let code = recovery_code();
        assert_eq!(code.len(), 19);
        assert_eq!(code.matches('-').count(), 3);
        assert_ne!(code, recovery_code());
        assert_eq!(
            code_hash(&code),
            code_hash(&code.to_uppercase().replace('-', " "))
        );
        assert_ne!(code_hash(&code), code_hash(&recovery_code()));
    }
}
