// SPDX-License-Identifier: AGPL-3.0-only
//! Admin-issued password resets (ADR-0026).
//!
//! An owner or admin asks for a single-use code for a member; only its SHA-256 is
//! stored and it expires in an hour. The member applies it with a new password at an
//! unauthenticated endpoint, which revokes their sessions. An admin can't reset an
//! owner. The code is never logged.
//!
//! | Method | Path | |
//! |---|---|---|
//! | POST | `/v1/workspaces/{ws}/members/{user}/password-reset` | a new code: `{code, expires_at}` (owners and admins) |
//! | POST | `/v1/password-reset` `{email, code, password}` | apply a code (no auth); 204, one 400 for unknown email, wrong or expired code |

use std::sync::Arc;

use axum::extract::{Extension, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use chrono::{Duration, SecondsFormat, Utc};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::api::{ApiError, Auth, blocking, normalize_email};
use crate::limits::ClientIp;
use crate::revocation::Recorder;
use crate::store::{PasswordReset, Store};
use crate::{password, random_token, token_hash};

/// How long a reset code lives.
const CODE_TTL: Duration = Duration::hours(1);

pub fn routes<S: Store>() -> Router<Arc<Auth<S>>> {
    Router::new()
        .route(
            "/v1/workspaces/{ws}/members/{user}/password-reset",
            post(issue::<S>),
        )
        .route("/v1/password-reset", post(apply::<S>))
}

fn id(s: &str, what: &'static str) -> Result<i64, ApiError> {
    s.trim()
        .parse()
        .ok()
        .filter(|id| *id > 0)
        .ok_or(ApiError::BadRequest(what))
}

/// A new reset code for a member, replacing any earlier one. The plain code is the
/// reply and is shown once; only its hash is stored.
async fn issue<S: Store>(
    State(auth): State<Arc<Auth<S>>>,
    headers: HeaderMap,
    Path((ws, user)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    let issuer = crate::sso_policy::caller(&auth, &headers)?;
    let (workspace_id, target) = (id(&ws, "invalid workspace")?, id(&user, "invalid user")?);
    let issuer_role = auth
        .store
        .role(workspace_id, issuer)
        .await?
        .ok_or(ApiError::Forbidden(
            "only the workspace's owners and admins",
        ))?;
    if issuer_role != "owner" && issuer_role != "admin" {
        return Err(ApiError::Forbidden(
            "only the workspace's owners and admins",
        ));
    }
    let target_role = auth
        .store
        .role(workspace_id, target)
        .await?
        .ok_or(ApiError::Forbidden("not a member of this workspace"))?;
    if target_role == "owner" && issuer_role != "owner" {
        return Err(ApiError::Forbidden(
            "an admin can't reset the owner's password",
        ));
    }
    let code = random_token(32);
    let expires_at = Utc::now() + CODE_TTL;
    auth.store
        .save_password_reset(target, &token_hash(&code), expires_at)
        .await?;
    tracing::info!(
        workspace_id,
        user_id = target,
        issuer,
        "password reset code issued"
    );
    Ok(Json(json!({
        "code": code,
        "expires_at": expires_at.to_rfc3339_opts(SecondsFormat::Secs, true),
    })))
}

#[derive(Deserialize)]
pub struct Apply {
    email: String,
    code: String,
    password: String,
}

/// Applies a code: one 400 for an unknown email, a wrong code or an expired one, so
/// the endpoint can't be used to probe. Rate-limited like login. Success changes the
/// password, consumes the code and revokes every session of the user.
async fn apply<S: Store>(
    State(auth): State<Arc<Auth<S>>>,
    Extension(ip): Extension<ClientIp>,
    Json(req): Json<Apply>,
) -> Result<StatusCode, ApiError> {
    let account = req.email.trim().to_lowercase();
    let mut checks = vec![(format!("login:a:{account}"), auth.limits.login_per_account)];
    checks.extend(ip.key("login", auth.limits.login_per_ip));
    auth.limits.check(&checks).await?;
    let email = normalize_email(&req.email)?;
    password::validate(&req.password).map_err(ApiError::BadRequest)?;
    let hash = blocking(move || password::hash(&req.password)).await?;
    let now = Utc::now();
    let recorder = Recorder::new(now, auth.access_ttl_secs, "password reset");
    let reset = auth
        .store
        .reset_password(
            &email,
            &token_hash(req.code.trim()),
            &hash,
            now,
            &|user, device| recorder.encode(user, device),
        )
        .await?;
    let PasswordReset::Applied { user_id, devices } = reset else {
        tracing::warn!(
            email,
            "password reset refused: unknown email, wrong or expired code"
        );
        return Err(ApiError::BadRequest("invalid or expired code"));
    };
    recorder.publish(auth.revocations.as_ref());
    tracing::info!(user_id, sessions = devices.len(), "password reset applied");
    Ok(StatusCode::NO_CONTENT)
}
