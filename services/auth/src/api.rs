// SPDX-License-Identifier: AGPL-3.0-only
//! HTTP API (plan §5.2). JSON in and out; errors are `{"error": "..."}`.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::keys::Keys;
use crate::store::{NewRefresh, Rotation, Store, StoreError};
use crate::{new_id, password, random_token, token_hash};

pub struct Auth<S> {
    pub store: S,
    pub keys: Keys,
    pub access_ttl_secs: i64,
    pub refresh_ttl_secs: i64,
}

pub fn router<S: Store>(auth: Arc<Auth<S>>) -> Router {
    Router::new()
        .route("/v1/signup", post(signup::<S>))
        .route("/v1/login", post(login::<S>))
        .route("/v1/token/refresh", post(refresh::<S>))
        .route("/v1/logout", post(logout::<S>))
        .route("/.well-known/jwks.json", get(jwks::<S>))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(auth)
}

pub enum ApiError {
    BadRequest(&'static str),
    Unauthorized,
    Conflict(&'static str),
    Internal,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            ApiError::Unauthorized => (StatusCode::UNAUTHORIZED, "invalid credentials"),
            ApiError::Conflict(m) => (StatusCode::CONFLICT, m),
            ApiError::Internal => (StatusCode::SERVICE_UNAVAILABLE, "try again later"),
        };
        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}

impl From<StoreError> for ApiError {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::EmailTaken => ApiError::Conflict("email already registered"),
            StoreError::Db(e) => {
                tracing::error!(error = %e, "store failed");
                ApiError::Internal
            }
        }
    }
}

#[derive(Deserialize)]
pub struct Signup {
    email: String,
    password: String,
    name: String,
}

#[derive(Serialize)]
pub struct SignupReply {
    user_id: String,
}

#[derive(Deserialize)]
pub struct Login {
    email: String,
    password: String,
    #[serde(default = "default_platform")]
    platform: String,
}

fn default_platform() -> String {
    "web".into()
}

#[derive(Deserialize)]
pub struct RefreshRequest {
    refresh_token: String,
}

/// IDs are strings in JSON: JavaScript numbers lose precision above 2^53.
#[derive(Serialize, Deserialize, Debug)]
pub struct Tokens {
    pub access_token: String,
    pub refresh_token: String,
    pub token_type: String,
    pub expires_in: i64,
    pub user_id: String,
    pub session_id: String,
}

/// Lower-cased and trimmed, so one person can't register twice by case.
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

async fn signup<S: Store>(
    State(auth): State<Arc<Auth<S>>>,
    Json(req): Json<Signup>,
) -> Result<(StatusCode, Json<SignupReply>), ApiError> {
    let email = normalize_email(&req.email)?;
    password::validate(&req.password).map_err(ApiError::BadRequest)?;
    let name = req.name.trim();
    if name.is_empty() || name.chars().count() > 80 {
        return Err(ApiError::BadRequest("name must be 1 to 80 characters"));
    }
    let hash = blocking(move || password::hash(&req.password)).await?;
    let user_id = new_id();
    auth.store.create_user(user_id, &email, &hash, name).await?;
    tracing::info!(user_id, "signed up");
    Ok((
        StatusCode::CREATED,
        Json(SignupReply {
            user_id: user_id.to_string(),
        }),
    ))
}

async fn login<S: Store>(
    State(auth): State<Arc<Auth<S>>>,
    Json(req): Json<Login>,
) -> Result<Json<Tokens>, ApiError> {
    let email = normalize_email(&req.email).map_err(|_| ApiError::Unauthorized)?;
    let credentials = auth.store.credentials(&email).await?;
    // Unknown users are checked against a dummy hash so both paths take the same time.
    let (user_id, hash) = match credentials {
        Some(c) => (Some(c.user_id), c.password_hash),
        None => (None, None),
    };
    let hash = hash.unwrap_or_else(|| password::dummy_hash().to_string());
    let ok = blocking(move || password::verify(&hash, &req.password)).await?;
    let Some(user_id) = user_id.filter(|_| ok) else {
        return Err(ApiError::Unauthorized);
    };
    let platform: String = req.platform.chars().take(32).collect();
    let now = Utc::now();
    let device_id = new_id();
    let (refresh_token, refresh) = auth.new_refresh(now);
    auth.store
        .start_session(user_id, device_id, &platform, refresh)
        .await?;
    tracing::info!(user_id, device_id, "logged in");
    Ok(Json(auth.tokens(user_id, device_id, refresh_token, now)))
}

async fn refresh<S: Store>(
    State(auth): State<Arc<Auth<S>>>,
    Json(req): Json<RefreshRequest>,
) -> Result<Json<Tokens>, ApiError> {
    let now = Utc::now();
    let (refresh_token, new) = auth.new_refresh(now);
    match auth
        .store
        .rotate(&token_hash(&req.refresh_token), new, now)
        .await?
    {
        Rotation::Rotated { user_id, device_id } => {
            Ok(Json(auth.tokens(user_id, device_id, refresh_token, now)))
        }
        Rotation::Invalid => Err(ApiError::Unauthorized),
        Rotation::Reused { device_id } => {
            tracing::warn!(device_id, "revoked refresh token reused: session revoked");
            Err(ApiError::Unauthorized)
        }
    }
}

async fn logout<S: Store>(
    State(auth): State<Arc<Auth<S>>>,
    Json(req): Json<RefreshRequest>,
) -> Result<StatusCode, ApiError> {
    auth.store
        .revoke(&token_hash(&req.refresh_token), Utc::now())
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn jwks<S: Store>(State(auth): State<Arc<Auth<S>>>) -> impl IntoResponse {
    (
        [("cache-control", "public, max-age=300")],
        Json(auth.keys.jwks()),
    )
}

impl<S> Auth<S> {
    fn new_refresh(&self, now: DateTime<Utc>) -> (String, NewRefresh) {
        let token = random_token(32);
        let refresh = NewRefresh {
            id: new_id(),
            token_hash: token_hash(&token),
            expires_at: now + Duration::seconds(self.refresh_ttl_secs),
        };
        (token, refresh)
    }

    fn tokens(
        &self,
        user_id: i64,
        device_id: i64,
        refresh_token: String,
        now: DateTime<Utc>,
    ) -> Tokens {
        Tokens {
            access_token: self.keys.issue(
                user_id,
                device_id,
                now.timestamp(),
                self.access_ttl_secs,
            ),
            refresh_token,
            token_type: "Bearer".into(),
            expires_in: self.access_ttl_secs,
            user_id: user_id.to_string(),
            session_id: device_id.to_string(),
        }
    }
}

/// Argon2 is deliberately slow: keep it off the async workers.
async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> T + Send + 'static,
) -> Result<T, ApiError> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|_| ApiError::Internal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Credentials;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use tower::ServiceExt;

    struct Token {
        user_id: i64,
        device_id: i64,
        expires_at: DateTime<Utc>,
        revoked: bool,
    }

    #[derive(Default)]
    struct Memory {
        users: Mutex<HashMap<String, Credentials>>,
        tokens: Mutex<HashMap<String, Token>>,
    }

    impl Store for Memory {
        async fn create_user(
            &self,
            id: i64,
            email: &str,
            hash: &str,
            _name: &str,
        ) -> Result<(), StoreError> {
            let mut users = self.users.lock().unwrap();
            if users.contains_key(email) {
                return Err(StoreError::EmailTaken);
            }
            users.insert(
                email.into(),
                Credentials {
                    user_id: id,
                    password_hash: Some(hash.into()),
                },
            );
            Ok(())
        }
        async fn credentials(&self, email: &str) -> Result<Option<Credentials>, StoreError> {
            Ok(self.users.lock().unwrap().get(email).cloned())
        }
        async fn start_session(
            &self,
            user_id: i64,
            device_id: i64,
            _p: &str,
            r: NewRefresh,
        ) -> Result<(), StoreError> {
            let token = Token {
                user_id,
                device_id,
                expires_at: r.expires_at,
                revoked: false,
            };
            self.tokens.lock().unwrap().insert(r.token_hash, token);
            Ok(())
        }
        async fn rotate(
            &self,
            old: &str,
            new: NewRefresh,
            now: DateTime<Utc>,
        ) -> Result<Rotation, StoreError> {
            let mut tokens = self.tokens.lock().unwrap();
            let Some(t) = tokens.get_mut(old) else {
                return Ok(Rotation::Invalid);
            };
            if t.revoked {
                let device_id = t.device_id;
                tokens
                    .values_mut()
                    .filter(|t| t.device_id == device_id)
                    .for_each(|t| t.revoked = true);
                return Ok(Rotation::Reused { device_id });
            }
            if t.expires_at <= now {
                return Ok(Rotation::Invalid);
            }
            t.revoked = true;
            let (user_id, device_id) = (t.user_id, t.device_id);
            tokens.insert(
                new.token_hash,
                Token {
                    user_id,
                    device_id,
                    expires_at: new.expires_at,
                    revoked: false,
                },
            );
            Ok(Rotation::Rotated { user_id, device_id })
        }
        async fn revoke(&self, hash: &str, _now: DateTime<Utc>) -> Result<(), StoreError> {
            if let Some(t) = self.tokens.lock().unwrap().get_mut(hash) {
                t.revoked = true;
            }
            Ok(())
        }
    }

    fn app() -> (Router, Arc<Auth<Memory>>) {
        let auth = Arc::new(Auth {
            store: Memory::default(),
            keys: Keys::generate(),
            access_ttl_secs: 900,
            refresh_ttl_secs: 3600,
        });
        (router(auth.clone()), auth)
    }

    async fn call(
        app: &Router,
        path: &str,
        body: serde_json::Value,
    ) -> (StatusCode, serde_json::Value) {
        let request = Request::post(path)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
        )
    }

    async fn signed_up(app: &Router) -> String {
        let (status, body) = call(app, "/v1/signup", serde_json::json!({"email": " Ada@Example.org ", "password": "analytical engine", "name": "Ada"})).await;
        assert_eq!(status, StatusCode::CREATED);
        body["user_id"].as_str().unwrap().to_string()
    }

    async fn logged_in(app: &Router) -> Tokens {
        let (status, body) = call(
            app,
            "/v1/login",
            serde_json::json!({"email": "ada@example.org", "password": "analytical engine"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        serde_json::from_value(body).unwrap()
    }

    #[tokio::test]
    async fn signup_then_login_issues_a_valid_access_token() {
        let (app, auth) = app();
        let user_id = signed_up(&app).await;
        let tokens = logged_in(&app).await;
        let claims = auth.keys.verify(&tokens.access_token).unwrap();
        assert_eq!(claims.sub, user_id);
        assert_eq!(claims.sid.to_string(), tokens.session_id);
        assert_eq!(tokens.expires_in, 900);
    }

    #[tokio::test]
    async fn bad_signups_are_refused() {
        let (app, _) = app();
        signed_up(&app).await;
        let again = call(&app, "/v1/signup", serde_json::json!({"email": "ada@EXAMPLE.org", "password": "analytical engine", "name": "Ada"})).await;
        assert_eq!(again.0, StatusCode::CONFLICT, "same email in another case");
        let weak = call(
            &app,
            "/v1/signup",
            serde_json::json!({"email": "b@example.org", "password": "short", "name": "B"}),
        )
        .await;
        assert_eq!(weak.0, StatusCode::BAD_REQUEST);
        let email = call(
            &app,
            "/v1/signup",
            serde_json::json!({"email": "nope", "password": "long enough pass", "name": "B"}),
        )
        .await;
        assert_eq!(email.0, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn wrong_password_and_unknown_user_look_the_same() {
        let (app, _) = app();
        signed_up(&app).await;
        let wrong = call(
            &app,
            "/v1/login",
            serde_json::json!({"email": "ada@example.org", "password": "difference engine"}),
        )
        .await;
        let unknown = call(
            &app,
            "/v1/login",
            serde_json::json!({"email": "bob@example.org", "password": "difference engine"}),
        )
        .await;
        assert_eq!(wrong, unknown);
        assert_eq!(wrong.0, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn refresh_rotates_and_reuse_revokes_the_session() {
        let (app, _) = app();
        signed_up(&app).await;
        let first = logged_in(&app).await;
        let (status, body) = call(
            &app,
            "/v1/token/refresh",
            serde_json::json!({"refresh_token": first.refresh_token}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let second: Tokens = serde_json::from_value(body).unwrap();
        assert_ne!(second.refresh_token, first.refresh_token);
        assert_eq!(second.session_id, first.session_id);

        // Replaying the old token (theft) kills the whole session, including the new token.
        let replay = call(
            &app,
            "/v1/token/refresh",
            serde_json::json!({"refresh_token": first.refresh_token}),
        )
        .await;
        assert_eq!(replay.0, StatusCode::UNAUTHORIZED);
        let after = call(
            &app,
            "/v1/token/refresh",
            serde_json::json!({"refresh_token": second.refresh_token}),
        )
        .await;
        assert_eq!(after.0, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn logout_revokes_the_refresh_token() {
        let (app, _) = app();
        signed_up(&app).await;
        let tokens = logged_in(&app).await;
        let (status, _) = call(
            &app,
            "/v1/logout",
            serde_json::json!({"refresh_token": tokens.refresh_token}),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let after = call(
            &app,
            "/v1/token/refresh",
            serde_json::json!({"refresh_token": tokens.refresh_token}),
        )
        .await;
        assert_eq!(after.0, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn jwks_publishes_the_signing_key() {
        let (app, auth) = app();
        let request = Request::get("/.well-known/jwks.json")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let jwks: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(jwks, auth.keys.jwks());
    }
}
