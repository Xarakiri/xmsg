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
use crate::revocation::Recorder;
use crate::store::{NewRefresh, Rotation, Store, StoreError};
use crate::{new_id, password, random_token, token_hash};

pub struct Auth<S> {
    pub store: S,
    pub keys: Keys,
    pub access_ttl_secs: i64,
    pub refresh_ttl_secs: i64,
    /// How long a rotated refresh token may still be presented (lost reply, racing tabs).
    pub reuse_grace_secs: i64,
    /// Fast path for revocations (NATS `auth.revocations`), fed after commit; `None`
    /// leaves them to the outbox alone.
    pub revocations: Option<tokio::sync::mpsc::UnboundedSender<Vec<u8>>>,
    /// External identity providers (single sign-on), when any are registered.
    pub sso: Option<crate::sso::Sso>,
}

pub fn router<S: Store>(auth: Arc<Auth<S>>) -> Router {
    Router::new()
        .route("/v1/signup", post(signup::<S>))
        .route("/v1/login", post(login::<S>))
        .route("/v1/token/refresh", post(refresh::<S>))
        .route("/v1/logout", post(logout::<S>))
        .route("/.well-known/jwks.json", get(jwks::<S>))
        .route("/healthz", get(|| async { "ok" }))
        .merge(crate::sso::routes::<S>())
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
    let tokens = auth.start_session(user_id, &req.platform).await?;
    tracing::info!(user_id, session_id = tokens.session_id, "logged in");
    Ok(Json(tokens))
}

async fn refresh<S: Store>(
    State(auth): State<Arc<Auth<S>>>,
    Json(req): Json<RefreshRequest>,
) -> Result<Json<Tokens>, ApiError> {
    let now = Utc::now();
    let (refresh_token, new) = auth.new_refresh(now);
    let revoked = Recorder::new(now, auth.access_ttl_secs, "refresh token reused");
    let rotation = auth
        .store
        .rotate(
            &token_hash(&req.refresh_token),
            new,
            now,
            Duration::seconds(auth.reuse_grace_secs),
            &|user, device| revoked.encode(user, device),
        )
        .await?;
    revoked.publish(auth.revocations.as_ref());
    match rotation {
        Rotation::Rotated { user_id, device_id } => {
            Ok(Json(auth.tokens(user_id, device_id, refresh_token, now)))
        }
        Rotation::Reissued { user_id, device_id } => {
            tracing::info!(
                device_id,
                "rotated refresh token reused within grace: reissued"
            );
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
    let now = Utc::now();
    let revoked = Recorder::new(now, auth.access_ttl_secs, "logout");
    auth.store
        .revoke(&token_hash(&req.refresh_token), now, &|user, device| {
            revoked.encode(user, device)
        })
        .await?;
    revoked.publish(auth.revocations.as_ref());
    Ok(StatusCode::NO_CONTENT)
}

async fn jwks<S: Store>(State(auth): State<Arc<Auth<S>>>) -> impl IntoResponse {
    (
        [("cache-control", "public, max-age=300")],
        Json(auth.keys.jwks()),
    )
}

impl<S: Store> Auth<S> {
    /// A new session (a device) for a user who has proven who they are: by password, or
    /// through an external identity provider.
    pub async fn start_session(&self, user_id: i64, platform: &str) -> Result<Tokens, ApiError> {
        let platform: String = platform.chars().take(32).collect();
        let now = Utc::now();
        let device_id = new_id();
        let (refresh_token, refresh) = self.new_refresh(now);
        self.store
            .start_session(user_id, device_id, &platform, refresh)
            .await?;
        Ok(self.tokens(user_id, device_id, refresh_token, now))
    }
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
        revoked_at: Option<DateTime<Utc>>,
        /// Revoked by rotation rather than logout (`replaced_by IS NOT NULL`).
        replaced: bool,
    }

    #[derive(Default)]
    struct Memory {
        users: Mutex<HashMap<String, Credentials>>,
        tokens: Mutex<HashMap<String, Token>>,
        /// What the Postgres store writes to the outbox (`session.revoked` payloads).
        outbox: Mutex<Vec<Vec<u8>>>,
        sso: Mutex<HashMap<String, crate::store::SsoState>>,
        links: Mutex<HashMap<String, i64>>,
    }

    impl Store for Memory {
        async fn save_sso_state(
            &self,
            state: &str,
            s: &crate::store::SsoState,
        ) -> Result<(), StoreError> {
            self.sso.lock().unwrap().insert(state.into(), s.clone());
            Ok(())
        }
        async fn take_sso_state(
            &self,
            state: &str,
            _max_age: Duration,
        ) -> Result<Option<crate::store::SsoState>, StoreError> {
            Ok(self.sso.lock().unwrap().remove(state))
        }
        async fn external_user(
            &self,
            provider: &str,
            subject: &str,
            email: &str,
            _name: &str,
            new_id: i64,
        ) -> Result<i64, StoreError> {
            let key = format!("{provider}:{subject}");
            if let Some(id) = self.links.lock().unwrap().get(&key) {
                return Ok(*id);
            }
            let id = {
                let mut users = self.users.lock().unwrap();
                users
                    .entry(email.into())
                    .or_insert(Credentials {
                        user_id: new_id,
                        password_hash: None,
                    })
                    .user_id
            };
            self.links.lock().unwrap().insert(key, id);
            Ok(id)
        }
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
                revoked_at: None,
                replaced: false,
            };
            self.tokens.lock().unwrap().insert(r.token_hash, token);
            Ok(())
        }
        async fn rotate(
            &self,
            old: &str,
            new: NewRefresh,
            now: DateTime<Utc>,
            grace: Duration,
            revoked: &crate::store::Revoked<'_>,
        ) -> Result<Rotation, StoreError> {
            let mut tokens = self.tokens.lock().unwrap();
            let Some(t) = tokens.get_mut(old) else {
                return Ok(Rotation::Invalid);
            };
            let (user_id, device_id, expires_at) = (t.user_id, t.device_id, t.expires_at);
            let rotation = match t.revoked_at {
                Some(at) => {
                    let in_grace = t.replaced && now < at + grace && expires_at > now;
                    let live = tokens.values().any(|t| {
                        t.device_id == device_id && t.revoked_at.is_none() && t.expires_at > now
                    });
                    if !(in_grace && live) {
                        let mut ended = 0;
                        tokens
                            .values_mut()
                            .filter(|t| t.device_id == device_id && t.revoked_at.is_none())
                            .for_each(|t| {
                                t.revoked_at = Some(now);
                                ended += 1;
                            });
                        if ended > 0 {
                            self.outbox
                                .lock()
                                .unwrap()
                                .push(revoked(user_id, device_id));
                        }
                        return Ok(Rotation::Reused { device_id });
                    }
                    Rotation::Reissued { user_id, device_id }
                }
                None if expires_at <= now => return Ok(Rotation::Invalid),
                None => {
                    t.revoked_at = Some(now);
                    t.replaced = true;
                    Rotation::Rotated { user_id, device_id }
                }
            };
            tokens.insert(
                new.token_hash,
                Token {
                    user_id,
                    device_id,
                    expires_at: new.expires_at,
                    revoked_at: None,
                    replaced: false,
                },
            );
            Ok(rotation)
        }
        async fn revoke(
            &self,
            hash: &str,
            now: DateTime<Utc>,
            revoked: &crate::store::Revoked<'_>,
        ) -> Result<(), StoreError> {
            let mut tokens = self.tokens.lock().unwrap();
            let Some(t) = tokens.get(hash).filter(|t| t.revoked_at.is_none()) else {
                return Ok(());
            };
            let (user_id, device_id) = (t.user_id, t.device_id);
            tokens
                .values_mut()
                .filter(|t| t.device_id == device_id && t.revoked_at.is_none())
                .for_each(|t| t.revoked_at = Some(now));
            self.outbox
                .lock()
                .unwrap()
                .push(revoked(user_id, device_id));
            Ok(())
        }
    }

    fn app() -> (Router, Arc<Auth<Memory>>) {
        let auth = Arc::new(Auth {
            store: Memory::default(),
            keys: Keys::generate(),
            access_ttl_secs: 900,
            refresh_ttl_secs: 3600,
            reuse_grace_secs: 30,
            revocations: None,
            sso: None,
        });
        (router(auth.clone()), auth)
    }

    /// Like `app`, with the revocation fast path captured.
    fn app_with_revocations() -> (
        Router,
        Arc<Auth<Memory>>,
        tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let auth = Arc::new(Auth {
            store: Memory::default(),
            keys: Keys::generate(),
            access_ttl_secs: 900,
            refresh_ttl_secs: 3600,
            reuse_grace_secs: 30,
            revocations: Some(tx),
            sso: None,
        });
        (router(auth.clone()), auth, rx)
    }

    /// A provider standing in for OIDC: the code it's given is the subject.
    struct Fake;
    impl crate::sso::IdentityProvider for Fake {
        fn id(&self) -> &str {
            "fake"
        }
        fn display_name(&self) -> &str {
            "Fake IdP"
        }
        fn authorize_url(
            &self,
            state: &str,
            nonce: &str,
            challenge: &str,
            redirect: &str,
        ) -> String {
            format!(
                "https://idp.example/authorize?state={state}&nonce={nonce}&code_challenge={challenge}&redirect_uri={redirect}"
            )
        }
        fn finish<'a>(
            &'a self,
            code: &'a str,
            verifier: &'a str,
            _nonce: &'a str,
            _r: &'a str,
        ) -> crate::sso::BoxFuture<'a, Result<crate::sso::ExternalIdentity, String>> {
            Box::pin(async move {
                if verifier.is_empty() {
                    return Err("no PKCE verifier".into());
                }
                Ok(crate::sso::ExternalIdentity {
                    subject: code.into(),
                    email: if code == "unverified" {
                        "u@example.org".into()
                    } else {
                        "Ann@Example.org".into()
                    },
                    email_verified: code != "unverified",
                    name: "Ann".into(),
                })
            })
        }
    }

    async fn get(app: &Router, path: &str) -> (StatusCode, String) {
        let response = app
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let location = response
            .headers()
            .get("location")
            .map(|v| v.to_str().unwrap().to_string())
            .unwrap_or_default();
        (status, location)
    }

    fn query_param(url: &str, name: &str) -> String {
        url.split(['?', '&', '#'])
            .find_map(|kv| kv.strip_prefix(&format!("{name}=")))
            .unwrap_or_default()
            .to_string()
    }

    #[tokio::test]
    async fn single_sign_on_links_by_verified_email_and_starts_a_session() {
        let auth = Arc::new(Auth {
            store: Memory::default(),
            keys: Keys::generate(),
            access_ttl_secs: 900,
            refresh_ttl_secs: 3600,
            reuse_grace_secs: 30,
            revocations: None,
            sso: Some(crate::sso::Sso::new(
                vec![Arc::new(Fake)],
                "https://chat.example/auth/".into(),
                "https://chat.example".into(),
            )),
        });
        let app = router(auth.clone());
        // An existing password account with the same email.
        let (status, _) = call(&app, "/v1/signup", serde_json::json!({"email": "ann@example.org", "password": "correct horse battery", "name": "Ann"})).await;
        assert_eq!(status, StatusCode::CREATED);
        let existing = auth.store.users.lock().unwrap()["ann@example.org"].user_id;

        let (status, to_idp) = get(&app, "/v1/sso/fake/start?return_to=/channels/7").await;
        assert_eq!(status, StatusCode::FOUND);
        assert!(to_idp.starts_with("https://idp.example/authorize?"));
        assert!(to_idp.contains("redirect_uri=https://chat.example/auth/v1/sso/fake/callback"));
        let state = query_param(&to_idp, "state");

        let (status, back) = get(
            &app,
            &format!("/v1/sso/fake/callback?code=sub-1&state={state}"),
        )
        .await;
        assert_eq!(status, StatusCode::FOUND);
        assert!(
            back.starts_with("https://chat.example/channels/7#access_token="),
            "{back}"
        );
        assert_eq!(
            query_param(&back, "user_id"),
            existing.to_string(),
            "linked to the account with that email"
        );
        assert!(!query_param(&back, "refresh_token").is_empty());

        // The state is used once.
        let (_, again) = get(
            &app,
            &format!("/v1/sso/fake/callback?code=sub-1&state={state}"),
        )
        .await;
        assert!(again.contains("#sso_error="), "{again}");
        // An unverified email never signs in.
        let (_, to_idp) = get(&app, "/v1/sso/fake/start").await;
        let (_, refused) = get(
            &app,
            &format!(
                "/v1/sso/fake/callback?code=unverified&state={}",
                query_param(&to_idp, "state")
            ),
        )
        .await;
        assert!(
            refused.starts_with("https://chat.example/#sso_error="),
            "{refused}"
        );
        // Off-site return paths are replaced.
        let (_, to_idp) = get(&app, "/v1/sso/fake/start?return_to=https://evil.example").await;
        let (_, back) = get(
            &app,
            &format!(
                "/v1/sso/fake/callback?code=sub-1&state={}",
                query_param(&to_idp, "state")
            ),
        )
        .await;
        assert!(
            back.starts_with("https://chat.example/#access_token="),
            "{back}"
        );
        // Unknown providers, and the provider list.
        assert_eq!(
            get(&app, "/v1/sso/nope/start").await.0,
            StatusCode::NOT_FOUND
        );
        let response = app
            .clone()
            .oneshot(
                Request::get("/v1/sso/providers")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), 1 << 16)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap()["providers"][0]["name"],
            "Fake IdP"
        );
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

    async fn refreshed(app: &Router, refresh_token: &str) -> (StatusCode, Option<Tokens>) {
        let (status, body) = call(
            app,
            "/v1/token/refresh",
            serde_json::json!({ "refresh_token": refresh_token }),
        )
        .await;
        (status, serde_json::from_value(body).ok())
    }

    /// Moves a token's revocation into the past, as if it was rotated `secs` ago.
    fn revoked_ago(auth: &Auth<Memory>, refresh_token: &str, secs: i64) {
        let mut tokens = auth.store.tokens.lock().unwrap();
        let t = tokens.get_mut(&token_hash(refresh_token)).unwrap();
        t.revoked_at = Some(t.revoked_at.unwrap() - Duration::seconds(secs));
    }

    #[tokio::test]
    async fn refresh_rotates_and_reuse_revokes_the_session() {
        let (app, auth) = app();
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

        // Replaying the old token (theft) after the grace kills the whole session,
        // including the new token.
        revoked_ago(&auth, &first.refresh_token, 31);
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
    async fn reuse_within_the_grace_reissues_and_keeps_the_session() {
        let (app, _) = app();
        signed_up(&app).await;
        let first = logged_in(&app).await;
        // The reply to this refresh is lost: the client still holds `first`.
        let (status, lost) = refreshed(&app, &first.refresh_token).await;
        assert_eq!(status, StatusCode::OK);
        let lost = lost.unwrap();

        let (status, retry) = refreshed(&app, &first.refresh_token).await;
        assert_eq!(status, StatusCode::OK);
        let retry = retry.unwrap();
        assert_eq!(retry.session_id, first.session_id);
        assert_ne!(retry.refresh_token, lost.refresh_token);
        // Neither successor was revoked: the chain continues from either.
        assert_eq!(
            refreshed(&app, &retry.refresh_token).await.0,
            StatusCode::OK
        );
        assert_eq!(refreshed(&app, &lost.refresh_token).await.0, StatusCode::OK);
    }

    #[tokio::test]
    async fn reuse_after_the_grace_revokes_the_session() {
        let (app, auth) = app();
        signed_up(&app).await;
        let first = logged_in(&app).await;
        let second = refreshed(&app, &first.refresh_token).await.1.unwrap();
        revoked_ago(&auth, &first.refresh_token, 31);
        assert_eq!(
            refreshed(&app, &first.refresh_token).await.0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            refreshed(&app, &second.refresh_token).await.0,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn logged_out_tokens_get_no_grace() {
        let (app, _) = app();
        signed_up(&app).await;
        let tokens = logged_in(&app).await;
        call(
            &app,
            "/v1/logout",
            serde_json::json!({"refresh_token": tokens.refresh_token}),
        )
        .await;
        assert_eq!(
            refreshed(&app, &tokens.refresh_token).await.0,
            StatusCode::UNAUTHORIZED
        );

        // Rotated moments ago, but its successor was logged out: the session stays dead.
        let first = logged_in(&app).await;
        let second = refreshed(&app, &first.refresh_token).await.1.unwrap();
        call(
            &app,
            "/v1/logout",
            serde_json::json!({"refresh_token": second.refresh_token}),
        )
        .await;
        assert_eq!(
            refreshed(&app, &first.refresh_token).await.0,
            StatusCode::UNAUTHORIZED
        );
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

    fn decode_revocation(bytes: &[u8]) -> xmsg_proto::v1::SessionRevoked {
        use xmsg_proto::Message;
        match xmsg_proto::v1::AuthEvent::decode(bytes).unwrap().event {
            Some(xmsg_proto::v1::auth_event::Event::SessionRevoked(r)) => r,
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn logout_and_theft_each_revoke_the_session_once_via_outbox_and_fast_path() {
        let (app, auth, mut fast) = app_with_revocations();
        signed_up(&app).await;
        let tokens = logged_in(&app).await;
        let (status, _) = call(
            &app,
            "/v1/logout",
            serde_json::json!({"refresh_token": tokens.refresh_token}),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let outbox = auth.store.outbox.lock().unwrap().clone();
        assert_eq!(outbox.len(), 1, "logout writes one session.revoked");
        assert_eq!(
            fast.try_recv().unwrap(),
            outbox[0],
            "and publishes the same bytes"
        );
        let r = decode_revocation(&outbox[0]);
        assert_eq!(
            (
                r.user_id.to_string(),
                r.session_id.to_string(),
                r.reason.as_str()
            ),
            (tokens.user_id.clone(), tokens.session_id.clone(), "logout")
        );
        assert!(
            r.refuse_until_ms >= r.revoked_at_ms + 900_000,
            "refused until every access token has expired"
        );

        // Logging out again (or with an unknown token) changes nothing and emits nothing.
        call(
            &app,
            "/v1/logout",
            serde_json::json!({"refresh_token": tokens.refresh_token}),
        )
        .await;
        assert_eq!(auth.store.outbox.lock().unwrap().len(), 1);
        assert!(fast.try_recv().is_err());

        // Theft: a rotated token replayed after the grace window ends the session.
        let second = logged_in(&app).await;
        let (_, body) = call(
            &app,
            "/v1/token/refresh",
            serde_json::json!({"refresh_token": second.refresh_token}),
        )
        .await;
        assert!(body["refresh_token"].is_string());
        revoked_ago(&auth, &second.refresh_token, 60);
        let (status, _) = call(
            &app,
            "/v1/token/refresh",
            serde_json::json!({"refresh_token": second.refresh_token}),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let theft = decode_revocation(&fast.try_recv().unwrap());
        assert_eq!(
            (theft.session_id.to_string(), theft.reason.as_str()),
            (second.session_id, "refresh token reused")
        );
        assert_eq!(auth.store.outbox.lock().unwrap().len(), 2);
    }
}
