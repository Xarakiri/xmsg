// SPDX-License-Identifier: AGPL-3.0-only
//! Single sign-on through external identity providers: the auth service's extension
//! point (ADR-0022). The core knows nothing about OIDC, SAML or LDAP; a provider is
//! anything that can send the browser to sign in and turn the result into a verified
//! identity. The Enterprise Edition registers its providers through `serve`.
//!
//! Flow (authorization code with PKCE):
//! 1. `GET /v1/sso/{provider}/start?return_to=/path` stores a one-time state (with the
//!    nonce and PKCE verifier) and redirects to the provider.
//! 2. `GET /v1/sso/{provider}/callback?code=&state=` takes the state (once, within 10
//!    minutes), has the provider verify the result, finds or creates the user, starts a
//!    session like a password login, and redirects to the web app with the tokens in the
//!    URL fragment (never sent to a server, kept out of logs and referrers).
//!
//! An identity must come with a **verified** email; that's how an existing account is
//! matched the first time.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use chrono::Duration;
use serde::Deserialize;

use crate::api::Auth;
use crate::store::{SsoState, Store};
use crate::{new_id, random_token};

/// What a provider vouches for after a successful sign-in.
#[derive(Debug, Clone, PartialEq)]
pub struct ExternalIdentity {
    /// The provider's stable ID for the person (OIDC `sub`).
    pub subject: String,
    pub email: String,
    pub email_verified: bool,
    pub name: String,
}

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// An external identity provider (OIDC, SAML, ...).
pub trait IdentityProvider: Send + Sync + 'static {
    /// URL-safe ID, used in paths: `/v1/sso/{id}/...`.
    fn id(&self) -> &str;
    /// What the sign-in button says.
    fn display_name(&self) -> &str;
    /// Where to send the browser to sign in.
    fn authorize_url(
        &self,
        state: &str,
        nonce: &str,
        code_challenge: &str,
        redirect_uri: &str,
    ) -> String;
    /// Turns the provider's answer into a verified identity (or why not).
    fn finish<'a>(
        &'a self,
        code: &'a str,
        code_verifier: &'a str,
        nonce: &'a str,
        redirect_uri: &'a str,
    ) -> BoxFuture<'a, Result<ExternalIdentity, String>>;
}

/// Registered providers and where the browser reaches us.
pub struct Sso {
    pub providers: HashMap<String, Arc<dyn IdentityProvider>>,
    /// The auth service as the browser sees it, e.g. https://chat.example.com/auth.
    pub public_url: String,
    /// Where signed-in users land, e.g. https://chat.example.com.
    pub web_url: String,
}

impl Sso {
    pub fn new(
        providers: Vec<Arc<dyn IdentityProvider>>,
        public_url: String,
        web_url: String,
    ) -> Self {
        Sso {
            providers: providers
                .into_iter()
                .map(|p| (p.id().to_string(), p))
                .collect(),
            public_url: public_url.trim_end_matches('/').to_string(),
            web_url: web_url.trim_end_matches('/').to_string(),
        }
    }

    fn redirect_uri(&self, provider: &str) -> String {
        format!("{}/v1/sso/{provider}/callback", self.public_url)
    }
}

/// A sign-in must finish within this long.
pub const STATE_MAX_AGE_SECS: i64 = 600;

/// PKCE (RFC 7636, S256): the challenge sent at the start for the verifier kept here.
pub fn code_challenge(verifier: &str) -> String {
    use base64::Engine;
    use sha2::{Digest, Sha256};
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

/// Only paths on our own site: anything else could send a fresh session elsewhere.
pub fn safe_return_to(return_to: Option<&str>) -> String {
    match return_to {
        Some(p)
            if p.starts_with('/')
                && !p.starts_with("//")
                && !p.contains('\\')
                && p.len() <= 512 =>
        {
            p.to_string()
        }
        _ => "/".to_string(),
    }
}

pub fn routes<S: Store>() -> Router<Arc<Auth<S>>> {
    Router::new()
        .route("/v1/sso/providers", get(providers::<S>))
        .route("/v1/sso/{provider}/start", get(start::<S>))
        .route("/v1/sso/{provider}/callback", get(callback::<S>))
}

fn plain(status: StatusCode, message: &str) -> Response {
    (status, Json(serde_json::json!({ "error": message }))).into_response()
}

fn redirect(location: &str) -> Response {
    (
        StatusCode::FOUND,
        [
            (header::LOCATION, location.to_string()),
            (header::CACHE_CONTROL, "no-store".into()),
        ],
    )
        .into_response()
}

/// `{providers: [{id, name}]}`: the sign-in buttons to show.
async fn providers<S: Store>(State(auth): State<Arc<Auth<S>>>) -> Response {
    let list: Vec<serde_json::Value> = auth
        .sso
        .as_ref()
        .map(|s| {
            let mut list: Vec<_> = s
                .providers
                .values()
                .map(|p| serde_json::json!({ "id": p.id(), "name": p.display_name() }))
                .collect();
            list.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
            list
        })
        .unwrap_or_default();
    Json(serde_json::json!({ "providers": list })).into_response()
}

#[derive(Deserialize)]
pub struct StartQuery {
    return_to: Option<String>,
}

async fn start<S: Store>(
    State(auth): State<Arc<Auth<S>>>,
    Path(provider_id): Path<String>,
    Query(q): Query<StartQuery>,
) -> Response {
    let Some(sso) = &auth.sso else {
        return plain(StatusCode::NOT_FOUND, "single sign-on is not set up");
    };
    let Some(provider) = sso.providers.get(&provider_id) else {
        return plain(StatusCode::NOT_FOUND, "unknown identity provider");
    };
    let (state, nonce, verifier) = (random_token(24), random_token(24), random_token(32));
    let saved = SsoState {
        provider: provider_id.clone(),
        nonce: nonce.clone(),
        code_verifier: verifier.clone(),
        return_to: safe_return_to(q.return_to.as_deref()),
    };
    if let Err(e) = auth.store.save_sso_state(&state, &saved).await {
        tracing::error!(error = ?e, "saving the sign-in state failed");
        return plain(StatusCode::SERVICE_UNAVAILABLE, "try again later");
    }
    redirect(&provider.authorize_url(
        &state,
        &nonce,
        &code_challenge(&verifier),
        &sso.redirect_uri(&provider_id),
    ))
}

#[derive(Deserialize)]
pub struct CallbackQuery {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

async fn callback<S: Store>(
    State(auth): State<Arc<Auth<S>>>,
    Path(provider_id): Path<String>,
    Query(q): Query<CallbackQuery>,
) -> Response {
    let Some(sso) = &auth.sso else {
        return plain(StatusCode::NOT_FOUND, "single sign-on is not set up");
    };
    // Failures send the person back to the sign-in page with a reason, not a JSON error.
    let back =
        |reason: &str| redirect(&format!("{}/#sso_error={}", sso.web_url, urlencode(reason)));
    if let Some(e) = q.error {
        return back(&format!("the identity provider said: {e}"));
    }
    let (Some(code), Some(state)) = (q.code, q.state) else {
        return back("incomplete answer from the identity provider");
    };
    let saved = match auth
        .store
        .take_sso_state(&state, Duration::seconds(STATE_MAX_AGE_SECS))
        .await
    {
        Ok(Some(s)) if s.provider == provider_id => s,
        Ok(_) => return back("this sign-in link has expired; please try again"),
        Err(e) => {
            tracing::error!(error = ?e, "reading the sign-in state failed");
            return back("try again later");
        }
    };
    let Some(provider) = sso.providers.get(&provider_id) else {
        return back("unknown identity provider");
    };
    let identity = match provider
        .finish(
            &code,
            &saved.code_verifier,
            &saved.nonce,
            &sso.redirect_uri(&provider_id),
        )
        .await
    {
        Ok(i) => i,
        Err(e) => {
            tracing::warn!(provider = provider_id, error = e, "single sign-on failed");
            return back("the identity provider's answer couldn't be verified");
        }
    };
    let email = identity.email.trim().to_lowercase();
    if !identity.email_verified || !email.contains('@') {
        return back("your account at the identity provider has no verified email");
    }
    let name: String = if identity.name.trim().is_empty() {
        email.split('@').next().unwrap_or("").to_string()
    } else {
        identity.name.trim().chars().take(80).collect()
    };
    let user_id = match auth
        .store
        .external_user(&provider_id, &identity.subject, &email, &name, new_id())
        .await
    {
        Ok(id) => id,
        Err(e) => {
            tracing::error!(error = ?e, "linking the external identity failed");
            return back("try again later");
        }
    };
    let tokens = match auth.start_session(user_id, "web").await {
        Ok(t) => t,
        Err(_) => return back("try again later"),
    };
    tracing::info!(
        user_id,
        provider = provider_id,
        "signed in with single sign-on"
    );
    let fragment = format!(
        "access_token={}&refresh_token={}&user_id={}&session_id={}&expires_in={}",
        tokens.access_token,
        tokens.refresh_token,
        tokens.user_id,
        tokens.session_id,
        tokens.expires_in
    );
    redirect(&format!("{}{}#{fragment}", sso.web_url, saved.return_to))
}

fn urlencode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn return_paths_stay_on_our_site() {
        assert_eq!(safe_return_to(Some("/channels/7")), "/channels/7");
        for bad in [
            "https://evil.example",
            "//evil.example",
            "/\\evil",
            "javascript:x",
        ] {
            assert_eq!(safe_return_to(Some(bad)), "/", "{bad}");
        }
        assert_eq!(safe_return_to(None), "/");
    }

    #[test]
    fn pkce_challenge_is_rfc7636_s256() {
        // RFC 7636, Appendix B.
        assert_eq!(
            code_challenge("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
    }
}
