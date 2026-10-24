// SPDX-License-Identifier: LicenseRef-xmsg-EE
//! OpenID Connect single sign-on (Enterprise Edition, ADR-0022): an `IdentityProvider`
//! for the auth service. Authorization code flow with PKCE; the ID token is verified
//! against the provider's published keys (signature, issuer, audience, expiry, nonce).
//!
//! Works with any OIDC provider that publishes a discovery document: Okta, Entra ID,
//! Google Workspace, Keycloak, Auth0, ...

use std::sync::Arc;

use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{DecodingKey, Validation, decode, decode_header};
use serde::Deserialize;
use tokio::sync::RwLock;
use xmsg_auth::sso::{BoxFuture, ExternalIdentity, IdentityProvider};

#[derive(Debug, Clone)]
pub struct Config {
    /// URL-safe ID used in paths, e.g. `okta`.
    pub id: String,
    /// The sign-in button's text, e.g. "Okta".
    pub display_name: String,
    /// The issuer, e.g. https://acme.okta.com (its discovery document is at
    /// `{issuer}/.well-known/openid-configuration`).
    pub issuer: String,
    pub client_id: String,
    pub client_secret: String,
    /// Some providers don't send `email_verified` (they verify every address). Only set
    /// this for such a provider.
    pub trust_unverified_email: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct Discovery {
    issuer: String,
    authorization_endpoint: String,
    token_endpoint: String,
    jwks_uri: String,
}

#[derive(Deserialize)]
struct TokenResponse {
    id_token: String,
}

#[derive(Deserialize)]
struct Claims {
    sub: String,
    #[serde(default)]
    email: String,
    #[serde(default)]
    email_verified: Option<bool>,
    #[serde(default)]
    name: String,
    #[serde(default)]
    nonce: String,
}

pub struct Oidc {
    config: Config,
    discovery: Discovery,
    keys: Arc<RwLock<JwkSet>>,
    http: reqwest::Client,
}

fn encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

impl Oidc {
    /// Reads the provider's discovery document and keys; fails if they can't be read.
    pub async fn discover(config: Config) -> Result<Oidc, String> {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|e| e.to_string())?;
        let url = format!(
            "{}/.well-known/openid-configuration",
            config.issuer.trim_end_matches('/')
        );
        let discovery: Discovery = http
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("{url}: {e}"))?
            .error_for_status()
            .map_err(|e| format!("{url}: {e}"))?
            .json()
            .await
            .map_err(|e| format!("{url}: {e}"))?;
        if discovery.issuer.trim_end_matches('/') != config.issuer.trim_end_matches('/') {
            return Err(format!(
                "the discovery document names issuer {}, not {}",
                discovery.issuer, config.issuer
            ));
        }
        let keys = fetch_keys(&http, &discovery.jwks_uri).await?;
        Ok(Oidc {
            config,
            discovery,
            keys: Arc::new(RwLock::new(keys)),
            http,
        })
    }

    async fn key_for(&self, kid: Option<&str>) -> Result<DecodingKey, String> {
        let find = |set: &JwkSet| match kid {
            Some(kid) => set.find(kid).cloned(),
            None => set.keys.first().cloned(),
        };
        if let Some(jwk) = find(&*self.keys.read().await) {
            return DecodingKey::from_jwk(&jwk).map_err(|e| e.to_string());
        }
        // An unknown key ID: the provider may have rotated its keys.
        let fresh = fetch_keys(&self.http, &self.discovery.jwks_uri).await?;
        let jwk = find(&fresh).ok_or("the ID token's key isn't published by the provider")?;
        *self.keys.write().await = fresh;
        DecodingKey::from_jwk(&jwk).map_err(|e| e.to_string())
    }

    async fn verify(
        &self,
        code: &str,
        verifier: &str,
        nonce: &str,
        redirect_uri: &str,
    ) -> Result<ExternalIdentity, String> {
        let response = self
            .http
            .post(&self.discovery.token_endpoint)
            .form(&[
                ("grant_type", "authorization_code"),
                ("code", code),
                ("redirect_uri", redirect_uri),
                ("client_id", self.config.client_id.as_str()),
                ("client_secret", self.config.client_secret.as_str()),
                ("code_verifier", verifier),
            ])
            .send()
            .await
            .map_err(|e| format!("token endpoint: {e}"))?;
        if !response.status().is_success() {
            return Err(format!("token endpoint answered {}", response.status()));
        }
        let tokens: TokenResponse = response
            .json()
            .await
            .map_err(|e| format!("token response: {e}"))?;
        let header = decode_header(&tokens.id_token).map_err(|e| format!("ID token: {e}"))?;
        let key = self.key_for(header.kid.as_deref()).await?;
        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&[self.discovery.issuer.as_str()]);
        validation.set_audience(&[self.config.client_id.as_str()]);
        validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
        let claims = decode::<Claims>(&tokens.id_token, &key, &validation)
            .map_err(|e| format!("ID token: {e}"))?
            .claims;
        if claims.nonce != nonce {
            return Err("ID token nonce doesn't match this sign-in".into());
        }
        Ok(ExternalIdentity {
            subject: claims.sub,
            email: claims.email,
            email_verified: claims
                .email_verified
                .unwrap_or(self.config.trust_unverified_email),
            name: claims.name,
        })
    }
}

async fn fetch_keys(http: &reqwest::Client, url: &str) -> Result<JwkSet, String> {
    http.get(url)
        .send()
        .await
        .map_err(|e| format!("{url}: {e}"))?
        .error_for_status()
        .map_err(|e| format!("{url}: {e}"))?
        .json()
        .await
        .map_err(|e| format!("{url}: {e}"))
}

impl IdentityProvider for Oidc {
    fn id(&self) -> &str {
        &self.config.id
    }

    fn display_name(&self) -> &str {
        &self.config.display_name
    }

    fn authorize_url(
        &self,
        state: &str,
        nonce: &str,
        code_challenge: &str,
        redirect_uri: &str,
    ) -> String {
        let endpoint = &self.discovery.authorization_endpoint;
        let sep = if endpoint.contains('?') { '&' } else { '?' };
        format!(
            "{endpoint}{sep}response_type=code&scope={}&client_id={}&redirect_uri={}&state={}&nonce={}&code_challenge={}&code_challenge_method=S256",
            encode("openid email profile"),
            encode(&self.config.client_id),
            encode(redirect_uri),
            encode(state),
            encode(nonce),
            encode(code_challenge),
        )
    }

    fn finish<'a>(
        &'a self,
        code: &'a str,
        code_verifier: &'a str,
        nonce: &'a str,
        redirect_uri: &'a str,
    ) -> BoxFuture<'a, Result<ExternalIdentity, String>> {
        Box::pin(self.verify(code, code_verifier, nonce, redirect_uri))
    }
}
