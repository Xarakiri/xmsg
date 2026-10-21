// SPDX-License-Identifier: AGPL-3.0-only
//! Access-token validation for HTTP services (plan §5.2): the auth service's
//! Ed25519 JWTs, checked locally against its JWKS. Same rules as the gateway
//! (`XmsgGateway.Auth`): `alg` EdDSA with a known `kid`, `iss`/`aud` = `xmsg`,
//! `exp` with 30 s leeway, numeric `sub`.
//!
//! Keys are fetched on first use, refreshed every 5 minutes, and early when a
//! token names an unknown `kid` (at most every 30 s), so key rotation needs no restart.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use tokio::sync::RwLock;

const REFRESH: Duration = Duration::from_secs(300);
const MIN_REFRESH: Duration = Duration::from_secs(30);

/// Who a valid token belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Principal {
    pub user_id: i64,
    /// Session (device) ID.
    pub session_id: i64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    /// Malformed, expired, wrongly signed or for another audience.
    Invalid,
    /// Signed with a key we don't have, even after refreshing.
    UnknownKey,
    /// The JWKS couldn't be fetched (callers answer 503, not 401).
    KeysUnavailable,
}

#[derive(Deserialize)]
struct Claims {
    sub: String,
    sid: i64,
}

#[derive(Deserialize)]
struct Jwks {
    keys: Vec<Jwk>,
}

#[derive(Deserialize)]
struct Jwk {
    kty: String,
    #[serde(default)]
    crv: String,
    #[serde(default)]
    kid: String,
    #[serde(default)]
    x: String,
    #[serde(rename = "use", default)]
    use_: Option<String>,
}

struct Keys {
    by_kid: HashMap<String, DecodingKey>,
    fetched_at: Option<Instant>,
}

pub struct Verifier {
    jwks_url: String,
    http: reqwest::Client,
    keys: RwLock<Keys>,
    validation: Validation,
}

impl Verifier {
    pub fn new(jwks_url: impl Into<String>) -> Self {
        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.set_issuer(&["xmsg"]);
        validation.set_audience(&["xmsg"]);
        validation.leeway = 30;
        Verifier {
            jwks_url: jwks_url.into(),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .build()
                .expect("HTTP client"),
            keys: RwLock::new(Keys {
                by_kid: HashMap::new(),
                fetched_at: None,
            }),
            validation,
        }
    }

    /// Validates `Authorization: Bearer <token>` header values or bare tokens.
    pub async fn verify(&self, token: &str) -> Result<Principal, Error> {
        let token = token.strip_prefix("Bearer ").unwrap_or(token).trim();
        let header = jsonwebtoken::decode_header(token).map_err(|_| Error::Invalid)?;
        if header.alg != Algorithm::EdDSA {
            return Err(Error::Invalid);
        }
        let kid = header.kid.ok_or(Error::Invalid)?;
        let claims = {
            let key = self.key(&kid).await?;
            jsonwebtoken::decode::<Claims>(token, &key, &self.validation)
                .map_err(|_| Error::Invalid)?
                .claims
        };
        match claims.sub.parse::<i64>() {
            Ok(user_id) if user_id > 0 => Ok(Principal {
                user_id,
                session_id: claims.sid,
            }),
            _ => Err(Error::Invalid),
        }
    }

    async fn key(&self, kid: &str) -> Result<DecodingKey, Error> {
        {
            let keys = self.keys.read().await;
            let fresh = keys.fetched_at.is_some_and(|t| t.elapsed() < REFRESH);
            if let Some(key) = keys.by_kid.get(kid).filter(|_| fresh) {
                return Ok(key.clone());
            }
        }
        let mut keys = self.keys.write().await;
        // Another task may have refreshed while we waited for the lock.
        let since = keys.fetched_at.map(|t| t.elapsed());
        let stale = since.is_none_or(|s| s >= REFRESH);
        let unknown = !keys.by_kid.contains_key(kid);
        if stale || (unknown && since.is_none_or(|s| s >= MIN_REFRESH)) {
            match self.fetch().await {
                Ok(fetched) => {
                    keys.by_kid = fetched;
                    keys.fetched_at = Some(Instant::now());
                }
                // Keep serving the last known keys while the auth service is unreachable.
                Err(e) if !keys.by_kid.is_empty() => {
                    tracing::warn!(error = %e, "JWKS refresh failed")
                }
                Err(e) => {
                    tracing::warn!(error = %e, "JWKS fetch failed");
                    return Err(Error::KeysUnavailable);
                }
            }
        }
        keys.by_kid.get(kid).cloned().ok_or(Error::UnknownKey)
    }

    async fn fetch(&self) -> Result<HashMap<String, DecodingKey>, String> {
        let jwks: Jwks = self
            .http
            .get(&self.jwks_url)
            .send()
            .await
            .and_then(|r| r.error_for_status())
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;
        Ok(jwks
            .keys
            .into_iter()
            .filter(|k| {
                k.kty == "OKP" && k.crv == "Ed25519" && k.use_.as_deref().unwrap_or("sig") == "sig"
            })
            .filter_map(|k| {
                DecodingKey::from_ed_components(&k.x)
                    .ok()
                    .map(|key| (k.kid, key))
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use xmsg_auth::keys::Keys as SigningKeys;

    /// Serves `keys`' JWKS; returns the URL and a fetch counter.
    async fn serve(keys: Arc<std::sync::Mutex<Vec<SigningKeys>>>) -> (String, Arc<AtomicUsize>) {
        let hits = Arc::new(AtomicUsize::new(0));
        let counter = hits.clone();
        let app = axum::Router::new().route(
            "/jwks",
            axum::routing::get(move || {
                counter.fetch_add(1, Ordering::SeqCst);
                let all: Vec<serde_json::Value> = keys
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|k| k.jwks()["keys"][0].clone())
                    .collect();
                async move { axum::Json(serde_json::json!({ "keys": all })) }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/jwks", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (url, hits)
    }

    fn now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    #[tokio::test]
    async fn valid_tokens_pass_and_others_fail() {
        let signer = SigningKeys::generate();
        let token = signer.issue(42, 7, now(), 900);
        let keys = Arc::new(std::sync::Mutex::new(vec![signer]));
        let (url, hits) = serve(keys.clone()).await;
        let verifier = Verifier::new(url);

        assert_eq!(
            verifier.verify(&format!("Bearer {token}")).await,
            Ok(Principal {
                user_id: 42,
                session_id: 7
            })
        );
        assert_eq!(verifier.verify(&token).await.unwrap().user_id, 42);
        assert_eq!(hits.load(Ordering::SeqCst), 1, "keys are cached");

        let expired = keys.lock().unwrap()[0].issue(42, 7, now() - 3_600, 900);
        assert_eq!(verifier.verify(&expired).await, Err(Error::Invalid));
        let mut tampered = token.clone();
        tampered.replace_range(token.len() - 4.., "AAAA");
        assert_eq!(verifier.verify(&tampered).await, Err(Error::Invalid));
        assert_eq!(verifier.verify("garbage").await, Err(Error::Invalid));
    }

    #[tokio::test]
    async fn a_rotated_key_is_picked_up_but_refetches_are_rate_limited() {
        let first = SigningKeys::generate();
        let first_token = first.issue(1, 1, now(), 900);
        let keys = Arc::new(std::sync::Mutex::new(vec![first]));
        let (url, hits) = serve(keys.clone()).await;
        let verifier = Verifier::new(url);
        assert!(verifier.verify(&first_token).await.is_ok());

        // A token from a key nobody published: one refetch at most, then UnknownKey.
        let stranger = SigningKeys::generate().issue(1, 1, now(), 900);
        assert_eq!(verifier.verify(&stranger).await, Err(Error::UnknownKey));
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "within 30 s of a fetch an unknown kid doesn't refetch"
        );

        // Pretend the last fetch was a minute ago; the newly published key is then fetched.
        let second = SigningKeys::generate();
        let second_token = second.issue(2, 2, now(), 900);
        keys.lock().unwrap().push(second);
        verifier.keys.write().await.fetched_at = Some(Instant::now() - Duration::from_secs(60));
        assert_eq!(verifier.verify(&second_token).await.unwrap().user_id, 2);
        assert_eq!(hits.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn an_unreachable_auth_service_is_not_an_invalid_token() {
        let token = SigningKeys::generate().issue(1, 1, now(), 900);
        let verifier = Verifier::new("http://127.0.0.1:9/jwks");
        assert_eq!(verifier.verify(&token).await, Err(Error::KeysUnavailable));
    }
}
