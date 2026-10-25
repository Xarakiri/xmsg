// SPDX-License-Identifier: AGPL-3.0-only
//! Auth service (plan §5.2): sign-up, login, short-lived Ed25519 access tokens,
//! rotating refresh tokens with reuse detection, and a JWKS endpoint so the
//! gateway and Envoy validate tokens locally.

pub mod api;
pub mod config;
pub mod keys;
pub mod password;
pub mod revocation;
pub mod sso;
pub mod sso_policy;
pub mod store;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use api::{Auth, router};
use config::Config;
use store::Postgres;

/// Time-ordered 63-bit IDs: milliseconds since 2026-01-01 in the high bits,
/// 22 random bits below (no coordination needed for users, devices, tokens).
pub fn new_id() -> i64 {
    const EPOCH_MS: u64 = 1_767_225_600_000;
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let mut random = [0u8; 4];
    getrandom::fill(&mut random).expect("OS random");
    let low = u32::from_le_bytes(random) as u64 & ((1 << 22) - 1);
    (((ms - EPOCH_MS) << 22) | low) as i64
}

/// Random bytes as base64url (no padding), for opaque tokens.
pub fn random_token(bytes: usize) -> String {
    use base64::Engine;
    let mut buf = vec![0u8; bytes];
    getrandom::fill(&mut buf).expect("OS random");
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(buf)
}

/// Refresh tokens are stored only as SHA-256 hashes.
pub fn token_hash(token: &str) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(token.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// What the Enterprise Edition adds to the auth service; the Community Edition passes
/// `Extensions::default()`.
#[derive(Default)]
pub struct Extensions {
    /// External identity providers (single sign-on).
    pub providers: Vec<std::sync::Arc<dyn sso::IdentityProvider>>,
    /// Whether workspaces may change their SSO policy (the licence allows configuration).
    pub sso_policy_editable: bool,
    /// A directory (LDAP) that the password form checks before local passwords.
    pub directory: Option<std::sync::Arc<dyn sso::Directory>>,
}

/// Runs the auth service: config from the environment, Postgres, the HTTP API, plus
/// whatever the Enterprise Edition passes in.
pub async fn serve(extensions: Extensions) {
    let Extensions {
        providers,
        sso_policy_editable,
        directory,
    } = extensions;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let config = Config::from_env().unwrap_or_else(|e| {
        eprintln!("config: {e}");
        std::process::exit(2);
    });
    let store = Postgres::connect(&config.database_url)
        .await
        .unwrap_or_else(|e| {
            eprintln!("postgres: {e}");
            std::process::exit(1);
        });
    let revocations = match &config.nats_url {
        Some(url) => {
            let client = async_nats::connect(url).await.unwrap_or_else(|e| {
                eprintln!("nats {url}: {e}");
                std::process::exit(1);
            });
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
            tokio::spawn(async move {
                while let Some(event) = rx.recv().await {
                    // Best effort: the outbox copy reaches gateways via meta-relay anyway.
                    if let Err(e) = client.publish(revocation::SUBJECT, event.into()).await {
                        tracing::warn!(error = %e, "publishing a revocation failed; the outbox will deliver it");
                    }
                }
            });
            Some(tx)
        }
        None => {
            tracing::warn!(
                "XMSG_NATS_URL not set: revocations reach gateways through the outbox only"
            );
            None
        }
    };
    let sso = if providers.is_empty() {
        None
    } else {
        let public_url = std::env::var("XMSG_AUTH_PUBLIC_URL").unwrap_or_else(|_| {
            eprintln!("XMSG_AUTH_PUBLIC_URL must be set for single sign-on (the auth service as browsers reach it)");
            std::process::exit(2);
        });
        let web_url = std::env::var("XMSG_WEB_URL").unwrap_or_else(|_| {
            eprintln!("XMSG_WEB_URL must be set for single sign-on (where signed-in users land)");
            std::process::exit(2);
        });
        tracing::info!(providers = providers.len(), "single sign-on enabled");
        let mut sso = sso::Sso::new(providers, public_url, web_url);
        sso.policy_editable = sso_policy_editable;
        Some(sso)
    };
    let auth = Arc::new(Auth {
        store,
        keys: config.keys,
        access_ttl_secs: config.access_ttl_secs,
        refresh_ttl_secs: config.refresh_ttl_secs,
        reuse_grace_secs: config.reuse_grace_secs,
        revocations,
        sso,
        directory,
    });
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", config.port))
        .await
        .unwrap_or_else(|e| {
            eprintln!("bind :{}: {e}", config.port);
            std::process::exit(1);
        });
    tracing::info!(port = config.port, "auth ready");
    axum::serve(listener, router(auth))
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server");
}

/// SIGINT or SIGTERM. Kubernetes and systemd stop services with SIGTERM; without a
/// handler the process dies on the spot and skips the clean shutdown below.
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("installing the SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_positive_and_increase_over_time() {
        let a = new_id();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let b = new_id();
        assert!(a > 0 && b > a);
    }

    #[test]
    fn tokens_are_random_and_hashes_are_stable() {
        let (a, b) = (random_token(32), random_token(32));
        assert_ne!(a, b);
        assert_eq!(a.len(), 43);
        assert_eq!(token_hash(&a), token_hash(&a));
        assert_ne!(token_hash(&a), token_hash(&b));
        assert_eq!(token_hash(&a).len(), 64);
    }
}
