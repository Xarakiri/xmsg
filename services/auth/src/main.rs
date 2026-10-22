// SPDX-License-Identifier: AGPL-3.0-only
//! Auth service entry point.

use std::sync::Arc;

use xmsg_auth::api::{Auth, router};
use xmsg_auth::config::Config;
use xmsg_auth::revocation;
use xmsg_auth::store::Postgres;

#[tokio::main]
async fn main() {
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
    let auth = Arc::new(Auth {
        store,
        keys: config.keys,
        access_ttl_secs: config.access_ttl_secs,
        refresh_ttl_secs: config.refresh_ttl_secs,
        reuse_grace_secs: config.reuse_grace_secs,
        revocations,
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
