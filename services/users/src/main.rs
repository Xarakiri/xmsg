// SPDX-License-Identifier: AGPL-3.0-only
//! Users service entry point.

use std::sync::Arc;

use xmsg_users::api::{App, router};
use xmsg_users::config::Config;
use xmsg_users::store::Postgres;

fn fail(message: &str) -> ! {
    eprintln!("{message}");
    std::process::exit(2);
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let config = Config::from_env().unwrap_or_else(|e| fail(&format!("config: {e}")));
    let store = Postgres::connect(&config.database_url)
        .await
        .unwrap_or_else(|e| fail(&format!("postgres: {e}")));
    let app = Arc::new(App {
        store,
        verifier: xmsg_authn::Verifier::from_env(config.jwks_url)
            .await
            .unwrap_or_else(|e| fail(&format!("revocations: {e}"))),
    });
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", config.port))
        .await
        .unwrap_or_else(|e| fail(&format!("bind :{}: {e}", config.port)));
    tracing::info!(port = config.port, "users ready");
    axum::serve(listener, router(app))
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
