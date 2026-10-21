// SPDX-License-Identifier: AGPL-3.0-only
//! Auth service entry point.

use std::sync::Arc;

use xmsg_auth::api::{Auth, router};
use xmsg_auth::config::Config;
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
    let auth = Arc::new(Auth {
        store,
        keys: config.keys,
        access_ttl_secs: config.access_ttl_secs,
        refresh_ttl_secs: config.refresh_ttl_secs,
    });
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", config.port))
        .await
        .unwrap_or_else(|e| {
            eprintln!("bind :{}: {e}", config.port);
            std::process::exit(1);
        });
    tracing::info!(port = config.port, "auth ready");
    axum::serve(listener, router(auth))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .expect("server");
}
