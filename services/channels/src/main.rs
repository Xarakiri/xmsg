// SPDX-License-Identifier: AGPL-3.0-only
//! Channels service entry point.

use std::sync::Arc;
use std::time::Duration;

use xmsg_channels::api::{App, router};
use xmsg_channels::commands::Pending;
use xmsg_channels::config::Config;
use xmsg_channels::kafka::{KafkaLog, TOPIC_IN, follow_results};
use xmsg_channels::memberships::Cassandra;
use xmsg_channels::store::Postgres;

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
    let log = KafkaLog::new(&config.kafka_bootstrap, Duration::from_millis(1_500))
        .unwrap_or_else(|e| fail(&e));
    match log.partition_count(TOPIC_IN) {
        Ok(n) if n == config.partitions => {}
        Ok(n) => fail(&format!(
            "{TOPIC_IN} has {n} partitions but XMSG_PARTITIONS is {} (ADR-0021)",
            config.partitions
        )),
        Err(e) => fail(&e),
    }
    let memberships = Cassandra::connect(&config.cql_nodes, &config.keyspace)
        .await
        .unwrap_or_else(|e| fail(&e));
    let pending = Arc::new(Pending::default());
    follow_results(&config.kafka_bootstrap, pending.clone())
        .await
        .unwrap_or_else(|e| fail(&e));
    let app = Arc::new(App {
        store,
        log,
        memberships,
        pending,
        verifier: xmsg_authn::Verifier::new(config.jwks_url),
        command_timeout: Duration::from_millis(config.command_timeout_ms),
    });
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", config.port))
        .await
        .unwrap_or_else(|e| fail(&format!("bind :{}: {e}", config.port)));
    tracing::info!(port = config.port, "channels ready");
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
