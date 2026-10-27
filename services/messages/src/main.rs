// SPDX-License-Identifier: AGPL-3.0-only
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use tracing_subscriber::EnvFilter;
use xmsg_messages::cassandra::Cassandra;
use xmsg_messages::config::Config;
use xmsg_messages::history;
use xmsg_messages::kafka::{KafkaLog, TOPIC_IN, TOPIC_REACTIONS};
use xmsg_messages::{QUEUE_GROUP, SEND_SUBJECT, handle};

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let config = match Config::from_env() {
        Ok(c) => c,
        Err(e) => return fail(&e),
    };
    let log = match KafkaLog::new(
        &config.kafka_bootstrap,
        Duration::from_millis(config.append_timeout_ms),
    ) {
        Ok(l) => Arc::new(l),
        Err(e) => return fail(&e),
    };

    // Startup check (ADR-0021): never write to a topic with a different partition count.
    match log.partition_count(TOPIC_IN) {
        Ok(n) if n == config.partitions => {}
        Ok(n) => {
            return fail(&format!(
                "{TOPIC_IN} has {n} partitions but XMSG_PARTITIONS is {}; never change the count in place (ADR-0021)",
                config.partitions
            ));
        }
        Err(e) => return fail(&e),
    }

    let client = match async_nats::connect(&config.nats_url).await {
        Ok(c) => c,
        Err(e) => return fail(&format!("connecting to NATS at {}: {e}", config.nats_url)),
    };
    let mut requests = match client
        .queue_subscribe(SEND_SUBJECT, QUEUE_GROUP.to_string())
        .await
    {
        Ok(s) => s,
        Err(e) => return fail(&format!("subscribing to {SEND_SUBJECT}: {e}")),
    };
    // Files in messages (ADR-0024) are checked against Cassandra, which needs the same
    // configuration as history reads; without it, sends with files are refused.
    let mut files = xmsg_messages::attachments::Lookup::Off;
    // History reads over HTTP (plan §5.5), when the auth service's JWKS is configured.
    if let Some(jwks_url) = config.jwks_url.clone() {
        files = match xmsg_messages::attachments::CassandraFiles::connect(
            &config.cql_nodes,
            &config.keyspace,
        )
        .await
        {
            Ok(f) => xmsg_messages::attachments::Lookup::Cassandra(Box::new(f)),
            Err(e) => return fail(&e),
        };
        let store = match Cassandra::connect(&config.cql_nodes, &config.keyspace).await {
            Ok(s) => s,
            Err(e) => return fail(&e),
        };
        let reactions = match KafkaLog::with_topic(
            &config.kafka_bootstrap,
            Duration::from_millis(config.append_timeout_ms),
            TOPIC_REACTIONS,
        ) {
            Ok(l) => Arc::new(l),
            Err(e) => return fail(&e),
        };
        // Same check as messages.in: reactions.in is keyed by channel with the same count.
        match reactions.partition_count(TOPIC_REACTIONS) {
            Ok(n) if n == config.partitions => {}
            Ok(n) => {
                return fail(&format!(
                    "{TOPIC_REACTIONS} has {n} partitions but XMSG_PARTITIONS is {} (ADR-0021)",
                    config.partitions
                ));
            }
            Err(e) => return fail(&e),
        }
        let app = Arc::new(history::App {
            store,
            log: log.clone(),
            reactions,
            // Unset or empty: search is off (503).
            search: std::env::var("XMSG_SEARCH_URL")
                .ok()
                .filter(|url| !url.is_empty())
                .map(xmsg_messages::search::Search::new),
            verifier: match xmsg_authn::Verifier::from_env(jwks_url).await {
                Ok(v) => v,
                Err(e) => return fail(&e),
            },
        });
        let listener = match tokio::net::TcpListener::bind(("0.0.0.0", config.http_port)).await {
            Ok(l) => l,
            Err(e) => return fail(&format!("bind :{}: {e}", config.http_port)),
        };
        tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, history::router(app)).await {
                tracing::error!(error = %e, "history HTTP server stopped");
            }
        });
        tracing::info!(port = config.http_port, "history reads ready");
    }

    let limits = std::sync::Arc::new(xmsg_messages::SendLimits {
        limiter: xmsg_ratelimit::Limiter::from_env("messages").await,
        per_user: match xmsg_ratelimit::Rule::from_env(
            "XMSG_RATE_SENDS",
            xmsg_messages::DEFAULT_SEND_RATE,
        ) {
            Ok(rule) => rule,
            Err(e) => return fail(&e),
        },
    });
    let files = Arc::new(files);
    tracing::info!(
        subject = SEND_SUBJECT,
        queue = QUEUE_GROUP,
        partitions = config.partitions,
        "messages service ready"
    );

    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            msg = requests.next() => {
                let Some(msg) = msg else { break };
                let Some(reply_to) = msg.reply.clone() else { continue };
                let (log, client, limits, files) =
                    (log.clone(), client.clone(), limits.clone(), files.clone());
                tokio::spawn(async move {
                    let reply = handle(log.as_ref(), &limits, files.as_ref(), &msg.payload).await;
                    if let Err(e) = client.publish(reply_to, reply.into()).await {
                        tracing::warn!(error = %e, "reply failed");
                    }
                });
            }
            _ = &mut shutdown => break,
        }
    }
    tracing::info!("shutting down");
    let _ = requests.unsubscribe().await;
    let _ = client.flush().await;
    log.flush();
    ExitCode::SUCCESS
}

fn fail(message: &str) -> ExitCode {
    eprintln!("xmsg-messages: {message}");
    ExitCode::from(2)
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
