// SPDX-License-Identifier: AGPL-3.0-only
//! xmsg-search-indexer: `messages.persisted` → OpenSearch (plan §5.12).

use std::process::ExitCode;
use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::Message as _;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use tracing_subscriber::EnvFilter;
use xmsg_proto::Message;
use xmsg_proto::v1::SequencedEvent;
use xmsg_search_indexer::{INDEX, bulk_lines, bulk_ok, mapping};

const TOPIC: &str = "messages.persisted";
/// Send a bulk request at least this often, or when this many lines are waiting.
const FLUSH_EVERY: Duration = Duration::from_secs(1);
const FLUSH_AT_LINES: usize = 2_000;

fn fail(message: &str) -> ExitCode {
    eprintln!("xmsg-search-indexer: {message}");
    ExitCode::from(2)
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let get =
        |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.to_string());
    let bootstrap = get("XMSG_KAFKA_BOOTSTRAP", "127.0.0.1:9092");
    let opensearch = get("XMSG_OPENSEARCH_URL", "http://127.0.0.1:9200")
        .trim_end_matches('/')
        .to_string();
    let group = get("XMSG_SEARCH_INDEXER_GROUP", "xmsg-search-indexer");
    let http = reqwest::Client::new();

    // The index, created with its mapping on first start (an existing one is kept).
    loop {
        let created = http
            .put(format!("{opensearch}/{INDEX}"))
            .header("content-type", "application/json")
            .body(mapping().to_string())
            .send()
            .await;
        match created {
            Ok(r) if r.status().is_success() || r.status() == 400 => break, // 400: already exists
            Ok(r) => tracing::warn!(status = %r.status(), "creating the index; retrying"),
            Err(e) => tracing::warn!(error = %e, "OpenSearch not reachable yet; retrying"),
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    let consumer: StreamConsumer = match ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("group.id", &group)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("isolation.level", "read_committed")
        .create()
    {
        Ok(c) => c,
        Err(e) => return fail(&format!("creating Kafka consumer: {e}")),
    };
    if let Err(e) = consumer.subscribe(&[TOPIC]) {
        return fail(&format!("subscribing to {TOPIC}: {e}"));
    }
    tracing::info!(index = INDEX, "search indexer ready");

    let mut lines: Vec<String> = Vec::new();
    let mut tick = tokio::time::interval(FLUSH_EVERY);
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            received = consumer.recv() => match received {
                Ok(message) => {
                    if let Some(Ok(event)) = message.payload().map(SequencedEvent::decode) {
                        let at = message.timestamp().to_millis().unwrap_or(0);
                        lines.extend(bulk_lines(&event, at).into_iter().map(|l| l.to_string()));
                    }
                    let _ = consumer.store_offset_from_message(&message);
                    if lines.len() < FLUSH_AT_LINES {
                        continue;
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Kafka receive error");
                    continue;
                }
            },
            _ = tick.tick() => {}
            _ = &mut shutdown => break,
        }
        if !lines.is_empty() {
            // Retried until OpenSearch takes the whole batch: offsets are committed after.
            let body = lines.join("\n") + "\n";
            loop {
                match bulk(&http, &opensearch, &body).await {
                    Ok(()) => break,
                    Err(e) => {
                        tracing::warn!(error = %e, "bulk indexing failed; retrying");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
            lines.clear();
        }
        if let Err(e) = consumer.commit_consumer_state(CommitMode::Async) {
            tracing::debug!(error = %e, "offset commit skipped");
        }
    }
    tracing::info!("shutting down");
    ExitCode::SUCCESS
}

async fn bulk(http: &reqwest::Client, opensearch: &str, body: &str) -> Result<(), String> {
    let response = http
        .post(format!("{opensearch}/_bulk"))
        .header("content-type", "application/x-ndjson")
        .body(body.to_string())
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let status = response.status();
    let text = response.text().await.map_err(|e| e.to_string())?;
    if !status.is_success() {
        return Err(format!("{status}: {text}"));
    }
    bulk_ok(&serde_json::from_str(&text).map_err(|e| e.to_string())?)
}

/// SIGINT or SIGTERM (Kubernetes and systemd stop services with SIGTERM).
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("installing the SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}
