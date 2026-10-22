// SPDX-License-Identifier: AGPL-3.0-only
//! xmsg-outbox-relay: the built-in outbox poller (Small/Medium tiers, ADR-0020).

use std::process::ExitCode;
use std::time::{Duration, Instant};

use futures_util::future::try_join_all;
use rdkafka::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use tracing_subscriber::EnvFilter;
use xmsg_outbox::postgres::Leader;
use xmsg_outbox::{OutboxRow, key_for, known_aggregate_types, topic_for};

const BATCH: i64 = 500;
const KEEP_DAYS: i32 = 7;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let get =
        |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.to_string());
    let database_url = get(
        "XMSG_DATABASE_URL",
        "postgres://xmsg:xmsg-dev-only@127.0.0.1:5432/xmsg",
    );
    let bootstrap = get("XMSG_KAFKA_BOOTSTRAP", "127.0.0.1:9092");
    let poll = Duration::from_millis(get("XMSG_OUTBOX_POLL_MS", "200").parse().unwrap_or(200));

    let producer: FutureProducer = match ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("linger.ms", "5")
        // Kafka's Java default partitioner, so partitions match spec/partitioning (ADR-0004).
        .set("partitioner", "murmur2_random")
        .set("message.timeout.ms", "10000")
        .create()
    {
        Ok(p) => p,
        Err(e) => return fail(&format!("creating Kafka producer: {e}")),
    };

    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    let known = known_aggregate_types();
    let mut announced_standby = false;

    'outer: loop {
        // Become the active poller, or wait for the one that is.
        let mut leader = loop {
            match Leader::try_acquire(&database_url).await {
                Ok(Some(leader)) => break leader,
                Ok(None) if !announced_standby => {
                    tracing::info!("another outbox relay is active; standing by");
                    announced_standby = true;
                }
                Ok(None) => {}
                Err(e) => tracing::warn!(error = %e, "connecting to Postgres"),
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(2)) => {}
                _ = &mut shutdown => break 'outer,
            }
        };
        tracing::info!("outbox relay ready (active)");
        let mut last_cleanup = Instant::now() - Duration::from_secs(3600);
        let mut last_lag_log = Instant::now();

        loop {
            match relay_batch(&mut leader, &producer, &known).await {
                // A full batch: more are waiting, go again at once.
                Ok(n) if n as i64 == BATCH => continue,
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(error = %e, "relay batch failed; will retry");
                    if e.starts_with("postgres") {
                        // The connection (and with it the lock) may be gone: start over.
                        continue 'outer;
                    }
                }
            }
            if last_cleanup.elapsed() >= Duration::from_secs(3600) {
                match leader.delete_published_before(KEEP_DAYS).await {
                    Ok(n) if n > 0 => tracing::info!(deleted = n, "old outbox rows removed"),
                    Ok(_) => {}
                    Err(e) => tracing::warn!(error = %e, "outbox cleanup failed"),
                }
                last_cleanup = Instant::now();
            }
            if last_lag_log.elapsed() >= Duration::from_secs(60) {
                if let Ok(lag) = leader.lag_secs().await
                    && lag > 5.0
                {
                    tracing::warn!(lag_secs = lag, "outbox is behind");
                }
                last_lag_log = Instant::now();
            }
            tokio::select! {
                _ = tokio::time::sleep(poll) => {}
                _ = &mut shutdown => break 'outer,
            }
        }
    }
    tracing::info!("shutting down");
    ExitCode::SUCCESS
}

/// Publishes one batch in commit order and marks it published once Kafka has every row.
async fn relay_batch(
    leader: &mut Leader,
    producer: &FutureProducer,
    known: &[String],
) -> Result<usize, String> {
    let rows = leader
        .unpublished(known, BATCH)
        .await
        .map_err(|e| format!("postgres: {e}"))?;
    if rows.is_empty() {
        return Ok(0);
    }
    // Sent concurrently but created in order: the idempotent producer keeps per-partition order.
    let sends = rows.iter().map(|row| send(producer, row));
    try_join_all(sends).await?;
    let ids: Vec<i64> = rows.iter().map(|r| r.id).collect();
    leader
        .mark_published(&ids)
        .await
        .map_err(|e| format!("postgres: {e}"))?;
    tracing::debug!(
        count = rows.len(),
        last_id = ids.last(),
        "outbox rows published"
    );
    Ok(rows.len())
}

async fn send(producer: &FutureProducer, row: &OutboxRow) -> Result<(), String> {
    let topic = topic_for(&row.aggregate_type)
        .ok_or_else(|| format!("no topic for {}", row.aggregate_type))?;
    let key = key_for(row);
    let id = row.id.to_string();
    let headers = OwnedHeaders::new()
        .insert(Header {
            key: "outbox_id",
            value: Some(&id),
        })
        .insert(Header {
            key: "event_type",
            value: Some(&row.event_type),
        });
    let record = FutureRecord::to(topic)
        .key(&key)
        .payload(&row.payload)
        .headers(headers);
    producer
        .send(record, Duration::from_secs(10))
        .await
        .map(|_| ())
        .map_err(|(e, _)| format!("kafka: publishing outbox row {} to {topic}: {e}", row.id))
}

fn fail(message: &str) -> ExitCode {
    eprintln!("xmsg-outbox-relay: {message}");
    ExitCode::from(2)
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
