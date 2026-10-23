// SPDX-License-Identifier: AGPL-3.0-only
//! xmsg-reaction-batcher: `reactions.in` → one `ReactionBatch` per (message, emoji) per
//! second → `messages.in` (ADR-0015).

use std::process::ExitCode;
use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::Message as _;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use tracing_subscriber::EnvFilter;
use xmsg_proto::Message;
use xmsg_proto::v1::ReactionCommand;
use xmsg_reaction_batcher::Pending;

const TOPIC_REACTIONS: &str = "reactions.in";
const TOPIC_IN: &str = "messages.in";
/// Flush at least this often (ADR-0015: reactions appear for others within ~1 s).
const FLUSH_EVERY: Duration = Duration::from_secs(1);
/// ...or sooner when this many operations are waiting.
const FLUSH_AT_OPS: usize = 10_000;

fn fail(message: &str) -> ExitCode {
    eprintln!("xmsg-reaction-batcher: {message}");
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
    let group = get("XMSG_REACTION_BATCHER_GROUP", "xmsg-reaction-batcher");
    let Some(partitions) = std::env::var("XMSG_PARTITIONS")
        .ok()
        .and_then(|p| p.parse::<usize>().ok())
    else {
        return fail("XMSG_PARTITIONS must be set to this installation's partition count");
    };

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
    let producer: FutureProducer = match ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("linger.ms", "5")
        // Kafka's Java default partitioner, so partitions match spec/partitioning (ADR-0004).
        .set("partitioner", "murmur2_random")
        .create()
    {
        Ok(p) => p,
        Err(e) => return fail(&format!("creating Kafka producer: {e}")),
    };

    // Startup check (ADR-0021): both topics are keyed by channel with the same count.
    for topic in [TOPIC_REACTIONS, TOPIC_IN] {
        match consumer.fetch_metadata(Some(topic), Duration::from_secs(10)) {
            Ok(md) => match md.topics().first() {
                Some(t) if t.error().is_none() && t.partitions().len() == partitions => {}
                Some(t) if t.error().is_none() => {
                    return fail(&format!(
                        "{topic} has {} partitions but XMSG_PARTITIONS is {partitions} (ADR-0021)",
                        t.partitions().len()
                    ));
                }
                _ => return fail(&format!("topic {topic} does not exist")),
            },
            Err(e) => return fail(&format!("fetching metadata for {topic}: {e}")),
        }
    }
    if let Err(e) = consumer.subscribe(&[TOPIC_REACTIONS]) {
        return fail(&format!("subscribing to {TOPIC_REACTIONS}: {e}"));
    }
    tracing::info!(group, "reaction-batcher ready");

    let mut pending = Pending::default();
    let mut tick = tokio::time::interval(FLUSH_EVERY);
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            received = consumer.recv() => match received {
                Ok(message) => {
                    match message.payload().map(ReactionCommand::decode) {
                        Some(Ok(command)) => pending.add(command),
                        _ => tracing::warn!(offset = message.offset(), "undecodable reaction command skipped"),
                    }
                    // Committed only after the batches holding it are in messages.in.
                    let _ = consumer.store_offset_from_message(&message);
                    if pending.ops() < FLUSH_AT_OPS {
                        continue;
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Kafka receive error");
                    continue;
                }
            },
            _ = tick.tick() => {}
            _ = &mut shutdown => {
                flush(&producer, &mut pending).await;
                let _ = consumer.commit_consumer_state(CommitMode::Sync);
                break;
            }
        }
        if !pending.is_empty() {
            flush(&producer, &mut pending).await;
        }
        if let Err(e) = consumer.commit_consumer_state(CommitMode::Async) {
            // Nothing stored since the last commit is the usual cause; anything else means
            // the commands are batched again after a restart, which the counters make harmless.
            tracing::debug!(error = %e, "offset commit skipped");
        }
    }
    tracing::info!("shutting down");
    ExitCode::SUCCESS
}

/// Sends every pending batch, retrying each until Kafka has it (at least once: the
/// sequencer ignores operations it has already applied).
async fn flush(producer: &FutureProducer, pending: &mut Pending) {
    for event in pending.drain() {
        let key = xmsg_partition::channel_key(event.channel_id);
        let payload = event.encode_to_vec();
        loop {
            let record = FutureRecord::to(TOPIC_IN).key(&key[..]).payload(&payload);
            match producer.send(record, Duration::from_secs(10)).await {
                Ok(_) => break,
                Err((e, _)) => {
                    tracing::warn!(error = %e, channel_id = event.channel_id, "producing a reaction batch failed; retrying");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }
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
