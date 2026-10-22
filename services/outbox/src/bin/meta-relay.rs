// SPDX-License-Identifier: AGPL-3.0-only
//! xmsg-meta-relay (plan §5, ADR-0010): copies channel events from `channels.meta` into
//! `messages.in`, and forwards revocations from `auth.events` to NATS `auth.revocations`.
//! At least once: offsets are committed after delivery; the sequencer drops repeats by
//! dedupe ID and gateways apply a revocation twice harmlessly.

use std::process::ExitCode;
use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::Message as _;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use tracing_subscriber::EnvFilter;
use xmsg_outbox::{
    SUBJECT_REVOCATIONS, TOPIC_AUTH_EVENTS, TOPIC_CHANNELS_META, TOPIC_MESSAGES_IN,
    relay_auth_event, relay_channel_event,
};

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let get =
        |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.to_string());
    let bootstrap = get("XMSG_KAFKA_BOOTSTRAP", "127.0.0.1:9092");
    let partitions: i32 = match std::env::var("XMSG_PARTITIONS")
        .ok()
        .and_then(|p| p.parse().ok())
    {
        Some(p) => p,
        None => return fail("XMSG_PARTITIONS must be set to this installation's partition count"),
    };

    let consumer: StreamConsumer = match ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("group.id", get("XMSG_META_RELAY_GROUP", "xmsg-meta-relay"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
    {
        Ok(c) => c,
        Err(e) => return fail(&format!("creating Kafka consumer: {e}")),
    };
    let producer: FutureProducer = match ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("linger.ms", "1")
        .set("partitioner", "murmur2_random")
        .set("message.timeout.ms", "10000")
        .create()
    {
        Ok(p) => p,
        Err(e) => return fail(&format!("creating Kafka producer: {e}")),
    };

    // Startup check (ADR-0021): never write to a messages.in with another partition count.
    match consumer.fetch_metadata(Some(TOPIC_MESSAGES_IN), Duration::from_secs(10)) {
        Ok(md) => match md.topics().first() {
            Some(t) if t.error().is_none() && t.partitions().len() as i32 == partitions => {}
            Some(t) if t.error().is_none() => {
                return fail(&format!(
                    "{TOPIC_MESSAGES_IN} has {} partitions but XMSG_PARTITIONS is {partitions} (ADR-0021)",
                    t.partitions().len()
                ));
            }
            _ => return fail(&format!("topic {TOPIC_MESSAGES_IN} does not exist")),
        },
        Err(e) => return fail(&format!("fetching metadata: {e}")),
    }
    let nats = match std::env::var("XMSG_NATS_URL") {
        Ok(url) => match async_nats::connect(&url).await {
            Ok(c) => Some(c),
            Err(e) => return fail(&format!("connecting to NATS at {url}: {e}")),
        },
        Err(_) => {
            tracing::warn!(
                "XMSG_NATS_URL not set: revocations are not forwarded (only auth's fast path)"
            );
            None
        }
    };
    let topics: &[&str] = if nats.is_some() {
        &[TOPIC_CHANNELS_META, TOPIC_AUTH_EVENTS]
    } else {
        &[TOPIC_CHANNELS_META]
    };
    if let Err(e) = consumer.subscribe(topics) {
        return fail(&format!("subscribing to {topics:?}: {e}"));
    }
    tracing::info!("meta-relay ready");

    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    loop {
        let message = tokio::select! {
            m = consumer.recv() => m,
            _ = &mut shutdown => break,
        };
        let message = match message {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "Kafka receive error");
                continue;
            }
        };
        if message.topic() == TOPIC_AUTH_EVENTS {
            match relay_auth_event(message.payload().unwrap_or_default()) {
                Ok(Some(event)) => {
                    let nats = nats
                        .as_ref()
                        .expect("subscribed to auth.events only with NATS");
                    let mut backoff = Duration::from_millis(100);
                    while let Err(e) = nats
                        .publish(SUBJECT_REVOCATIONS, event.clone().into())
                        .await
                    {
                        tracing::warn!(error = %e, "publishing a revocation failed; retrying");
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(5));
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::error!(error = %e, offset = message.offset(), "skipping an auth.events record")
                }
            }
            if let Err(e) = consumer.commit_message(&message, CommitMode::Async) {
                tracing::warn!(error = %e, "offset commit failed (events will be relayed again, harmlessly)");
            }
            continue;
        }
        match relay_channel_event(message.key(), message.payload().unwrap_or_default()) {
            Ok((channel_id, payload)) => {
                let key = xmsg_partition::channel_key(channel_id);
                // Retried until it lands: skipping would lose the event, and order within the
                // partition matters, so nothing after it is read meanwhile.
                let mut backoff = Duration::from_millis(100);
                while let Err((e, _)) = producer
                    .send(
                        FutureRecord::to(TOPIC_MESSAGES_IN)
                            .key(&key[..])
                            .payload(&payload),
                        Duration::from_secs(10),
                    )
                    .await
                {
                    tracing::warn!(error = %e, channel_id, "producing to messages.in failed; retrying");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                }
            }
            Err(e) => tracing::error!(
                error = %e,
                partition = message.partition(),
                offset = message.offset(),
                "skipping a channels.meta record that can't be relayed"
            ),
        }
        if let Err(e) = consumer.commit_message(&message, CommitMode::Async) {
            tracing::warn!(error = %e, "offset commit failed (events will be relayed again, harmlessly)");
        }
    }
    let _ = consumer.commit_consumer_state(CommitMode::Sync);
    if let Some(nats) = nats {
        let _ = nats.flush().await;
    }
    tracing::info!("shutting down");
    ExitCode::SUCCESS
}

fn fail(message: &str) -> ExitCode {
    eprintln!("xmsg-meta-relay: {message}");
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
