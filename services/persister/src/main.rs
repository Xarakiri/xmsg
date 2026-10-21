// SPDX-License-Identifier: AGPL-3.0-only
use std::process::ExitCode;
use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::Message as _;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use tracing_subscriber::EnvFilter;
use xmsg_persister::cassandra::Store;
use xmsg_persister::config::Config;
use xmsg_persister::writes;
use xmsg_proto::Message;
use xmsg_proto::v1::SequencedEvent;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let config = match Config::from_env() {
        Ok(c) => c,
        Err(e) => return fail(&e),
    };

    let consumer: StreamConsumer = match ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_bootstrap)
        .set("group.id", &config.group_id)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        // Only committed sequencer output (exactly-once upstream, ADR-0002).
        .set("isolation.level", "read_committed")
        .create()
    {
        Ok(c) => c,
        Err(e) => return fail(&format!("creating Kafka consumer: {e}")),
    };

    // Startup check (ADR-0021).
    match consumer.fetch_metadata(Some(&config.topic), Duration::from_secs(10)) {
        Ok(md) => match md.topics().first() {
            Some(t) if t.error().is_none() && t.partitions().len() as i32 == config.partitions => {}
            Some(t) if t.error().is_none() => {
                return fail(&format!(
                    "{} has {} partitions but XMSG_PARTITIONS is {}; never change the count in place (ADR-0021)",
                    config.topic,
                    t.partitions().len(),
                    config.partitions
                ));
            }
            _ => return fail(&format!("topic {} does not exist", config.topic)),
        },
        Err(e) => return fail(&format!("fetching metadata: {e}")),
    }

    let store = match Store::connect(&config.cql_nodes, &config.keyspace).await {
        Ok(s) => s,
        Err(e) => return fail(&e),
    };
    if let Err(e) = consumer.subscribe(&[&config.topic]) {
        return fail(&format!("subscribing to {}: {e}", config.topic));
    }
    tracing::info!(topic = %config.topic, keyspace = %config.keyspace, "persister ready");

    loop {
        let message = tokio::select! {
            m = consumer.recv() => m,
            _ = tokio::signal::ctrl_c() => break,
        };
        let message = match message {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "Kafka receive error");
                continue;
            }
        };
        let Some(payload) = message.payload() else {
            continue;
        };
        let event = match SequencedEvent::decode(payload) {
            Ok(e) => e,
            Err(e) => {
                // Can't happen with a healthy sequencer; skip rather than block the partition forever.
                tracing::error!(error = %e, partition = message.partition(), offset = message.offset(), "undecodable event, skipped");
                continue;
            }
        };
        let event_time = message.timestamp().to_millis().unwrap_or_default();

        // Retry until written: the partition must not move past an event that isn't stored.
        let plan = writes(&event, event_time);
        let mut backoff = Duration::from_millis(100);
        while let Err(e) = store.apply(&plan).await {
            tracing::warn!(error = %e, channel_id = event.channel_id, event_seq = event.event_seq, "write failed, retrying");
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(5));
        }
        if let Err(e) = consumer.commit_message(&message, CommitMode::Async) {
            tracing::warn!(error = %e, "offset commit failed (events will be re-applied, harmlessly)");
        }
    }
    tracing::info!("shutting down");
    let _ = consumer.commit_consumer_state(CommitMode::Sync);
    ExitCode::SUCCESS
}

fn fail(message: &str) -> ExitCode {
    eprintln!("xmsg-persister: {message}");
    ExitCode::from(2)
}
