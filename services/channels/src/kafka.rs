// SPDX-License-Identifier: AGPL-3.0-only
//! `messages.in` producer and `commands.results` reader (rdkafka / librdkafka).

use std::sync::Arc;
use std::time::Duration;

use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use xmsg_proto::Message as _;
use xmsg_proto::v1::{CommandResult, InboundEvent};

use crate::commands::Pending;

pub const TOPIC_IN: &str = "messages.in";
pub const TOPIC_RESULTS: &str = "commands.results";

/// Where commands and fast-path events go. Kafka in production; tests use a fake.
pub trait EventLog: Send + Sync + 'static {
    fn append(&self, event: &InboundEvent) -> impl Future<Output = Result<(), String>> + Send;
}

pub struct KafkaLog {
    producer: FutureProducer,
    timeout: Duration,
}

impl KafkaLog {
    pub fn new(bootstrap: &str, timeout: Duration) -> Result<Self, String> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap)
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("linger.ms", "1")
            // Kafka's Java default partitioner, so partitions match spec/partitioning (ADR-0004).
            .set("partitioner", "murmur2_random")
            .set("message.timeout.ms", timeout.as_millis().to_string())
            .create()
            .map_err(|e| format!("creating Kafka producer: {e}"))?;
        Ok(KafkaLog { producer, timeout })
    }

    /// Partition count of `topic`, or an error if it doesn't exist.
    pub fn partition_count(&self, topic: &str) -> Result<i32, String> {
        partition_count(self.producer.client(), topic)
    }
}

fn partition_count<C: rdkafka::client::ClientContext>(
    client: &rdkafka::client::Client<C>,
    topic: &str,
) -> Result<i32, String> {
    let metadata = client
        .fetch_metadata(Some(topic), Duration::from_secs(10))
        .map_err(|e| format!("fetching metadata for {topic}: {e}"))?;
    match metadata.topics().first() {
        Some(t) if t.error().is_none() && !t.partitions().is_empty() => {
            Ok(t.partitions().len() as i32)
        }
        _ => Err(format!("topic {topic} does not exist")),
    }
}

impl EventLog for KafkaLog {
    async fn append(&self, event: &InboundEvent) -> Result<(), String> {
        let key = xmsg_partition::channel_key(event.channel_id);
        let payload = event.encode_to_vec();
        let record = FutureRecord::to(TOPIC_IN).key(&key[..]).payload(&payload);
        self.producer
            .send(record, self.timeout)
            .await
            .map(|_| ())
            .map_err(|(e, _)| e.to_string())
    }
}

/// Reads every partition of `commands.results` from its current end (no consumer
/// group: each instance needs all results) and completes matching waiters.
/// Returns once the start positions are fixed, so no later result can be missed.
pub async fn follow_results(bootstrap: &str, pending: Arc<Pending>) -> Result<(), String> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", "xmsg-channels-results-unused")
        .set("enable.auto.commit", "false")
        .create()
        .map_err(|e| format!("creating Kafka consumer: {e}"))?;
    let partitions = partition_count(consumer.client(), TOPIC_RESULTS)?;
    let mut assignment = TopicPartitionList::new();
    for partition in 0..partitions {
        let (_, high) = consumer
            .fetch_watermarks(TOPIC_RESULTS, partition, Duration::from_secs(10))
            .map_err(|e| format!("watermarks of {TOPIC_RESULTS}/{partition}: {e}"))?;
        assignment
            .add_partition_offset(TOPIC_RESULTS, partition, Offset::Offset(high))
            .map_err(|e| e.to_string())?;
    }
    consumer
        .assign(&assignment)
        .map_err(|e| format!("assigning {TOPIC_RESULTS}: {e}"))?;
    tokio::spawn(async move {
        loop {
            match consumer.recv().await {
                Ok(message) => {
                    if let Some(Ok(result)) = message.payload().map(CommandResult::decode) {
                        pending.complete(result);
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "commands.results receive error");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    });
    Ok(())
}
