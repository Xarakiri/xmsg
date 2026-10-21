// SPDX-License-Identifier: AGPL-3.0-only
//! `messages.in` producer (rdkafka / librdkafka).

use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};

use crate::EventLog;

pub const TOPIC_IN: &str = "messages.in";

pub struct KafkaLog {
    producer: FutureProducer,
    timeout: Duration,
    topic: String,
}

impl KafkaLog {
    pub fn new(bootstrap: &str, timeout: Duration) -> Result<Self, String> {
        Self::with_topic(bootstrap, timeout, TOPIC_IN)
    }

    /// Same as [`KafkaLog::new`] but writing to `topic` (tests use their own topics).
    pub fn with_topic(bootstrap: &str, timeout: Duration, topic: &str) -> Result<Self, String> {
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
        Ok(KafkaLog {
            producer,
            timeout,
            topic: topic.to_string(),
        })
    }

    /// Partition count of `topic`, or an error if it doesn't exist.
    pub fn partition_count(&self, topic: &str) -> Result<i32, String> {
        let metadata = self
            .producer
            .client()
            .fetch_metadata(Some(topic), Duration::from_secs(10))
            .map_err(|e| format!("fetching metadata for {topic}: {e}"))?;
        let topic_md = metadata
            .topics()
            .first()
            .ok_or_else(|| format!("no metadata for {topic}"))?;
        if topic_md.error().is_some() || topic_md.partitions().is_empty() {
            return Err(format!("topic {topic} does not exist"));
        }
        Ok(topic_md.partitions().len() as i32)
    }

    pub fn flush(&self) {
        let _ = self.producer.flush(Duration::from_secs(5));
    }
}

impl EventLog for KafkaLog {
    async fn append(&self, channel_id: i64, event: Vec<u8>) -> Result<(), String> {
        let key = xmsg_partition::channel_key(channel_id);
        let record = FutureRecord::to(&self.topic).key(&key[..]).payload(&event);
        self.producer
            .send(record, self.timeout)
            .await
            .map(|_| ())
            .map_err(|(e, _)| e.to_string())
    }
}
