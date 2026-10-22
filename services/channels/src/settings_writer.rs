// SPDX-License-Identifier: AGPL-3.0-only
//! The settings-writer (ADR-0016): the only writer of `member_settings`. It consumes the
//! compacted `member.settings` topic (partitioned by user) in its own consumer group and
//! stores each event's absolute values, so a replay after a crash ends in the same state.

use std::sync::Arc;
use std::time::Duration;

use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message as _};
use xmsg_proto::Message;
use xmsg_proto::v1::MemberSettings;

use crate::kafka::TOPIC_SETTINGS;
use crate::memberships::Memberships;

pub async fn run<M: Memberships>(memberships: Arc<M>, bootstrap: String) {
    let consumer: StreamConsumer = match ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("group.id", "xmsg-channels-settings-writer")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
    {
        Ok(c) => c,
        Err(e) => return tracing::error!(error = %e, "settings-writer: Kafka consumer"),
    };
    if let Err(e) = consumer.subscribe(&[TOPIC_SETTINGS]) {
        return tracing::error!(error = %e, "settings-writer: subscribing");
    }
    tracing::info!("settings-writer ready");
    loop {
        let message = match consumer.recv().await {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "settings-writer: receive error");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        if let Ok(settings) = MemberSettings::decode(message.payload().unwrap_or_default()) {
            let mut backoff = Duration::from_millis(100);
            while let Err(e) = memberships.save_settings(&settings).await {
                tracing::warn!(error = %e, "settings-writer: write failed; retrying");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(5));
            }
        }
        let _ = consumer.commit_message(&message, CommitMode::Async);
    }
}
