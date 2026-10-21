// SPDX-License-Identifier: AGPL-3.0-only
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use tracing_subscriber::EnvFilter;
use xmsg_messages::config::Config;
use xmsg_messages::kafka::{KafkaLog, TOPIC_IN};
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
    tracing::info!(
        subject = SEND_SUBJECT,
        queue = QUEUE_GROUP,
        partitions = config.partitions,
        "messages service ready"
    );

    loop {
        tokio::select! {
            msg = requests.next() => {
                let Some(msg) = msg else { break };
                let Some(reply_to) = msg.reply.clone() else { continue };
                let (log, client) = (log.clone(), client.clone());
                tokio::spawn(async move {
                    let reply = handle(log.as_ref(), &msg.payload).await;
                    if let Err(e) = client.publish(reply_to, reply.into()).await {
                        tracing::warn!(error = %e, "reply failed");
                    }
                });
            }
            _ = tokio::signal::ctrl_c() => break,
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
