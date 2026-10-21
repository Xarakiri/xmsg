// SPDX-License-Identifier: AGPL-3.0-only
//! Settings from the environment.

#[derive(Debug, Clone)]
pub struct Config {
    pub nats_url: String,
    pub kafka_bootstrap: String,
    /// This installation's partition count (ADR-0021); checked against `messages.in` at startup.
    pub partitions: i32,
    /// Max time to wait for Kafka to acknowledge an append before replying "unavailable".
    pub append_timeout_ms: u64,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let get =
            |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.to_string());
        Ok(Config {
            nats_url: get("XMSG_NATS_URL", "nats://127.0.0.1:4222"),
            kafka_bootstrap: get("XMSG_KAFKA_BOOTSTRAP", "127.0.0.1:9092"),
            partitions: std::env::var("XMSG_PARTITIONS")
                .map_err(|_| {
                    "XMSG_PARTITIONS must be set to this installation's partition count".to_string()
                })?
                .parse()
                .map_err(|_| "XMSG_PARTITIONS must be a number".to_string())?,
            append_timeout_ms: get("XMSG_APPEND_TIMEOUT_MS", "1500")
                .parse()
                .map_err(|_| "XMSG_APPEND_TIMEOUT_MS must be a number".to_string())?,
        })
    }
}
