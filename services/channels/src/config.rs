// SPDX-License-Identifier: AGPL-3.0-only
//! Settings from the environment.

#[derive(Debug, Clone)]
pub struct Config {
    pub port: u16,
    pub database_url: String,
    pub kafka_bootstrap: String,
    /// This installation's partition count (ADR-0021); checked against `messages.in` at startup.
    pub partitions: i32,
    pub jwks_url: String,
    /// How long a membership action waits for the sequencer before answering "pending".
    pub command_timeout_ms: u64,
    pub cql_nodes: Vec<String>,
    pub keyspace: String,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let get =
            |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.to_string());
        Ok(Config {
            port: get("XMSG_CHANNELS_PORT", "8082")
                .parse()
                .map_err(|_| "XMSG_CHANNELS_PORT must be a port".to_string())?,
            database_url: get(
                "XMSG_DATABASE_URL",
                "postgres://xmsg:xmsg-dev-only@127.0.0.1:5432/xmsg",
            ),
            kafka_bootstrap: get("XMSG_KAFKA_BOOTSTRAP", "127.0.0.1:9092"),
            partitions: std::env::var("XMSG_PARTITIONS")
                .map_err(|_| {
                    "XMSG_PARTITIONS must be set to this installation's partition count".to_string()
                })?
                .parse()
                .map_err(|_| "XMSG_PARTITIONS must be a number".to_string())?,
            jwks_url: std::env::var("XMSG_AUTH_JWKS_URL").map_err(|_| {
                "XMSG_AUTH_JWKS_URL must be set, e.g. http://auth:8081/.well-known/jwks.json"
                    .to_string()
            })?,
            command_timeout_ms: get("XMSG_COMMAND_TIMEOUT_MS", "2000")
                .parse()
                .map_err(|_| "XMSG_COMMAND_TIMEOUT_MS must be a number".to_string())?,
            cql_nodes: get("XMSG_CQL_NODES", "127.0.0.1:9042")
                .split(',')
                .map(str::to_string)
                .collect(),
            keyspace: get("XMSG_CQL_KEYSPACE", "xmsg"),
        })
    }
}
