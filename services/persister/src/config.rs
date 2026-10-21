// SPDX-License-Identifier: AGPL-3.0-only
//! Settings from the environment.

#[derive(Debug, Clone)]
pub struct Config {
    pub kafka_bootstrap: String,
    pub cql_nodes: Vec<String>,
    pub keyspace: String,
    pub topic: String,
    pub group_id: String,
    /// This installation's partition count (ADR-0021); checked against the topic at startup.
    pub partitions: i32,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let get =
            |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.to_string());
        Ok(Config {
            kafka_bootstrap: get("XMSG_KAFKA_BOOTSTRAP", "127.0.0.1:9092"),
            cql_nodes: get("XMSG_CQL_NODES", "127.0.0.1:9042")
                .split(',')
                .map(str::to_string)
                .collect(),
            keyspace: get("XMSG_CQL_KEYSPACE", "xmsg"),
            topic: get("XMSG_PERSISTED_TOPIC", "messages.persisted"),
            group_id: get("XMSG_PERSISTER_GROUP", "xmsg-persister"),
            partitions: std::env::var("XMSG_PARTITIONS")
                .map_err(|_| {
                    "XMSG_PARTITIONS must be set to this installation's partition count".to_string()
                })?
                .parse()
                .map_err(|_| "XMSG_PARTITIONS must be a number".to_string())?,
        })
    }
}
