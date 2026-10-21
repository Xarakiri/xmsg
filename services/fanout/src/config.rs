// SPDX-License-Identifier: AGPL-3.0-only
//! Settings from the environment.

#[derive(Debug, Clone)]
pub struct Config {
    pub kafka_bootstrap: String,
    pub nats_url: String,
    pub cql_nodes: Vec<String>,
    pub keyspace: String,
    pub partitions: i32,
    pub group_id: String,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let get =
            |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.to_string());
        Ok(Config {
            kafka_bootstrap: get("XMSG_KAFKA_BOOTSTRAP", "127.0.0.1:9092"),
            nats_url: get("XMSG_NATS_URL", "nats://127.0.0.1:4222"),
            cql_nodes: get("XMSG_CQL_NODES", "127.0.0.1:9042")
                .split(',')
                .map(str::to_string)
                .collect(),
            keyspace: get("XMSG_CQL_KEYSPACE", "xmsg"),
            group_id: get("XMSG_FANOUT_GROUP", "xmsg-fanout"),
            partitions: std::env::var("XMSG_PARTITIONS")
                .map_err(|_| {
                    "XMSG_PARTITIONS must be set to this installation's partition count".to_string()
                })?
                .parse()
                .map_err(|_| "XMSG_PARTITIONS must be a number".to_string())?,
        })
    }
}
