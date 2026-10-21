// SPDX-License-Identifier: AGPL-3.0-only
//! Settings from the environment.

#[derive(Debug, Clone)]
pub struct Config {
    pub port: u16,
    pub database_url: String,
    pub jwks_url: String,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let get =
            |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.to_string());
        Ok(Config {
            port: get("XMSG_USERS_PORT", "8084")
                .parse()
                .map_err(|_| "XMSG_USERS_PORT must be a port".to_string())?,
            database_url: get(
                "XMSG_DATABASE_URL",
                "postgres://xmsg:xmsg-dev-only@127.0.0.1:5432/xmsg",
            ),
            jwks_url: std::env::var("XMSG_AUTH_JWKS_URL").map_err(|_| {
                "XMSG_AUTH_JWKS_URL must be set, e.g. http://auth:8081/.well-known/jwks.json"
                    .to_string()
            })?,
        })
    }
}
