// SPDX-License-Identifier: AGPL-3.0-only
//! Settings from the environment.

use crate::keys::Keys;

pub struct Config {
    pub port: u16,
    pub database_url: String,
    pub keys: Keys,
    pub access_ttl_secs: i64,
    pub refresh_ttl_secs: i64,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let get =
            |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.to_string());
        let number = |name: &str, default: &str| {
            get(name, default)
                .parse::<i64>()
                .map_err(|_| format!("{name} must be a number"))
        };
        let keys = match std::env::var("XMSG_AUTH_KEY") {
            Ok(seed) => Keys::from_seed_base64(&seed)?,
            Err(_) if get("XMSG_AUTH_DEV_KEY", "false") == "true" => {
                tracing::warn!(
                    "using an ephemeral signing key: tokens die with this process (dev only)"
                );
                Keys::generate()
            }
            Err(_) => {
                return Err(
                    "XMSG_AUTH_KEY must be set to a base64 32-byte Ed25519 seed \
                            (or XMSG_AUTH_DEV_KEY=true for an ephemeral dev key)"
                        .into(),
                );
            }
        };
        Ok(Config {
            port: get("XMSG_AUTH_PORT", "8081")
                .parse()
                .map_err(|_| "XMSG_AUTH_PORT must be a port".to_string())?,
            database_url: get(
                "XMSG_DATABASE_URL",
                "postgres://xmsg:xmsg-dev-only@127.0.0.1:5432/xmsg",
            ),
            keys,
            access_ttl_secs: number("XMSG_AUTH_ACCESS_TTL_SECS", "900")?,
            refresh_ttl_secs: number("XMSG_AUTH_REFRESH_TTL_SECS", "2592000")?,
        })
    }
}
