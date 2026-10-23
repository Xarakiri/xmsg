// SPDX-License-Identifier: AGPL-3.0-only
//! Presence (plan §5.9): online / away / offline from the gateways' per-connection pings,
//! kept in Valkey (any Redis-protocol server, ADR-0020) with TTLs.
//!
//! - `presence:{user}` is a hash of the user's connections: `conn_id → "{1|0}:{expires_ms}"`
//!   (1 = active). A connection not pinged within its TTL counts as gone.
//! - Status: **online** if any live connection is active, **away** if only idle ones are
//!   live, **offline** if none.
//! - `presence:last:{user}` is the last status published, so a change is published
//!   exactly once on `presence.{user_id}`, whoever notices it first: a ping, the sweeper
//!   (connections that expired because their gateway died), or a query.
//! - Everything for one user happens in one Lua script, so concurrent pings from
//!   several gateways can't interleave.

use xmsg_proto::v1::{ConnectionState, PresenceStatus};

/// Not under `presence.*`: gateways listen there for status changes only.
pub const PINGS_SUBJECT: &str = "xmsg.svc.presence.ping";
pub const QUERY_SUBJECT: &str = "xmsg.svc.presence.get";
pub const QUEUE_GROUP: &str = "xmsg-presence";
pub const EXPIRIES_KEY: &str = "presence:expiries";
/// Most users one query may ask about (a screen's worth).
pub const MAX_QUERY: usize = 200;

pub fn update_subject(user_id: i64) -> String {
    format!("presence.{user_id}")
}

/// Applies one change (or none, for a recompute) and returns `[before, after]` as
/// `PresenceStatus` numbers. KEYS: hash, last-status key, expiries zset.
/// ARGV: conn_id, state (0 recompute, 1 active, 2 idle, 3 gone), now_ms, ttl_ms, user_id.
pub const SCRIPT: &str = r#"
local now = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
local function status()
  local all = redis.call('HGETALL', KEYS[1])
  local s = 1
  for i = 1, #all, 2 do
    local active, expires = string.match(all[i + 1], '^(%d):(%d+)$')
    if expires == nil or tonumber(expires) <= now then
      redis.call('HDEL', KEYS[1], all[i])
    elseif active == '1' then
      s = 3
    elseif s < 2 then
      s = 2
    end
  end
  return s
end
local before = tonumber(redis.call('GET', KEYS[2]) or '1')
if ARGV[2] == '3' then
  redis.call('HDEL', KEYS[1], ARGV[1])
elseif ARGV[2] == '1' or ARGV[2] == '2' then
  local flag = '0'
  if ARGV[2] == '1' then flag = '1' end
  redis.call('HSET', KEYS[1], ARGV[1], flag .. ':' .. (now + ttl))
  redis.call('PEXPIRE', KEYS[1], ttl * 2)
  redis.call('ZADD', KEYS[3], now + ttl, ARGV[5])
end
local after = status()
if after ~= before then
  if after == 1 then
    redis.call('DEL', KEYS[2])
  else
    redis.call('SET', KEYS[2], after, 'PX', ttl * 2)
  end
end
return {before, after}
"#;

/// The script's `state` argument for a ping (0 means "just recompute").
pub fn state_arg(state: i32) -> &'static str {
    match ConnectionState::try_from(state) {
        Ok(ConnectionState::Active) => "1",
        Ok(ConnectionState::Idle) => "2",
        Ok(ConnectionState::Gone) => "3",
        _ => "0",
    }
}

pub fn status_from(n: i64) -> PresenceStatus {
    match n {
        3 => PresenceStatus::Online,
        2 => PresenceStatus::Away,
        _ => PresenceStatus::Offline,
    }
}

pub fn keys(user_id: i64) -> [String; 3] {
    [
        format!("presence:{user_id}"),
        format!("presence:last:{user_id}"),
        EXPIRIES_KEY.to_string(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn script_arguments_and_results_map_both_ways() {
        assert_eq!(state_arg(ConnectionState::Active as i32), "1");
        assert_eq!(state_arg(ConnectionState::Idle as i32), "2");
        assert_eq!(state_arg(ConnectionState::Gone as i32), "3");
        assert_eq!(state_arg(0), "0", "unspecified only recomputes");
        assert_eq!(status_from(3), PresenceStatus::Online);
        assert_eq!(status_from(2), PresenceStatus::Away);
        assert_eq!(status_from(1), PresenceStatus::Offline);
        assert_eq!(keys(7)[0], "presence:7");
        assert_eq!(update_subject(7), "presence.7");
    }
}
