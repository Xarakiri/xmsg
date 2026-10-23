<!-- SPDX-License-Identifier: AGPL-3.0-only -->
# Presence (Rust + Valkey)

Phase 2 ([plan §5.9](../../docs/architecture-plan.md)). Who is online, away or offline.

## How it works
- **Pings:** every gateway connection reports its state on NATS `xmsg.svc.presence.ping` (queue group `xmsg-presence`): **active** on connect, **active / idle** when the client's activity changes (tab hidden, or 5 minutes without input), the same again every 30 s, and **gone** when it closes. The gateway's session tracker sends the last one, so a crashed socket still says goodbye.
- **State in Valkey** (any Redis-protocol server, ADR-0020): `presence:{user}` is a hash of the user's connections, `conn_id → "{1|0}:{expires_ms}"`; a connection not pinged within the TTL (75 s) counts as gone.
- **Status:** **online** if any live connection is active, **away** if only idle ones are live, **offline** if none.
- **Changes are published once** on `presence.{user_id}`: one Lua script applies the ping, drops expired connections, and compares with `presence:last:{user}` (the last status published), all atomically, so pings from several gateways can't interleave.
- **Sweeper:** every 5 s, users whose latest connection expiry has passed (their gateway died without saying goodbye) are recomputed, so they go offline on time.
- **Queries:** `xmsg.svc.presence.get` (`PresenceQuery` → `PresenceReply`): current statuses, offline when unknown.
- **Gateway side:** a client sends `WatchPresence{user_ids}` for the people on its screen (up to 200; the set is replaced each time). The gateway answers with their current statuses and forwards changes as `PresenceUpdates` frames. For now each gateway listens to all of `presence.*` and filters for its watchers; per-user refcounted subscriptions come later.
- Presence is never backed up: it rebuilds from pings after a restart (plan §8).

## Not yet
- Refcounted `presence.{user_id}` subscriptions per gateway, released ~30 s after the last local watcher.
- Custom statuses, do-not-disturb, "last seen".

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `XMSG_VALKEY_URL` | `redis://127.0.0.1:6379` | Valkey (or Redis) |
| `XMSG_NATS_URL` | `nats://127.0.0.1:4222` | NATS |
| `XMSG_PRESENCE_TTL_MS` | `75000` | A connection not pinged for this long counts as gone |

## Tests

```bash
cargo test -p xmsg-presence
```

The gateway's tests cover pings and watches against an in-memory bus; the end-to-end smoke (`tools/e2e-smoke`) runs the whole path with Valkey: online, away when idle, online again, and offline for someone never connected.
