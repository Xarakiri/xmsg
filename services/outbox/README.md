<!-- SPDX-License-Identifier: AGPL-3.0-only -->
# Outbox relay and meta-relay (Rust)

Phase 1b ([plan §5, ADR-0010](../../docs/adr/0010-outbox-with-fast-path.md), [ADR-0020](../../docs/adr/0020-pluggable-backends.md)). The safety net behind the fast path: every Postgres state change also writes an `outbox` row in the same transaction, and these two services make sure it reaches Kafka even when the fast path didn't.

```
channels/users/auth ──commit──▶ Postgres outbox ──xmsg-outbox-relay──▶ channels.meta / users.events / auth.events
                                                                         channels.meta ──xmsg-meta-relay──▶ messages.in ──▶ sequencer
```

## `xmsg-outbox-relay`: the built-in poller (Small and Medium tiers; Debezium on Large)
- **One active instance.** All work runs on a connection holding a Postgres session advisory lock. Standbys wait for it, and when the active poller dies its connection closes and a standby takes over within about 2 s. A single publisher keeps every aggregate's events in commit (`id`) order.
- **Polling:** every 200 ms, it reads up to 500 unpublished rows in `id` order and publishes each to its aggregate's topic. Topics: `channel` → `channels.meta`, `user`/`workspace` → `users.events`, `session`/`auth` → `auth.events`. Records are keyed by aggregate (the channel key of [spec/partitioning](../../spec/partitioning/README.md)), with `outbox_id` and `event_type` headers.
- **Delivery:** it uses an idempotent producer with `acks=all`, and marks rows published only after Kafka has every one of them. A crash in between republishes them (at least once), and consumers dedupe by the outbox event.
- **Unknown aggregate types** stay unpublished for a newer relay instead of going to the wrong topic or blocking the rest.
- **Retention and lag:** published rows are kept 7 days for replays, then deleted (checked hourly). A lag above 5 s is logged.

## `xmsg-meta-relay`
- Copies `channels.meta` records unchanged into `messages.in`, keyed by channel, so the sequencer orders metadata events with the channel's messages.
- **Revocations:** when `XMSG_NATS_URL` is set, it also forwards `auth.events` revocations (`AuthEvent{SessionRevoked}`) unchanged to NATS `auth.revocations`. This is the safety net behind auth's own fast path; a gateway applies a repeat harmlessly. Other auth events are skipped.
- **Refuses what it can't route safely** (logged and skipped): a payload that isn't an `InboundEvent`, a key that names another channel, or a missing dedupe ID. Without a dedupe ID, the sequencer couldn't drop the fast-path copy.
- **At least once:** offsets are committed after delivery, and a failed produce is retried until it lands, in order. The sequencer drops repeats by dedupe ID, and `channel.created` is a no-op for an existing channel.
- **Startup check:** exits with code 2 if `messages.in`'s partition count differs from `XMSG_PARTITIONS` (ADR-0021).

## Not yet
- The failover reconciler (republish what a Postgres failover lost, ADR-0010) and metrics beyond the lag log.

## Configuration

| Variable | Default | Used by | Meaning |
|---|---|---|---|
| `XMSG_DATABASE_URL` | `postgres://xmsg:xmsg-dev-only@127.0.0.1:5432/xmsg` | outbox-relay | Postgres with the outbox |
| `XMSG_KAFKA_BOOTSTRAP` | `127.0.0.1:9092` | both | Kafka |
| `XMSG_OUTBOX_POLL_MS` | `200` | outbox-relay | Poll interval when caught up |
| `XMSG_PARTITIONS` | *(required)* | meta-relay | The installation's partition count |
| `XMSG_META_RELAY_GROUP` | `xmsg-meta-relay` | meta-relay | Consumer group |
| `XMSG_NATS_URL` | *(none)* | meta-relay | Forward revocations to NATS `auth.revocations` |

## Tests

```bash
cargo test -p xmsg-outbox
XMSG_IT_PG=postgres://xmsg:xmsg-dev-only@127.0.0.1:5432/xmsg XMSG_IT_KAFKA=127.0.0.1:9092 \
  cargo test -p xmsg-outbox --test integration -- --ignored --test-threads=1
```

The integration test runs both binaries. It checks that a committed `channel.created` row goes outbox → `channels.meta` → `messages.in` unchanged and is marked published, that an unknown aggregate type is left alone, and that only one poller holds the lock. The end-to-end smoke (`XMSG_EXPECT_OUTBOX_RELAY=true`) checks that a channel's `channel.created` reaches `messages.in` by both routes and is still sequenced once.
