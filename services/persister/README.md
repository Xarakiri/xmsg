<!-- SPDX-License-Identifier: AGPL-3.0-only -->
# Persister (Rust)

Phase 1a ([plan §5.7](../../docs/architecture-plan.md)). Reads `messages.persisted` (committed sequencer output only) and writes the wide-column tables it owns, through the CQL contract: Cassandra 5 (bundled) or ScyllaDB ([ADR-0020](../../docs/adr/0020-pluggable-backends.md), [ADR-0023](../../docs/adr/0023-osi-bundled-backends-and-wide-column-choice.md)).

## What it writes

| Event | Writes (in this order) |
|---|---|
| `message.created` | `messages` row; `channel_events` **pointer** (the `msg_seq`, not the body); `channel_heads` incl. `msg_seq` |
| `message.edited` | `messages` body and `edited_at`, unless the row is already deleted (read first; the persister is the only writer, so this is race-free); event **pointer**; head (no `msg_seq`) |
| `message.deleted` | `messages` body cleared, `deleted = true`; the event (it has no body); head |
| `channel.created` | `members_by_channel` + `channels_by_user` for each initial member (roles from the sequencer); event; head |
| `member.joined` / `member.left` | membership rows added / removed; event; head |
| `channel.mode_changed` | `channels_by_user.fanout_mode` for every member; event; head |
| rejections (`event_seq` 0) | nothing |

- **Only writer** of `messages`, `channel_events`, `members_by_channel`, `channels_by_user` and `channel_heads` (rule 3, plan §2).
- **Order:** membership rows first, then the `membership_version` in `channel_heads` that announces them; all at `LOCAL_QUORUM` (ADR-0006).
- **Idempotent:** every write is an upsert or delete of absolute values from the event, so replays converge (verified by the integration test, which replays everything).
- **No overwrites:** `message_id` is in the `messages` key (ADR-0014).
- A failed write is retried with backoff before the offset is committed, so the partition never moves past an event that isn't stored. Offsets are committed after the writes (at-least-once, made safe by idempotence).
- **Startup check:** exits with code 2 if the topic is missing or its partition count differs from `XMSG_PARTITIONS` (ADR-0021).

## Not yet
- Redis `chan_head` (raise-only) cache, the derived `channels.membership` feed, thread tables, reactions, mentions.
- Parallelism per partition (today one consumer task processes partitions in order).

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `XMSG_KAFKA_BOOTSTRAP` | `127.0.0.1:9092` | Kafka bootstrap servers |
| `XMSG_CQL_NODES` | `127.0.0.1:9042` | Comma-separated CQL contact points |
| `XMSG_CQL_KEYSPACE` | `xmsg` | Keyspace |
| `XMSG_PARTITIONS` | *(required)* | This installation's partition count |
| `XMSG_PERSISTED_TOPIC` / `XMSG_PERSISTER_GROUP` | `messages.persisted` / `xmsg-persister` | Overrides for tests |

## Tests

```bash
cargo test -p xmsg-persister   # event → writes mapping (order, pointers, determinism)
XMSG_IT_KAFKA=127.0.0.1:9092 XMSG_IT_CQL=127.0.0.1:9042 \
  cargo test -p xmsg-persister --test integration -- --ignored
```

The integration test applies the Small-tier schema, feeds a channel's events through a real Kafka topic, checks every table, then replays all events and checks nothing changed.
