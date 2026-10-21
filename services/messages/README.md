<!-- SPDX-License-Identifier: AGPL-3.0-only -->
# Messages service (Rust)

First Phase 1a service ([plan §5.5](../../docs/architecture-plan.md)). Receives sends from the gateway over **NATS request/reply** and appends them to **`messages.in`**, keyed by channel, for the sequencer.

## What it does
- Subscribes to `xmsg.svc.messages.send` in the queue group `xmsg-messages`, so any number of instances share the load.
- **Early, best-effort checks** (the sequencer makes the authoritative ones, ADR-0006): the payload is a `send_message`; positive channel and actor IDs; a 1–128 byte `client_msg_id`; no `request_id`; a non-blank body of at most 40,000 characters.
- **Appends** the unchanged `InboundEvent` with an idempotent producer (`acks=all`) and librdkafka's `murmur2_random` partitioner, which matches [spec/partitioning](../../spec/partitioning/README.md) (verified by an integration test).
- **Replies** with `SendResult`: `accepted`, or a rejection with a reason. If Kafka doesn't acknowledge within `XMSG_APPEND_TIMEOUT_MS`, the reply is `UNAVAILABLE`, and the gateway stays silent so the client retries (dedupe makes the retry safe).
- **Startup check:** exits with code 2 if `messages.in` is missing or its partition count differs from `XMSG_PARTITIONS` (ADR-0021).

## Not yet
- Membership and permission pre-checks from a Redis cache, `@channel` rules, rate limits, edits/deletes/reactions, history reads and the sync API.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `XMSG_NATS_URL` | `nats://127.0.0.1:4222` | NATS server |
| `XMSG_KAFKA_BOOTSTRAP` | `127.0.0.1:9092` | Kafka bootstrap servers |
| `XMSG_PARTITIONS` | *(required)* | This installation's partition count |
| `XMSG_APPEND_TIMEOUT_MS` | `1500` | Max wait for Kafka to acknowledge an append |
| `RUST_LOG` | `info` | Log filter |

## Tests

```bash
cargo test -p xmsg-messages                     # unit tests (validation, replies)
XMSG_IT_KAFKA=127.0.0.1:9092 XMSG_IT_NATS=nats://127.0.0.1:4222 \
  cargo test -p xmsg-messages --test integration -- --ignored --test-threads=1
```

The integration tests check that records land on the spec partition (300 channels), that the real binary accepts and appends a send over NATS and rejects an invalid one, and that a wrong partition count stops it with exit code 2.

The whole path today is covered by [`tools/e2e-smoke`](../../tools/e2e-smoke/src/main.rs): client → gateway → messages → Kafka → sequencer → `messages.persisted`. CI runs both in the `pipeline` job.
