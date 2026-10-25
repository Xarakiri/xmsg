<!-- SPDX-License-Identifier: AGPL-3.0-only -->
# Messages service (Rust)

First Phase 1a service ([plan §5.5](../../docs/architecture-plan.md)). Receives sends from the gateway over **NATS request/reply** and appends them to **`messages.in`**, keyed by channel, for the sequencer.

## What it does
- Subscribes to `xmsg.svc.messages.send` in the queue group `xmsg-messages`, so any number of instances share the load.
- **Early, best-effort checks** (the sequencer makes the authoritative ones, ADR-0006): the payload is a `send_message`; positive channel and actor IDs; a 1–128 byte `client_msg_id`; no `request_id`; a non-blank body of at most 40,000 characters.
- **Rate limit:** at most `XMSG_RATE_SENDS` sends per person (default 20 per 10 s), counted across instances in Valkey. Over it, the reply is `RATE_LIMITED` with `retry_after_ms`, which the gateway passes on to the client; nothing is appended.
- **Appends** the unchanged `InboundEvent` with an idempotent producer (`acks=all`) and librdkafka's `murmur2_random` partitioner, which matches [spec/partitioning](../../spec/partitioning/README.md) (verified by an integration test).
- **Replies** with `SendResult`: `accepted`, or a rejection with a reason. If Kafka doesn't acknowledge within `XMSG_APPEND_TIMEOUT_MS`, the reply is `UNAVAILABLE`, and the gateway stays silent so the client retries (dedupe makes the retry safe).
- **History reads** (§6.6 lazy step), over HTTP when `XMSG_AUTH_JWKS_URL` is set: `GET /v1/channels/{id}/events?after=<event_seq>&limit=<n ≤ 500>` with a bearer token returns a protobuf `EventPage` (`application/x-protobuf`): the channel's events after `after`, rebuilt exactly as they were delivered live (from `channel_events`, with message pointers filled from `messages`, including the sender's `client_msg_id`), plus `has_more`, the head and `truncated` when the position is older than the 30-day log. Members only; anyone else gets 404, so channel IDs don't leak.
- **Edits and deletes** (§5.5), over the same HTTP server, bearer token required:
  - `PATCH /v1/channels/{id}/messages/{msg_seq}` with `{body, edit_id}`: only the author edits a message that isn't deleted (else 403 / 404). `edit_id` makes a retried request count once (dedupe `edit:{msg_seq}:{edit_id}`).
  - `DELETE /v1/channels/{id}/messages/{msg_seq}`: the author deletes their own; anyone else's goes out with `as_admin`, and the sequencer refuses it unless the actor is a channel admin (the refusal reaches the actor as a rejection). Already deleted: 202 `already_deleted`.
  - The target is read at LOCAL_QUORUM. Members see messages live before the persister writes them, so a target that isn't visible yet is retried for up to 1 s before the answer is 404. Non-members get 404 at once.
  - The answer is 202: the change is sequenced (an `event_seq`, no `msg_seq`) and reaches every member, the caller included, as `message.edited` / `message.deleted`.
  - History rebuilds edits from the current row too, so it shows the latest body, and none once deleted.
- **Search** (§5.12): `GET /v1/search?q=&limit=` (≤ 50) over the [search-indexer](../search-indexer)'s OpenSearch index, filtered to the caller's channels (`channels_by_user`, up to 1,000), so results never come from a channel they can't read. Returns `{results: [{channel_id, msg_seq, message_id, author_id, kind, root_msg_seq, body}]}`. Off (503) unless `XMSG_SEARCH_URL` is set.
- **Threads** (ADR-0005): replies are sends with a `thread_root` (the root's `msg_seq` and `message_id`), checked by the sequencer. `GET /v1/channels/{id}/threads/{root_msg_seq}?after=&limit=` returns `{replies: [{reply_seq, message_id, author_id, body}], has_more}` from `thread_messages`; members only (404 otherwise).
- **Reactions** (§6.8, ADR-0015): `PUT` / `DELETE /v1/channels/{id}/messages/{msg_seq}/reactions/{emoji}?counter=N` checks the caller is a member and the message exists and isn't deleted, then produces a `ReactionCommand` to `reactions.in` (keyed by channel) for the [reaction-batcher](../reaction-batcher). `counter` increases per (user, message, emoji); a timestamp will do. 202; the result reaches members as `reactions.changed`.
- **Startup check:** exits with code 2 if `messages.in` is missing or its partition count differs from `XMSG_PARTITIONS` (ADR-0021).

## Not yet
- Membership and permission pre-checks from a Redis cache, `@channel` rules, per-workspace and per-channel rate limits, thread followers and the Threads view, editing or deleting replies.
- Reading backwards (older pages by `msg_seq`), the latest page for a channel far behind, and Redis `chan_head` with sync tokens (Phase 1b).

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `XMSG_NATS_URL` | `nats://127.0.0.1:4222` | NATS server (sends; also feeds the history API's revocation deny set, rebuilt from Kafka `auth.events` on start) |
| `XMSG_KAFKA_BOOTSTRAP` | `127.0.0.1:9092` | Kafka bootstrap servers |
| `XMSG_PARTITIONS` | *(required)* | This installation's partition count |
| `XMSG_APPEND_TIMEOUT_MS` | `1500` | Max wait for Kafka to acknowledge an append |
| `XMSG_AUTH_JWKS_URL` | *(none)* | Auth service JWKS; enables the history HTTP API |
| `XMSG_MESSAGES_PORT` | `8083` | History HTTP port |
| `XMSG_CQL_NODES` / `XMSG_CQL_KEYSPACE` | `127.0.0.1:9042` / `xmsg` | Wide-column store for history |
| `XMSG_VALKEY_URL` | *(none)* | Valkey for rate limits shared by all instances (without it each counts its own) |
| `XMSG_RATE_SENDS` | `20/10s` | Sends per person per window |
| `RUST_LOG` | `info` | Log filter |

## Tests

```bash
cargo test -p xmsg-messages                     # unit tests (validation, replies)
XMSG_IT_KAFKA=127.0.0.1:9092 XMSG_IT_NATS=nats://127.0.0.1:4222 \
  cargo test -p xmsg-messages --test integration -- --ignored --test-threads=1
```

The integration tests check that records land on the spec partition (300 channels), that the real binary accepts and appends a send over NATS and rejects an invalid one, and that a wrong partition count stops it with exit code 2.

The whole path today is covered by [`tools/e2e-smoke`](../../tools/e2e-smoke/src/main.rs): client → gateway → messages → Kafka → sequencer → `messages.persisted`. CI runs both in the `pipeline` job.
