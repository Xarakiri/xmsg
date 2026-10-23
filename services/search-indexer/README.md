<!-- SPDX-License-Identifier: AGPL-3.0-only -->
# Search indexer (Rust + OpenSearch)

Phase 2 ([plan §5.12](../../docs/architecture-plan.md)). Keeps OpenSearch in step with `messages.persisted`, so messages can be searched. Search is optional on the Small tier (compose profile `search`).

## How it works
- Consumes `messages.persisted` (consumer group `xmsg-search-indexer`) and writes OpenSearch through its bulk API, flushing every second or every 2,000 lines.
  - `message.created` and `thread.reply` → a document keyed by `message_id`: `workspace_id`, `channel_id`, `msg_seq` (the `reply_seq` for replies), `message_id`, `author_id`, `kind` (`message` / `reply`), `root_msg_seq`, `body`, `at_ms`.
  - `message.edited` → the new body; `message.deleted` → the document is removed. A missing document is fine for both (messages indexed before search was turned on).
- Offsets are committed only after OpenSearch accepted the whole batch (a failed batch is retried). A replay re-applies events in channel order, so it ends in the same state.
- Creates the index `xmsg-messages-v1` with a strict mapping on first start, waiting for OpenSearch if it isn't up yet.
- **Queries** are served by the messages service (`GET /v1/search?q=`), scoped to the caller's channels.

## Not yet
- Monthly indices routed by workspace, and `update_by_query` when a channel switches between public and private (plan §5.12).
- Per-user cached channel lists from `channels.membership` (today each query reads `channels_by_user`, capped at 1,000).
- Highlighting, filters (in:channel, from:user, dates) and pagination.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `XMSG_OPENSEARCH_URL` | `http://127.0.0.1:9200` | OpenSearch (or Elasticsearch) |
| `XMSG_KAFKA_BOOTSTRAP` | `127.0.0.1:9092` | Kafka |
| `XMSG_SEARCH_INDEXER_GROUP` | `xmsg-search-indexer` | Consumer group |

## Tests

```bash
cargo test -p xmsg-search-indexer
```

The end-to-end smoke (`tools/e2e-smoke`) checks that a member finds a fresh message and that someone who left the channel finds nothing.
