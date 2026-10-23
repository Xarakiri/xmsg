<!-- SPDX-License-Identifier: AGPL-3.0-only -->
# Read state (Rust)

Phase 1b ([plan §5.10](../../docs/architecture-plan.md), [ADR-0012](../../docs/adr/0012-unread-counts-and-read-state.md)). Read positions and unread counts.

**Unread = the channel's `msg_seq` − the user's `last_read_msg_seq`.** There are no counters, so a replay can't double-count and nothing drifts.

## How it works
- **One writer per user.** Every change to a read position is a `ReadStateEvent` on `readstate.events`, keyed by `user_id`: a mark-read from any device, an own send, a join, or a mark-unread. The **owner** (consumer group `xmsg-readstate-owner`) applies each user's events in partition order. It is the only writer of `read_state`.
  - Positions merge with **max**, so a late mark-read from a slow device can't undo another's progress. "Unread from here" is the one explicit step back.
  - Offsets are committed after the write. After a crash, events are re-applied in order, which converges to the same position.
- **Feeder** (consumer group `xmsg-readstate-feeder`) reads `messages.persisted` and **never writes `read_state`**. It only produces events:
  - an **own send** marks the channel read up to that message;
  - a **join** starts at the channel's current message, so history isn't unread. The feeder learns each channel's latest `msg_seq` from its messages, and falls back to `channel_heads` after a restart.
  - a message's **mentions** (chosen by the sequencer) become `user_mentions` rows, inserted by the feeder itself before it stores the offset: idempotent inserts, so a crash just repeats them (ADR-0012). `@channel` rows are the persister's (`channel_mentions`).
  - Feeder events are all "read up to", which merge with max, so they're produced without waiting. Deliveries are checked, and offsets committed, every second.
- **API** (bearer token, as everywhere):

| Method | Path | Body | Reply |
|---|---|---|---|
| GET | `/v1/me/read-state` | | `{positions: [{channel_id, last_read_msg_seq}]}` (no row: 0) |
| GET | `/v1/me/mentions` | | `{mentions: [{channel_id, msg_seqs}]}`: unread mentions of you and `@channel` after your read position, at most 100 per channel; channels with none are left out |
| POST | `/v1/channels/{id}/read` | `{msg_seq}` | 202: read up to it (never moves back) |
| POST | `/v1/channels/{id}/unread` | `{msg_seq}` | 202: unread from it (position = msg_seq − 1) |

Clients show changes at once; the owner applies them a few milliseconds later. A position in a channel you can't read reveals nothing, so membership isn't checked on writes.

## Not yet
- `read_state.last_mention_seq` (set by the owner), so the mentions read can skip channels with nothing new without reading their rows; today it reads each channel's `user_mentions` partition, and `channel_mentions` only when the head has a newer `@channel`.
- Mentions in thread replies and in edits.
- Thread read state.
- Mark-read through the gateway socket instead of HTTP.
- An owner-side cache (today each event costs one read and one write).

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `XMSG_READSTATE_PORT` | `8085` | HTTP port |
| `XMSG_AUTH_JWKS_URL` | *(required)* | Auth service JWKS |
| `XMSG_NATS_URL` | *(none)* | Refuse revoked sessions (see `xmsg-authn`) |
| `XMSG_KAFKA_BOOTSTRAP` | `127.0.0.1:9092` | Kafka |
| `XMSG_CQL_NODES` / `XMSG_CQL_KEYSPACE` | `127.0.0.1:9042` / `xmsg` | Wide-column store |

## Tests

```bash
cargo test -p xmsg-readstate
```

The end-to-end smoke (`XMSG_READSTATE_URL`) checks real positions. Own sends and a late join set them; a mark-read can't move a position back, and a mark-unread can.
