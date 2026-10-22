<!-- SPDX-License-Identifier: AGPL-3.0-only -->
# Channels (Rust)

Phase 1a ([plan §5.4](../../docs/architecture-plan.md)). Creates channels and DMs, and turns membership actions into commands for the sequencer. It never writes membership itself ([ADR-0006](../../docs/adr/0006-membership-as-sequenced-commands.md)).

## API

Every request needs `Authorization: Bearer <access token>` from the auth service (checked locally against its JWKS by [`xmsg-authn`](../../crates/xmsg-authn)). IDs are JSON strings; errors are `{"error": "..."}`.

| Method | Path | Body | Reply |
|---|---|---|---|
| POST | `/v1/channels` | `{type: public\|private\|broadcast, name, topic?, member_ids?, workspace_id?}` | 201 `{channel_id, status: "created"}`; 202 `status: "pending"` (see below); 400 |
| GET | `/v1/me/channels` | | `{channels: [{channel_id, workspace_id, type, name, event_seq, msg_seq}]}`: my channels with their heads (sync's cheap step) |
| POST | `/v1/dms` | `{user_ids, workspace_id?}` | one other user: the pair's DM, 201 the first time, then 200 `status: "exists"`; 2–8 others: a new group DM |
| POST | `/v1/channels/{id}/members` | `{user_id?}` | join yourself (public channels), or invite `user_id` (any member, not in DMs) |
| DELETE | `/v1/channels/{id}/members/{user_id}` | | leave (yourself) or remove someone (admins) |
| GET | `/healthz` | | `ok` |

Membership replies: 200 `{status: "accepted", event_seq}`, 403 forbidden, 404 unknown channel or not a member, 202 `{status: "pending", request_id}` when the sequencer hasn't answered within 2 s (the client then learns the outcome from the channel's events), 503 when the command couldn't be sent.

## How it works

- **Create** (§6.2, [ADR-0010](../../docs/adr/0010-outbox-with-fast-path.md)): the `channels` row (plus `dm_pairs` for a DM) and an outbox `channel.created` commit in one transaction; then the same event goes straight to `messages.in` (**fast path**). Its dedupe ID is `create:{channel_id}`, so the sequencer turns the fast-path and outbox copies into one creation. If the fast path fails the answer is 202 `pending`: the channel exists and the outbox relay ([`services/outbox`](../outbox)) delivers it.
- **Workspaces:** checked against Postgres `workspace_members` (written by the auth and users services). The creator must belong to the channel's workspace (else 403, also for unknown workspaces), and so must every initial member (else 400). A join or invite is refused (403) unless the joining user belongs to the channel's workspace; an unknown channel is 404. A DM goes to the lowest-numbered workspace all its people share, or 403 if they share none.
- **DMs:** one per pair (`dm_pairs`, `ON CONFLICT DO NOTHING`); two people opening it at the same moment get the same channel.
- **Membership** (§6.3): `member.join_requested` / `member.kick_requested` with a fresh `request_id` (dedupe ID `cmd:{request_id}`). The waiter is registered before producing; every instance reads all of `commands.results` from its end at startup (no consumer group) and completes its own waiters. The rules (who may join, invite, remove) live in the sequencer.
- **My channels** (§6.6 cheap step): membership and heads from the wide-column store (`channels_by_user`, `channel_heads`, written by the persister), names from Postgres. Clients compare heads with what they have and fetch events only for channels that moved. Redis `chan_head` and sync tokens replace the per-channel head reads in Phase 1b.
- **Startup check:** exits with code 2 if `messages.in` is missing or its partition count differs from `XMSG_PARTITIONS` (ADR-0021).

## Not yet
- Revoking channel membership when someone leaves a workspace (§6.5).
- Channel reads and lists, rename/topic/archive (`channel.updated` via `meta-relay`), pins, personal settings (`member.settings`, ADR-0016).

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `XMSG_AUTH_JWKS_URL` | (required) | e.g. `http://auth:8081/.well-known/jwks.json` |
| `XMSG_PARTITIONS` | (required) | the installation's partition count |
| `XMSG_CHANNELS_PORT` | `8082` | HTTP port |
| `XMSG_DATABASE_URL` | `postgres://xmsg:xmsg-dev-only@127.0.0.1:5432/xmsg` | PostgreSQL with the Small-tier schema |
| `XMSG_KAFKA_BOOTSTRAP` | `127.0.0.1:9092` | Kafka |
| `XMSG_COMMAND_TIMEOUT_MS` | `2000` | wait for a membership verdict before answering `pending` |
| `XMSG_CQL_NODES` / `XMSG_CQL_KEYSPACE` | `127.0.0.1:9042` / `xmsg` | wide-column store for my channels |

## Tests

```bash
cargo test -p xmsg-channels
XMSG_IT_PG=postgres://xmsg:xmsg-dev-only@127.0.0.1:5432/xmsg cargo test -p xmsg-channels --test integration -- --ignored
```

The end-to-end smoke (`tools/e2e-smoke` with `XMSG_CHANNELS_URL`) creates its channel here and checks membership verdicts against the real sequencer.
