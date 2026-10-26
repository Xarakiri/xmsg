<!-- SPDX-License-Identifier: AGPL-3.0-only -->
# Gateway (Elixir)

Phase 0 skeleton of the realtime gateway ([plan §5.1](../../docs/architecture-plan.md), [ADR-0009](../../docs/adr/0009-nats-delivery-and-gap-detection.md), [ADR-0013](../../docs/adr/0013-tls-at-the-load-balancer.md)).

## What it does today
- **WebSockets** on `/ws` (Bandit), one BEAM process per connection, binary Protobuf frames only (`proto/xmsg/v1/client.proto`). `/healthz` for probes.
- **Connect first:** the first frame must be `Connect` with a valid token within the auth timeout (5 s), otherwise the socket closes with code 1008.
- **Tokens** are the auth service's Ed25519 JWTs, checked locally (signature, `iss`/`aud` = `xmsg`, `exp` with 30 s leeway, numeric `sub`) against its JWKS. The key set is fetched at startup (retried every 2 s until it loads), refreshed every 5 minutes, and early when a token names an unknown `kid` (at most every 30 s), so key rotation needs no restart. Keys sit in `:persistent_term`; a connect never calls the auth service. On success the client gets `Welcome` with a resume token and the installation's **partition count** (ADR-0021).
- **Sends** go to the messages service over **NATS request/reply** (`xmsg.svc.messages.send`) as an `InboundEvent` whose `actor_id` comes from the token, never from the client. A reply produces `SendAck`; no reply means no ack, and the client retries from its outbox (ADR-0011).
- **Delivery:** subscribes to `gw.{gateway_id}`; each `GatewayBatch` carries frames **encoded once** by fan-out, and the same binary is pushed to every recipient socket. Batches are routed by partition to a pool of delivery workers (one per scheduler), so per-stream order is kept without a single-process bottleneck. Local measurements: [docs/spikes/2026-10-21-gateway-local.md](../../docs/spikes/2026-10-21-gateway-local.md).
- **Gap detection** (ADR-0009): consecutive `seq` per stream `(partition, owner_epoch)`; `StreamHeads` on `stream_heads.{gateway_id}` catch a lost last batch; a new epoch resets tracking; on a gap, local clients get `Resync{partition}`; after a NATS resubscription, everyone gets a full `Resync`.
- **Sessions:** each authenticated connection is published to Kafka `sessions.events` (compacted, key = connection ID) with brod; a monitor-based tracker publishes the tombstone however the socket ends (clean close, abrupt disconnect or crash). A heartbeat goes to NATS `gateway.alive.{gateway_id}` every 2 s.
- **Typing** (§5.9): a `Typing{channel_id}` frame goes to NATS `typing.{partition}` as `TypingIndicator{channel_id, user_id}` (the user from the token), at most once per channel every 3 s per connection, without waiting. Fan-out's answers arrive on `gw.{gateway_id}.ephemeral` (plain `Delivery` messages, no stream) and go straight to the users' sockets as `TypingIndicator` frames.
- **Read receipts** (ADR-0012): a `Receipt{channel_id, msg_seq}` frame goes to NATS `receipts.{partition}` as `ReadReceipt` (the user from the token): at once, then at most once a second per channel with the latest position (coalesced), never backwards. The fan-out's answers arrive on `gw.{id}.ephemeral` like typing.
- **Presence** (§5.9): each connection pings the presence service (`xmsg.svc.presence.ping`) on connect, on `Activity` changes, every 30 s, and when it closes (from the session tracker, so crashes count). `WatchPresence{user_ids}` sets the users a socket watches (registry `XmsgGateway.PresenceWatchers`); the gateway answers with current statuses (`xmsg.svc.presence.get`) and forwards changes from `presence.*` as `PresenceUpdates` frames.
- Ping/Pong.

- **Revocations** (§6.4): `XmsgGateway.Revocations` subscribes to NATS `auth.revocations`. A `SessionRevoked` closes every socket of that session (close code 1008, "session revoked") and puts the session in an ETS deny set until `refuse_until_ms`, so its still-valid access tokens can't reconnect. The deny check runs before *and* after a connecting socket registers its session, so a revocation landing in between isn't missed. Development tokens (session 0) are never revoked.

- **After a (re)start** the deny set is rebuilt from the last 16 minutes of Kafka `auth.events` (the outbox copy of every revocation; `XmsgGateway.Revocations.Replay`). Nothing older can matter, because no access token lives longer. Turn it off with `XMSG_REVOCATIONS_REPLAY=false`.

- **Admission control** (`XmsgGateway.Admission`), checked before the WebSocket upgrade: at most `XMSG_ADMISSION_RATE` new sessions a second per pod, an optional ceiling of `XMSG_MAX_CONNECTIONS`, and at most `XMSG_PREAUTH_PER_IP` sockets per client address that haven't authenticated yet. A refused upgrade gets `503` with a `Retry-After` of 1–5 s, and clients retry with backoff and jitter, so a reconnect storm spreads out. Client addresses come from `X-Forwarded-For` behind `XMSG_PROXY_HOPS` trusted proxies.
- **Per-connection frame budget:** a token bucket of `XMSG_FRAME_RATE` frames a second with bursts up to `XMSG_FRAME_BURST`; a connection far over it is closed (`1008 too many frames`) and comes back through admission control.

- **Drain on shutdown** (`XmsgGateway.Drain`, from `Application.prep_stop/1`): on SIGTERM `/healthz` turns 503 (the readiness probe takes the pod out of its Service), upgrades are refused, and connections are closed with `1012 service restart` spread at random over `XMSG_DRAIN_SECONDS` (default 300), so clients reconnect to other pods a few at a time. The chart gives the pod `drainSeconds` + 30 s of grace and rolls a quarter of the pods at a time.

- **Pull-mode channels** (ADR-0008, `XmsgGateway.Pull`): large channels arrive once per gateway on `ch.{channel_id}` instead of per member. At connect a socket fetches its user's pull-mode channels from the channels service (`GET /v1/me/channels?mode=pull` with the user's token; again 5 s later in case a join raced it) and registers under each; the gateway subscribes to `ch.{id}` while it has a local member (unused subscriptions are dropped every 30 s, and all are renewed after a NATS reconnect). Fan-out's `PullControl` messages on `gw.{gateway_id}.control` add or remove a connected user's channels as they join, leave or switch modes. Frames on `ch.{id}` carry no stream sequence: clients notice gaps by `event_seq` and drop duplicates the same way. Off without `XMSG_CHANNELS_URL`.
- **Viewers and bumps** (plan §6.7): a client says which channel it has open (`ViewChannel`, again after each reconnect). In pull-mode channels a socket gets full events only for that channel, plus membership changes always; for the others it gets a `ChannelBump` with the latest `event_seq` / `msg_seq`, at most one per channel per 5 s (`XmsgGateway.Config.bump_ms`), and the client fetches the events when the channel is opened. A client that never sends `ViewChannel` (an older app) keeps getting every event in full.

## Not yet
- Resume tokens are issued but not yet accepted on reconnect.
- Refcounted per-user presence subscriptions, `user.banned` / `workspace.member_removed`.
- Hibernation of idle connections, kernel/BEAM tuning, and the 100k-connections-per-pod capacity spike.

## Configuration (environment, `config/runtime.exs`)

| Variable | Default | Meaning |
|---|---|---|
| `XMSG_GATEWAY_ID` | `$HOSTNAME` | Gateway identity in NATS subjects |
| `XMSG_GATEWAY_PORT` | `4000` | WebSocket port (TLS ends before the gateway in Medium/Large) |
| `XMSG_PARTITIONS` | `16` | This installation's partition count, sent to clients |
| `XMSG_NATS_HOST` / `XMSG_NATS_PORT` | `127.0.0.1` / `4222` | NATS server |
| `XMSG_AUTH_JWKS_URL` | (none) | Auth service key set, e.g. `http://auth:8081/.well-known/jwks.json`; without it only dev tokens work |
| `XMSG_REVOCATIONS_REPLAY` | `true` | Rebuild the revocation deny set from Kafka `auth.events` on start |
| `XMSG_DEV_AUTH` | `false` | Also accept `dev:<user_id>` tokens (development only) |
| `XMSG_KAFKA_BOOTSTRAP` | `127.0.0.1:9092` | Kafka, for `sessions.events` |
| `XMSG_ADMISSION_RATE` | `2000` | New sessions a second per pod (`0`: unlimited) |
| `XMSG_MAX_CONNECTIONS` | `0` | Connections per pod (`0`: unlimited) |
| `XMSG_PREAUTH_PER_IP` | `100` | Unauthenticated sockets per client address (`0`: unlimited) |
| `XMSG_PROXY_HOPS` | `0` | Trusted proxies appending to `X-Forwarded-For` (`0`: the TCP peer) |
| `XMSG_FRAME_RATE` / `XMSG_FRAME_BURST` | `30` / `100` | Client frames a second per connection, and the burst above it |
| `XMSG_DRAIN_SECONDS` | `300` | On shutdown, connections are closed spread over this long |
| `XMSG_CHANNELS_URL` | (none) | The channels service, e.g. `http://channels:8082`, for each user's pull-mode channels; without it pull mode is off on this gateway |

## Protobuf modules
`lib/xmsg_gateway/proto/` is generated from `proto/` by `tools/gen-elixir-proto.sh` (needs `protoc` and `mix escript.install hex protobuf 0.17.1`). CI fails if it's out of date.

## Tests

```bash
mix test
```

Tests run the real HTTP/WebSocket server on a random port with an in-memory bus (`test/support/local_bus.ex`) in place of NATS, and a small Mint-based client (`test/support/ws_client.ex`).
