<!-- SPDX-License-Identifier: AGPL-3.0-only -->
# Gateway (Elixir)

Phase 0 skeleton of the realtime gateway ([plan §5.1](../../docs/architecture-plan.md), [ADR-0009](../../docs/adr/0009-nats-delivery-and-gap-detection.md), [ADR-0013](../../docs/adr/0013-tls-at-the-load-balancer.md)).

## What it does today
- **WebSockets** on `/ws` (Bandit), one BEAM process per connection, binary Protobuf frames only (`proto/xmsg/v1/client.proto`). `/healthz` for probes.
- **Connect first:** the first frame must be `Connect` with a valid token within the auth timeout (5 s), otherwise the socket closes with code 1008. On success the client gets `Welcome` with a resume token and the installation's **partition count** (ADR-0021).
- **Sends** go to the messages service over **NATS request/reply** (`xmsg.svc.messages.send`) as an `InboundEvent` whose `actor_id` comes from the token, never from the client. A reply produces `SendAck`; no reply means no ack, and the client retries from its outbox (ADR-0011).
- **Delivery:** subscribes to `gw.{gateway_id}`; each `GatewayBatch` carries frames **encoded once** by fan-out, and the same binary is pushed to every recipient socket. Batches are routed by partition to a pool of delivery workers (one per scheduler), so per-stream order is kept without a single-process bottleneck. Local measurements: [docs/spikes/2026-10-21-gateway-local.md](../../docs/spikes/2026-10-21-gateway-local.md).
- **Gap detection** (ADR-0009): consecutive `seq` per stream `(partition, owner_epoch)`; `StreamHeads` on `stream_heads.{gateway_id}` catch a lost last batch; a new epoch resets tracking; on a gap, local clients get `Resync{partition}`; after a NATS resubscription, everyone gets a full `Resync`.
- Ping/Pong.

## Not yet
- Real token validation (Ed25519 JWT via JWKS). Today only `dev:<user_id>` tokens work, and only with `XMSG_DEV_AUTH=true`, which is off in production config.
- Resume tokens are issued but not yet accepted on reconnect.
- Session events to Kafka (`sessions.events`), presence, typing, pull-mode channel subscriptions (`ch.{id}`), revocations, admission control, per-connection limits, drain on deploy.
- Hibernation of idle connections, kernel/BEAM tuning, and the 100k-connections-per-pod capacity spike.

## Configuration (environment, `config/runtime.exs`)

| Variable | Default | Meaning |
|---|---|---|
| `XMSG_GATEWAY_ID` | `$HOSTNAME` | Gateway identity in NATS subjects |
| `XMSG_GATEWAY_PORT` | `4000` | WebSocket port (TLS ends before the gateway in Medium/Large) |
| `XMSG_PARTITIONS` | `16` | This installation's partition count, sent to clients |
| `XMSG_NATS_HOST` / `XMSG_NATS_PORT` | `127.0.0.1` / `4222` | NATS server |
| `XMSG_DEV_AUTH` | `false` | Accept `dev:<user_id>` tokens (development only) |

## Protobuf modules
`lib/xmsg_gateway/proto/` is generated from `proto/` by `tools/gen-elixir-proto.sh` (needs `protoc` and `mix escript.install hex protobuf 0.17.1`). CI fails if it's out of date.

## Tests

```bash
mix test
```

Tests run the real HTTP/WebSocket server on a random port with an in-memory bus (`test/support/local_bus.ex`) in place of NATS, and a small Mint-based client (`test/support/ws_client.ex`).
