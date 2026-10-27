# xmsg

A Slack-style messenger designed for 1M concurrent users, that companies can also run on their own infrastructure.

- **Architecture:** [docs/architecture-plan.md](docs/architecture-plan.md)
- **Decisions:** [docs/adr/](docs/adr/README.md)
- **Status:** Phase 0 local work done; Phase 1a started. **The first chat round trip works:** a message goes client → gateway → messages → Kafka → sequencer, is stored by the persister in Cassandra, and is delivered by fan-out to the other members' WebSockets (`tools/e2e-smoke`, CI job `pipeline`). Nothing here is production-ready.

## Repository layout

| Path | What | Licence |
|---|---|---|
| `proto/` | Protobuf contracts (Buf): client protocol, events, commands | Apache-2.0 |
| `spec/partitioning/` | Partition function spec and shared test vectors | Apache-2.0 |
| `crates/` | Shared Rust libraries: `xmsg-partition`, `xmsg-proto` (prost types for `proto/`), `xmsg-authn` (access-token checks) | AGPL-3.0-only |
| `services/sequencer/` | Sequencer (Kotlin + Kafka Streams) | AGPL-3.0-only |
| `services/gateway/` | Realtime gateway (Elixir) | AGPL-3.0-only |
| `services/auth/` | Auth (Rust): sign-up, login, Ed25519 JWTs, refresh tokens, JWKS | AGPL-3.0-only |
| `services/channels/` | Channels (Rust): channels and DMs (outbox + fast path), membership commands | AGPL-3.0-only |
| `services/messages/` | Messages service (Rust): NATS sends → `messages.in` | AGPL-3.0-only |
| `services/persister/` | Persister (Rust): `messages.persisted` → Cassandra/ScyllaDB | AGPL-3.0-only |
| `services/outbox/` | Outbox poller (Postgres → Kafka) and meta-relay (`channels.meta` → `messages.in`) (Rust) | AGPL-3.0-only |
| `services/readstate/` | Read state (Rust): read positions and unread counts, one writer per user | AGPL-3.0-only |
| `services/fanout/` | Fan-out (Rust): sequenced events → gateways → clients | AGPL-3.0-only |
| `clients/core/` | Shared TypeScript client core: protocol, connection, state | Apache-2.0 |
| `clients/web/` | Minimal web client (React + Vite) | Apache-2.0 |
| `ee/`, `*/ee/` | Enterprise Edition code | Commercial ([ee/LICENSE](ee/LICENSE)) |
| `deploy/compose/` | Small-tier Docker Compose stack (also local dev) | AGPL-3.0-only |
| `tools/` | Repo scripts, `tools/loadgen` (WebSocket load generator), `tools/e2e-smoke` (pipeline smoke test) | AGPL-3.0-only |
| `docs/spikes/` | Phase 0 measurement reports | — |

Every source file starts with an SPDX licence identifier; `tools/check-spdx.sh` enforces it.

## Prerequisites (local)

- Rust 1.99+ and `protoc`, Elixir 1.20 / OTP 29, JDK 27 + Gradle 9.8+ (Kotlin 2.4 via the wrapper), Node 26+ (runs TypeScript tests directly), Buf
- Docker with Compose v2, and **at least 10–12 GB of memory for the Docker VM** (Colima: `colima start --memory 12 --cpu 6`)

## Common commands

```bash
make test        # all unit tests: Rust, Elixir, Kotlin, TypeScript, partition vectors
make proto-lint  # buf lint
make up          # start the Small-tier stack (Kafka, Cassandra 5, Postgres, Valkey, NATS)
make down        # stop it and keep data
```

See [deploy/compose/README.md](deploy/compose/README.md) for the stack. Operating a Small-tier installation: [docs/operations/small-tier.md](docs/operations/small-tier.md).

## Contributing

Contributions require a signed Contributor License Agreement (CLA); see [CONTRIBUTING.md](CONTRIBUTING.md).
