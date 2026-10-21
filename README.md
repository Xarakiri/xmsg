# xmsg

A Slack-style messenger designed for 1M concurrent users, that companies can also run on their own infrastructure.

- **Architecture:** [docs/architecture-plan.md](docs/architecture-plan.md)
- **Decisions:** [docs/adr/](docs/adr/README.md)
- **Status:** Phase 0 local work done; Phase 1a started. Messages already flow client → gateway → messages → Kafka → sequencer → persister → Cassandra (`tools/e2e-smoke`, CI job `pipeline`). Nothing here is production-ready.

## Repository layout

| Path | What | Licence |
|---|---|---|
| `proto/` | Protobuf contracts (Buf): client protocol, events, commands | Apache-2.0 |
| `spec/partitioning/` | Partition function spec and shared test vectors | Apache-2.0 |
| `crates/` | Shared Rust libraries: `xmsg-partition`, `xmsg-proto` (prost types for `proto/`) | AGPL-3.0-only |
| `services/sequencer/` | Sequencer (Kotlin + Kafka Streams) | AGPL-3.0-only |
| `services/gateway/` | Realtime gateway (Elixir) | AGPL-3.0-only |
| `services/messages/` | Messages service (Rust): NATS sends → `messages.in` | AGPL-3.0-only |
| `services/persister/` | Persister (Rust): `messages.persisted` → Cassandra/ScyllaDB | AGPL-3.0-only |
| `clients/core/` | Shared TypeScript client core | Apache-2.0 |
| `ee/`, `*/ee/` | Enterprise Edition code | Commercial ([ee/LICENSE](ee/LICENSE)) |
| `deploy/compose/` | Small-tier Docker Compose stack (also local dev) | AGPL-3.0-only |
| `tools/` | Repo scripts, `tools/loadgen` (WebSocket load generator), `tools/e2e-smoke` (pipeline smoke test) | AGPL-3.0-only |
| `docs/spikes/` | Phase 0 measurement reports | — |

Every source file starts with an SPDX licence identifier; `tools/check-spdx.sh` enforces it.

## Prerequisites (local)

- Rust 1.85+ and `protoc`, Elixir 1.20 / OTP 29, JDK 21 + Gradle 8.10+, Node 22.18+ (runs TypeScript tests directly), Buf
- Docker with Compose v2, and **at least 10–12 GB of memory for the Docker VM** (Colima: `colima start --memory 12 --cpu 6`)

## Common commands

```bash
make test        # all unit tests: Rust, Elixir, Kotlin, TypeScript, partition vectors
make proto-lint  # buf lint
make up          # start the Small-tier stack (Kafka, Cassandra 5, Postgres, Valkey, NATS)
make down        # stop it and keep data
```

See [deploy/compose/README.md](deploy/compose/README.md) for the stack.

## Contributing

Contributions require a signed Contributor License Agreement (CLA); see [CONTRIBUTING.md](CONTRIBUTING.md).
