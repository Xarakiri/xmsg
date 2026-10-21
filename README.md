# xmsg

A Slack-style messenger designed for 1M concurrent users, that companies can also run on their own infrastructure.

- **Architecture:** [docs/architecture-plan.md](docs/architecture-plan.md)
- **Decisions:** [docs/adr/](docs/adr/README.md)
- **Status:** Phase 0 (foundations and spikes). Nothing here is production-ready.

## Repository layout

| Path | What | Licence |
|---|---|---|
| `proto/` | Protobuf contracts (Buf): client protocol, events, commands | Apache-2.0 |
| `spec/partitioning/` | Partition function spec and shared test vectors | Apache-2.0 |
| `crates/` | Shared Rust libraries | AGPL-3.0-only |
| `services/sequencer/` | Sequencer (Kotlin + Kafka Streams) | AGPL-3.0-only |
| `services/gateway/` | Realtime gateway (Elixir) | AGPL-3.0-only |
| `clients/core/` | Shared TypeScript client core | Apache-2.0 |
| `ee/`, `*/ee/` | Enterprise Edition code | Commercial ([ee/LICENSE](ee/LICENSE)) |
| `deploy/compose/` | Small-tier Docker Compose stack (also local dev) | AGPL-3.0-only |
| `tools/` | Repo scripts | AGPL-3.0-only |

Every source file starts with an SPDX licence identifier; `tools/check-spdx.sh` enforces it.

## Prerequisites (local)

- Rust 1.85+, Elixir 1.17+ / OTP 27, JDK 21 + Gradle 8.10+, Node 22.18+ (runs TypeScript tests directly), Buf
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
