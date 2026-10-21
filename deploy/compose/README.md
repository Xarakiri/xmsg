<!-- SPDX-License-Identifier: AGPL-3.0-only -->
# Small-tier stack (Docker Compose)

The Small self-hosting tier ([ADR-0019](../../docs/adr/0019-self-hosting-and-deployment-tiers.md)) and the local development environment. It bundles only open-source backends ([ADR-0023](../../docs/adr/0023-osi-bundled-backends-and-wide-column-choice.md)).

| Service | Image | Host port | Memory limit |
|---|---|---|---|
| Kafka (KRaft, 1 node, transactions enabled) | `apache/kafka:3.9.1` | 9092 | 1.5 GB |
| Cassandra 5 | `cassandra:5.0` | 9042 | 2.5 GB |
| PostgreSQL 17 | `postgres:17` | 5432 | 512 MB |
| Valkey | `valkey/valkey:8.0` | 6379 | 256 MB |
| NATS | `nats:2.10-alpine` | 4222 (monitoring 8222) | 256 MB |
| OpenSearch *(profile `search`)* | `opensearchproject/opensearch:2.18.0` | 9200 | 1.5 GB |

One-shot jobs create the Kafka topics (`kafka-init`, N partitions from `XMSG_PARTITIONS`, default 16) and the Cassandra schema (`cassandra-init`). Postgres runs `init/postgres/*.sql` on first start.

## Requirements
Docker with Compose v2 and **at least 6 GB of memory for the Docker VM** (8 GB with search). With Colima:

```bash
colima stop && colima start --memory 8 --cpu 4
```

## Use

```bash
docker compose up -d                   # core stack
docker compose --profile search up -d  # with OpenSearch
docker compose ps                      # kafka-init and cassandra-init should show "Exited (0)"
docker compose down                    # stop, keep data
docker compose down -v                 # stop and delete all data
```

## Notes
- All ports bind to `127.0.0.1`. Passwords here are for local development only; a real Small install generates its own (the install script, Phase 4).
- **The partition count is fixed once topics exist** ([ADR-0021](../../docs/adr/0021-partition-count-per-installation.md)). `kafka-init` refuses to continue if existing topics have a different count.
