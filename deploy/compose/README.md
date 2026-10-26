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

## Install (Small tier)
On a Linux VM with Docker and the Compose plugin (8 GB of memory or more):

```bash
./install.sh
```

It asks for the domain and how to get the TLS certificate (Let's Encrypt, your own, or an internal one for evaluation), generates the secrets into `.env` (kept private, mode 600), builds or pulls the images, starts everything and waits until `https://<domain>/` answers. Run it again to change the domain or TLS settings: the secrets are kept, and the partition count can't change (ADR-0021). Non-interactive:

```bash
./install.sh --yes --domain chat.example.com --tls acme --email ops@example.com
./install.sh --yes --domain chat.internal --tls cert --cert ./chat.pem --key ./chat-key.pem
./install.sh --yes --registry registry.example.com/xmsg --version 0.4.0   # published images
```

Back up `.env` together with the data: without its signing key every session ends, and without its Postgres password the database doesn't open.

## The whole application, by hand
`docker-compose.yml` is the backends, also used for local development with the services run from source. `docker-compose.app.yml` adds the application on top: every xmsg service, the sequencer, the gateway, the web app, and **Caddy** in front for HTTPS (Let's Encrypt, your own certificate, or Caddy's internal CA for evaluation).

```bash
export COMPOSE_FILE=docker-compose.yml:docker-compose.app.yml
docker compose build          # images from this checkout (or set XMSG_REGISTRY / XMSG_VERSION)
docker compose up -d --wait
```

It reads its settings from `.env`:

| Variable | Meaning |
|---|---|
| `XMSG_AUTH_KEY` | **Required.** 32 random bytes, base64: the signing key for access tokens |
| `XMSG_PUSH_VAPID_PRIVATE_KEY` | **Required.** The Web Push key (see `services/push`) |
| `XMSG_POSTGRES_PASSWORD` | Postgres password (default `xmsg-dev-only`: change it) |
| `XMSG_KAFKA_CLUSTER_ID` | A base64 UUID, fixed per installation |
| `XMSG_DOMAIN` | The name people use, e.g. `chat.example.com` (default `localhost`) |
| `XMSG_TLS` | `tls` arguments for Caddy: an email for Let's Encrypt, `/certs/cert.pem /certs/key.pem` for your own (in `./certs`), or `internal` |
| `XMSG_REGISTRY` / `XMSG_VERSION` | Where the images come from (default `xmsg` / `dev`, i.e. built locally) |
| `XMSG_HTTP_PORT` / `XMSG_HTTPS_PORT` | Published ports (80 / 443) |

The web app (nginx) is also on `127.0.0.1:8080` and the sequencer's membership endpoint on `127.0.0.1:8090`, for health checks on the machine itself. CI runs the end-to-end smoke test against this bundle.

## Notes
- All ports bind to `127.0.0.1`. Passwords here are for local development only; a real Small install generates its own (the install script, Phase 4).
- **The partition count is fixed once topics exist** ([ADR-0021](../../docs/adr/0021-partition-count-per-installation.md)). `kafka-init` refuses to continue if existing topics have a different count.
