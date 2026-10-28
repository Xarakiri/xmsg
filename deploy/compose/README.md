<!-- SPDX-License-Identifier: AGPL-3.0-only -->
# Small-tier stack (Docker Compose)

The Small self-hosting tier ([ADR-0019](../../docs/adr/0019-self-hosting-and-deployment-tiers.md)) and the local development environment. It bundles only open-source backends ([ADR-0023](../../docs/adr/0023-osi-bundled-backends-and-wide-column-choice.md)). Running it in production (sizing, monitoring, troubleshooting): [docs/operations/small-tier.md](../../docs/operations/small-tier.md).

| Service | Image | Host port | Memory limit |
|---|---|---|---|
| Kafka (KRaft, 1 node, transactions enabled) | `apache/kafka:3.9.1` | 9092 | 1.5 GB |
| Cassandra 5 | `cassandra:5.0` | 9042 | 2.5 GB |
| PostgreSQL 17 | `postgres:17` | 5432 | 512 MB |
| Valkey | `valkey/valkey:8.0` | 6379 | 256 MB |
| NATS | `nats:2.10-alpine` | 4222 (monitoring 8222) | 256 MB |
| OpenSearch *(profile `search`)* | `opensearchproject/opensearch:2.18.0` | 9200 | 1.5 GB |
| Garage (files, [ADR-0024](../../docs/adr/0024-media-and-object-storage.md)) | `dxflrs/garage:v2.5.0` | 3900 (admin 3903) | 256 MB |
| LiveKit *(profile `calls`, [ADR-0025](../../docs/adr/0025-voice-and-video-calls.md))* | `livekit/livekit-server:v1.13.1` | 7880 (local), 7881/tcp, 7882/udp | 1 GB |
| coturn *(profile `calls`)* | `coturn/coturn:4.6.3` | 3478 udp/tcp, 49160–49200/udp | 256 MB |

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
./install.sh --search                                                    # with search (+1.5 GB)
```

Back up with [`xmsgctl backup`](../../tools/xmsgctl/README.md), which archives `.env` together with the data: without its signing key every session ends, and without its Postgres password the database doesn't open. `xmsgctl health` checks a running installation.

## Air-gapped install
For a machine without access to container registries, build a bundle where there is access: every image the installation runs (xmsg's and the backends'), the compose files, `install.sh` and optionally `xmsgctl`, in one archive.

```bash
./make-bundle.sh --registry registry.example.com/xmsg --version 0.5.0 --xmsgctl ./xmsgctl
# or from this checkout: ./make-bundle.sh --version dev --build
```

The images must match the target machine's CPU (`linux-amd64` or `linux-arm64` in the name). The bundle includes search unless built with `--no-search`, since it can't be added later without registry access. Copy `xmsg-small-<version>-linux-<arch>.tar` across, then:

```bash
tar -xf xmsg-small-0.5.0-linux-amd64.tar && cd xmsg-small-0.5.0-linux-amd64
./install.sh                                   # loads the images from images/, pulls nothing
./xmsgctl health --wait 300
```

`install.sh` checks the bundle against `SHA256SUMS` before loading it. To upgrade, unpack the newer bundle, copy `.env` (and `certs/` and `docker-compose.local.yml` if you have them) into it, and run `./install.sh` there: the bundle's version replaces the installed one, and the rest of [Upgrade](#upgrade) applies. CI's `air-gapped` job builds a bundle, deletes every image, makes registries unreachable, installs from the bundle and runs the end-to-end smoke test.

## Upgrade
Data and secrets live in Docker volumes and `.env`, not in this directory, so an upgrade swaps the release and keeps both:

```bash
xmsgctl backup --out before-upgrade.tar          # the way back (stops the stack for a few minutes)
# get the new release's deploy/compose (git checkout v0.5.0, or unpack it); if it's a new
# directory, copy .env (and certs/, docker-compose.local.yml) into it
./install.sh --yes --version 0.5.0               # keeps the secrets and settings, pulls, restarts
xmsgctl health --wait 300
```

Releases say in their notes when they need more than this (a state-format migration, ADR-0003). The sequencer refuses to start if its state changelogs are missing on an installation that has processed data, rather than silently restarting its counters at zero. To go back, install the previous release again and `xmsgctl restore before-upgrade.tar --yes`: newer data formats aren't guaranteed to be readable by older versions.

CI's `upgrade` job tests this path for every change: it installs the version the change lands on, seeds a channel (messages, membership, a reaction, client message ids), upgrades with the steps above, and checks that the history, an old access token, the channel's counters, dedupe, membership and reaction counts all survived.

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
