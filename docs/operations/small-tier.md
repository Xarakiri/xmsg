<!-- SPDX-License-Identifier: AGPL-3.0-only -->
# Operating the Small tier

The Small tier is xmsg on one Linux machine with Docker Compose, for up to about 10,000 users (plan §12, [ADR-0019](../adr/0019-self-hosting-and-deployment-tiers.md)). This guide covers sizing, installing, day-to-day operation, backups, upgrades, monitoring and troubleshooting. The commands run in the installation's directory (`deploy/compose`, or the unpacked bundle).

- [What runs](#what-runs)
- [Sizing](#sizing)
- [Install](#install)
- [Day to day](#day-to-day)
- [Backups](#backups)
- [Upgrades](#upgrades)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)
- [Not yet](#not-yet)

## What runs
Everything is one Compose project named `xmsg`. Only Caddy listens on public ports; everything else is bound to `127.0.0.1`.

| Service | Role | Memory cap | Port (host) |
|---|---|---|---|
| caddy | TLS and the public entry point | 128 MB | 80, 443 |
| web | the web app, and the proxy to the APIs and `/ws` | 128 MB | 127.0.0.1:8080 |
| gateway | WebSocket connections | 512 MB | — |
| sequencer | orders every message and membership change (Kafka Streams) | 1 GB | 127.0.0.1:8090 |
| 12 Rust services | auth, users, channels, messages, readstate, persister, fanout, outbox-relay, meta-relay, reaction-batcher, presence, push | 256 MB each | — |
| search-indexer | only with search | 256 MB | — |
| kafka | the event log | 1.5 GB | 127.0.0.1:9092 |
| cassandra | messages and membership | 2.5 GB | 127.0.0.1:9042 |
| postgres | accounts, workspaces, channels, settings | 512 MB | 127.0.0.1:5432 |
| valkey | rate limits and presence | 256 MB | 127.0.0.1:6379 |
| nats | real-time delivery between services | 256 MB | 127.0.0.1:4222, monitoring 127.0.0.1:8222 |
| opensearch | only with search | 1.5 GB | 127.0.0.1:9200 |

`kafka-init` and `cassandra-init` run once at every start and exit with status 0; that's expected.

The data lives in Docker volumes, not in this directory: `xmsg_postgres-data`, `xmsg_kafka-data`, `xmsg_cassandra-data`, `xmsg_sequencer-state`, `xmsg_caddy-data` (certificates), and `xmsg_opensearch-data` with search. `.env` holds the secrets: the token signing key, the Postgres password, Kafka's cluster ID and the Web Push key. Losing `.env` signs everyone out and leaves the data unreadable, so it's part of every backup.

## Sizing
These are starting points from the memory caps above, **not load-test results**: the tier hasn't been benchmarked yet, and published performance targets cover the Large tier only (plan §12).

| | Evaluation | Production (up to ~10k users) |
|---|---|---|
| Memory | 8 GB (12 GB with search) | 16 GB (24 GB with search) |
| CPU | 2 vCPU | 4–8 vCPU |
| Disk | 40 GB SSD | SSD sized for a year of messages (below), 250 GB at least |
| OS | any 64-bit Linux with Docker Engine and Compose v2 | the same; amd64 or arm64 |

- **Memory:** the caps add up to about 10 GB (12 GB with search). Services rarely sit at their caps, which is why 8 GB is enough to try it; production leaves room for the OS page cache, which Cassandra and Kafka rely on. A service over its cap is killed and restarted (see [Troubleshooting](#troubleshooting)).
- **Disk:** Cassandra holds every message for good, roughly 1 KB per message with its indexes (an estimate; long messages cost more). 10,000 users sending 50 messages a day each is 500,000 messages a day, about 15 GB a month, so plan for a year of that plus room to grow. Kafka keeps recent events only (3 days for incoming messages, 7 for sequenced ones), a few GB at that load. Backups need the same space again while they're written. Keep a quarter of the disk free: Cassandra's compaction needs room to work.
- **One machine:** the Small tier has no failover. For high availability, use the Medium tier (Helm on Kubernetes).
- **Partitions are fixed:** the Small tier uses 16 Kafka partitions, set at install and never changed in place ([ADR-0021](../adr/0021-partition-count-per-installation.md)).

## Install
[deploy/compose/README.md](../../deploy/compose/README.md#install-small-tier) has the details. In short:

```bash
./install.sh --domain chat.example.com --tls acme --email ops@example.com --yes
./xmsgctl health --wait 300      # or the xmsgctl you installed
```

- **TLS:** `acme` (Let's Encrypt: needs the domain's DNS pointing at the machine and ports 80 and 443 reachable from the internet), `cert` (your own certificate and key), or `internal` (a self-signed certificate, for evaluation).
- **Firewall:** open 80 and 443 only. Everything else is bound to `127.0.0.1`.
- **Without registry access:** build an [air-gapped bundle](../../deploy/compose/README.md#air-gapped-install) elsewhere and install from it.
- **Search** (`--search`) needs another 1.5 GB of memory. It stays on once enabled.
- Re-running `install.sh` keeps the secrets, so it's also how you change the domain or TLS settings.

## Day to day

```bash
docker compose ps                      # every service up (healthy where it has a check)
docker compose logs -f --tail 100 messages gateway
docker compose restart push            # one service
docker compose stop && docker compose up -d --wait   # everything (the data stays)
./xmsgctl health                       # containers and every service's /healthz
```

**Logs.** Services log to stdout, which Docker keeps in its default `json-file` logs. These aren't rotated unless you configure it, so set rotation once, in `/etc/docker/daemon.json`, then restart Docker:

```json
{ "log-driver": "local", "log-opts": { "max-size": "50m", "max-file": "5" } }
```

**Your own settings.** Put changes in `docker-compose.local.yml` next to the compose files; `install.sh` adds it to `COMPOSE_FILE` and keeps it on every run. Then run `./install.sh --yes` again, or `docker compose up -d --wait`. For example, a bigger Cassandra and a gateway that accepts connections faster:

```yaml
services:
  cassandra:
    mem_limit: 4g
  gateway:
    environment:
      XMSG_ADMISSION_RATE: "5000"     # new connections per second (default 2000)
```

Gateway settings worth knowing: `XMSG_ADMISSION_RATE` (new connections per second, default 2000), `XMSG_MAX_CONNECTIONS` (default 0, no limit), `XMSG_PREAUTH_PER_IP` (connections per IP that haven't signed in yet, default 100) and `XMSG_DRAIN_SECONDS` (how long a restart spreads reconnects over; 30 in this bundle).

## Backups

```bash
./xmsgctl backup --out /backups/xmsg-$(date +%F).tar
```

- **The stack stops while it runs:** expect a few minutes of downtime on a small install. Every volume and `.env` are archived together, consistently. Schedule it for a quiet hour, for example from cron:

  ```cron
  30 3 * * *  cd /opt/xmsg/deploy/compose && ./xmsgctl backup --out /backups/xmsg-$(date +\%F).tar
  ```

- **The archive holds the secrets** (mode 600). Copy it off the machine to storage that's as private as `.env`, and prune old ones.
- **Restore** onto the same or a new machine: `./xmsgctl restore FILE --yes`, adding `--with-env` on a new machine to take the backup's `.env`. Then run `./xmsgctl health --wait 300`.
- **Test a restore** now and then, on a spare machine. A backup you've never restored is a guess.

See [xmsgctl](../../tools/xmsgctl/README.md) for what it refuses and why.

## Upgrades
The steps are in [deploy/compose/README.md](../../deploy/compose/README.md#upgrade): back up, get the new release (keep `.env`, `certs/` and `docker-compose.local.yml`), run `./install.sh --yes --version NEW`, then check health. Read the release notes first: a release that changes a stored format says so and gives the extra step. CI upgrades from the previous version on every change and checks that messages, sign-ins, counters and membership survive. To roll back, install the previous release and restore the backup taken before the upgrade.

## Monitoring
The services don't export metrics (Prometheus) yet. Until they do, watch these:

- **Health:** run `./xmsgctl health` every few minutes from cron or your monitoring agent, and alert when it exits non-zero. It checks every container (including healthchecks), the HTTP services' `/healthz` through the proxy, the sequencer and the web app, and prints what's wrong.
- **Disk:** alert at 75% full on the volume behind `/var/lib/docker`. Cassandra and Kafka degrade badly when disks fill.
- **Memory:** `docker stats --no-stream`. A service near its cap for long will be killed; raise its cap in `docker-compose.local.yml`.
- **Restarts:** `docker compose ps` shows a service's uptime; a recent start you didn't cause means it crashed or was killed. `docker inspect --format '{{.State.OOMKilled}}' xmsg-messages-1` says whether memory was the reason.
- **Kafka consumer lag** (how far behind each consumer is; it should stay near 0):

  ```bash
  docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --describe --group xmsg-sequencer     # also xmsg-persister, xmsg-fanout, xmsg-readstate, xmsg-search-indexer
  ```

- **NATS:** `curl -s 127.0.0.1:8222/varz` (connections, slow consumers) and `/connz`.
- **Certificates:** with `--tls acme`, Caddy renews them by itself; `docker compose logs caddy | grep -i error` shows failed renewals, usually because ports 80 and 443 or DNS changed.

## Troubleshooting
Start with `./xmsgctl health` and `docker compose ps -a`, then `docker compose logs SERVICE` for the service it names.

| Symptom | Likely cause | What to do |
|---|---|---|
| `required variable XMSG_AUTH_KEY is missing` (or `XMSG_PUSH_VAPID_PRIVATE_KEY`) | `.env` is missing or wasn't written by `install.sh` | Run `./install.sh`; when restoring from a backup, use `xmsgctl restore --with-env` |
| `install.sh`: `Let's Encrypt needs a public domain name` | `--tls acme` with `localhost` | Use a real domain, or `--tls internal` for evaluation |
| `install.sh`: `the partition count is fixed once set` | `--partitions` differs from the installation's | Leave it out; moving tiers is a migration, not a setting ([ADR-0021](../adr/0021-partition-count-per-installation.md)) |
| `https://<domain>/ didn't answer` after install | DNS not pointing here, ports 80 and 443 blocked, or a Let's Encrypt rate limit | `docker compose logs caddy`; check DNS and the firewall; try `--tls internal` to rule out certificates |
| The sequencer exits at start: `topic messages.in has N partitions but this installation is configured for M` | `.env` and Kafka's data disagree on the partition count (often a restore with the wrong `.env`) | Restore with the matching `.env`; never change the partition count in place |
| The sequencer exits at start: `changelog ... is missing although the sequencer has processed data` | Its state topics are gone while the data says it ran before (a partial restore, or an incompatible upgrade) | Don't force it: starting would reset message numbering. Restore the latest full backup, and report it if an upgrade caused it |
| Sending works but nobody receives messages | The sequencer, fanout or NATS is down or lagging | `./xmsgctl health`; check `xmsg-sequencer` lag (above); `docker compose logs sequencer fanout nats` |
| 502 on `/ws` or the APIs for a few seconds after a start | Services still starting | Wait for `./xmsgctl health --wait 120` |
| Clients get 429 from the APIs, or 503 with `Retry-After` when opening their WebSocket | API rate limits (429), or gateway admission control (503) | Usually a script or a reconnect storm; if it's real load, raise `XMSG_ADMISSION_RATE` (see [Day to day](#day-to-day)) |
| Search answers 503 | Search isn't enabled | `./install.sh --yes --search` (+1.5 GB of memory) |
| Browser notifications never arrive | The site isn't on trusted HTTPS (browsers require it), or the user blocked notifications | Use `--tls acme` or `cert`; `docker compose logs push` |
| A service keeps restarting, and `OOMKilled` is `true` | Memory cap too low for the load | Raise its `mem_limit` in `docker-compose.local.yml` |
| Cassandra won't start after a disk filled up | No room to compact or write its commit log | Free space (old backups, `docker system prune` for unused images), then `docker compose up -d --wait` |
| `xmsgctl restore` refuses: `XMSG_POSTGRES_PASSWORD in .env differs from the backup's` | Restoring onto a different installation | Add `--with-env` to take the backup's `.env` |
| `xmsgctl restore` refuses: `the backup has ... partitions` | A backup from an installation with another partition count | It can't be restored here; restore it on a matching installation |

When reporting a problem, include the output of `./xmsgctl health`, `docker compose ps -a` and `docker compose logs --tail 200` for the failing services. Check them for anything secret before sharing.

## Not yet
- Metrics endpoints and dashboards.
- Online backups (without stopping the stack).
- A support-bundle command.
- `xmsgctl migrate-partitions` (moving from Small to Medium).
- Sizing from load tests.
