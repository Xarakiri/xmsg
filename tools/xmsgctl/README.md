<!-- SPDX-License-Identifier: AGPL-3.0-only -->
# xmsgctl

Operating a Small-tier installation ([plan §12.2](../../docs/architecture-plan.md)): the Docker Compose bundle that [`deploy/compose/install.sh`](../../deploy/compose/install.sh) sets up. Run it in the installation's directory, or pass `--dir`.

```bash
xmsgctl health [--wait 120]           # every container, every service's /healthz
xmsgctl backup --out xmsg.tar         # a consistent backup (the stack stops meanwhile)
xmsgctl restore xmsg.tar --yes        # replaces all data with the backup's
xmsgctl restore xmsg.tar --yes --with-env   # a new machine: take the backup's .env too
```

## Backups
- **Cold and consistent:** the stack stops, every data volume (Postgres, Kafka, Cassandra, the sequencer's state, and OpenSearch and Caddy's certificates when present) is archived, and the stack starts again. Expect a few minutes of downtime on a Small install; schedule it.
- **The archive holds `.env`** (the installation's secrets) next to the data, with a manifest (format, time, version, partition count). It's written with mode 600: keep it as private as `.env`.
- Archiving runs in a throwaway `postgres:17` container, an image every installation already has, so nothing is downloaded (air-gapped installs work).
- If archiving fails the stack is started again anyway.

## Restores
- Refused without `--yes`, for a different backup format, and for a different partition count (ADR-0021).
- The data is bound to two secrets, Postgres's password and Kafka's cluster ID. If `.env` has different ones, the restore is refused unless `--with-env` takes the backup's `.env` (the usual case on a new machine).
- The stack stops, each volume is emptied and refilled, the stack starts, and `xmsgctl health --wait` tells when it's ready.

CI's `small-tier` job runs the whole round trip on a live installation: the end-to-end smoke test, then backup, `docker compose down -v`, restore, health, user counts compared, and the smoke test again.

## Not yet
Online backups (no downtime), support bundles, `migrate-partitions` (Small → Medium).
