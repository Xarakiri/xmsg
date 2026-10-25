<!-- SPDX-License-Identifier: AGPL-3.0-only -->
# xmsg Helm chart (Medium tier)

[Plan §12.1](../../../docs/architecture-plan.md): xmsg for up to ~100k users / 50k concurrent, on 3+ Kubernetes nodes, with **your own** Kafka, wide-column store and PostgreSQL. NATS and Valkey are bundled (one pod each) unless you point at your own.

## What it runs

| Workload | Kind | Notes |
|---|---|---|
| auth, users, channels, messages, readstate, push | Deployment + Service | HTTP, `/healthz` probes |
| persister, fanout, outbox-relay, meta-relay, reaction-batcher, presence, search-indexer | Deployment | Kafka / NATS workers |
| sequencer | StatefulSet + headless Service | RocksDB state on a PVC; each pod advertises its stable name for the membership endpoint |
| gateway | Deployment + Service | WebSockets on 4000 |
| web | Deployment + Service (+ Ingress) | nginx: the app, plus `/auth`, `/channels`, `/messages`, `/users`, `/readstate`, `/push` and `/ws` forwarded to the services: **the single entry point** |
| nats, valkey | Deployment + Service | when `nats.bundled` / `valkey.bundled` (nothing to keep: both rebuild after restarts) |
| init: topics, keyspace, Postgres schema | Jobs (pre-install / pre-upgrade hooks) | idempotent; the base Postgres schema runs only on an empty database |

## Install

Images are built from the repository (`deploy/docker/*.Dockerfile`) and pushed to your registry:

```bash
for i in services sequencer gateway web; do
  docker build -f deploy/docker/$i.Dockerfile -t registry.example.com/xmsg/$i:0.1.0 . && docker push registry.example.com/xmsg/$i:0.1.0
done
```

Then, with a values file like:

```yaml
image: { registry: registry.example.com/xmsg, tag: 0.1.0 }
partitions: 64                      # fixed forever (ADR-0021)
kafka: { bootstrap: "kafka-0.kafka:9092,kafka-1.kafka:9092,kafka-2.kafka:9092" }
cassandra: { nodes: "cassandra-0.cassandra:9042", datacenter: dc1, replicationFactor: 3 }
postgres: { url: "postgres://xmsg:…@postgres:5432/xmsg" }
opensearch: { url: "" }             # optional: search is off without it
secrets:
  authKey: "…"                      # openssl rand -base64 32
  vapidPrivateKey: "…"              # see services/push (Web Push); or push.enabled: false
ingress: { enabled: true, className: nginx, host: chat.example.com, tls: [{ secretName: chat-tls, hosts: [chat.example.com] }] }
```

```bash
helm install chat deploy/helm/xmsg -n xmsg --create-namespace -f my-values.yaml
```

Secrets can come from your own Secret instead (`secrets.existingSecret`, keys `XMSG_DATABASE_URL`, `XMSG_AUTH_KEY`, `XMSG_PUSH_VAPID_PRIVATE_KEY`).

## Enterprise Edition
Build the enterprise image too (its Dockerfile lives with the Enterprise Edition sources; see their README), then:

```yaml
enterprise:
  enabled: true                     # auth runs xmsg-auth-ee from <registry>/enterprise
  license: "xmsg-license-v1.…"      # or existingSecret with XMSG_EE_LICENSE
  auth:
    env: { XMSG_EE_OIDC_ISSUER: https://acme.okta.com, XMSG_EE_OIDC_CLIENT_ID: xmsg }
  secretEnv: { XMSG_EE_OIDC_CLIENT_SECRET: "…" }
  scim:                             # optional: provisioning at https://<host>/scim/v2
    enabled: true
    workspaceId: "1"
    provisioningToken: "…"          # any long random string (users service ↔ SCIM)
  # and XMSG_EE_SCIM_TOKEN (what the identity provider sends) in secretEnv
```

The admin console's API runs too, at `https://<host>/admin/api`. `XMSG_AUTH_PUBLIC_URL` and `XMSG_WEB_URL` default to `https://<ingress.host>/auth` and `https://<ingress.host>`. LDAP settings go in `auth.env` and `secretEnv` the same way (`XMSG_EE_LDAP_*`). Without a valid licence, auth behaves like the Community Edition and SCIM doesn't start.

## Notes
- **Partitions** are fixed per installation; changing `partitions` on an existing install is refused at startup by every service (ADR-0021).
- **Outbox:** the Rust poller by default; with Debezium relaying the outbox, set `outbox.mode: cleanup` (see `services/outbox`).
- **Gateways** need nothing sticky: a client reconnecting to another pod resumes through sync.
- **TLS** ends at your ingress (ADR-0013); the services speak plain HTTP inside the cluster.

## Not yet
- Bundled Kafka / Cassandra / PostgreSQL (use operators such as Strimzi, K8ssandra and CloudNativePG, or managed services).
- Published images and a chart repository; PodDisruptionBudgets, autoscaling, NetworkPolicies and ServiceMonitors.
- An install test on a real cluster in CI (today CI lints, renders with two value sets, validates against the Kubernetes 1.31 schemas and builds the images).
