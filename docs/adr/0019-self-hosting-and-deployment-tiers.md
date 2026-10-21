# ADR 0019: Self-hosting with three deployment tiers and an optional platform layer

**Status:** Accepted (user decision, 2026-10-21). Narrows the scope of [0017](0017-network-layer-encryption.md) to deployments we operate. Bundled backends amended by [0023](0023-osi-bundled-backends-and-wide-column-choice.md) (open-source only).

## Context
Self-hosting by other companies is a goal: regulated organisations (government, banks, healthcare) often require on-premise messaging. The design so far assumed **one operator running 1M concurrent users**: six stateful systems on Kubernetes, 512 partitions, Cilium and Linkerd required. A company with 2,000 or 20,000 employees can't reasonably run that, and the products it compares us with are much lighter (Mattermost: one binary + Postgres; Zulip: Python + Postgres + RabbitMQ + Redis).

## Decision
**One codebase, three deployment tiers.** The architecture (sequenced events, one writer per channel, the same services) is identical in every tier; only sizes, packaging and which backends are bundled change.

| | **Small** | **Medium** | **Large** |
|---|---|---|---|
| Users / concurrent | ≤ 10k / ≤ 5k | ≤ 100k / ≤ 50k | 10–20M / 1M+ |
| Packaging | Docker Compose on **one VM** (8 vCPU / 32 GB) | **Helm chart** on 3+ nodes (customer's Kubernetes) | Our full deployment (this plan, §11) |
| Event log | **Apache Kafka**, single node, bundled | Bundled Kafka (3 nodes) or customer's Kafka API service | Kafka (Strimzi), 6–9 brokers |
| Wide-column store | **Apache Cassandra 5**, single node, bundled | Bundled Cassandra 5 (3 nodes), or customer's ScyllaDB / Cassandra | ScyllaDB or Cassandra 5, decided by benchmark ([0023](0023-osi-bundled-backends-and-wide-column-choice.md)) |
| Cache | **Valkey**, bundled | Valkey or customer's Redis-protocol service | Valkey cluster |
| Partitions (fixed at install, [0021](0021-partition-count-per-installation.md)) | 16 | 64 | 512 |
| Sequencer | 1 instance, no standby | 2–3 instances, 1 standby | 8–16 instances |
| Outbox relay | Built-in Rust outbox poller | Poller (default) or Debezium | Debezium |
| Search | Optional single-node OpenSearch, or off | OpenSearch 3 nodes or customer's | OpenSearch cluster |
| Redundancy | None (backups only) | Across nodes (and zones if the cluster has them) | 3 zones + off-region backups |
| TLS to clients | Gateway terminates TLS itself (few connections), or the customer's reverse proxy | Customer's ingress / load balancer | L4 LB ([0013](0013-tls-at-the-load-balancer.md)) |

**Pluggable backends:** each stateful dependency is reached through a documented compatibility contract, so customers can bring services they already run ([0020](0020-pluggable-backends.md)).

**Optional platform layer:** Cilium/WireGuard, Linkerd, Vault, Argo CD, Strimzi and the operators are **how we run the Large tier**, not requirements of the product. Self-hosted tiers get security from **application-level settings**:
- TLS on every internal connection the app makes (gRPC between services, NATS, Kafka API, Postgres, CQL, Redis), with certificates from the customer's CA or generated at install;
- TLS to clients at the gateway or the customer's ingress;
- secrets from Kubernetes Secrets or files, with optional Vault;
- observability as standard Prometheus metrics and OTLP traces, for the customer's own stack.

**Push notifications through our relay:** the mobile apps in the app stores are signed with our keys, so a self-hosted server can't talk to APNs/FCM directly. Self-hosted servers send pushes through an **xmsg push relay** that we operate. Pushes carry **only IDs** (no message text); the app fetches content from the customer's server. Customers with their own white-labelled apps can point `push` at APNs/FCM directly.

**What self-hosters get as a product:**
- multi-arch container images, a Docker Compose bundle (Small) and a Helm chart (Medium), later a Kubernetes operator for upgrades;
- **air-gapped installs:** an image bundle plus offline chart, no calls home except the optional push relay;
- versioned upgrades with schema migrations run as jobs, and a stated upgrade path between versions;
- `xmsgctl`: backup, restore, health checks, and the tier-migration tool ([0021](0021-partition-count-per-installation.md));
- **SSO (SAML/OIDC), LDAP and SCIM**, and an admin UI, moved earlier in the roadmap because enterprise buyers require them.

## Consequences
- **CI must test every supported combination** that we claim: at least Small (Kafka, single-node Cassandra 5, poller) and Large (Kafka, Scylla cluster, Debezium), plus the external-backend contracts. This is real ongoing cost.
- Every feature must work without the platform layer; anything that relies on Cilium, Linkerd or Vault is a bug in self-hosted tiers.
- Small installs have no redundancy: availability depends on the VM and on the customer's backups.
- We operate one shared service for self-hosters: the push relay.
- Support, licensing and documentation become product work (open questions in the plan).

## Alternatives considered
- **SaaS only:** simpler, but gives up the on-premise market.
- **A separate lightweight "self-hosted edition" with different storage (e.g. everything in Postgres):** easiest to install, but two architectures to build and keep correct.
- **Large tier only, sold with professional services:** reaches only very large customers.
