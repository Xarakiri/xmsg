# ADR 0020: Pluggable backends behind compatibility contracts

**Status:** Accepted. Amended by [0023](0023-osi-bundled-backends-and-wide-column-choice.md): bundled defaults are OSI-licensed only, and the table below lists licences.

## Context
Self-hosting customers ([0019](0019-self-hosting-and-deployment-tiers.md)) often already run Kafka, Postgres, Redis or Elasticsearch, sometimes as managed cloud services, and don't want a second copy. We need to support that without letting every component accept "anything".

## Decision
Each dependency has a **contract** (the features we rely on) and a list of **tested implementations**. The app checks the contract at startup and refuses to start with a clear error if a feature is missing.

| Dependency | Contract (features we rely on) | Tested implementations (licence) |
|---|---|---|
| Event log | Kafka API with **transactions (EOS v2)**, log compaction, idempotent producers, consumer groups; broker ≥ Kafka 3.x semantics | **Apache Kafka 3.7+ (Apache-2.0, bundled)**; Redpanda 24.x+ (BSL, customer's choice); Amazon MSK, Confluent Cloud, Aiven (managed) |
| Wide-column store | CQL with lightweight transactions not required; `LOCAL_QUORUM`; TTLs; clustering order | **Apache Cassandra 5.x (Apache-2.0, bundled)**; ScyllaDB 6.2 (last open-source release) and later source-available releases or ScyllaDB Cloud (customer's licence). Compaction settings are chosen per backend (ICS on Scylla, UCS on Cassandra) |
| Relational | PostgreSQL 15+; logical replication **only** if Debezium is used (and 17+ for failover slots) | Self-hosted Postgres, CloudNativePG, Amazon RDS / Aurora, Cloud SQL, Azure Database |
| Outbox relay | (Our component) | **Built-in Rust outbox poller** (works on any Postgres 15+; `SKIP LOCKED` batches, marks rows published) or Debezium |
| Cache / presence | Redis protocol: hashes, TTLs, Lua, cluster or single node | **Valkey 8+ (BSD, bundled)**; Redis 8+ (AGPL option) or earlier source-available versions (customer's licence); Dragonfly (BSL); ElastiCache, Memorystore (managed) |
| Realtime messaging | NATS core (pub/sub, request/reply, queue groups, leaf nodes) | NATS 2.10+ (bundled; it's a single small binary), Synadia Cloud |
| Search (optional) | OpenSearch/Elasticsearch REST APIs we use (bulk, routing, `update_by_query`) | OpenSearch 2.x, Elasticsearch 8.x, or **search disabled** |
| Object storage | S3 API (SigV4, path-style; presigned URLs for the Large tier) | Garage (bundled, [ADR-0024](0024-media-and-object-storage.md)), Ceph RGW, Amazon S3, GCS (S3 mode), SeaweedFS; MinIO no longer released (archived 2026) |

Rules:
- **Code talks to contracts, not products:** one Kafka client, one CQL driver, one Postgres driver; no product-specific features outside the contract unless guarded by a capability check (e.g. Scylla's shard-aware driver is used when available).
- **Bundled by default, external by choice:** the Helm chart bundles every dependency, and each can be switched off and pointed at an external service (`kafka.enabled=false`, `externalKafka.bootstrap=...`).
- **Version support policy:** each release lists the tested versions; others are "may work, not supported".

## Consequences
- The outbox poller is now first-class (default for Small and Medium), not only a fallback for Debezium ([0010](0010-outbox-with-fast-path.md)). Its failover behaviour is simpler: it reads the table, so no replication slot can be lost.
- Integration tests run against a matrix of backends; the matrix is kept small on purpose (one row per tested implementation family, latest versions).
- Cassandra, ScyllaDB and Redpanda behave differently under load (compaction, transactions); performance targets are stated for the Large tier on the store chosen by the Phase 0 benchmark ([0023](0023-osi-bundled-backends-and-wide-column-choice.md)).
- CI checks that CE bundles contain only OSI-licensed components.

## Alternatives considered
- **Support only our bundled stack:** easiest to test; rejected because customers with existing Kafka/Postgres won't run a second copy.
- **Abstraction layers per backend (e.g. our own "event log" interface with several implementations):** more code and lowest-common-denominator features; the Kafka API, CQL and the Postgres protocol are already the abstraction.
