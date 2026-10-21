# ADR 0023: Bundle only open-source backends; choose the Large tier's wide-column store by benchmark

**Status:** Accepted. Amends [0019](0019-self-hosting-and-deployment-tiers.md) and [0020](0020-pluggable-backends.md). Licence facts below reflect terms as known in mid-2026 and are part of the legal review (plan, open question 9).

## Context
Review #10 found that several backends the plan bundled are no longer open source:
- **ScyllaDB:** the open-source line ended with **6.2** (announced December 2024). Later releases are source-available, free only up to a size limit (announced as 50 vCPUs and 10 TB per cluster), with a commercial licence above it. Our Large tier (6–9 nodes × 16–32 vCPUs = 96–288 vCPUs) is far above the limit.
- **Redpanda Community Edition:** Business Source Licence 1.1 (source-available).
- **Redis:** 7.4 moved to source-available licences; Redis 8 added AGPL-3.0 as an option.

Bundling these in the open-source Community Edition ([0022](0022-open-core-licensing.md)) would be misleading, and some customers' policies forbid non-OSI software. For our own service, a ScyllaDB licence is a real cost that wasn't budgeted.

## Decision

### Bundled backends (Small and Medium, both editions)
Only OSI-licensed software is bundled by default:

| Role | Bundled default | Licence |
|---|---|---|
| Event log | **Apache Kafka** (KRaft; 1 node in Small, 3 in Medium) | Apache-2.0 |
| Wide-column store | **Apache Cassandra 5** (1 node in Small, 3 in Medium) | Apache-2.0 |
| Cache / presence | **Valkey** | BSD-3-Clause |
| Relational | PostgreSQL | PostgreSQL |
| Messaging | NATS | Apache-2.0 |
| Search (optional) | OpenSearch | Apache-2.0 |

**ScyllaDB, Redpanda, Redis and Dragonfly remain supported backends** through the contracts in [0020](0020-pluggable-backends.md). A customer who chooses one runs it under that product's licence terms.

### Large tier (our SaaS): decided by a Phase 0 benchmark
The wide-column store for our own service is either:
- **(a) ScyllaDB with a commercial licence** (or ScyllaDB Cloud): lowest latency and fewest nodes, plus a licence cost; or
- **(b) Apache Cassandra 5:** no licence cost, more nodes for the same load (vendor benchmarks often show 2–3×; to be measured), more JVM tuning.

Phase 0 runs both on our real access patterns (message writes and history pages, membership snapshot reads, read-state updates) at Large-tier rates, and the result is compared with a ScyllaDB licence quote. The choice is recorded as a follow-up ADR. Until then, "Scylla" in the plan means "the wide-column store (CQL)".

### Guardrails
- The backend contract table lists each tested implementation's **licence**.
- **CI checks that CE bundles contain only OSI-licensed components.**
- Schema and compaction settings are defined per backend: ICS on ScyllaDB, UCS on Cassandra 5.

## Consequences
- Small installs need ~2–3 GB more RAM (Kafka and Cassandra are JVM-based); the Small VM size (8 vCPU / 32 GB) still fits, and local dev rises to ~10–12 GB.
- The Phase 0 benchmark becomes a gate for Large-tier sizing and budget.
- Cassandra becomes the most-tested CQL backend; ScyllaDB stays in the test matrix as a supported option.

## Alternatives considered
- **Keep ScyllaDB and Redpanda bundled:** smaller footprint, but the Community Edition wouldn't be fully open source.
- **Pin ScyllaDB 6.2, the last open-source release:** no future fixes or security updates.
- **Decide the Large-tier store now:** without measurements, either choice could cost a lot (licence fees, or 2–3× more nodes).
