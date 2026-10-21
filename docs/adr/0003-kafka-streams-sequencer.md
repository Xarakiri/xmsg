# ADR 0003: Build the sequencer on Kafka Streams (Kotlin)

**Status:** Accepted (supersedes the earlier "sequencer in Rust" plan)

## Context
By review #3 the sequencer needed: exactly-once transactions (KIP-447), embedded RocksDB state, changelog topics with restore, standby replicas, cooperative rebalancing, windowed dedupe and per-key point lookups. Rust has no mature framework for this; we would have had to write one, and its bugs show up mainly under crashes and rebalances.

## Decision
- The sequencer is a **Kafka Streams** application in **Kotlin** (Processor API), on JDK 27 (upgraded from 21 in 2026-10) with **Generational ZGC** to keep pauses below a millisecond.
- `processing.guarantee = exactly_once_v2`, `commit.interval.ms` = 5–10, producer `linger.ms` 0–1.
- **Static membership** (`group.instance.id`) and warm-up replicas, so restarts don't move tasks; `num.standby.replicas = 1` for fast failover.
- State lives in RocksDB stores backed by compacted changelogs: `channel_heads`, `members`, `threads`, `reaction_counts`, `dedupe` (windowed).
- An **Interactive Queries** gRPC endpoint serves a channel's membership from the `members` store, as the fallback snapshot source ([0006](0006-membership-as-sequenced-commands.md)).
- Tests: `TopologyTestDriver` for rules, Testcontainers Kafka for EOS and rebalance behaviour.
- **Explicit names as a contract** (added after review #10): every processor, state store and changelog topic is named explicitly in code (`Named.as`, `Materialized.as`, fixed `application.id`), never derived from the topology. The names are part of the versioned upgrade contract: renaming one needs a migration step. Kafka Streams otherwise generates names from the topology, so a refactor could make an upgraded sequencer start with empty stores.
- **Upgrade test in CI** for every release and both editions: run version N, write data, upgrade to N+1, verify counters, membership and dedupe state survived. At startup the sequencer checks that its expected changelog topics exist and aren't empty unless the installation is new.

## Consequences
- One JVM service in an otherwise Rust backend: separate build (Gradle), linting (ktlint, detekt) and on-call knowledge.
- Merging fan-out into the sequencer (a possible ~3–6 ms saving) would mean moving fan-out logic into the JVM, so that option now has a higher bar.
- Commit latency at 5–10 ms intervals and restore times with billions of memberships must be measured in Phase 0.

## Alternatives considered
- **Rust with a custom framework:** +6–8 weeks plus a crash/rebalance test suite, with the highest-risk code being ours. Rejected.
- **Rust, reduced state (no standbys):** cuts work but not the hardest part (EOS + state + restore).
- **Apache Flink:** capable, but a much heavier platform to run for one small service.
