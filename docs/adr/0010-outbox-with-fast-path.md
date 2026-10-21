# ADR 0010: Transactional outbox with Debezium, plus a fast path

**Status:** Accepted. Extended by [0020](0020-pluggable-backends.md): the built-in Rust outbox poller is the default relay in the Small and Medium tiers; Debezium is used in the Large tier.

## Context
`auth`, `users` and `channels` write PostgreSQL and must publish events. Writing to Postgres and then to Kafka is a dual write: if the second step fails, caches, search and gateways drift silently. But CDC adds ~0.1–2 s, which is too slow for interactive actions such as opening a new DM.

## Decision
- Every Postgres state change writes its event to an **`outbox`** table in the same transaction. **Debezium** publishes it to Kafka.
- **Fast path:** for interactive events (`channel.created`, rename, archive, revocations, workspace removal) the service also publishes the same event (same outbox event ID) directly to Kafka or NATS right after commit. Consumers dedupe by outbox event ID, and these events are idempotent (absolute values, "create if missing"), so the second copy is harmless.
- **Failover safety:** PostgreSQL 17+ with failover logical slots (`failover = true`, `sync_replication_slots = on`), outbox rows kept 7 days, and a reconciler that republishes anything missing after a failover.

## Consequences
- A change is published if and only if it committed; interactive actions still feel instant.
- Kafka Connect becomes part of the platform; Phase 0 must confirm CloudNativePG supports failover slots for logical replication.

## Alternatives considered
- **Publish after commit only:** dual write.
- **Outbox only, no fast path:** first DM messages rejected or delayed by CDC lag.
- **Custom Rust outbox poller:** possible fallback if Debezium is too heavy.
