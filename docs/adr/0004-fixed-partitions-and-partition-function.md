# ADR 0004: 512 partitions, never resized, and one partition function

**Status:** Accepted. Amended by [0021](0021-partition-count-per-installation.md): the count is fixed **per installation** (Small 16, Medium 64, Large 512), and the function uses `% N`.

## Context
Ordering and sequencer state depend on which partition a channel lands in. Adding partitions to a keyed topic moves keys and breaks both. Several components outside Kafka (gateway typing, partition-scoped resync on clients) also need to know a channel's partition.

## Decision
- `messages.in`, `messages.persisted` and the sequencer changelogs have **512 partitions, fixed forever**. Throughput alone needs ~10–50; 512 gives growth room and up to 512 parallel tasks, while keeping transaction-marker overhead acceptable (to be measured: commit p99 < 10 ms at 100k events/s).
- If 512 is ever outgrown, migrate to a new topic pair with a planned cut-over.
- **One partition function:** `partition(channel_id) = toPositive(murmur2(key)) % 512`, key = 8-byte big-endian `channel_id`. This is Kafka's default partitioner.
  - Kotlin: Kafka's default partitioner.
  - Rust: librdkafka `partitioner = murmur2_random` (its default, `consistent_random`, differs).
  - Elixir (gateway) and TypeScript (clients): own implementations.
  - All four are checked against a shared test-vector file in CI.

## Consequences
- No resizing as an operational lever; scaling is by adding consumers up to 512.
- Every new language or client must implement and test the function.

## Alternatives considered
- **1,024 partitions:** more headroom, roughly twice the transaction-marker load. Rejected in favour of 512.
- **Virtual shards mapped to partitions:** allows resizing, but adds a mapping table and migration logic we don't need yet.
