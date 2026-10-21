# ADR 0002: Kafka is the source of truth, with one writer per channel

**Status:** Accepted

## Context
Messages in a channel must have one order that every client agrees on, with no duplicates or gaps, even across crashes, rebalances and retries. Distributed locks or database-generated IDs would put a coordination step on every message.

## Decision
- Every channel change (message, edit, reaction batch, membership change, metadata change) is an event produced to **`messages.in`, keyed by `channel_id`**.
- A **sequencer** owns each partition and assigns per-channel counters ([0005](0005-two-sequence-counters-and-threads.md)). One owner per partition means one writer per channel, with no locks.
- The sequencer runs **exactly-once** (Kafka transactions): output, state and input offsets commit together, and an old owner's late commit is fenced by the broker. It keeps its state in Kafka changelogs, never reading it back from Scylla in normal operation.
- A separate **persister** writes `messages.persisted` to ScyllaDB with idempotent upserts. Values that change (counts, heads) are set to absolute values from the event, never incremented. Fan-out reads the same topic in parallel.
- Clients send a `client_msg_id`; the sequencer dedupes within a 15-minute window, and clients stop automatic retries after 10 minutes.

## Consequences
- Ordering is correct by construction; the cost is a Kafka hop plus a transaction commit (~8–15 ms) on the send path.
- Scylla can lag the stream by milliseconds, so anything that reads Scylla and the stream together needs a freshness check ([0006](0006-membership-as-sequenced-commands.md)).
- In a disaster where Kafka itself is restored from a copy, this rule needs an explicit reconciliation step ([0014](0014-disaster-recovery-reconciliation.md)).

## Alternatives considered
- **Sequence numbers from Redis `INCR`:** fast, but a failover can lose increments and reuse numbers.
- **Scylla lightweight transactions per message:** correct but slow (Paxos round per message).
- **Snowflake IDs alone for ordering:** clock skew between writers reorders messages; we still use Snowflake IDs as stable references.
