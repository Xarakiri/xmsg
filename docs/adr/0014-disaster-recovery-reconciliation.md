# ADR 0014: Reconcile sequencer state with Scylla after a Kafka recovery

**Status:** Accepted

## Context
Normally Kafka is the source of truth and the sequencer never reads its state from Scylla ([0002](0002-kafka-single-writer-sequencer.md)). In a disaster this breaks: a Kafka cluster restored from its asynchronous MirrorMaker 2 copy has changelogs slightly behind what Scylla already holds. The sequencer would reuse `msg_seq` values, and the persister (upserting by `msg_seq`) would overwrite real messages.

## Decision
- **Recovery mode:** after any Kafka recovery, before accepting traffic, a one-off Kafka Streams job raises every channel's counters to `max(changelog, Scylla channel_heads) + 1,000`, and each partition's last `message_id` to the same maximum. Gaps in sequence numbers are harmless; reuse isn't.
- **No overwrites by key design** (amended in review #7): `message_id` is part of the clustering key of `messages` and `thread_messages`, so a reused `msg_seq` produces a second row rather than overwriting the first. No read-before-write is needed. History reads that see two rows for one `msg_seq` report them to `persister.quarantine` and raise an alert. (The first version of this ADR checked before every write, which cost an extra read per message and was racy without LWT.)
- **MirrorMaker 2** copies with `isolation.level = read_committed`, so cancelled transactions don't appear in the DR cluster.
- **Scylla snapshots** are taken more often than Kafka's 7-day retention on `messages.persisted`, so a Scylla restore can always be completed by replay.
- A **Kafka-loss DR drill** is part of the Phase 3 chaos tests.

## Consequences
- Membership's recovery point is the MirrorMaker 2 lag, made safe by reconciliation.
- One more operational runbook, and one more topic (`persister.quarantine`).

## Alternatives considered
- **Synchronous cross-cluster replication:** too much latency on the send path.
- **LWT `IF NOT EXISTS` on every message insert:** prevents overwrites but costs a Paxos round per message.
