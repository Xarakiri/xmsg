# ADR 0021: The partition count is fixed per installation, chosen by tier

**Status:** Accepted. Amends [0004](0004-fixed-partitions-and-partition-function.md), which fixed the count at 512 for everyone.

## Context
512 partitions across three topics and five sequencer changelogs (~4,300 partitions) fits our Large tier, but is far too heavy for a single-VM Small install or a laptop. The reason for a fixed count still holds: changing it moves channels between partitions and breaks ordering and sequencer state.

## Decision
- **The count is set once at install time and never changed in place:** Small **16**, Medium **64**, Large **512** (local dev uses 8–16).
- It's stored in the installation's config and checked at startup against the actual topics; a mismatch stops the service.
- The partition function becomes `toPositive(murmur2(key)) % N`, with `N` read from config. CI test vectors cover N = 16, 64 and 512 in all four languages ([0004](0004-fixed-partitions-and-partition-function.md)).
- **Moving up a tier** is a planned migration done by `xmsgctl migrate-partitions`, during a maintenance window:
  1. stop writes (clients see "maintenance", sends wait in their outboxes);
  2. let the sequencer, persister and fan-out drain;
  3. create the new topic set with the new count;
  4. rebuild the sequencer's state stores into the new partitioning from the old changelogs (a one-off Kafka Streams job re-keying every store);
  5. switch config, start services, resume writes.

  Expected downtime is minutes to an hour depending on state size, and it is tested in CI on Small → Medium.

## Consequences
- Each tier gets a partition count matched to its size.
- Growing past a tier needs a maintenance window, which must be clear in the sizing docs so customers choose the right tier up front.
- Clients receive `N` in their connection handshake, because they compute partitions for scoped resync ([0009](0009-nats-delivery-and-gap-detection.md)).

## Alternatives considered
- **512 everywhere:** too heavy for Small installs.
- **Virtual shards mapped onto partitions:** would allow growth without downtime, but adds a mapping layer and migration logic on the hot path; reconsider if tier migrations turn out to be common.
