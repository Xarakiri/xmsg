# Architecture Decision Records

Each ADR records one decision: the problem, what we chose, what it costs, and what we rejected. The plan ([../architecture-plan.md](../architecture-plan.md)) says *what* we build; these say *why*.

All ADRs below are **Accepted** as of 2026-10-21. To change one, add a new ADR that supersedes it rather than editing history.

| # | Decision | Came from |
|---|---|---|
| [0001](0001-language-split.md) | Elixir gateway, Rust services, Kotlin sequencer | Initial plan, review #3 |
| [0002](0002-kafka-single-writer-sequencer.md) | Kafka is the source of truth; one writer per channel | Reviews #1, #2 |
| [0003](0003-kafka-streams-sequencer.md) | Build the sequencer on Kafka Streams | Review #3 |
| [0004](0004-fixed-partitions-and-partition-function.md) | 512 partitions, never resized; one partition function | Reviews #1, #3, #4 |
| [0005](0005-two-sequence-counters-and-threads.md) | `event_seq` + `msg_seq`; threads in their own table | Review #1 |
| [0006](0006-membership-as-sequenced-commands.md) | Membership changes are commands the sequencer validates | Reviews #2–#6 |
| [0007](0007-membership-in-scylla.md) | Membership lives in ScyllaDB, not Postgres | Review #1 |
| [0008](0008-hybrid-fanout-and-mode-switching.md) | Push/pull fan-out, switched by the sequencer | Initial plan, review #4 |
| [0009](0009-nats-delivery-and-gap-detection.md) | NATS for delivery; gaps detected by gateways | Reviews #1–#3 |
| [0010](0010-outbox-with-fast-path.md) | Transactional outbox + Debezium, with a fast path | Reviews #2, #4, #5 |
| [0011](0011-two-step-sync-and-local-first-client.md) | Cheap sync step + lazy bodies; local-first client | Reviews #1, #2, performance section |
| [0012](0012-unread-counts-and-read-state.md) | Unread from sequence math; mentions as rows; one writer per user | Reviews #1, #3, #4 |
| [0013](0013-tls-at-the-load-balancer.md) | Terminate WebSocket TLS before the gateway | Review #1 |
| [0014](0014-disaster-recovery-reconciliation.md) | Reconcile sequencer state with Scylla after Kafka recovery | Review #6 |
| [0015](0015-reaction-batching.md) | Reactions through a partitioned batcher | Reviews #5, #6 |
| [0016](0016-member-settings-kafka-first.md) | Member settings Kafka-first into Scylla (amends 0006) | Review #7 |
| [0017](0017-network-layer-encryption.md) | WireGuard node encryption for LB → gateway; gateway port outside the mesh (corrects 0013) | Review #9 |
| [0018](0018-region-loss-recovery-target.md) | Region loss: 1–3 days for full history, degraded service sooner | Review #9, user decision |
| [0019](0019-self-hosting-and-deployment-tiers.md) | Self-hosting: Small / Medium / Large tiers, optional platform layer, push relay | User decision |
| [0020](0020-pluggable-backends.md) | Pluggable backends behind compatibility contracts; Rust outbox poller | Self-hosting |
| [0021](0021-partition-count-per-installation.md) | Partition count fixed per installation, chosen by tier (amends 0004) | Self-hosting |
| [0022](0022-open-core-licensing.md) | Open core: AGPL-3.0 core, Apache-2.0 clients, commercial Enterprise Edition; per-feature licence expiry | User decision, review #10 |
| [0023](0023-osi-bundled-backends-and-wide-column-choice.md) | Bundle only open-source backends (Kafka, Cassandra 5, Valkey); Large-tier store by benchmark | Review #10 |

The review history that produced these is in [../architecture-review.md](../architecture-review.md) through [../architecture-review-10.md](../architecture-review-10.md). The plan as it stood before the rewrite is in [../archive/architecture-plan-v1.md](../archive/architecture-plan-v1.md).
