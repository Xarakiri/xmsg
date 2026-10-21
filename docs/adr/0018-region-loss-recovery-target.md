# ADR 0018: Recovery from losing the whole region: 1–3 days for full history, degraded service sooner

**Status:** Accepted (user decision, 2026-10-21)

## Context
v1 runs in **one region with three availability zones**. Earlier versions of the plan promised a 1-hour recovery time for messages without saying what had been lost. Review #9 separated three failure scopes:
- **One zone lost:** covered by replication factor 3 across zones; recovery in minutes, no restore.
- **One cluster corrupted** (bad migration, operator bug): restore from backup plus replay from Kafka; hours.
- **The whole region lost:** there is no second region. Restoring Scylla from snapshots means moving hundreds of terabytes after the first year (2–3 TB of new disk per day), which takes days.

The alternative, a warm second region (Scylla second datacenter, Kafka and Postgres replicas, services ready to start), gives ~1 hour recovery at roughly 40–60% more infrastructure cost.

## Decision
**Option A:** for region loss, accept **RTO of 1–3 days for full message history**, with **degraded service much sooner**:
1. **Backups live outside the region:** Scylla snapshots, Postgres WAL archives and the search index source data go to object storage replicated to a second region. MirrorMaker 2 copies Kafka (`read_committed`) to a **small standby Kafka cluster** in the second region, which holds the last 7 days of `messages.persisted`, the sequencer changelogs and the compacted topics. No services run there.
2. **Degraded service first (target: within ~4–8 hours):** bring up the platform in the second region from Terraform and Argo CD; restore Postgres (small) from WAL; use the standby Kafka; restore Scylla's small tables first (`channel_heads`, membership, settings, read state); run sequencer recovery mode ([0014](0014-disaster-recovery-reconciliation.md)). New messages, membership and unread counts work; history older than the last 7 days shows as "loading".
3. **Full history (1–3 days):** load message snapshots into the live Scylla cluster in the background (load-and-stream), then replay `messages.persisted` from the standby Kafka to close the gap. Search is rebuilt last.
- RPO for region loss stays ≤ 1 minute for anything that reached the standby Kafka (MirrorMaker 2 lag).

## Consequences
- No second running region in v1; the extra cost is object-storage replication and a small standby Kafka cluster.
- A **region-loss runbook** and a yearly **region-loss drill** (restore into a scratch environment in the second region) are required; quarterly drills cover zone loss and cluster restores.
- Revisit when revenue or contracts require a stricter target (Option B), or when multi-region data (roadmap Phase 5) arrives.

## Alternatives considered
- **Option B, warm DR region:** ~1 h RTO, +40–60% infrastructure cost. Deferred, not rejected.
- **No off-region copies at all:** region loss would mean losing data permanently.
