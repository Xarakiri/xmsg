# xmsg: Architecture Review #9

**Reviewed:** [architecture-plan.md](architecture-plan.md) and the [ADRs](adr/README.md) after review #8.
**Date:** 2026-10-21
**Status:** ✅ All recommendations below were applied on 2026-10-21. P1 became [ADR-0017](adr/0017-network-layer-encryption.md); for P2 the user chose **Option A**, recorded in [ADR-0018](adr/0018-region-loss-recovery-target.md).

**Focus of this pass:** reviews #1–#8 concentrated on the message pipeline, which now has nothing left to find on paper. This pass deliberately looked at the parts that got less attention: **security in transit, disaster recovery at real data volumes, and whether the Phase 0 spikes test the right things.** It found one high and two medium issues there. None of them changes the pipeline.

Severity: 🟠 high · 🟡 medium · ⚪ low.

---

## 🟠 High

### P1. The "encrypted LB → gateway hop" claim doesn't hold, and the mesh would double gateway load
**Where:** §10 ("Linkerd mTLS between all pods"), [ADR-0013](adr/0013-tls-at-the-load-balancer.md) ("the LB → gateway hop is covered by Linkerd mTLS")

Two problems:
- **The claim is false for a cloud NLB.** Linkerd encrypts traffic between *meshed pods*. A cloud load balancer isn't a meshed pod, so after it terminates TLS, traffic to the gateway is **plaintext** inside the VPC. The plan's security model says "TLS in transit", so this is a gap in a stated guarantee, not just wording.
- **Meshing the gateway's inbound port is very expensive.** If the gateway's WebSocket port were meshed, every one of its ~100k connections per pod would also pass through the Linkerd sidecar proxy. That roughly doubles connection memory and adds CPU and latency on exactly the component we sized most carefully.

**Fix:**
- **Exclude the gateway's WebSocket port from the mesh** (`config.linkerd.io/skip-inbound-ports`). Keep Linkerd for service-to-service traffic, where connection counts are small.
- **Encrypt the LB → gateway hop at the network layer** instead of per connection: use **node-to-node transparent encryption** (e.g. Cilium with WireGuard), or the cloud's own VPC-level encryption where it guarantees it. This encrypts all traffic between nodes with no per-connection cost in the BEAM.
- On bare metal, run HAProxy as in-cluster edge pods on the same encrypted pod network, so the hop to the gateway is covered by the same WireGuard layer.
- Correct ADR-0013 and §10 to say this, and add the CNI choice (Cilium) to the stack.

---

## 🟡 Medium

### P2. A 1-hour recovery time isn't realistic for losing the region
**Where:** §8.2 recovery targets (messages: RTO ≤ 1 h), §11 ("one region, 3 availability zones")

The recovery table doesn't say **what is lost**, and the answer changes everything:
- **One availability zone lost:** already handled. Everything runs with RF 3 across 3 zones, so recovery takes minutes, with no restore.
- **One cluster corrupted** (a bad migration, an operator bug): restore from backup plus replay from Kafka. Hours, depending on size.
- **The whole region lost:** there is no second region. Restoring Scylla from snapshots in another region means moving a lot of data: at 2–3 TB of new disk per day, after one year that's **hundreds of terabytes**, which takes **days**, not 1 hour. MirrorMaker 2 to a "DR cluster" also needs a place to run.

**Fix:** split §8.2 into these three scopes with honest numbers, and make the region-loss target a **decision**:
- **Option A (cheaper, v1 default):** accept that losing the whole region means **RTO of 1–3 days** for full history. Service can come back sooner in degraded mode: new messages work in a fresh region while history restores in the background.
- **Option B (more expensive):** run a **warm DR region**: a second Scylla datacenter with asynchronous replication, Kafka MirrorMaker 2 there, a Postgres replica there, and services scaled to zero until needed. RTO ~1 h, at roughly +40–60% infrastructure cost.

Add this to the open questions. It's a business decision about cost.

### P3. The Phase 0 gateway spike measures the wrong thing
**Where:** §13 Phase 0 ("Elixir NATS request/reply at 50k RPS per pod"), §5.1 (~100k connections per pod)

The riskiest numbers for the gateway are **how many connections fit in a pod** and **how many frames per second it can deliver**. Request/reply throughput is the easy part. At target load, each of ~20 pods delivers about **50k–250k frames per second** to its sockets, with only 8 vCPUs. If the real figure is half, we need twice as many pods and the sizing in §5.1 and §11 is wrong.

**Fix:** change the Phase 0 gateway spike to:
- **100k idle connections in one pod** (8 vCPU / 16 GB), with heartbeats and hibernation, measuring memory per connection;
- **delivery throughput:** 250k frames/s spread across those connections, measuring p99 delivery latency and CPU;
- request/reply at 50k RPS (as now);
- a design rule for the gateway: **encode each event once** and send the same binary to every recipient process (shared refc binaries), never re-encoding per connection.

---

## ⚪ Low

- **State store schema versions:** the sequencer's RocksDB values and changelog records live for a long time, so their format will change. Version them (a version byte plus Protobuf, with `buf breaking` checks) and write a migration step into the sequencer upgrade runbook.
- **Deploy time for gateways:** each pod drains over ~5 minutes. Rolling 20 pods one at a time takes ~100 minutes. Use a surge of 4–5 pods at a time (~25 minutes) and say so in §11.

---

## Decisions needed

| # | Decision | Recommendation |
|---|---|---|
| P1 | Encryption of the LB → gateway hop | Cilium WireGuard node encryption (or VPC encryption); gateway WebSocket port outside the Linkerd mesh |
| P2 | Region-loss recovery target | Your call: 1–3 days with degraded service sooner (A), or a warm DR region at +40–60% cost (B) |
| P3 | Phase 0 gateway spike | Test 100k connections per pod and 250k frames/s, not just request/reply |

## Where things stand
- The message pipeline has had no new correctness findings for two reviews.
- This pass found real issues only by looking at security, DR and capacity, areas the earlier reviews skipped.
- After P1–P3, the remaining risks are measurements, which Phase 0 is now set up to take.
