# xmsg: Architecture Review #11

**Reviewed:** [architecture-plan.md](architecture-plan.md) and all 23 [ADRs](adr/README.md) after review #10 (open-source bundled backends, per-feature licence expiry, explicit Kafka Streams naming).
**Date:** 2026-10-21

**Verdict:** **No critical or high findings.** The review #10 changes are consistent across the plan and the ADRs. This pass looked at the one area no review has covered, the **client devices**, and found one medium gap there. Everything else is configuration detail.

**This is the point to stop reviewing on paper.** The last three reviews found real issues only in *newly added* areas (security, self-hosting, licensing), not in the core design. What's left to learn needs running code: the Phase 0 spikes and the ScyllaDB vs Cassandra benchmark.

Severity: 🟡 medium · ⚪ low.

---

## 🟡 Medium

### D1. Message data on devices isn't protected
**Where:** §9.2 local-first client, §10 security, [ADR-0011](adr/0011-two-step-sync-and-local-first-client.md)

The local-first client keeps message history in SQLite (mobile, desktop) and IndexedDB (web), so it opens instantly. That's the right call for speed, but the plan says nothing about protecting that data, and regulated customers (the main self-hosting market) will ask:
- A lost phone or laptop keeps a readable copy of private conversations.
- Revoking a session or removing a user closes their sockets, but **the local copy stays**.
- Shared or kiosk browsers keep IndexedDB data after the tab closes.

**Fix:**
- **Encrypt local storage at rest:** SQLCipher on mobile and desktop, with the key in the platform keystore (iOS Keychain, Android Keystore, OS credential store on desktop). On web, encrypt IndexedDB records with a per-session key held in memory or a non-extractable WebCrypto key.
- **Remote wipe:** `session.revoked`, `user.banned` and `workspace.member_removed` carry a **wipe** flag. The client deletes local data for that workspace (or everything) the next time it connects. The server also refuses sync for revoked devices, so a device that comes back online learns it was revoked before receiving anything new.
- **Logout clears local data** by default; "keep me signed in" is a workspace policy.
- **EE controls** ([ADR-0022](adr/0022-open-core-licensing.md)): workspace policies to disable local caching on web, require app passcode or biometrics on mobile, limit cached history (e.g. last 30 days), and support MDM app configuration.
- Record this as a new ADR on client data protection.

---

## ⚪ Low

- **Dedupe window clock:** say that the sequencer's 15-minute dedupe window is measured in **event time** (the Kafka record timestamp set when `messages` produces), not processing time. Otherwise a sequencer outage or long lag could make retried copies of one message fall outside the window once processing resumes.
- **Single-node Kafka in the Small tier:** transactions won't start with Kafka's defaults on one broker. The install script must set `transaction.state.log.replication.factor=1`, `transaction.state.log.min.isr=1`, `offsets.topic.replication.factor=1` and `min.insync.replicas=1`, and the startup contract check should verify them ([ADR-0020](adr/0020-pluggable-backends.md)).
- **Data residency for the SaaS:** our Large tier runs in one region. Customers who need data kept in a specific country can self-host today; offering a regional choice in the SaaS depends on multi-region data (Phase 5). Add this as an open question for sales.

---

## Decisions needed

| # | Decision | Recommendation |
|---|---|---|
| D1 | Client data protection | Encrypted local storage, remote wipe on revocation, logout clears data, EE policies for caching and passcodes; new ADR |
| — | What next | Apply D1 and the low items, then **stop paper reviews** and start Phase 0 |

## Review history in one line each
1. Pipeline correctness under failure (fencing, partitions, counters, threads).
2. Membership ordering, dual writes, Redis heads, sequencer state.
3. Snapshots vs stream, Kafka Streams instead of a home-grown framework.
4. Push/pull switching, outbox across failover, mention counters.
5. Product flows: channel creation, workspace removal, reactions, receipts.
6. Two writers on one row, disaster recovery overwrites.
7. One-writer rule violations found by checking the rewritten plan.
8. `event_seq` meaning for clients.
9. Security in transit, region loss, gateway capacity testing.
10. Licences of bundled backends, licence expiry, upgrade safety.
11. Device data protection (this review).
