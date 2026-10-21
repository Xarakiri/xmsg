# xmsg: Architecture Review #10

**Reviewed:** [architecture-plan.md](architecture-plan.md) and the new ADRs for self-hosting and open core ([0019](adr/0019-self-hosting-and-deployment-tiers.md), [0020](adr/0020-pluggable-backends.md), [0021](adr/0021-partition-count-per-installation.md), [0022](adr/0022-open-core-licensing.md)).
**Date:** 2026-10-21
**Status:** ✅ All recommendations below were applied on 2026-10-21: [ADR-0023](adr/0023-osi-bundled-backends-and-wide-column-choice.md) (open-source bundled backends, Large-tier store by benchmark), per-feature licence expiry in ADR-0022, explicit Kafka Streams naming and upgrade test in ADR-0003.

**Verdict:** The tier and edition design is sound, but it has **one high finding that affects both the product and our costs**: several components we planned to bundle into the "open-source" Community Edition are no longer open source, and one of them needs a paid licence at our own Large-tier scale. There are also two medium findings about what happens when an Enterprise licence expires, and around upgrades for self-hosters.

> **Licences change often.** The facts in H1 reflect licence terms as of my knowledge (to mid-2026). Check each project's current licence before acting on them; this belongs in the legal review already listed as open question 9.

Severity: 🟠 high · 🟡 medium · ⚪ low.

---

## 🟠 High

### Q1. Bundled backends aren't all open source, and ScyllaDB has a size limit for free use
**Where:** §12.1 tiers, [ADR-0019](adr/0019-self-hosting-and-deployment-tiers.md), [ADR-0020](adr/0020-pluggable-backends.md), [ADR-0022](adr/0022-open-core-licensing.md), §11 (Large tier)

| Component | Licence situation | Effect on us |
|---|---|---|
| **ScyllaDB** | The open-source line ended with **6.2** (announced December 2024). Newer releases are **source-available**, free only up to a size limit (announced as 50 vCPUs and 10 TB per cluster), with a commercial licence above that. | **Large tier:** 6–9 nodes × 16–32 vCPUs is 96–288 vCPUs, well over the free limit, so our SaaS needs a **paid ScyllaDB licence** (or ScyllaDB Cloud). That cost isn't in the plan. **CE:** bundling a source-available database in an "open-source" edition is misleading, and some customers' policies forbid it. |
| **Redpanda** (Community Edition) | **Business Source Licence (BSL 1.1)**: source-available, not open source | Bundling it in CE has the same problem, and BSL restricts some uses. |
| **Redis** | 7.4 moved to source-available licences (RSAL/SSPL); Redis 8 added **AGPL-3.0** as an option | Usable, but "Redis 7+" in ADR-0020 includes versions that aren't open source. **Valkey** (BSD) avoids the question. |
| Kafka, Cassandra, Postgres, NATS, OpenSearch, Debezium, Kafka Streams | Apache-2.0 / PostgreSQL / similar | No issue. |

**Fix:**
- **CE bundles only OSI-licensed backends by default:** **Apache Kafka** (single-node KRaft in Small; ~1–2 GB more RAM than Redpanda), **Apache Cassandra 5** as the bundled wide-column store, **Valkey** for caching. ScyllaDB and Redpanda stay **supported backends** that customers may choose under their own licence terms ([ADR-0020](adr/0020-pluggable-backends.md) contracts already allow this).
- **Large tier: make the wide-column store a costed decision.** Either:
  - **(a) ScyllaDB with a commercial licence** (or ScyllaDB Cloud): best latency and fewest nodes; add the licence to the budget; or
  - **(b) Apache Cassandra 5:** no licence cost, but more nodes for the same load (often 2–3× by vendor benchmarks; to be measured), and more JVM tuning.

  Recommendation: decide after a **Phase 0 benchmark** of Scylla vs Cassandra 5 on our access pattern, with the licence quote in hand.
- Update ADR-0020's tested list: "ScyllaDB 6.2 (last open-source release) and later source-available releases (customer's licence)", "Redis 8+ under AGPL, or Valkey".
- Add a **licence column** to the backend contract table, and a CI check that CE bundles contain only OSI-licensed components.

---

## 🟡 Medium

### Q2. An expired Enterprise licence could lock users out or drop legal holds
**Where:** [ADR-0022](adr/0022-open-core-licensing.md) ("an expired key falls back to CE behaviour after a grace period; it never blocks messaging or access to data")

Falling back to CE behaviour sounds safe, but two EE features make it unsafe:
- **SSO-only workspaces:** if SSO is enforced, users have no passwords. After a fallback to CE, nobody can sign in, which contradicts "never blocks access".
- **Legal hold and retention policies:** if a legal hold stops being enforced when the key expires, retention or erasure jobs could **delete data the customer is legally required to keep**. That's a serious liability for the customer, and for us.

**Fix:** define licence expiry per feature:
- **Sign-in:** SSO, LDAP and SCIM **keep working** after expiry; only *changing* their configuration is blocked. Admins get a break-glass login (one-time recovery codes created at setup).
- **Compliance:** existing legal holds and retention policies **stay enforced indefinitely**. Only creating or editing them, and eDiscovery exports, need a valid key.
- **Everything else** (admin console extras, analytics) turns read-only or off after the grace period.
- Write this table into ADR-0022, with a test for each row.

### Q3. Self-hosted upgrades can break the sequencer's state if Kafka Streams names change
**Where:** §5.6 sequencer, §12.2 upgrades

Kafka Streams derives changelog topic names and state store names from the processing topology. If a release changes the topology without explicit names, names can shift, so an upgraded sequencer **starts with empty state stores** or orphans the old changelogs. For us that's an incident; for a self-hoster upgrading without help, it can silently reset counters.

**Fix:**
- **Name every processor, store and changelog explicitly** in the sequencer code, and treat the names as a public, versioned contract.
- CI test: start version N, write data, upgrade to N+1, verify counters, membership and dedupe state survived. Run it for every release, in both editions.
- The startup check that already compares the partition count with config ([ADR-0021](adr/0021-partition-count-per-installation.md)) also checks that expected changelog topics exist and aren't empty when the installation isn't new.

---

## ⚪ Low

- **Small-tier certificates:** the gateway terminates TLS itself, but nothing says where its certificate comes from. Support both ACME (Let's Encrypt) for internet-facing installs and a customer-provided certificate for internal ones; the install script asks which.
- **Air-gapped installs and mobile push:** "nothing calls home except the push relay" means air-gapped customers get **no mobile push** with the store apps. Say this plainly in the sizing docs; the answer for them is white-labelled apps with their own APNs/FCM credentials, or no push.

---

## Decisions needed

| # | Decision | Recommendation |
|---|---|---|
| Q1 | Wide-column store for our Large tier | Benchmark ScyllaDB (paid licence) against Cassandra 5 in Phase 0, then decide with the licence quote |
| Q1 | Backends bundled in CE | Apache Kafka, Cassandra 5, Valkey; Scylla and Redpanda as optional customer-licensed backends |
| Q2 | Licence-expiry behaviour | SSO keeps working; holds and retention stay enforced; only configuration and exports need a valid key |
| Q3 | Upgrade safety | Explicit Kafka Streams names as a contract; upgrade test in CI |

## What's solid
- One codebase, three tiers, same event model: no "lite" architecture to keep in sync.
- Backend contracts are checked at startup, so unsupported setups fail clearly.
- Enterprise features sit behind extension points; the core doesn't depend on `ee/` code.
- High availability stays in CE, which keeps the open-source edition credible.
