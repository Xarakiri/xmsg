# ADR 0022: Open core: open-source messenger, commercial enterprise features

**Status:** Accepted (user decision, 2026-10-21). The specific licences below are the recommended choice and need a legal review before the first public release.

## Context
Self-hosting is a goal ([0019](0019-self-hosting-and-deployment-tiers.md)). The licensing model decides how xmsg is distributed, who can run it, and how it makes money. The user chose **open core**: the messenger itself is open source, and enterprise features are commercial.

## Decision

### Editions
| | **Community Edition (CE)**: open source | **Enterprise Edition (EE)**: commercial |
|---|---|---|
| Messaging | Everything: workspaces, channels, DMs, group DMs, threads, reactions, mentions, unread, presence, typing, receipts, broadcast channels, search | Same |
| Deployment | Small and Medium tiers, including multi-node Medium (high availability is not paywalled) | Same, plus supported upgrade paths and help with tier migrations |
| Sign-in | Email + password, magic links, Google/GitHub OAuth | **SAML and OIDC SSO**, **LDAP**, enforced SSO per workspace |
| Provisioning | Manual invites | **SCIM** user and group provisioning |
| Administration | Basic: users, workspaces, channels, bans | **Admin console:** custom roles and permissions, workspace policies, usage analytics, bulk operations |
| Compliance | Workspace-wide retention setting; GDPR erasure | **Retention policies per channel/team, legal hold, eDiscovery export, audit log viewer and export, DLP hooks** (outgoing webhook to the customer's scanner) |
| Push | Through the xmsg push relay, fair-use limits | Push relay with an SLA and higher limits |
| Support | Community | Commercial support tiers |

Our own SaaS (Large tier) runs EE.

### Licences
- **Server code (core):** **AGPL-3.0.** Anyone can self-host and modify it; anyone offering a modified version as a network service must publish their changes. This stops a cloud provider from running a closed fork as a competing service, while staying a true OSI open-source licence.
- **Client apps, the TypeScript sync core, Protobuf schemas and client SDKs:** **Apache-2.0**, so third parties can build integrations and alternative clients without copyleft concerns.
- **Enterprise code:** a **commercial licence** (source-available in the same repository, usable only with a valid subscription), in `ee/` directories.
- **Contributions:** a **Contributor License Agreement (CLA)** is required, so we can ship community contributions in EE and offer the core under commercial terms to customers who can't accept the AGPL.

### How the code is split
- **One monorepo.** Enterprise code lives only in `ee/` directories (`services/*/ee/`, `clients/*/ee/`, `ee/` services such as `scim`).
- **Two builds:**
  - **CE images** are built from core code only: Cargo features off, Gradle and Mix configurations excluding `ee/`, a web build without EE modules. They contain no commercial code.
  - **EE images** include `ee/` code. Enterprise features stay disabled until a valid **licence key** is installed.
- **Licence keys** are signed files (Ed25519) checked offline, so they work in air-gapped installs. They state the customer, expiry, user count and enabled features. An expired key degrades to CE behaviour after a grace period; it never blocks messaging or locks people out of their data.
- **Licence expiry rules per feature** (added after review #10). Falling back to CE behaviour for everything would lock out SSO-only workspaces and could drop legal holds, so expiry works per feature:

  | Feature | After the key expires (and the grace period ends) |
  |---|---|
  | SSO (SAML/OIDC), LDAP, SCIM sign-in and provisioning | **Keep working.** Only changing their configuration is blocked. |
  | Break-glass admin access | Always available: one-time recovery codes created when SSO is set up. |
  | Legal holds and per-channel retention policies | **Stay enforced indefinitely.** Creating or editing them is blocked. |
  | eDiscovery and audit exports | Blocked until a valid key is installed. |
  | Admin console extras, analytics, custom role editing | Read-only (existing roles keep applying). |
  | Messaging and all CE features | Never affected. |

  Each row has an automated test.
- **Feature checks happen on the server**, in each service's EE code paths, never only in the client.
- **Extension points in the core** keep EE code out of core logic:
  - an identity-provider interface in `auth` (SAML/OIDC/LDAP implement it),
  - a provisioning API that SCIM calls,
  - a policy hook in `channels` and `messages` for permissions and DLP,
  - a retention and legal-hold hook that the deletion commands consult before deleting (legal hold blocks retention and erasure deletes),
  - the existing `audit` stream, which EE adds a viewer, search and export to.
- The core must be **complete and good on its own**: CE is the product most people meet first, not a demo.

## Consequences
- CI builds and tests both editions, and checks that CE images contain no `ee/` code.
- **Dependency licences** must be compatible with AGPL-3.0 for the core and with commercial distribution for EE; `cargo deny`, a Gradle licence plugin and an npm licence checker enforce this in CI.
- A CLA bot on the public repository; contributions without a signed CLA can't be merged.
- Every file carries a licence header (`AGPL-3.0-only`, `Apache-2.0` or the commercial licence) via SPDX identifiers, checked in CI.
- The push relay's terms of service and fair-use limits need writing.
- Some enterprises avoid AGPL software; they can buy EE, whose commercial licence also covers the core.

## Alternatives considered
- **Apache-2.0 for everything in the core:** maximum adoption, but cloud providers could offer it as a service without contributing back.
- **Source-available core (BSL, SSPL, Elastic Licence):** stronger protection, but not open source, which conflicts with the user's decision and puts off some adopters.
- **Fully commercial:** gives up community adoption, which is how self-hosted products usually find enterprise customers.
- **Paywalling high availability:** some competitors do; rejected because it pushes medium-sized self-hosters to unreliable setups and damages trust in CE.
