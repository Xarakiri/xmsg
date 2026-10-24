<!-- SPDX-License-Identifier: LicenseRef-xmsg-EE -->
# xmsg Enterprise Edition

Commercial features (ADR-0022), source-available under [`LICENSE`](LICENSE). They are a separate Cargo workspace that depends on the core; the core never depends on them, and Community Edition builds never compile them (`tools/check-spdx.sh` checks both).

| Path | What |
|---|---|
| `crates/license` | Licence keys: offline Ed25519 verification, expiry with a 30-day grace period, and the per-feature rules after it |
| `crates/oidc` | OpenID Connect single sign-on, implementing the core auth service's `IdentityProvider` |
| `services/auth` | `xmsg-auth-ee`: the auth service plus SSO, gated by the licence |
| `services/scim` | `xmsg-scim-ee`: SCIM 2.0 user provisioning for a workspace, gated by the licence |
| `tools/license` | `xmsg-license`: the vendor's key and signing tool (never shipped) |

## Licences
A licence is one line, `xmsg-license-v1.<payload>.<signature>`: customer, dates, user count and features (`sso`, `scim`, `admin`, `compliance`), signed by xmsg's licensing key and checked offline, so air-gapped installs work.

**Expiry never locks anyone out** (ADR-0022): for 30 days nothing changes but warnings; after that SSO, LDAP and SCIM sign-in keep working but can't be reconfigured, legal holds and retention stay enforced, the admin console turns read-only, and exports stop. Messaging is never licensed.

**The built-in key is for testing only:** its signing half appeared in a chat transcript. Replace it with a privately generated key before any real customer licence is issued.

**Setting up the vendor key (once):** run `cargo run -p xmsg-license-tool -- keygen`, keep the signing key secret, and put the public key into `VENDOR_KEYS` in `crates/license/src/lib.rs`. Until then no licence verifies in a release build. Then:

```bash
cargo run -p xmsg-license-tool -- sign "$SIGNING_KEY" "Acme Corp" 2027-12-31 500 sso,scim > acme.licence
```

Tests and CI use throwaway keys through the `test-keys` feature (`XMSG_EE_LICENSE_TEST_KEY`), which release builds don't have.

## Single sign-on (OIDC)
Run `xmsg-auth-ee` instead of `xmsg-auth`, with:

| Variable | Meaning |
|---|---|
| `XMSG_EE_LICENSE` / `XMSG_EE_LICENSE_FILE` | The licence (with `sso`) |
| `XMSG_EE_OIDC_ISSUER` | e.g. `https://acme.okta.com` (its discovery document is read at start) |
| `XMSG_EE_OIDC_CLIENT_ID` / `XMSG_EE_OIDC_CLIENT_SECRET` | The app registered at the provider |
| `XMSG_EE_OIDC_ID` / `XMSG_EE_OIDC_NAME` | Path ID (default `oidc`) and button text (default "Single sign-on") |
| `XMSG_AUTH_PUBLIC_URL` | The auth service as browsers reach it, e.g. `https://chat.example.com/auth`; the provider's redirect URI is `{this}/v1/sso/{id}/callback` |
| `XMSG_WEB_URL` | Where signed-in users land, e.g. `https://chat.example.com` |

The flow is the authorization code flow with PKCE; the ID token's signature, issuer, audience, expiry and nonce are checked. An account is matched by the provider's subject, or the first time by its **verified** email (and linked); otherwise a new user is created. The web app shows a "Continue with …" button per provider.

## SCIM provisioning
`xmsg-scim-ee` lets an identity provider (Okta, Entra ID, …) create, update and deactivate a workspace's people, at `/scim/v2` (SCIM 2.0 Users: filter by `userName eq`, create, get, replace, PATCH and delete). It changes nothing itself: it calls the core users service's provisioning API, so a deactivation is the ordinary workspace removal (the person stops receiving anything at once and leaves its channels).

| Variable | Meaning |
|---|---|
| `XMSG_EE_LICENSE` / `XMSG_EE_LICENSE_FILE` | The licence (with `scim`); an expired one keeps provisioning working |
| `XMSG_EE_SCIM_TOKEN` | The bearer token to give the identity provider (at least 32 characters) |
| `XMSG_EE_SCIM_WORKSPACE` | The workspace ID this endpoint provisions |
| `XMSG_USERS_URL` | The users service (default `http://users:8084`) |
| `XMSG_PROVISIONING_TOKEN` | The same value as the users service's `XMSG_PROVISIONING_TOKEN`, which turns its provisioning API on |
| `XMSG_EE_SCIM_PUBLIC_URL` | This API as the provider reaches it, e.g. `https://chat.example.com/scim/v2` |
| `XMSG_EE_SCIM_PORT` | Default 8095 |

`userName` is the person's email address (else their primary email is used); provisioned accounts have no password and sign in through SSO. Deactivated people stay visible with `active: false`, also after a DELETE, and creating or activating them again brings them back with the same ID. The workspace owner can't be deactivated. Groups aren't provisioned yet.

## Not yet
- SAML and LDAP providers, enforced SSO per workspace and break-glass recovery codes.
- SCIM groups, the admin console, compliance features.
- EE container images and a Helm switch for them.
