<!-- SPDX-License-Identifier: LicenseRef-xmsg-EE -->
# xmsg Enterprise Edition

Commercial features (ADR-0022), source-available under [`LICENSE`](LICENSE). They are a separate Cargo workspace that depends on the core; the core never depends on them, and Community Edition builds never compile them (`tools/check-spdx.sh` checks both).

| Path | What |
|---|---|
| `crates/license` | Licence keys: offline Ed25519 verification, expiry with a 30-day grace period, and the per-feature rules after it |
| `crates/oidc` | OpenID Connect single sign-on, implementing the core auth service's `IdentityProvider` |
| `services/auth` | `xmsg-auth-ee`: the auth service plus SSO, gated by the licence |
| `tools/license` | `xmsg-license`: the vendor's key and signing tool (never shipped) |

## Licences
A licence is one line, `xmsg-license-v1.<payload>.<signature>`: customer, dates, user count and features (`sso`, `scim`, `admin`, `compliance`), signed by xmsg's licensing key and checked offline, so air-gapped installs work.

**Expiry never locks anyone out** (ADR-0022): for 30 days nothing changes but warnings; after that SSO, LDAP and SCIM sign-in keep working but can't be reconfigured, legal holds and retention stay enforced, the admin console turns read-only, and exports stop. Messaging is never licensed.

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

## Not yet
- SAML and LDAP providers, enforced SSO per workspace and break-glass recovery codes.
- SCIM provisioning, the admin console, compliance features.
- EE container images and a Helm switch for them.
