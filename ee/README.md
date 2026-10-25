<!-- SPDX-License-Identifier: LicenseRef-xmsg-EE -->
# xmsg Enterprise Edition

Commercial features (ADR-0022), source-available under [`LICENSE`](LICENSE). They are a separate Cargo workspace that depends on the core; the core never depends on them, and Community Edition builds never compile them (`tools/check-spdx.sh` checks both).

| Path | What |
|---|---|
| `crates/license` | Licence keys: offline Ed25519 verification, expiry with a 30-day grace period, and the per-feature rules after it |
| `crates/oidc` | OpenID Connect single sign-on, implementing the core auth service's `IdentityProvider` |
| `crates/ldap` | LDAP / Active Directory sign-in, implementing the core auth service's `Directory` |
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

## LDAP and Active Directory
With `XMSG_EE_LDAP_URL` set (and a licence with `sso`), `xmsg-auth-ee` checks the sign-in form's name and password against the directory **before** local passwords. It binds as a service account, finds exactly one entry matching the user filter (the login is escaped), and binds as that entry with the password given. The entry's email, name and stable ID become the identity: the account is matched and linked like an SSO sign-in, and a directory sign-in counts as SSO when the workspace enforces it.

If the directory doesn't know the login, the password is wrong, or the directory can't be reached (logged), the form falls back to local passwords, which enforced SSO still refuses.

| Variable | Meaning |
|---|---|
| `XMSG_EE_LDAP_URL` | `ldaps://dc.example.com` (or `ldap://…` with `XMSG_EE_LDAP_STARTTLS=true`) |
| `XMSG_EE_LDAP_BIND_DN` / `XMSG_EE_LDAP_BIND_PASSWORD` | The read-only service account that searches |
| `XMSG_EE_LDAP_BASE_DN` | Where people are, e.g. `ou=people,dc=example,dc=com` |
| `XMSG_EE_LDAP_USER_FILTER` | Default `(|(uid={login})(mail={login}))`; for Active Directory e.g. `(&(objectClass=user)(|(sAMAccountName={login})(mail={login})))` |
| `XMSG_EE_LDAP_EMAIL_ATTRIBUTE` / `XMSG_EE_LDAP_NAME_ATTRIBUTE` | Defaults `mail` and `displayName` (else `cn`); entries without an email can't sign in |
| `XMSG_EE_LDAP_SUBJECT_ATTRIBUTE` | A stable ID: `entryUUID` (OpenLDAP) or `objectGUID` (Active Directory). Without it the DN is used, which changes if the entry moves |

## Enforced SSO and recovery codes
A workspace's owners and admins can require single sign-on: `PUT /auth/v1/workspaces/{ws}/sso {"enforced": true}`. From then on its members can't sign in with a password (`403`), only through the identity provider.

- Whoever enforces it must have signed in through SSO once (so it works for them), and gets **10 one-time recovery codes** in the reply. Keep them somewhere safe: they're the way in when the provider is down (`POST /auth/v1/login/recovery {email, code}`, or "Use a recovery code" on the sign-in page).
- Any owner or admin can get fresh codes at any time (`POST /auth/v1/workspaces/{ws}/sso/recovery-codes`), which replaces their old ones. A code stops working once used, or when its holder is no longer an owner or admin.
- Changing the policy needs a current licence with `sso`; recovery codes never do. Enforcement only applies while an identity provider is configured: back on the Community Edition, passwords work again, so nobody is locked out (ADR-0022).
- Sessions that already exist keep working until they expire or sign out.

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

## Images and Helm
`deploy/Dockerfile` builds one image with `xmsg-auth-ee` and `xmsg-scim-ee` (from the repository root: `docker build -f ee/deploy/Dockerfile -t registry.example.com/xmsg/enterprise:0.1.0 .`). The Helm chart's `enterprise` values run auth from it and add SCIM; see the chart's README.

## Not yet
- SAML.
- SCIM groups, the admin console, compliance features.
