<!-- SPDX-License-Identifier: AGPL-3.0-only -->
# Auth (Rust)

Phase 1a ([plan §5.2](../../docs/architecture-plan.md)). Sign-up, login, short-lived Ed25519 access tokens, rotating refresh tokens, and the JWKS the gateway (and Envoy) validate tokens with, locally and without calling this service. State is in PostgreSQL (`users`, `devices`, `refresh_tokens`).

## API

| Method | Path | Body | Reply |
|---|---|---|---|
| POST | `/v1/signup` | `{email, password, name}` | 201 `{user_id}`; 409 email taken; 400 invalid |
| POST | `/v1/login` | `{email, password, platform?}` | `{access_token, refresh_token, token_type, expires_in, user_id, session_id}`; 401 |
| POST | `/v1/token/refresh` | `{refresh_token}` | same as login, with a **new** refresh token; 401 |
| POST | `/v1/logout` | `{refresh_token}` | 204 (also for unknown tokens) |
| GET | `/.well-known/jwks.json` | | OKP / Ed25519 key set (`Cache-Control: max-age=300`) |
| GET | `/healthz` | | `ok` |

IDs are JSON strings (JavaScript numbers lose precision above 2^53). Errors are `{"error": "..."}`.

## Tokens

- **Access token:** EdDSA (Ed25519) JWT, 15 minutes. Claims: `sub` (user ID), `sid` (session = device ID), `iat`, `exp`, `iss`/`aud` = `xmsg`; header `kid` derived from the public key.
- **Refresh token:** 32 random bytes, 30 days; only its SHA-256 is stored. Every refresh **rotates** it (row lock, so concurrent refreshes with one token rotate once).
- **Reuse detection:** presenting a refresh token that was already rotated or logged out revokes every token of that session, so a stolen token dies at the latest when the real client next refreshes.
- **Reuse grace:** a token revoked *by rotation* less than `XMSG_AUTH_REUSE_GRACE_SECS` (30 s) ago is rotated again instead: a new refresh token for the same session, nothing revoked. This covers a reply lost in flight (tab closed, network drop) and two tabs refreshing with one stored token. Rotation records the successor in `refresh_tokens.replaced_by`; logout leaves it NULL, so logged-out tokens never get grace, and neither does a session with no live token left (logged out or already revoked for reuse). The trade-off: a thief replaying a token within those seconds gets a session too, until the next reuse outside the window. Clients still serialize refreshes (the web client across tabs with a Web Lock).
- **Passwords:** Argon2id (crate defaults), 10 to 256 characters; hashing runs on the blocking pool. Unknown emails are checked against a dummy hash, so "no such user" and "wrong password" look and time the same. Emails are trimmed and lower-cased.

## Revocations

Logout, and a reused refresh token outside the grace window (theft), end the **session** (the device). Every live refresh token of the device is revoked, and an `AuthEvent{SessionRevoked}` goes out by two routes:
1. **Outbox** (`aggregate_type = 'session'`, `event_type = 'session.revoked'`), in the same transaction as the revocation. Then outbox relay → `auth.events` → meta-relay → NATS (the safety net).
2. **Fast path:** straight to NATS `auth.revocations` after commit, when `XMSG_NATS_URL` is set.

Gateways close the session's sockets and refuse its access tokens until `refuse_until_ms` (now + access-token lifetime + 30 s leeway). Measured locally: 8 ms with the fast path, about 200 ms through the outbox alone; the plan's target is p99 under 5 s. A replay of an already-ended session emits nothing.

## Not yet
- Magic links, Google/GitHub OAuth (the identity-provider interface); SAML/OIDC/LDAP and SCIM are Enterprise Edition ([ADR-0022](../../docs/adr/0022-open-core-licensing.md)).
- `user.banned` (revoking every session of a user) and an admin API to end someone's sessions.
- Login rate limiting, email verification, password reset, signing-key rotation (the JWKS can already publish a next key).

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `XMSG_AUTH_KEY` | (required) | base64 32-byte Ed25519 seed: `openssl rand -base64 32` |
| `XMSG_AUTH_DEV_KEY` | `false` | `true` = ephemeral key when `XMSG_AUTH_KEY` is unset (dev only; tokens die with the process) |
| `XMSG_AUTH_PORT` | `8081` | HTTP port |
| `XMSG_DATABASE_URL` | `postgres://xmsg:xmsg-dev-only@127.0.0.1:5432/xmsg` | PostgreSQL with the [Small-tier schema](../../deploy/compose/init/postgres/001_schema.sql) |
| `XMSG_AUTH_ACCESS_TTL_SECS` | `900` | access-token lifetime |
| `XMSG_NATS_URL` | *(none)* | NATS for the revocation fast path (without it, revocations travel through the outbox only) |
| `XMSG_AUTH_REFRESH_TTL_SECS` | `2592000` | refresh-token lifetime (30 days) |
| `XMSG_AUTH_REUSE_GRACE_SECS` | `30` | how long a rotated refresh token may still be presented; `0` disables the grace |

## Tests

```bash
cargo test -p xmsg-auth
```

Integration tests against real PostgreSQL (schema applied first):

```bash
XMSG_IT_PG=postgres://xmsg:xmsg-dev-only@127.0.0.1:5432/xmsg cargo test -p xmsg-auth --test integration -- --ignored
```

A database created before `refresh_tokens.replaced_by` existed needs the column first:

```sql
ALTER TABLE refresh_tokens ADD COLUMN IF NOT EXISTS replaced_by BIGINT;
```
