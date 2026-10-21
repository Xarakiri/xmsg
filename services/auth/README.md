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
- **Reuse detection:** presenting a refresh token that was already rotated or logged out revokes every token of that session, so a stolen token dies at the latest when the real client next refreshes. A client that sends two refreshes at once loses its session the same way; clients serialize refreshes.
- **Passwords:** Argon2id (crate defaults), 10 to 256 characters; hashing runs on the blocking pool. Unknown emails are checked against a dummy hash, so "no such user" and "wrong password" look and time the same. Emails are trimmed and lower-cased.

## Not yet
- Magic links, Google/GitHub OAuth (the identity-provider interface); SAML/OIDC/LDAP and SCIM are Enterprise Edition ([ADR-0022](../../docs/adr/0022-open-core-licensing.md)).
- Revocations to the outbox and NATS `auth.revocations` (until then a logged-out session's access token lives out its 15 minutes).
- Login rate limiting, email verification, password reset, signing-key rotation (the JWKS can already publish a next key).

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `XMSG_AUTH_KEY` | (required) | base64 32-byte Ed25519 seed: `openssl rand -base64 32` |
| `XMSG_AUTH_DEV_KEY` | `false` | `true` = ephemeral key when `XMSG_AUTH_KEY` is unset (dev only; tokens die with the process) |
| `XMSG_AUTH_PORT` | `8081` | HTTP port |
| `XMSG_DATABASE_URL` | `postgres://xmsg:xmsg-dev-only@127.0.0.1:5432/xmsg` | PostgreSQL with the [Small-tier schema](../../deploy/compose/init/postgres/001_schema.sql) |
| `XMSG_AUTH_ACCESS_TTL_SECS` | `900` | access-token lifetime |
| `XMSG_AUTH_REFRESH_TTL_SECS` | `2592000` | refresh-token lifetime (30 days) |

## Tests

```bash
cargo test -p xmsg-auth
```

Integration tests against real PostgreSQL (schema applied first):

```bash
XMSG_IT_PG=postgres://xmsg:xmsg-dev-only@127.0.0.1:5432/xmsg cargo test -p xmsg-auth --test integration -- --ignored
```
