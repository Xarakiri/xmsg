<!-- SPDX-License-Identifier: AGPL-3.0-only -->
# Users (Rust)

Phase 1a ([plan §5.3](../../docs/architecture-plan.md)). Profiles, workspaces, workspace roles and invites, in PostgreSQL (`users`, `workspaces`, `workspace_members`, `workspace_invites`). Sign-up stays in the auth service, which adds every new user to the default workspace `1` in the same transaction.

## API

Every request needs `Authorization: Bearer <access token>` (checked locally against the auth service's JWKS by [`xmsg-authn`](../../crates/xmsg-authn)). IDs are JSON strings; errors are `{"error": "..."}`.

| Method | Path | Body | Reply |
|---|---|---|---|
| GET | `/v1/me` | | `{user_id, name, avatar_url, email}` |
| PATCH | `/v1/me` | `{name?, avatar_url?}` (`null` or `""` clears the avatar) | the profile; 400 invalid |
| GET | `/v1/users?ids=a,b,c` | | `{users: [{user_id, name, avatar_url}]}`; at most 200 IDs; users sharing no workspace with the caller are omitted |
| POST | `/v1/workspaces` | `{name, slug}` | 201 `{workspace_id, name, slug, role: "owner"}`; 409 slug taken |
| GET | `/v1/me/workspaces` | | `{workspaces: [{workspace_id, name, slug, role}]}` |
| GET | `/v1/workspaces/{id}/members?after=&limit=` | | `{members: [{user_id, name, avatar_url, role}], next}`; 404 for non-members |
| POST | `/v1/workspaces/{id}/invites` | `{ttl_hours?}` (default 168, max 720) | 201 `{code, workspace_id, expires_at}`; owners and admins only (403) |
| POST | `/v1/invites/{code}/accept` | | `{workspace_id, status: "joined" \| "already_member"}`; 404 invalid or expired |
| GET | `/healthz` | | `ok` |

Invite codes are 16 random bytes (base64url); only their SHA-256 is stored, and a code works for anyone until it expires.

## Not yet
- Profile cache in Redis and `users.events`; workspace removal (§6.5); role changes; invite revocation.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `XMSG_AUTH_JWKS_URL` | (required) | e.g. `http://auth:8081/.well-known/jwks.json` |
| `XMSG_USERS_PORT` | `8084` | HTTP port |
| `XMSG_DATABASE_URL` | `postgres://xmsg:xmsg-dev-only@127.0.0.1:5432/xmsg` | PostgreSQL with the [Small-tier schema](../../deploy/compose/init/postgres/001_schema.sql) |

## Tests

```bash
cargo test -p xmsg-users
XMSG_IT_PG=postgres://xmsg:xmsg-dev-only@127.0.0.1:5432/xmsg cargo test -p xmsg-users --test integration -- --ignored
```
