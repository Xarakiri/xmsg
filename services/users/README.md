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
| DELETE | `/v1/workspaces/{id}/members/{user_id}` | | 204: removed (or left, when it's you); 403 not allowed; 404 not a member |
| GET | `/healthz` | | `ok` |

**Removing a member** (plan §6.5): owners can remove anyone but the owner, admins can remove members, and anyone but the owner can leave. The owner can't leave, or the workspace would have nobody in charge.
- The membership row is deleted together with a `workspace.member_removed` outbox event (`UsersEvent`, aggregate `workspace`, to `users.events`).
- Then the same removal goes to NATS `auth.revocations` as the fast path (`XMSG_NATS_URL`).
- Fan-out stops delivering the workspace to the user at once. The channels service's removal job then takes them out of each of the workspace's channels and records `workspace.member_removal_completed`.
- A login spans all of a user's workspaces, so their sockets stay open for the others.

Invite codes are 16 random bytes (base64url); only their SHA-256 is stored, and a code works for anyone until it expires.

## Not yet
- Profile cache in Redis; role changes and ownership transfer; invite revocation.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `XMSG_NATS_URL` | *(none)* | Refuse revoked sessions: deny set fed by NATS `auth.revocations`, rebuilt from Kafka `auth.events` (`XMSG_KAFKA_BOOTSTRAP`) on start ([xmsg-authn](../../crates/xmsg-authn)). Without it, a revoked session's access token works until it expires |
| `XMSG_AUTH_JWKS_URL` | (required) | e.g. `http://auth:8081/.well-known/jwks.json` |
| `XMSG_USERS_PORT` | `8084` | HTTP port |
| `XMSG_DATABASE_URL` | `postgres://xmsg:xmsg-dev-only@127.0.0.1:5432/xmsg` | PostgreSQL with the [Small-tier schema](../../deploy/compose/init/postgres/001_schema.sql) |

## Tests

```bash
cargo test -p xmsg-users
XMSG_IT_PG=postgres://xmsg:xmsg-dev-only@127.0.0.1:5432/xmsg cargo test -p xmsg-users --test integration -- --ignored
```
