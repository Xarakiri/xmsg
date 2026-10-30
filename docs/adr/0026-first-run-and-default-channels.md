# ADR-0026: First run, workspace owner and default channels

## Status

Accepted

## Context

Every account joins workspace 1 as a plain `member`. Nobody owns it, nothing tells the first person what to do, and a new user opens an empty sidebar. The admin console is an Enterprise component, so a Community install has no place to manage any of this.

## Decision

- **The first account owns the workspace.** Creating a user takes a transaction-scoped advisory lock and gives the role `owner` when workspace 1 has none, otherwise `member`. This covers password sign-up and SSO. An upgrade makes the earliest member of an ownerless workspace its owner and marks every existing member as already onboarded.
- **The owner finishes setup once.** Until `workspaces.setup_completed_at` is set, the owner sees one screen asking for the workspace name and first channel (`general` by default). It creates a public channel, makes it a default channel and completes setup.
- **Default channels are per workspace.** They are public channels listed in `workspace_default_channels`. Owners and admins manage them. Removing one never removes members.
- **New members join on first sign-in.** The client calls an idempotent endpoint at start-up. It joins the current defaults, then sets `workspace_members.defaults_joined_at`, so anyone who later leaves a default channel stays out. Signing up, SSO, SCIM and invites all arrive at this one path.
- **Adding a default can include existing members**, as an explicit choice, run in the background.
- **Workspace administration lives in the client**, in Community Edition, for owners and admins. The Enterprise console keeps licensing, usage and SSO.

## Consequences

- A user who has never signed in isn't in default channels yet, which nobody can observe in practice.
- On a publicly reachable install, whoever registers first owns it. Operators must create their account straight after installing.
- Only public channels can be defaults, so this grants no access a member couldn't already take.

## API

All IDs are strings. Errors are `{"error": "..."}`.

Users service:

- `GET /v1/me/workspaces`: each item gains `setup_completed: bool`.
- `PATCH /v1/workspaces/{id}` `{name?, setup_completed?: true}`: owner or admin.

Channels service:

- `GET /v1/workspaces/{id}/default-channels`: any member. Answers `{channels: [{channel_id, name}]}`.
- `POST /v1/workspaces/{id}/default-channels` `{channel_id, add_existing_members: bool}`: owner or admin. 202 when it starts adding members, otherwise 204. 400 if the channel isn't public or is in another workspace.
- `DELETE /v1/workspaces/{id}/default-channels/{channel_id}`: owner or admin. 204.
- `POST /v1/me/default-channels/join` `{workspace_id}`: any member. Idempotent. Answers `{joined: [channel_id]}`.
