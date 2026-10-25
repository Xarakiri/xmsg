-- SPDX-License-Identifier: AGPL-3.0-only
-- Enforced single sign-on per workspace and break-glass recovery codes (ADR-0022).

-- A workspace that enforces SSO: its members can't sign in with a password while an
-- identity provider is configured. Without one (e.g. back on the Community Edition)
-- passwords work again, so nobody is locked out.
CREATE TABLE IF NOT EXISTS workspace_sso (
    workspace_id BIGINT PRIMARY KEY REFERENCES workspaces(id),
    enforced     BOOLEAN NOT NULL,
    updated_by   BIGINT NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One-time recovery codes for a workspace's owners and admins, for when the identity
-- provider is down. Only the SHA-256 is stored; a code works once, and only while its
-- holder is still an owner or admin of that workspace.
CREATE TABLE IF NOT EXISTS recovery_codes (
    code_hash    TEXT PRIMARY KEY,
    user_id      BIGINT NOT NULL REFERENCES users(id),
    workspace_id BIGINT NOT NULL REFERENCES workspaces(id),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    used_at      TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS recovery_codes_holder ON recovery_codes (user_id, workspace_id);
