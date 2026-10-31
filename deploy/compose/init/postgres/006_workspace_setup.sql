-- SPDX-License-Identifier: AGPL-3.0-only
-- First run and default channels (ADR-0026): per-workspace default channels, the
-- owner's setup flag, and per-member onboarding onto defaults. Every statement is
-- idempotent: postgres-init reapplies this file at every start.

-- Public channels new members join on first sign-in.
CREATE TABLE IF NOT EXISTS workspace_default_channels (
    workspace_id BIGINT NOT NULL REFERENCES workspaces(id),
    channel_id   BIGINT NOT NULL REFERENCES channels(id),
    added_by     BIGINT NOT NULL REFERENCES users(id),
    added_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id, channel_id)
);

-- When the member onboarded onto the workspace's default channels (NULL: not yet;
-- they join on their next sign-in). Someone who leaves a default channel afterwards
-- stays out: the endpoint never joins a member whose timestamp is set.
ALTER TABLE workspace_members ADD COLUMN IF NOT EXISTS defaults_joined_at TIMESTAMPTZ;

-- The owner finished first-run setup (named the workspace, made the first channel).
ALTER TABLE workspaces ADD COLUMN IF NOT EXISTS setup_completed_at TIMESTAMPTZ;

-- Upgrade: an ownerless workspace is owned by its earliest-joining member (the
-- lowest user ID on a tie). Idempotent: afterwards the workspace has an owner.
WITH ownerless AS (
    SELECT m.workspace_id
    FROM workspace_members m
    GROUP BY m.workspace_id
    HAVING count(*) FILTER (WHERE m.role = 'owner') = 0
),
first_member AS (
    SELECT DISTINCT ON (m.workspace_id) m.workspace_id, m.user_id
    FROM workspace_members m
    JOIN ownerless o ON o.workspace_id = m.workspace_id
    ORDER BY m.workspace_id, m.joined_at, m.user_id
)
UPDATE workspace_members m
SET role = 'owner'
FROM first_member f
WHERE m.workspace_id = f.workspace_id AND m.user_id = f.user_id;

-- Upgrade: everyone already here is treated as onboarded, so nobody is pushed into
-- channels they left. Runs once per member (the WHERE keeps it a no-op afterwards).
UPDATE workspace_members SET defaults_joined_at = now() WHERE defaults_joined_at IS NULL;
