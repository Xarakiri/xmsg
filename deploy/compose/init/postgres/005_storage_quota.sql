-- SPDX-License-Identifier: AGPL-3.0-only
-- Per-workspace storage quotas for files (ADR-0024): NULL uses the installation's default
-- (XMSG_MEDIA_WORKSPACE_QUOTA_BYTES); 0 means unlimited. Set by the instance operator
-- through the users service's provisioning API.
ALTER TABLE workspaces ADD COLUMN IF NOT EXISTS storage_quota_bytes BIGINT;
