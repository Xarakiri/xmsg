-- SPDX-License-Identifier: AGPL-3.0-only
-- Web Push subscriptions (plan §5.11): one row per browser endpoint. The endpoint is
-- the key, so a browser that signs in as someone else moves to that user.
CREATE TABLE IF NOT EXISTS push_endpoints (
    endpoint   TEXT PRIMARY KEY,
    user_id    BIGINT NOT NULL,
    p256dh     TEXT NOT NULL,
    auth       TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS push_endpoints_user ON push_endpoints (user_id);
