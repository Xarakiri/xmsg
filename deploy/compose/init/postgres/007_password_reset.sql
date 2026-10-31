-- SPDX-License-Identifier: AGPL-3.0-only
-- Admin-issued password reset codes (ADR-0026): only the SHA-256 of a single-use
-- code is stored, one live code per user. Idempotent: postgres-init reapplies this
-- file at every start.
CREATE TABLE IF NOT EXISTS password_reset_codes (
    user_id    BIGINT PRIMARY KEY REFERENCES users(id),
    code_hash  TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
