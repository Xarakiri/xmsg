-- SPDX-License-Identifier: AGPL-3.0-only
-- Relational data (plan §7.1). Membership and settings live in the wide-column store.

CREATE TABLE users (
    id            BIGINT PRIMARY KEY,
    email         TEXT NOT NULL UNIQUE,
    password_hash TEXT,
    name          TEXT NOT NULL,
    avatar_url    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE workspaces (
    id            BIGINT PRIMARY KEY,
    name          TEXT NOT NULL,
    slug          TEXT NOT NULL UNIQUE,
    plan          TEXT NOT NULL DEFAULT 'community',
    settings_json JSONB NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE workspace_members (
    workspace_id BIGINT NOT NULL REFERENCES workspaces(id),
    user_id      BIGINT NOT NULL REFERENCES users(id),
    role         TEXT NOT NULL,
    joined_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id, user_id)
);

CREATE TABLE channels (
    id           BIGINT PRIMARY KEY,
    workspace_id BIGINT NOT NULL REFERENCES workspaces(id),
    type         TEXT NOT NULL,
    name         TEXT,
    topic        TEXT,
    is_archived  BOOLEAN NOT NULL DEFAULT false,
    created_by   BIGINT NOT NULL REFERENCES users(id),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE dm_pairs (
    user_low   BIGINT NOT NULL,
    user_high  BIGINT NOT NULL,
    channel_id BIGINT NOT NULL REFERENCES channels(id),
    PRIMARY KEY (user_low, user_high),
    CHECK (user_low < user_high)
);

CREATE TABLE devices (
    id           BIGINT PRIMARY KEY,
    user_id      BIGINT NOT NULL REFERENCES users(id),
    platform     TEXT NOT NULL,
    push_token   TEXT,
    last_seen_at TIMESTAMPTZ
);

CREATE TABLE refresh_tokens (
    id         BIGINT PRIMARY KEY,
    user_id    BIGINT NOT NULL REFERENCES users(id),
    device_id  BIGINT NOT NULL REFERENCES devices(id),
    token_hash TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ
);
-- Refresh is a lookup by hash; uniqueness also guards against a (2^-256) collision.
CREATE UNIQUE INDEX refresh_tokens_hash ON refresh_tokens (token_hash);
CREATE INDEX refresh_tokens_device ON refresh_tokens (device_id) WHERE revoked_at IS NULL;

-- Transactional outbox (ADR-0010). Relayed by the outbox poller or Debezium; kept 7 days.
CREATE TABLE outbox (
    id             BIGSERIAL PRIMARY KEY,
    aggregate_type TEXT NOT NULL,
    aggregate_id   BIGINT NOT NULL,
    event_type     TEXT NOT NULL,
    payload        BYTEA NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;
