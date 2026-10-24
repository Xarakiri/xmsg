-- SPDX-License-Identifier: AGPL-3.0-only
-- Single sign-on through external identity providers (the auth service's extension
-- point; the providers themselves are Enterprise Edition, ADR-0022).

-- One row per sign-in in progress: used once, within 10 minutes (CSRF state, OIDC nonce
-- and PKCE verifier).
CREATE TABLE IF NOT EXISTS sso_states (
    state         TEXT PRIMARY KEY,
    provider      TEXT NOT NULL,
    nonce         TEXT NOT NULL,
    code_verifier TEXT NOT NULL,
    return_to     TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Who an external account is here: (provider, the provider's subject) → user.
CREATE TABLE IF NOT EXISTS external_identities (
    provider   TEXT NOT NULL,
    subject    TEXT NOT NULL,
    user_id    BIGINT NOT NULL REFERENCES users(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (provider, subject)
);
CREATE INDEX IF NOT EXISTS external_identities_user ON external_identities (user_id);
