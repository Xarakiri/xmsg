// SPDX-License-Identifier: AGPL-3.0-only
//! Integration test against real PostgreSQL with the Small-tier schema.
//!
//! Ignored by default; run with
//!   XMSG_IT_PG=postgres://xmsg:xmsg-dev-only@127.0.0.1:5432/xmsg \
//!     cargo test -p xmsg-auth --test integration -- --ignored
//! The schema in deploy/compose/init/postgres must already be applied. Databases created
//! before `refresh_tokens.replaced_by` need
//!   ALTER TABLE refresh_tokens ADD COLUMN IF NOT EXISTS replaced_by BIGINT;

use chrono::{DateTime, Duration, Utc};
use xmsg_auth::store::{NewRefresh, Postgres, Rotation, Store, StoreError};
use xmsg_auth::{new_id, password, random_token, token_hash};

const GRACE: Duration = Duration::seconds(30);

/// The outbox payload of a revocation: the session, so tests can find their row.
fn revoked_event(user_id: i64, device_id: i64) -> Vec<u8> {
    format!("{user_id}:{device_id}").into_bytes()
}

/// `session.revoked` rows the store wrote to the outbox for `device_id`.
async fn revocations(device_id: i64) -> Vec<Vec<u8>> {
    let pool = sqlx::PgPool::connect(&std::env::var("XMSG_IT_PG").unwrap())
        .await
        .unwrap();
    sqlx::query_scalar(
        "SELECT payload FROM outbox WHERE aggregate_type = 'session' AND aggregate_id = $1 \
         AND event_type = 'session.revoked' ORDER BY id",
    )
    .bind(device_id)
    .fetch_all(&pool)
    .await
    .unwrap()
}

async fn store() -> Postgres {
    let url = std::env::var("XMSG_IT_PG").expect("XMSG_IT_PG must be set for integration tests");
    // Sign-up is invite-only by default (ADR-0026); these tests share one database
    // and sign users up freely, so the shared database's policy is opened here.
    // The policy behavior itself is covered by unit tests and, against real
    // PostgreSQL, by `signup_policy_*` on a throwaway database.
    let pool = sqlx::PgPool::connect(&url)
        .await
        .expect("connect to Postgres");
    sqlx::query(
        "UPDATE workspaces SET settings_json = jsonb_set(settings_json, '{signup}', '\"open\"') \
         WHERE id = 1",
    )
    .execute(&pool)
    .await
    .expect("open the sign-up policy for the shared test database");
    drop(pool);
    Postgres::connect(&url).await.expect("connect to Postgres")
}

/// A throwaway database with the Small-tier schema applied, for tests that need a
/// state the shared database can't have (no owner, a closed policy). The xmsg role
/// is the bootstrap superuser in every environment (CI's container sets
/// POSTGRES_USER=xmsg), so CREATE DATABASE works anywhere XMSG_IT_PG does.
/// Returns the store and the admin pool to drop the database with.
async fn throwaway_store(name: &str) -> (Postgres, sqlx::PgPool) {
    let url = std::env::var("XMSG_IT_PG").unwrap();
    let admin_url = format!("{}/postgres", url.rsplit_once('/').unwrap().0);
    let test_url = format!("{}/{name}", url.rsplit_once('/').unwrap().0);
    let admin = sqlx::PgPool::connect(&admin_url).await.unwrap();
    sqlx::query(&format!("DROP DATABASE IF EXISTS {name}"))
        .execute(&admin)
        .await
        .unwrap();
    sqlx::query(&format!("CREATE DATABASE {name}"))
        .execute(&admin)
        .await
        .unwrap();
    let fresh = sqlx::PgPool::connect(&test_url).await.unwrap();
    for file in [
        include_str!("../../../deploy/compose/init/postgres/001_schema.sql"),
        include_str!("../../../deploy/compose/init/postgres/002_push.sql"),
        include_str!("../../../deploy/compose/init/postgres/003_sso.sql"),
        include_str!("../../../deploy/compose/init/postgres/004_sso_policy.sql"),
        include_str!("../../../deploy/compose/init/postgres/005_storage_quota.sql"),
        include_str!("../../../deploy/compose/init/postgres/006_workspace_setup.sql"),
        include_str!("../../../deploy/compose/init/postgres/007_password_reset.sql"),
    ] {
        sqlx::raw_sql(file).execute(&fresh).await.unwrap();
    }
    drop(fresh);
    (Postgres::connect(&test_url).await.unwrap(), admin)
}

/// Best-effort cleanup of a `throwaway_store` database.
async fn drop_throwaway(admin: &sqlx::PgPool, name: &str) {
    sqlx::query(&format!("DROP DATABASE IF EXISTS {name}"))
        .execute(admin)
        .await
        .unwrap();
}

fn refresh(ttl: Duration) -> (String, NewRefresh) {
    let token = random_token(32);
    let new = NewRefresh {
        id: new_id(),
        token_hash: token_hash(&token),
        expires_at: Utc::now() + ttl,
    };
    (token, new)
}

/// A user with a unique email, logged in on one device.
async fn session(store: &Postgres, ttl: Duration) -> (i64, i64, String) {
    let user_id = new_id();
    let email = format!("it-{user_id}@example.org");
    store
        .create_user(
            user_id,
            &email,
            &password::hash("integration test"),
            "IT",
            None,
        )
        .await
        .unwrap();
    let device_id = new_id();
    let (token, new) = refresh(ttl);
    store
        .start_session(user_id, device_id, "test", new)
        .await
        .unwrap();
    (user_id, device_id, token)
}

/// Rotates `token` at `now`, returning the successor (meaningful only if it rotated).
async fn rotate(store: &Postgres, token: &str, now: DateTime<Utc>) -> (String, Rotation) {
    let (next, new) = refresh(Duration::days(30));
    let rotation = store
        .rotate(&token_hash(token), new, now, GRACE, &revoked_event)
        .await
        .unwrap();
    (next, rotation)
}

#[tokio::test]
#[ignore]
async fn duplicate_emails_are_refused_and_credentials_are_found() {
    let store = store().await;
    let id = new_id();
    let email = format!("dup-{id}@example.org");
    store
        .create_user(id, &email, "hash", "A", None)
        .await
        .unwrap();
    assert!(matches!(
        store.create_user(new_id(), &email, "hash", "B", None).await,
        Err(StoreError::EmailTaken)
    ));
    let found = store.credentials(&email).await.unwrap().unwrap();
    assert_eq!(
        (found.user_id, found.password_hash.as_deref()),
        (id, Some("hash"))
    );
    assert!(
        store
            .credentials("nobody@example.org")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
#[ignore]
async fn rotation_replaces_the_token_and_reuse_revokes_the_device() {
    let store = store().await;
    let (user_id, device_id, first) = session(&store, Duration::days(30)).await;
    let now = Utc::now();
    let (second, rotation) = rotate(&store, &first, now).await;
    assert_eq!(rotation, Rotation::Rotated { user_id, device_id });

    let later = now + GRACE + Duration::seconds(1);
    assert_eq!(
        rotate(&store, &first, later).await.1,
        Rotation::Reused { device_id }
    );
    // The reuse revoked the current token too.
    assert_eq!(
        rotate(&store, &second, later).await.1,
        Rotation::Reused { device_id }
    );
}

#[tokio::test]
#[ignore]
async fn reuse_within_the_grace_reissues_without_revoking() {
    let store = store().await;
    let (user_id, device_id, first) = session(&store, Duration::days(30)).await;
    let now = Utc::now();
    let (lost, rotation) = rotate(&store, &first, now).await;
    assert_eq!(rotation, Rotation::Rotated { user_id, device_id });

    let soon = now + Duration::seconds(10);
    let (retry, rotation) = rotate(&store, &first, soon).await;
    assert_eq!(rotation, Rotation::Reissued { user_id, device_id });
    // Both successors stay usable.
    assert_eq!(
        rotate(&store, &retry, soon).await.1,
        Rotation::Rotated { user_id, device_id }
    );
    assert_eq!(
        rotate(&store, &lost, soon).await.1,
        Rotation::Rotated { user_id, device_id }
    );
}

#[tokio::test]
#[ignore]
async fn logged_out_and_revoked_sessions_get_no_grace() {
    let store = store().await;
    // Rotated moments ago, but the successor was logged out.
    let (user_id, device_id, first) = session(&store, Duration::days(30)).await;
    let now = Utc::now();
    let (second, rotation) = rotate(&store, &first, now).await;
    assert_eq!(rotation, Rotation::Rotated { user_id, device_id });
    store
        .revoke(&token_hash(&second), now, &revoked_event)
        .await
        .unwrap();
    assert_eq!(
        rotate(&store, &first, now).await.1,
        Rotation::Reused { device_id }
    );

    // A session revoked for reuse can't be revived through a token still in its grace.
    let (_, device_id, first) = session(&store, Duration::days(30)).await;
    let (second, _) = rotate(&store, &first, now).await;
    let (third, _) = rotate(&store, &second, now).await;
    let later = now + GRACE + Duration::seconds(1);
    assert_eq!(
        rotate(&store, &first, later).await.1,
        Rotation::Reused { device_id }
    );
    assert_eq!(
        rotate(&store, &second, now).await.1,
        Rotation::Reused { device_id }
    );
    assert_eq!(
        rotate(&store, &third, now).await.1,
        Rotation::Reused { device_id }
    );
}

#[tokio::test]
#[ignore]
async fn expired_unknown_and_logged_out_tokens_do_not_rotate() {
    let store = store().await;
    let (_, _, expired) = session(&store, Duration::seconds(-1)).await;
    assert_eq!(
        rotate(&store, &expired, Utc::now()).await.1,
        Rotation::Invalid
    );
    assert_eq!(
        rotate(&store, "never issued", Utc::now()).await.1,
        Rotation::Invalid
    );

    let (_, device_id, token) = session(&store, Duration::days(30)).await;
    store
        .revoke(&token_hash(&token), Utc::now(), &revoked_event)
        .await
        .unwrap();
    store
        .revoke(&token_hash(&token), Utc::now(), &revoked_event)
        .await
        .unwrap(); // idempotent
    // Logout leaves `replaced_by` NULL: no grace, even right away.
    assert_eq!(
        rotate(&store, &token, Utc::now()).await.1,
        Rotation::Reused { device_id }
    );
}

#[tokio::test]
#[ignore]
async fn concurrent_refreshes_with_one_token_rotate_once() {
    let store = std::sync::Arc::new(store().await);
    let (_, _, token) = session(&store, Duration::days(30)).await;
    let tasks: Vec<_> = (0..8)
        .map(|_| {
            let (store, token) = (store.clone(), token.clone());
            tokio::spawn(async move { rotate(&store, &token, Utc::now()).await.1 })
        })
        .collect();
    let (mut rotated, mut reissued) = (0, 0);
    for task in tasks {
        match task.await.unwrap() {
            Rotation::Rotated { .. } => rotated += 1,
            Rotation::Reissued { .. } => reissued += 1,
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(rotated, 1, "the row lock lets exactly one refresh win");
    assert_eq!(
        reissued, 7,
        "the losers are within the grace, not treated as theft"
    );
}

#[tokio::test]
#[ignore]
async fn ending_a_session_writes_one_revocation_to_the_outbox() {
    let store = store().await;
    // Logout.
    let (user_id, device_id, token) = session(&store, Duration::days(30)).await;
    store
        .revoke(&token_hash(&token), Utc::now(), &revoked_event)
        .await
        .unwrap();
    store
        .revoke(&token_hash(&token), Utc::now(), &revoked_event)
        .await
        .unwrap();
    assert_eq!(
        revocations(device_id).await,
        vec![format!("{user_id}:{device_id}").into_bytes()],
        "once, not per call"
    );

    // Theft: replaying a rotated token after the grace window.
    let (user_id, device_id, first) = session(&store, Duration::days(30)).await;
    let now = Utc::now();
    rotate(&store, &first, now).await;
    let later = now + GRACE + Duration::seconds(1);
    assert_eq!(
        rotate(&store, &first, later).await.1,
        Rotation::Reused { device_id }
    );
    assert_eq!(
        rotate(&store, &first, later).await.1,
        Rotation::Reused { device_id }
    );
    assert_eq!(
        revocations(device_id).await,
        vec![format!("{user_id}:{device_id}").into_bytes()],
        "a replay of a dead session emits nothing"
    );
}

#[tokio::test]
#[ignore]
async fn concurrent_first_signups_give_one_owner_and_the_rest_members() {
    // Tests share one database, so workspace 1 usually has an owner already; this
    // test needs a genuinely ownerless workspace, in isolation.
    let (store, admin) = throwaway_store("xmsg_owner_election").await;
    // This test exercises the owner election, not the invite policy: open it, like
    // `store()` does for the shared database.
    sqlx::query(
        "UPDATE workspaces SET settings_json = jsonb_set(settings_json, '{signup}', '\"open\"') \
         WHERE id = 1",
    )
    .execute(store.pool())
    .await
    .unwrap();
    let store = std::sync::Arc::new(store);

    let tasks: Vec<_> = (0..4)
        .map(|i| {
            let store = store.clone();
            tokio::spawn(async move {
                let id = new_id();
                store
                    .create_user(
                        id,
                        &format!("first-{i}-{id}@example.org"),
                        &password::hash("integration test"),
                        "IT",
                        None,
                    )
                    .await
                    .map(|_| id)
            })
        })
        .collect();
    let mut ids = Vec::new();
    for task in tasks {
        ids.push(task.await.unwrap().unwrap());
    }

    let roles: Vec<String> = sqlx::query_scalar(
        "SELECT role FROM workspace_members WHERE workspace_id = 1 AND user_id = ANY($1)",
    )
    .bind(&ids)
    .fetch_all(store.pool())
    .await
    .unwrap();
    assert_eq!(roles.len(), 4);
    assert_eq!(
        roles.iter().filter(|r| *r == "owner").count(),
        1,
        "exactly one owner: {roles:?}"
    );
    assert!(roles.iter().all(|r| r == "owner" || r == "member"));

    // The next account is a plain member.
    let (late, _, _) = session(&store, Duration::hours(1)).await;
    assert_eq!(
        store.role(1, late).await.unwrap().as_deref(),
        Some("member")
    );

    // Best-effort cleanup; the next run drops the database first anyway.
    drop(store);
    drop_throwaway(&admin, "xmsg_owner_election").await;
}

#[tokio::test]
#[ignore]
async fn signup_policy_is_invite_only_by_default_and_invites_admit() {
    let (store, admin) = throwaway_store("xmsg_signup_policy").await;
    let hash = &password::hash("integration test");

    // A fresh install: the first account needs no invite and owns the workspace.
    assert_eq!(store.signup_policy().await.unwrap(), (true, true));
    let first = new_id();
    store
        .create_user(first, "first@example.org", hash, "First", None)
        .await
        .unwrap();
    assert_eq!(
        store.role(1, first).await.unwrap().as_deref(),
        Some("owner")
    );

    // After that the workspace is invite-only: no invite, no account. (A missing
    // `settings_json.signup` must read as invite-only, not as a store error.)
    assert_eq!(store.signup_policy().await.unwrap(), (false, true));
    let refused = store
        .create_user(new_id(), "second@example.org", hash, "Second", None)
        .await;
    assert!(
        matches!(refused, Err(StoreError::SignupForbidden)),
        "{refused:?}"
    );

    // A valid unexpired invite code admits the sign-up; an expired one doesn't.
    // The code is checked, not consumed, and sign-ups with it are plain members.
    let pool = store.pool();
    sqlx::query(
        "INSERT INTO workspace_invites (code_hash, workspace_id, created_by, expires_at) \
         VALUES ($2, 1, $1, now() + interval '1 hour')",
    )
    .bind(first)
    .bind(token_hash("valid"))
    .execute(pool)
    .await
    .unwrap();
    let invited = new_id();
    store
        .create_user(
            invited,
            "invited@example.org",
            hash,
            "Invited",
            Some("valid"),
        )
        .await
        .unwrap();
    assert_eq!(
        store.role(1, invited).await.unwrap().as_deref(),
        Some("member")
    );

    // The owner opens the policy; now anyone signs up.
    sqlx::query(
        "UPDATE workspaces SET settings_json = jsonb_set(settings_json, '{signup}', '\"open\"') \
         WHERE id = 1",
    )
    .execute(pool)
    .await
    .unwrap();
    assert_eq!(store.signup_policy().await.unwrap(), (false, false));
    store
        .create_user(new_id(), "open@example.org", hash, "Open", None)
        .await
        .unwrap();

    drop(store);
    drop_throwaway(&admin, "xmsg_signup_policy").await;
}

#[tokio::test]
#[ignore]
async fn enforced_sso_and_recovery_codes_follow_membership() {
    let store = store().await;
    let pool = sqlx::PgPool::connect(&std::env::var("XMSG_IT_PG").unwrap())
        .await
        .unwrap();
    let workspace_id = new_id();
    sqlx::query("INSERT INTO workspaces (id, name, slug) VALUES ($1, 'IT', $2)")
        .bind(workspace_id)
        .bind(format!("it-{workspace_id}"))
        .execute(&pool)
        .await
        .unwrap();
    let (admin, _, _) = session(&store, Duration::hours(1)).await;
    let (member, _, _) = session(&store, Duration::hours(1)).await;
    for (user, role) in [(admin, "admin"), (member, "member")] {
        sqlx::query(
            "INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, $3)",
        )
        .bind(workspace_id)
        .bind(user)
        .bind(role)
        .execute(&pool)
        .await
        .unwrap();
    }
    assert_eq!(
        store.role(workspace_id, admin).await.unwrap().as_deref(),
        Some("admin")
    );
    assert!(!store.sso_enforced(workspace_id).await.unwrap());
    assert!(!store.sso_required(member).await.unwrap());
    store
        .set_sso_enforced(workspace_id, true, admin)
        .await
        .unwrap();
    assert!(store.sso_enforced(workspace_id).await.unwrap());
    assert!(store.sso_required(member).await.unwrap());
    store
        .set_sso_enforced(workspace_id, false, admin)
        .await
        .unwrap();
    assert!(!store.sso_required(member).await.unwrap());
    assert!(!store.has_external_identity(admin).await.unwrap());

    // Unique per run: code_hash is a global primary key, and the shared database
    // outlives this test.
    let nonce = new_id();
    let hashes = vec![
        token_hash(&format!("code-a-{nonce}")),
        token_hash(&format!("code-b-{nonce}")),
    ];
    store
        .replace_recovery_codes(workspace_id, admin, &hashes)
        .await
        .unwrap();
    store
        .replace_recovery_codes(
            workspace_id,
            member,
            &[token_hash(&format!("code-m-{nonce}"))],
        )
        .await
        .unwrap();
    // Once each, only by the holder, only while an owner or admin.
    assert!(store.use_recovery_code(admin, &hashes[0]).await.unwrap());
    assert!(!store.use_recovery_code(admin, &hashes[0]).await.unwrap());
    assert!(!store.use_recovery_code(member, &hashes[1]).await.unwrap());
    assert!(
        !store
            .use_recovery_code(member, &token_hash(&format!("code-m-{nonce}")))
            .await
            .unwrap()
    );
    // Replacing drops the old ones.
    store
        .replace_recovery_codes(
            workspace_id,
            admin,
            &[token_hash(&format!("code-c-{nonce}"))],
        )
        .await
        .unwrap();
    assert!(!store.use_recovery_code(admin, &hashes[1]).await.unwrap());
    assert!(
        store
            .use_recovery_code(admin, &token_hash(&format!("code-c-{nonce}")))
            .await
            .unwrap()
    );
}
