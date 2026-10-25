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
    Postgres::connect(&url).await.expect("connect to Postgres")
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
        .create_user(user_id, &email, &password::hash("integration test"), "IT")
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
    store.create_user(id, &email, "hash", "A").await.unwrap();
    assert!(matches!(
        store.create_user(new_id(), &email, "hash", "B").await,
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

    let hashes = vec![token_hash("code-a"), token_hash("code-b")];
    store
        .replace_recovery_codes(workspace_id, admin, &hashes)
        .await
        .unwrap();
    store
        .replace_recovery_codes(workspace_id, member, &[token_hash("code-m")])
        .await
        .unwrap();
    // Once each, only by the holder, only while an owner or admin.
    assert!(store.use_recovery_code(admin, &hashes[0]).await.unwrap());
    assert!(!store.use_recovery_code(admin, &hashes[0]).await.unwrap());
    assert!(!store.use_recovery_code(member, &hashes[1]).await.unwrap());
    assert!(
        !store
            .use_recovery_code(member, &token_hash("code-m"))
            .await
            .unwrap()
    );
    // Replacing drops the old ones.
    store
        .replace_recovery_codes(workspace_id, admin, &[token_hash("code-c")])
        .await
        .unwrap();
    assert!(!store.use_recovery_code(admin, &hashes[1]).await.unwrap());
    assert!(
        store
            .use_recovery_code(admin, &token_hash("code-c"))
            .await
            .unwrap()
    );
}
