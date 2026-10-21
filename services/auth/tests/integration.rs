// SPDX-License-Identifier: AGPL-3.0-only
//! Integration test against real PostgreSQL with the Small-tier schema.
//!
//! Ignored by default; run with
//!   XMSG_IT_PG=postgres://xmsg:xmsg-dev-only@127.0.0.1:5432/xmsg \
//!     cargo test -p xmsg-auth --test integration -- --ignored
//! The schema in deploy/compose/init/postgres must already be applied.

use chrono::{Duration, Utc};
use xmsg_auth::store::{NewRefresh, Postgres, Rotation, Store, StoreError};
use xmsg_auth::{new_id, password, random_token, token_hash};

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
    let (second, new) = refresh(Duration::days(30));
    assert_eq!(
        store
            .rotate(&token_hash(&first), new, Utc::now())
            .await
            .unwrap(),
        Rotation::Rotated { user_id, device_id }
    );

    let (_, unused) = refresh(Duration::days(30));
    assert_eq!(
        store
            .rotate(&token_hash(&first), unused, Utc::now())
            .await
            .unwrap(),
        Rotation::Reused { device_id }
    );
    let (_, unused) = refresh(Duration::days(30));
    // The reuse revoked the current token too.
    assert_eq!(
        store
            .rotate(&token_hash(&second), unused, Utc::now())
            .await
            .unwrap(),
        Rotation::Reused { device_id }
    );
}

#[tokio::test]
#[ignore]
async fn expired_unknown_and_logged_out_tokens_do_not_rotate() {
    let store = store().await;
    let (_, _, expired) = session(&store, Duration::seconds(-1)).await;
    let (_, unused) = refresh(Duration::days(30));
    assert_eq!(
        store
            .rotate(&token_hash(&expired), unused, Utc::now())
            .await
            .unwrap(),
        Rotation::Invalid
    );

    let (_, unused) = refresh(Duration::days(30));
    assert_eq!(
        store
            .rotate(&token_hash("never issued"), unused, Utc::now())
            .await
            .unwrap(),
        Rotation::Invalid
    );

    let (_, device_id, token) = session(&store, Duration::days(30)).await;
    store.revoke(&token_hash(&token), Utc::now()).await.unwrap();
    store.revoke(&token_hash(&token), Utc::now()).await.unwrap(); // idempotent
    let (_, unused) = refresh(Duration::days(30));
    assert_eq!(
        store
            .rotate(&token_hash(&token), unused, Utc::now())
            .await
            .unwrap(),
        Rotation::Reused { device_id }
    );
}

#[tokio::test]
#[ignore]
async fn concurrent_refreshes_with_one_token_rotate_once() {
    let store = std::sync::Arc::new(store().await);
    let (_, _, token) = session(&store, Duration::days(30)).await;
    let hash = token_hash(&token);
    let tasks: Vec<_> = (0..8)
        .map(|_| {
            let (store, hash) = (store.clone(), hash.clone());
            tokio::spawn(async move {
                store
                    .rotate(&hash, refresh(Duration::days(30)).1, Utc::now())
                    .await
                    .unwrap()
            })
        })
        .collect();
    let mut rotated = 0;
    for task in tasks {
        if matches!(task.await.unwrap(), Rotation::Rotated { .. }) {
            rotated += 1;
        }
    }
    assert_eq!(rotated, 1, "the row lock lets exactly one refresh win");
}
