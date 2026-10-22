// SPDX-License-Identifier: AGPL-3.0-only
//! Integration test of the Postgres store with the Small-tier schema.
//!
//! Ignored by default; run with
//!   XMSG_IT_PG=postgres://xmsg:xmsg-dev-only@127.0.0.1:5432/xmsg \
//!     cargo test -p xmsg-users --test integration -- --ignored
//! The schema in deploy/compose/init/postgres must already be applied.

use chrono::{Duration, Utc};
use sqlx::PgPool;
use xmsg_users::store::{Accepted, NewInvite, Postgres, ProfileChange, Store, StoreError};
use xmsg_users::{code_hash, invite_code, new_id};

fn url() -> String {
    std::env::var("XMSG_IT_PG").expect("XMSG_IT_PG must be set for integration tests")
}

async fn setup() -> (PgPool, Postgres) {
    (
        PgPool::connect(&url()).await.unwrap(),
        Postgres::connect(&url()).await.unwrap(),
    )
}

/// A user row (the auth service owns sign-up; tests insert users directly), in no workspace.
async fn user(pool: &PgPool, name: &str) -> i64 {
    let id = new_id();
    sqlx::query("INSERT INTO users (id, email, name) VALUES ($1, $2, $3)")
        .bind(id)
        .bind(format!("users-it-{id}@example.org"))
        .bind(name)
        .execute(pool)
        .await
        .unwrap();
    id
}

async fn workspace(store: &Postgres, owner: i64) -> i64 {
    let id = new_id();
    store
        .create_workspace(id, "IT", &format!("it-{id}"), owner)
        .await
        .unwrap();
    id
}

async fn invite(store: &Postgres, workspace_id: i64, by: i64, ttl: Duration) -> String {
    let code = invite_code();
    store
        .create_invite(&NewInvite {
            code_hash: code_hash(&code),
            workspace_id,
            created_by: by,
            expires_at: Utc::now() + ttl,
        })
        .await
        .unwrap();
    code
}

#[tokio::test]
#[ignore]
async fn profiles_are_read_and_patched() {
    let (pool, store) = setup().await;
    let ada = user(&pool, "Ada").await;
    let (email, profile) = store.me(ada).await.unwrap().unwrap();
    assert_eq!(email, format!("users-it-{ada}@example.org"));
    assert_eq!((profile.name.as_str(), profile.avatar_url), ("Ada", None));

    let set = ProfileChange {
        name: None,
        avatar_url: Some(Some("https://example.org/a.png".into())),
    };
    let p = store.update_profile(ada, &set).await.unwrap().unwrap();
    assert_eq!(
        (p.name.as_str(), p.avatar_url.as_deref()),
        ("Ada", Some("https://example.org/a.png"))
    );
    let rename = ProfileChange {
        name: Some("Ada L.".into()),
        avatar_url: None,
    };
    let p = store.update_profile(ada, &rename).await.unwrap().unwrap();
    assert_eq!(
        (p.name.as_str(), p.avatar_url.is_some()),
        ("Ada L.", true),
        "an absent avatar_url is left alone"
    );
    let clear = ProfileChange {
        name: None,
        avatar_url: Some(None),
    };
    assert_eq!(
        store
            .update_profile(ada, &clear)
            .await
            .unwrap()
            .unwrap()
            .avatar_url,
        None
    );
    assert_eq!(store.update_profile(-1, &clear).await.unwrap(), None);
    assert_eq!(store.me(-1).await.unwrap(), None);
}

#[tokio::test]
#[ignore]
async fn profiles_are_visible_only_within_shared_workspaces() {
    let (pool, store) = setup().await;
    let (alice, bob, oscar) = (
        user(&pool, "Alice").await,
        user(&pool, "Bob").await,
        user(&pool, "Oscar").await,
    );
    let w = workspace(&store, alice).await;
    let code = invite(&store, w, alice, Duration::hours(1)).await;
    assert_eq!(
        store
            .accept_invite(&code_hash(&code), bob, Utc::now())
            .await
            .unwrap(),
        Accepted::Joined { workspace_id: w }
    );

    let mut seen: Vec<i64> = store
        .visible_profiles(alice, &[alice, bob, oscar])
        .await
        .unwrap()
        .iter()
        .map(|p| p.user_id)
        .collect();
    seen.sort();
    assert_eq!(seen, [alice, bob]);
    let oscar_sees: Vec<i64> = store
        .visible_profiles(oscar, &[alice, bob, oscar])
        .await
        .unwrap()
        .iter()
        .map(|p| p.user_id)
        .collect();
    assert_eq!(oscar_sees, [oscar], "only himself");
}

#[tokio::test]
#[ignore]
async fn workspaces_roles_members_and_unique_slugs() {
    let (pool, store) = setup().await;
    let (alice, bob) = (user(&pool, "Alice").await, user(&pool, "Bob").await);
    let w = workspace(&store, alice).await;
    assert_eq!(
        store
            .create_workspace(new_id(), "Again", &format!("it-{w}"), bob)
            .await,
        Err(StoreError::SlugTaken)
    );
    assert_eq!(
        store.role(w, alice).await.unwrap().as_deref(),
        Some("owner")
    );
    assert_eq!(store.role(w, bob).await.unwrap(), None);
    let mine = store.workspaces_of(alice).await.unwrap();
    assert_eq!(
        mine.iter()
            .map(|x| (x.id, x.role.as_str()))
            .collect::<Vec<_>>(),
        [(w, "owner")]
    );

    let code = invite(&store, w, alice, Duration::hours(1)).await;
    store
        .accept_invite(&code_hash(&code), bob, Utc::now())
        .await
        .unwrap();
    let members = store.members(w, 0, 10).await.unwrap();
    assert_eq!(
        members
            .iter()
            .map(|m| (m.user_id, m.role.as_str()))
            .collect::<Vec<_>>(),
        [(alice, "owner"), (bob, "member")]
    );
    let page = store.members(w, alice, 10).await.unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!((page[0].user_id, page[0].name.as_str()), (bob, "Bob"));
}

#[tokio::test]
#[ignore]
async fn invites_expire_and_accepting_twice_is_harmless() {
    let (pool, store) = setup().await;
    let (alice, bob) = (user(&pool, "Alice").await, user(&pool, "Bob").await);
    let w = workspace(&store, alice).await;
    let expired = invite(&store, w, alice, Duration::seconds(-1)).await;
    assert_eq!(
        store
            .accept_invite(&code_hash(&expired), bob, Utc::now())
            .await
            .unwrap(),
        Accepted::Invalid
    );
    assert_eq!(
        store
            .accept_invite(&code_hash("never issued"), bob, Utc::now())
            .await
            .unwrap(),
        Accepted::Invalid
    );
    let code = invite(&store, w, alice, Duration::hours(1)).await;
    assert_eq!(
        store
            .accept_invite(&code_hash(&code), alice, Utc::now())
            .await
            .unwrap(),
        Accepted::AlreadyMember { workspace_id: w },
        "the owner keeps their role"
    );
    assert_eq!(
        store.role(w, alice).await.unwrap().as_deref(),
        Some("owner")
    );
}
