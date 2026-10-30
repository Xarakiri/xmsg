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
    assert!(!mine[0].setup_completed);
    let (name, done) = store
        .update_workspace(w, Some("Renamed"), true)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((name.as_str(), done), ("Renamed", true));
    assert_eq!(
        store.update_workspace(new_id(), None, false).await.unwrap(),
        None
    );
    assert!(store.workspaces_of(alice).await.unwrap()[0].setup_completed);

    let code = invite(&store, w, alice, Duration::hours(1)).await;
    store
        .accept_invite(&code_hash(&code), bob, Utc::now())
        .await
        .unwrap();
    // Listed by user ID, which isn't creation order (IDs carry random bits).
    let mut expected = vec![(alice, "owner"), (bob, "member")];
    expected.sort();
    let members = store.members(w, 0, 10).await.unwrap();
    assert_eq!(
        members
            .iter()
            .map(|m| (m.user_id, m.role.as_str()))
            .collect::<Vec<_>>(),
        expected
    );
    // The page after the first member holds the other one.
    let (first, second) = (expected[0].0, expected[1].0);
    let page = store.members(w, first, 10).await.unwrap();
    assert_eq!(page.len(), 1);
    let name = if second == bob { "Bob" } else { "Alice" };
    assert_eq!((page[0].user_id, page[0].name.as_str()), (second, name));
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

#[tokio::test]
#[ignore]
async fn provisioning_creates_once_and_reactivates() {
    let (pool, store) = setup().await;
    let owner = user(&pool, "Owner").await;
    let ws = workspace(&store, owner).await;
    let email = format!("provisioned-{}@example.org", xmsg_users::new_id());
    let (id, created) = store
        .provision_user(ws, &email, "Pat", xmsg_users::new_id())
        .await
        .unwrap();
    assert!(created);
    let (again, created) = store
        .provision_user(ws, &email, "Pat", xmsg_users::new_id())
        .await
        .unwrap();
    assert_eq!((again, created), (id, false), "one user per email");
    // Role changes (the owner's PATCH) land on the membership row.
    assert!(store.set_role(ws, id, "admin").await.unwrap());
    assert_eq!(store.role(ws, id).await.unwrap().as_deref(), Some("admin"));
    assert!(
        !store
            .set_role(ws, xmsg_users::new_id(), "admin")
            .await
            .unwrap()
    );
    let p = store
        .provisioned_by_email(ws, &email)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((p.user_id, p.name.as_str(), p.member), (id, "Pat", true));
    // A user created by provisioning has no password: they sign in through the identity provider.
    let hash: Option<String> = sqlx::query_scalar("SELECT password_hash FROM users WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(hash, None);
    assert!(store.remove_member(ws, id, b"event").await.unwrap());
    assert!(
        !store.provisioned(ws, id).await.unwrap().unwrap().member,
        "deactivated"
    );
    store
        .provision_user(ws, &email, "Pat", xmsg_users::new_id())
        .await
        .unwrap();
    assert!(
        store.provisioned(ws, id).await.unwrap().unwrap().member,
        "reactivated"
    );
    let page = store.provisioned_members(ws, 0, 100).await.unwrap();
    assert!(page.iter().any(|m| m.user_id == id) && page.iter().any(|m| m.user_id == owner));
}

#[tokio::test]
#[ignore]
async fn workspace_setup_backfills_an_owner_and_onboarding() {
    let (pool, _store) = setup().await;
    let setup_sql = include_str!("../../../deploy/compose/init/postgres/006_workspace_setup.sql");
    let reapply = || {
        let pool = pool.clone();
        async move { sqlx::raw_sql(setup_sql).execute(&pool).await.unwrap() }
    };
    reapply().await;

    let ws = new_id();
    sqlx::query("INSERT INTO workspaces (id, name, slug) VALUES ($1, 'Ownerless', $2)")
        .bind(ws)
        .bind(format!("ownerless-{ws}"))
        .execute(&pool)
        .await
        .unwrap();
    // Everyone is a plain member; joined_at decides who becomes the owner.
    let mut members = Vec::new();
    for (i, name) in ["First", "Second", "Third"].iter().enumerate() {
        let id = user(&pool, name).await;
        sqlx::query(
            "INSERT INTO workspace_members (workspace_id, user_id, role, joined_at) \
             VALUES ($1, $2, 'member', now() - make_interval(days => $3))",
        )
        .bind(ws)
        .bind(id)
        .bind((3 - i) as f64)
        .execute(&pool)
        .await
        .unwrap();
        members.push(id);
    }
    // A workspace that already has an owner must stay untouched.
    let (pool2, store) = (
        pool.clone(),
        xmsg_users::store::Postgres::connect(&url()).await.unwrap(),
    );
    let _ = store;
    let kept = user(&pool, "Kept").await;
    let owned = new_id();
    sqlx::query("INSERT INTO workspaces (id, name, slug) VALUES ($1, 'Owned', $2)")
        .bind(owned)
        .bind(format!("owned-{owned}"))
        .execute(&pool2)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO workspace_members (workspace_id, user_id, role, joined_at) \
         VALUES ($1, $2, 'owner', now() - interval '1 day')",
    )
    .bind(owned)
    .bind(kept)
    .execute(&pool2)
    .await
    .unwrap();

    reapply().await;

    let role: String = sqlx::query_scalar(
        "SELECT role FROM workspace_members WHERE workspace_id = $1 AND user_id = $2",
    )
    .bind(ws)
    .bind(members[0])
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(role, "owner", "the earliest member inherits the workspace");
    for late in &members[1..] {
        let role: String = sqlx::query_scalar(
            "SELECT role FROM workspace_members WHERE workspace_id = $1 AND user_id = $2",
        )
        .bind(ws)
        .bind(late)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(role, "member");
    }
    let onboarded: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM workspace_members WHERE workspace_id = $1 AND defaults_joined_at IS NOT NULL",
    )
    .bind(ws)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(onboarded, 3, "existing members count as onboarded");

    // Reapplying (every start) changes nothing.
    reapply().await;
    let again: String = sqlx::query_scalar(
        "SELECT role FROM workspace_members WHERE workspace_id = $1 AND user_id = $2",
    )
    .bind(ws)
    .bind(members[0])
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(again, "owner");
    let kept_role: String = sqlx::query_scalar(
        "SELECT role FROM workspace_members WHERE workspace_id = $1 AND user_id = $2",
    )
    .bind(owned)
    .bind(kept)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(kept_role, "owner", "an existing owner is never replaced");
}

#[tokio::test]
#[ignore]
async fn storage_quotas_default_set_and_clear() {
    let (pool, store) = setup().await;
    let owner = user(&pool, "Owner").await;
    let ws = workspace(&store, owner).await;
    assert_eq!(
        store.storage_quota(ws).await.unwrap(),
        Some(None),
        "the default"
    );
    assert!(store.set_storage_quota(ws, Some(5_000_000)).await.unwrap());
    assert_eq!(
        store.storage_quota(ws).await.unwrap(),
        Some(Some(5_000_000))
    );
    assert!(store.set_storage_quota(ws, None).await.unwrap());
    assert_eq!(store.storage_quota(ws).await.unwrap(), Some(None));
    // No such workspace.
    assert_eq!(store.storage_quota(new_id()).await.unwrap(), None);
    assert!(!store.set_storage_quota(new_id(), Some(1)).await.unwrap());
}
