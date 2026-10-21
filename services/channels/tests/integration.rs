// SPDX-License-Identifier: AGPL-3.0-only
//! Integration test of the Postgres store with the Small-tier schema.
//!
//! Ignored by default; run with
//!   XMSG_IT_PG=postgres://xmsg:xmsg-dev-only@127.0.0.1:5432/xmsg \
//!     cargo test -p xmsg-channels --test integration -- --ignored
//! The schema in deploy/compose/init/postgres must already be applied.

use sqlx::PgPool;
use xmsg_channels::store::{Dm, NewChannel, Postgres, Store, StoreError};
use xmsg_channels::{channel_created, new_id};
use xmsg_proto::Message;
use xmsg_proto::v1::{ChannelType, InboundEvent};

fn url() -> String {
    std::env::var("XMSG_IT_PG").expect("XMSG_IT_PG must be set for integration tests")
}

async fn pool() -> PgPool {
    PgPool::connect(&url()).await.unwrap()
}

/// A user row (the auth service owns users; tests insert them directly).
async fn user(pool: &PgPool) -> i64 {
    let id = new_id();
    sqlx::query("INSERT INTO users (id, email, name) VALUES ($1, $2, 'IT')")
        .bind(id)
        .bind(format!("ch-it-{id}@example.org"))
        .execute(pool)
        .await
        .unwrap();
    id
}

fn channel(kind: &'static str, creator: i64) -> NewChannel {
    NewChannel {
        id: new_id(),
        workspace_id: 1,
        kind,
        name: Some("it".into()),
        topic: None,
        created_by: creator,
    }
}

fn event(c: &NewChannel, members: &[i64]) -> InboundEvent {
    channel_created(
        c.id,
        c.workspace_id,
        c.created_by,
        ChannelType::Private,
        "it",
        members,
    )
}

async fn outbox_payloads(pool: &PgPool, channel_id: i64) -> Vec<Vec<u8>> {
    sqlx::query_scalar(
        "SELECT payload FROM outbox WHERE aggregate_type = 'channel' AND aggregate_id = $1",
    )
    .bind(channel_id)
    .fetch_all(pool)
    .await
    .unwrap()
}

#[tokio::test]
#[ignore]
async fn a_channel_and_its_outbox_event_commit_together() {
    let (pool, store) = (pool().await, Postgres::connect(&url()).await.unwrap());
    let (alice, bob) = (user(&pool).await, user(&pool).await);
    let c = channel("private", alice);
    let e = event(&c, &[alice, bob]);
    store.create_channel(&c, &[alice, bob], &e).await.unwrap();

    let row: (String, Option<String>, i64) =
        sqlx::query_as("SELECT type, name, created_by FROM channels WHERE id = $1")
            .bind(c.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(row, ("private".into(), Some("it".into()), alice));
    let payloads = outbox_payloads(&pool, c.id).await;
    assert_eq!(payloads.len(), 1);
    assert_eq!(InboundEvent::decode(&payloads[0][..]).unwrap(), e);
}

#[tokio::test]
#[ignore]
async fn unknown_users_and_workspaces_roll_everything_back() {
    let (pool, store) = (pool().await, Postgres::connect(&url()).await.unwrap());
    let alice = user(&pool).await;
    let c = channel("private", alice);
    assert_eq!(
        store
            .create_channel(&c, &[alice, 999], &event(&c, &[alice, 999]))
            .await,
        Err(StoreError::UnknownUser)
    );
    let mut elsewhere = channel("private", alice);
    elsewhere.workspace_id = 424_242;
    assert_eq!(
        store
            .create_channel(&elsewhere, &[alice], &event(&elsewhere, &[alice]))
            .await,
        Err(StoreError::UnknownWorkspace)
    );
    for id in [c.id, elsewhere.id] {
        let channels: i64 = sqlx::query_scalar("SELECT count(*) FROM channels WHERE id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(channels, 0);
        assert!(outbox_payloads(&pool, id).await.is_empty());
    }
}

#[tokio::test]
#[ignore]
async fn concurrent_dm_opens_create_one_channel() {
    let (pool, store) = (
        pool().await,
        std::sync::Arc::new(Postgres::connect(&url()).await.unwrap()),
    );
    let (alice, bob) = (user(&pool).await, user(&pool).await);
    let tasks: Vec<_> = (0..6)
        .map(|i| {
            let store = store.clone();
            let (me, other) = if i % 2 == 0 {
                (alice, bob)
            } else {
                (bob, alice)
            };
            tokio::spawn(async move {
                let c = NewChannel {
                    name: None,
                    ..channel("dm", me)
                };
                let e = event(&c, &[me, other]);
                (c.id, store.create_dm(&c, other, &e).await.unwrap())
            })
        })
        .collect();
    let mut created = Vec::new();
    let mut existing = Vec::new();
    for task in tasks {
        match task.await.unwrap() {
            (id, Dm::Created) => created.push(id),
            (_, Dm::Existing(id)) => existing.push(id),
        }
    }
    assert_eq!(created.len(), 1, "exactly one open creates the DM");
    assert!(
        existing.iter().all(|id| *id == created[0]),
        "every other open gets that DM"
    );
    let dms: i64 =
        sqlx::query_scalar("SELECT count(*) FROM dm_pairs WHERE user_low = $1 AND user_high = $2")
            .bind(alice.min(bob))
            .bind(alice.max(bob))
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(dms, 1);
    let orphans: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM channels WHERE type = 'dm' AND created_by IN ($1, $2)",
    )
    .bind(alice)
    .bind(bob)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        orphans, 1,
        "losing transactions leave no channel or outbox row behind"
    );
}
