// SPDX-License-Identifier: LicenseRef-xmsg-EE
//! The usage queries against a real PostgreSQL with the xmsg schema. Ignored by default:
//!   XMSG_IT_PG=postgres://xmsg:xmsg-dev-only@127.0.0.1:5432/xmsg \
//!     cargo test -p xmsg-admin-ee --test postgres -- --ignored

use xmsg_admin_ee::{Postgres, Store};

#[tokio::test]
#[ignore]
async fn usage_queries_run_against_the_schema() {
    let store = Postgres::connect(&std::env::var("XMSG_IT_PG").expect("XMSG_IT_PG"))
        .await
        .unwrap();
    // The default workspace always exists; numbers depend on what's in the database.
    let usage = store.usage(1).await.unwrap();
    let members: i64 = usage.members_by_role.values().sum();
    assert!(members >= usage.joined_last_30_days);
    assert!(store.seats_used().await.unwrap() >= 0);
    assert!(!store.admin_anywhere(-1).await.unwrap());
    assert_eq!(store.role(1, -1).await.unwrap(), None);
}
