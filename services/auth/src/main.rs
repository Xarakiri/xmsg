// SPDX-License-Identifier: AGPL-3.0-only
//! Auth service entry point (Community Edition: no external identity providers).

#[tokio::main]
async fn main() {
    xmsg_auth::serve(xmsg_auth::Extensions::default()).await;
}
