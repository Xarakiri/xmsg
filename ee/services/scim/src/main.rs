// SPDX-License-Identifier: LicenseRef-xmsg-EE
//! xmsg-scim-ee: SCIM 2.0 provisioning for one workspace, gated by the licence (`scim`).
//!
//! `XMSG_EE_SCIM_TOKEN` (what the identity provider sends), `XMSG_EE_SCIM_WORKSPACE`,
//! `XMSG_USERS_URL` (default `http://users:8084`), `XMSG_PROVISIONING_TOKEN` (the users
//! service's), `XMSG_EE_SCIM_PUBLIC_URL` (e.g. `https://chat.example.com/scim/v2`) and
//! `XMSG_EE_SCIM_PORT` (default 8095), plus the licence (`XMSG_EE_LICENSE` or
//! `XMSG_EE_LICENSE_FILE`). An expired licence keeps provisioning working (ADR-0022).

use xmsg_ee_license::{Action, Feature, Status};
use xmsg_scim_ee::{Config, router};

fn fail(message: &str) -> ! {
    eprintln!("{message}");
    std::process::exit(2);
}

fn env(name: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| fail(&format!("{name} must be set")))
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let now = chrono::Utc::now();
    let Some(licence) = xmsg_ee_license::from_env() else {
        fail("SCIM provisioning needs an Enterprise licence (XMSG_EE_LICENSE)");
    };
    if !licence.allowed(Feature::Scim, Action::Use, now) {
        fail("the licence doesn't include SCIM provisioning");
    }
    if licence.status(now) != Status::Active {
        tracing::warn!(
            customer = licence.customer,
            expired = %licence.expires_at.date_naive(),
            "the licence has expired: provisioning keeps working, renew it"
        );
    }
    let token = env("XMSG_EE_SCIM_TOKEN");
    if token.len() < 32 {
        fail("XMSG_EE_SCIM_TOKEN must be at least 32 characters");
    }
    let config = Config {
        token,
        workspace_id: env("XMSG_EE_SCIM_WORKSPACE"),
        users_url: std::env::var("XMSG_USERS_URL").unwrap_or_else(|_| "http://users:8084".into()),
        provisioning_token: env("XMSG_PROVISIONING_TOKEN"),
        public_url: std::env::var("XMSG_EE_SCIM_PUBLIC_URL").unwrap_or_else(|_| "/scim/v2".into()),
    };
    let port: u16 = std::env::var("XMSG_EE_SCIM_PORT")
        .unwrap_or_else(|_| "8095".into())
        .parse()
        .unwrap_or_else(|_| fail("XMSG_EE_SCIM_PORT must be a port"));
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .unwrap_or_else(|e| fail(&format!("bind :{port}: {e}")));
    tracing::info!(
        port,
        workspace = config.workspace_id,
        "SCIM provisioning listening"
    );
    axum::serve(listener, router(config))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .unwrap_or_else(|e| fail(&format!("serve: {e}")));
}
