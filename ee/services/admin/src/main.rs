// SPDX-License-Identifier: LicenseRef-xmsg-EE
//! xmsg-admin-ee: the admin console's API. `XMSG_DATABASE_URL`, `XMSG_AUTH_JWKS_URL`,
//! `XMSG_EE_ADMIN_PORT` (default 8096), `XMSG_EE_ADMIN_CONSOLE_DIR` (the built console,
//! default `/usr/share/xmsg/admin-console`; the API alone if it isn't there) and the licence (`XMSG_EE_LICENSE` or
//! `XMSG_EE_LICENSE_FILE`; without one it reports "unlicensed" and shows no analytics).

use std::sync::Arc;

use xmsg_admin_ee::{App, Postgres, router};

fn fail(message: &str) -> ! {
    eprintln!("{message}");
    std::process::exit(2);
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let database_url = std::env::var("XMSG_DATABASE_URL")
        .unwrap_or_else(|_| fail("XMSG_DATABASE_URL must be set"));
    let jwks_url = std::env::var("XMSG_AUTH_JWKS_URL")
        .unwrap_or_else(|_| "http://auth:8081/.well-known/jwks.json".into());
    let store = Postgres::connect(&database_url)
        .await
        .unwrap_or_else(|e| fail(&format!("postgres: {e}")));
    let verifier = xmsg_authn::Verifier::from_env(jwks_url)
        .await
        .unwrap_or_else(|e| fail(&format!("token verification: {e}")));
    let app = Arc::new(App {
        store,
        verifier,
        licence: xmsg_ee_license::from_env(),
    });
    let port: u16 = std::env::var("XMSG_EE_ADMIN_PORT")
        .unwrap_or_else(|_| "8096".into())
        .parse()
        .unwrap_or_else(|_| fail("XMSG_EE_ADMIN_PORT must be a port"));
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .unwrap_or_else(|e| fail(&format!("bind :{port}: {e}")));
    tracing::info!(port, "admin API listening");
    let console = std::path::PathBuf::from(
        std::env::var("XMSG_EE_ADMIN_CONSOLE_DIR")
            .unwrap_or_else(|_| "/usr/share/xmsg/admin-console".into()),
    );
    let console = if console.join("index.html").is_file() {
        tracing::info!(dir = %console.display(), "serving the admin console at /admin/");
        Some(console)
    } else {
        tracing::warn!(dir = %console.display(), "no admin console there: serving the API only");
        None
    };
    axum::serve(listener, router(app, console.as_deref()))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .unwrap_or_else(|e| fail(&format!("serve: {e}")));
}
