// SPDX-License-Identifier: LicenseRef-xmsg-EE
//! xmsg-auth-ee: the auth service plus Enterprise Edition single sign-on.
//!
//! The licence (`XMSG_EE_LICENSE`, or a file at `XMSG_EE_LICENSE_FILE`) decides whether
//! SSO is offered. Without a valid licence this behaves exactly like the Community
//! Edition's `xmsg-auth`. An expired licence keeps SSO sign-in working (ADR-0022): nobody
//! is locked out of their workspace.
//!
//! OIDC provider settings: `XMSG_EE_OIDC_ISSUER`, `XMSG_EE_OIDC_CLIENT_ID`,
//! `XMSG_EE_OIDC_CLIENT_SECRET`, optionally `XMSG_EE_OIDC_ID` (default `oidc`),
//! `XMSG_EE_OIDC_NAME` (the button text, default "Single sign-on") and
//! `XMSG_EE_OIDC_TRUST_UNVERIFIED_EMAIL=true`.

use std::sync::Arc;

use xmsg_auth::sso::IdentityProvider;
use xmsg_ee_license::{Action, Feature, Licence, Status};

fn licence() -> Option<Licence> {
    let text = match (
        std::env::var("XMSG_EE_LICENSE"),
        std::env::var("XMSG_EE_LICENSE_FILE"),
    ) {
        (Ok(t), _) => t,
        (_, Ok(path)) => std::fs::read_to_string(&path).unwrap_or_else(|e| {
            eprintln!("XMSG_EE_LICENSE_FILE {path}: {e}");
            std::process::exit(2);
        }),
        _ => return None,
    };
    match xmsg_ee_license::verify(&text) {
        Ok(l) => Some(l),
        Err(e) => {
            // Not fatal: messaging must never depend on the licence.
            eprintln!("licence not accepted ({e}); Enterprise features are off");
            None
        }
    }
}

#[tokio::main]
async fn main() {
    let now = chrono::Utc::now();
    let licence = licence();
    let mut providers: Vec<Arc<dyn IdentityProvider>> = Vec::new();
    match &licence {
        Some(l) if l.allowed(Feature::Sso, Action::Use, now) => {
            if let Ok(issuer) = std::env::var("XMSG_EE_OIDC_ISSUER") {
                let env = |name: &str| {
                    std::env::var(name).unwrap_or_else(|_| {
                        eprintln!("{name} must be set with XMSG_EE_OIDC_ISSUER");
                        std::process::exit(2);
                    })
                };
                let config = xmsg_ee_oidc::Config {
                    id: std::env::var("XMSG_EE_OIDC_ID").unwrap_or_else(|_| "oidc".into()),
                    display_name: std::env::var("XMSG_EE_OIDC_NAME")
                        .unwrap_or_else(|_| "Single sign-on".into()),
                    issuer,
                    client_id: env("XMSG_EE_OIDC_CLIENT_ID"),
                    client_secret: env("XMSG_EE_OIDC_CLIENT_SECRET"),
                    trust_unverified_email: std::env::var("XMSG_EE_OIDC_TRUST_UNVERIFIED_EMAIL")
                        .as_deref()
                        == Ok("true"),
                };
                match xmsg_ee_oidc::Oidc::discover(config).await {
                    Ok(oidc) => providers.push(Arc::new(oidc)),
                    Err(e) => {
                        eprintln!("OIDC provider: {e}");
                        std::process::exit(2);
                    }
                }
            }
        }
        Some(_) => eprintln!("the licence doesn't include single sign-on"),
        None => {}
    }
    if let Some(l) = &licence {
        match l.status(now) {
            Status::Active => eprintln!(
                "licensed to {} until {}",
                l.customer,
                l.expires_at.date_naive()
            ),
            Status::Grace => eprintln!(
                "the licence for {} expired on {}: renew it; configuration changes stop after the grace period",
                l.customer,
                l.expires_at.date_naive()
            ),
            Status::Expired => eprintln!(
                "the licence for {} has expired: SSO sign-in still works, its configuration can't change",
                l.customer
            ),
        }
    }
    xmsg_auth::serve(providers).await;
}
