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
//!
//! LDAP / Active Directory sign-in (the password form checks the directory first):
//! `XMSG_EE_LDAP_URL`, `XMSG_EE_LDAP_BIND_DN`, `XMSG_EE_LDAP_BIND_PASSWORD`,
//! `XMSG_EE_LDAP_BASE_DN`, optionally `XMSG_EE_LDAP_USER_FILTER`,
//! `XMSG_EE_LDAP_EMAIL_ATTRIBUTE`, `XMSG_EE_LDAP_NAME_ATTRIBUTE`,
//! `XMSG_EE_LDAP_SUBJECT_ATTRIBUTE` and `XMSG_EE_LDAP_STARTTLS=true`.

use std::sync::Arc;

use xmsg_auth::sso::{Directory, IdentityProvider};
use xmsg_ee_license::{Action, Feature, Status};

/// The LDAP directory, when `XMSG_EE_LDAP_URL` is set.
fn ldap() -> Option<Arc<dyn Directory>> {
    let url = std::env::var("XMSG_EE_LDAP_URL")
        .ok()
        .filter(|u| !u.is_empty())?;
    let env = |name: &str| {
        std::env::var(name).unwrap_or_else(|_| {
            eprintln!("{name} must be set with XMSG_EE_LDAP_URL");
            std::process::exit(2);
        })
    };
    let optional = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
    let mut config = xmsg_ee_ldap::Config::new(
        url,
        env("XMSG_EE_LDAP_BIND_DN"),
        env("XMSG_EE_LDAP_BIND_PASSWORD"),
        env("XMSG_EE_LDAP_BASE_DN"),
    );
    if let Some(filter) = optional("XMSG_EE_LDAP_USER_FILTER") {
        config.user_filter = filter;
    }
    if let Some(attr) = optional("XMSG_EE_LDAP_EMAIL_ATTRIBUTE") {
        config.email_attribute = attr;
    }
    if let Some(attr) = optional("XMSG_EE_LDAP_NAME_ATTRIBUTE") {
        config.name_attribute = attr;
    }
    config.subject_attribute = optional("XMSG_EE_LDAP_SUBJECT_ATTRIBUTE");
    config.starttls = optional("XMSG_EE_LDAP_STARTTLS").as_deref() == Some("true");
    match xmsg_ee_ldap::Ldap::new(config) {
        Ok(ldap) => Some(Arc::new(ldap)),
        Err(e) => {
            eprintln!("LDAP: {e}");
            std::process::exit(2);
        }
    }
}

#[tokio::main]
async fn main() {
    let now = chrono::Utc::now();
    let licence = xmsg_ee_license::from_env();
    let mut providers: Vec<Arc<dyn IdentityProvider>> = Vec::new();
    let mut directory: Option<Arc<dyn Directory>> = None;
    match &licence {
        Some(l) if l.allowed(Feature::Sso, Action::Use, now) => {
            directory = ldap();
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
    // Changing a workspace's SSO policy is configuration: it needs a current licence.
    let sso_policy_editable = licence
        .as_ref()
        .is_some_and(|l| l.allowed(Feature::Sso, Action::Configure, now));
    xmsg_auth::serve(xmsg_auth::Extensions {
        providers,
        sso_policy_editable,
        directory,
    })
    .await;
}
