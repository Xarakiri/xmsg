// SPDX-License-Identifier: LicenseRef-xmsg-EE
//! Sign-in against a real OpenLDAP server. Ignored by default; run with a throwaway
//! server (CI starts one as a service):
//!
//!   docker run -d --rm -p 127.0.0.1:3389:389 -e LDAP_DOMAIN=example.org \
//!     -e LDAP_ADMIN_PASSWORD=admin-it-only -e LDAP_TLS=false osixia/openldap:1.5.0
//!   XMSG_IT_LDAP=ldap://127.0.0.1:3389 cargo test -p xmsg-ee-ldap --test openldap -- --ignored
//!
//! The test adds its own entries (unique per run) through the admin account.

use std::collections::HashSet;

use ldap3::LdapConnAsync;
use xmsg_auth::sso::Directory;
use xmsg_ee_ldap::{Config, Ldap};

const ADMIN_DN: &str = "cn=admin,dc=example,dc=org";
const ADMIN_PASSWORD: &str = "admin-it-only";

fn url() -> String {
    std::env::var("XMSG_IT_LDAP").expect("XMSG_IT_LDAP must be set for this test")
}

/// An `ou` of its own with Ann (mail and displayName) and a second, mail-less entry.
async fn seed(run: &str) -> String {
    let (conn, mut ldap) = LdapConnAsync::new(&url()).await.unwrap();
    ldap3::drive!(conn);
    ldap.simple_bind(ADMIN_DN, ADMIN_PASSWORD)
        .await
        .unwrap()
        .success()
        .unwrap();
    let base = format!("ou=people-{run},dc=example,dc=org");
    let set = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<HashSet<String>>();
    let attr = |name: &str, values: &[&str]| (name.to_string(), set(values));
    ldap.add(
        &base,
        vec![
            attr("objectClass", &["organizationalUnit"]),
            attr("ou", &[&format!("people-{run}")]),
        ],
    )
    .await
    .unwrap()
    .success()
    .unwrap();
    for (uid, mail) in [("ann", Some("Ann@Example.org")), ("nomail", None)] {
        let mut attrs = vec![
            attr("objectClass", &["inetOrgPerson"]),
            attr("uid", &[uid]),
            attr("cn", &[uid]),
            attr("sn", &["Test"]),
            attr("displayName", &["Ann Brown"]),
            attr("userPassword", &["directory secret"]),
        ];
        if let Some(mail) = mail {
            attrs.push(attr("mail", &[mail]));
        }
        ldap.add(&format!("uid={uid},{base}"), attrs)
            .await
            .unwrap()
            .success()
            .unwrap();
    }
    let _ = ldap.unbind().await;
    base
}

fn directory(base: &str) -> Ldap {
    let mut config = Config::new(url(), ADMIN_DN.into(), ADMIN_PASSWORD.into(), base.into());
    config.subject_attribute = Some("entryUUID".into());
    Ldap::new(config).unwrap()
}

#[tokio::test]
#[ignore]
async fn people_sign_in_with_their_directory_password() {
    let run = format!("{}", std::process::id() as u64 * 1000 + rand_suffix());
    let base = seed(&run).await;
    let ldap = directory(&base);

    let by_uid = ldap
        .authenticate("ann", "directory secret")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(by_uid.email, "Ann@Example.org");
    assert_eq!(by_uid.name, "Ann Brown");
    assert!(by_uid.email_verified);
    assert_eq!(by_uid.subject.len(), 36, "entryUUID: {}", by_uid.subject);
    let by_mail = ldap
        .authenticate("ann@example.org", "directory secret")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        by_mail.subject, by_uid.subject,
        "same person, same stable ID"
    );

    assert_eq!(ldap.authenticate("ann", "wrong").await, Ok(None));
    assert_eq!(
        ldap.authenticate("nobody", "directory secret").await,
        Ok(None)
    );
    assert_eq!(
        ldap.authenticate("*", "directory secret").await,
        Ok(None),
        "no wildcard logins"
    );
    assert_eq!(
        ldap.authenticate("nomail", "directory secret").await,
        Ok(None),
        "no email, no sign-in"
    );

    // A broken service account is an error (the form falls back), not a wrong password.
    let mut broken = Config::new(url(), ADMIN_DN.into(), "not it".into(), base.clone());
    broken.timeout = std::time::Duration::from_secs(5);
    assert!(
        Ldap::new(broken)
            .unwrap()
            .authenticate("ann", "directory secret")
            .await
            .is_err()
    );
}

fn rand_suffix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos() as u64
        % 1000
}
