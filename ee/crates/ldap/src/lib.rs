// SPDX-License-Identifier: LicenseRef-xmsg-EE
//! LDAP and Active Directory sign-in: the auth service's `Directory` extension point.
//!
//! For each sign-in: bind as the service account, search the base for exactly one entry
//! matching the user filter (with the login escaped), then bind as that entry with the
//! password given. The entry's email, name and subject (its DN, or a stable attribute
//! such as `entryUUID` / `objectGUID`) become the identity.

use std::collections::HashMap;
use std::time::Duration;

use base64::Engine;
use ldap3::{LdapConnAsync, LdapConnSettings, LdapError, Scope, SearchEntry, ldap_escape};
use xmsg_auth::sso::{BoxFuture, Directory, ExternalIdentity};

/// LDAP result code for a wrong password (or an unknown DN).
const INVALID_CREDENTIALS: u32 = 49;

#[derive(Debug, Clone)]
pub struct Config {
    /// `ldaps://dc.example.com` (or `ldap://` with `starttls`).
    pub url: String,
    pub starttls: bool,
    /// The service account that searches for people.
    pub bind_dn: String,
    pub bind_password: String,
    /// Where people are, e.g. `ou=people,dc=example,dc=com`.
    pub base_dn: String,
    /// `{login}` is replaced by the escaped login, e.g. `(|(uid={login})(mail={login}))`
    /// or, for Active Directory, `(&(objectClass=user)(|(sAMAccountName={login})(mail={login})))`.
    pub user_filter: String,
    pub email_attribute: String,
    pub name_attribute: String,
    /// A stable ID attribute; the DN when `None` (which changes if the entry moves).
    pub subject_attribute: Option<String>,
    pub timeout: Duration,
}

impl Config {
    /// Defaults for everything but the server and the service account.
    pub fn new(url: String, bind_dn: String, bind_password: String, base_dn: String) -> Self {
        Config {
            url,
            starttls: false,
            bind_dn,
            bind_password,
            base_dn,
            user_filter: "(|(uid={login})(mail={login}))".into(),
            email_attribute: "mail".into(),
            name_attribute: "displayName".into(),
            subject_attribute: None,
            timeout: Duration::from_secs(10),
        }
    }
}

pub struct Ldap {
    config: Config,
}

impl Ldap {
    pub fn new(config: Config) -> Result<Self, String> {
        if !config.user_filter.contains("{login}") {
            return Err("the user filter must contain {login}".into());
        }
        if !(config.url.starts_with("ldaps://") || config.url.starts_with("ldap://")) {
            return Err("the URL must start with ldaps:// or ldap://".into());
        }
        Ok(Ldap { config })
    }

    /// The search filter for a login, with LDAP filter metacharacters escaped.
    pub fn filter(&self, login: &str) -> String {
        self.config
            .user_filter
            .replace("{login}", &ldap_escape(login))
    }

    async fn sign_in(
        &self,
        login: &str,
        password: &str,
    ) -> Result<Option<ExternalIdentity>, LdapError> {
        let c = &self.config;
        let settings = LdapConnSettings::new()
            .set_conn_timeout(c.timeout)
            .set_starttls(c.starttls);
        let (conn, mut ldap) = LdapConnAsync::with_settings(settings, &c.url).await?;
        ldap3::drive!(conn);
        ldap.with_timeout(c.timeout)
            .simple_bind(&c.bind_dn, &c.bind_password)
            .await?
            .success()?;
        let mut attributes = vec![c.email_attribute.as_str(), c.name_attribute.as_str(), "cn"];
        if let Some(subject) = &c.subject_attribute {
            attributes.push(subject);
        }
        let (entries, _) = ldap
            .with_timeout(c.timeout)
            .search(&c.base_dn, Scope::Subtree, &self.filter(login), attributes)
            .await?
            .success()?;
        if entries.len() != 1 {
            if entries.len() > 1 {
                tracing::warn!(
                    matches = entries.len(),
                    "the login matches several directory entries; refusing"
                );
            }
            let _ = ldap.unbind().await;
            return Ok(None);
        }
        let entry = SearchEntry::construct(entries.into_iter().next().unwrap());
        // Rebinding as the person checks their password; the server compares, we never see it.
        let bound = ldap
            .with_timeout(c.timeout)
            .simple_bind(&entry.dn, password)
            .await?;
        let _ = ldap.unbind().await;
        if bound.rc == INVALID_CREDENTIALS {
            return Ok(None);
        }
        bound.success()?;
        Ok(identity(c, &entry))
    }
}

/// First value of an attribute, matched case-insensitively.
fn first<'a>(attrs: &'a HashMap<String, Vec<String>>, name: &str) -> Option<&'a str> {
    attrs
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .and_then(|(_, v)| v.first())
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
}

fn identity(c: &Config, entry: &SearchEntry) -> Option<ExternalIdentity> {
    let email = first(&entry.attrs, &c.email_attribute)?.to_string();
    let name = first(&entry.attrs, &c.name_attribute)
        .or_else(|| first(&entry.attrs, "cn"))
        .unwrap_or(&email)
        .to_string();
    let subject = match &c.subject_attribute {
        None => entry.dn.to_lowercase(),
        Some(attr) => match first(&entry.attrs, attr) {
            Some(text) => text.to_string(),
            // Binary IDs (Active Directory's objectGUID) arrive separately.
            None => entry
                .bin_attrs
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(attr))
                .and_then(|(_, v)| v.first())
                .map(|b| base64::engine::general_purpose::STANDARD.encode(b))?,
        },
    };
    Some(ExternalIdentity {
        subject,
        email,
        email_verified: true,
        name,
    })
}

impl Directory for Ldap {
    fn id(&self) -> &str {
        "ldap"
    }

    fn authenticate<'a>(
        &'a self,
        login: &'a str,
        password: &'a str,
    ) -> BoxFuture<'a, Result<Option<ExternalIdentity>, String>> {
        Box::pin(async move {
            // An empty password is an anonymous bind, which "succeeds": never sign in on it.
            if login.trim().is_empty() || password.is_empty() {
                return Ok(None);
            }
            match self.sign_in(login.trim(), password).await {
                Ok(Some(identity)) => Ok(Some(identity)),
                Ok(None) => Ok(None),
                Err(e) => Err(e.to_string()),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ldap() -> Ldap {
        Ldap::new(Config::new(
            "ldap://localhost".into(),
            "cn=svc".into(),
            "pw".into(),
            "dc=example,dc=org".into(),
        ))
        .unwrap()
    }

    #[test]
    fn logins_are_escaped_in_the_filter() {
        assert_eq!(ldap().filter("ann"), "(|(uid=ann)(mail=ann))");
        // A login can't widen the search.
        assert_eq!(
            ldap().filter("*)(uid=*"),
            r"(|(uid=\2a\29\28uid=\2a)(mail=\2a\29\28uid=\2a))"
        );
    }

    #[test]
    fn identities_come_from_the_configured_attributes() {
        let mut c = ldap().config;
        let entry = SearchEntry {
            dn: "uid=Ann,ou=People,dc=example,dc=org".into(),
            attrs: HashMap::from([
                ("mail".into(), vec!["ann@example.org".into()]),
                ("CN".into(), vec!["Ann Brown".into()]),
                ("entryUUID".into(), vec!["1234-abcd".into()]),
            ]),
            bin_attrs: HashMap::from([("objectGUID".into(), vec![vec![1, 2, 3]])]),
        };
        let id = identity(&c, &entry).unwrap();
        assert_eq!(
            (id.subject.as_str(), id.name.as_str()),
            ("uid=ann,ou=people,dc=example,dc=org", "Ann Brown")
        );
        c.subject_attribute = Some("entryuuid".into());
        assert_eq!(identity(&c, &entry).unwrap().subject, "1234-abcd");
        c.subject_attribute = Some("objectGUID".into());
        assert_eq!(identity(&c, &entry).unwrap().subject, "AQID");
        c.email_attribute = "userPrincipalName".into();
        assert!(identity(&c, &entry).is_none(), "no email, no sign-in");
    }

    #[test]
    fn bad_configuration_is_refused() {
        let mut c = ldap().config;
        c.user_filter = "(uid=*)".into();
        assert!(Ldap::new(c.clone()).is_err());
        c.user_filter = "(uid={login})".into();
        c.url = "http://dc".into();
        assert!(Ldap::new(c).is_err());
    }

    #[tokio::test]
    async fn empty_passwords_never_reach_the_server() {
        // The URL points nowhere: an attempt would be an error, not `None`.
        assert_eq!(ldap().authenticate("ann", "").await, Ok(None));
        assert_eq!(ldap().authenticate("  ", "pw").await, Ok(None));
    }
}
