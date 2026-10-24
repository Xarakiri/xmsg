// SPDX-License-Identifier: LicenseRef-xmsg-EE
//! Enterprise Edition licence keys (ADR-0022).
//!
//! - A licence is one line of text: `xmsg-license-v1.<payload>.<signature>`, both parts
//!   base64url. The payload is JSON (`Licence`); the signature is Ed25519 over
//!   `xmsg-license-v1.<payload>`, by xmsg's licensing key. It's checked offline, so it
//!   works in air-gapped installs.
//! - **Expiry never locks anyone out.** After `expires_at` there are `GRACE_DAYS` of
//!   warnings, then each feature follows its own rule (`allowed`): sign-in through SSO
//!   and SCIM keeps working but can't be reconfigured, the admin console turns read-only,
//!   exports stop. Messaging is never affected (it isn't licensed at all).

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, Duration, Utc};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};

pub const PREFIX: &str = "xmsg-license-v1";
pub const GRACE_DAYS: i64 = 30;

/// xmsg's licence-signing public keys (base64url, 32 bytes), generated with
/// `xmsg-license keygen`; the signing halves are kept by xmsg, never in this repository.
///
/// TESTING ONLY: this key's signing half appeared in a chat transcript, so it must be
/// replaced by a privately generated key before any real customer licence is issued.
pub const VENDOR_KEYS: &[&str] = &["DEZkr-8x2Di_HMST5adgqLPwsqhHtejZTV2fdUu1-S4"];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Feature {
    /// SAML / OIDC single sign-on and LDAP.
    Sso,
    /// SCIM user and group provisioning.
    Scim,
    /// Admin console extras: custom roles, policies, analytics, bulk operations.
    Admin,
    /// Retention policies, legal hold, eDiscovery and audit exports, DLP hooks.
    Compliance,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Licence {
    pub licence_id: String,
    pub customer: String,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    /// Licensed active users (reported, not enforced by locking anyone out).
    pub users: u32,
    pub features: Vec<Feature>,
}

/// What someone wants to do with a feature.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Sign in with it, have it applied (SSO login, SCIM provisioning, a hold enforced).
    Use,
    /// Change its configuration (an IdP, roles, a policy).
    Configure,
    /// Export data (eDiscovery, audit).
    Export,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Active,
    /// Expired less than `GRACE_DAYS` ago: everything still works, with warnings.
    Grace,
    Expired,
}

#[derive(Debug, PartialEq)]
pub enum LicenceError {
    Malformed(&'static str),
    BadSignature,
    NoTrustedKey,
}

impl std::fmt::Display for LicenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LicenceError::Malformed(why) => write!(f, "malformed licence: {why}"),
            LicenceError::BadSignature => write!(f, "the licence's signature doesn't verify"),
            LicenceError::NoTrustedKey => write!(f, "no licence-signing key is built in"),
        }
    }
}

impl Licence {
    pub fn status(&self, now: DateTime<Utc>) -> Status {
        if now <= self.expires_at {
            Status::Active
        } else if now <= self.expires_at + Duration::days(GRACE_DAYS) {
            Status::Grace
        } else {
            Status::Expired
        }
    }

    /// Whether `action` on `feature` is allowed now. Features not in the licence are never
    /// allowed; after expiry (and grace) each follows ADR-0022's table.
    pub fn allowed(&self, feature: Feature, action: Action, now: DateTime<Utc>) -> bool {
        if !self.features.contains(&feature) {
            return false;
        }
        if self.status(now) != Status::Expired {
            return true;
        }
        match (feature, action) {
            // Sign-in keeps working: nobody is locked out of their workspace.
            (Feature::Sso | Feature::Scim, Action::Use) => true,
            // Holds and retention policies stay enforced indefinitely.
            (Feature::Compliance, Action::Use) => true,
            // Existing roles keep applying; the console is read-only.
            (Feature::Admin, Action::Use) => true,
            (_, Action::Configure) | (_, Action::Export) => false,
        }
    }
}

/// Verifies a licence against the built-in vendor keys (plus, with the `test-keys`
/// feature, `XMSG_EE_LICENSE_TEST_KEY`).
pub fn verify(text: &str) -> Result<Licence, LicenceError> {
    #[cfg_attr(not(feature = "test-keys"), allow(unused_mut))]
    let mut keys: Vec<String> = VENDOR_KEYS.iter().map(|k| k.to_string()).collect();
    #[cfg(feature = "test-keys")]
    if let Ok(k) = std::env::var("XMSG_EE_LICENSE_TEST_KEY") {
        keys.push(k);
    }
    if keys.is_empty() {
        return Err(LicenceError::NoTrustedKey);
    }
    verify_with(text, &keys)
}

/// Verifies a licence against these public keys (base64url).
pub fn verify_with(text: &str, keys: &[String]) -> Result<Licence, LicenceError> {
    let text = text.trim();
    let mut parts = text.split('.');
    let (Some(prefix), Some(payload), Some(signature), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(LicenceError::Malformed(
            "expected three dot-separated parts",
        ));
    };
    if prefix != PREFIX {
        return Err(LicenceError::Malformed("unknown licence format"));
    }
    let signature: [u8; 64] = URL_SAFE_NO_PAD
        .decode(signature)
        .ok()
        .and_then(|s| s.try_into().ok())
        .ok_or(LicenceError::Malformed("signature"))?;
    let signature = Signature::from_bytes(&signature);
    let signed = format!("{PREFIX}.{payload}");
    let trusted = keys.iter().any(|k| {
        URL_SAFE_NO_PAD
            .decode(k)
            .ok()
            .and_then(|b| <[u8; 32]>::try_from(b).ok())
            .and_then(|b| VerifyingKey::from_bytes(&b).ok())
            .is_some_and(|key| key.verify(signed.as_bytes(), &signature).is_ok())
    });
    if !trusted {
        return Err(LicenceError::BadSignature);
    }
    let json = URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| LicenceError::Malformed("payload"))?;
    serde_json::from_slice(&json).map_err(|_| LicenceError::Malformed("payload JSON"))
}

/// Signs a licence (the vendor's tool; the key never ships with xmsg).
pub fn sign(licence: &Licence, signing_key: &ed25519_dalek::SigningKey) -> String {
    use ed25519_dalek::Signer;
    let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(licence).expect("licence JSON"));
    let signed = format!("{PREFIX}.{payload}");
    let signature = signing_key.sign(signed.as_bytes());
    format!("{signed}.{}", URL_SAFE_NO_PAD.encode(signature.to_bytes()))
}

pub fn public_key(signing_key: &ed25519_dalek::SigningKey) -> String {
    URL_SAFE_NO_PAD.encode(signing_key.verifying_key().to_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn key() -> ed25519_dalek::SigningKey {
        let mut seed = [0u8; 32];
        getrandom::fill(&mut seed).unwrap();
        ed25519_dalek::SigningKey::from_bytes(&seed)
    }

    fn licence(features: Vec<Feature>) -> Licence {
        Licence {
            licence_id: "lic-1".into(),
            customer: "Acme".into(),
            issued_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            expires_at: Utc.with_ymd_and_hms(2027, 1, 1, 0, 0, 0).unwrap(),
            users: 500,
            features,
        }
    }

    #[test]
    fn signed_licences_verify_and_tampering_doesnt() {
        let k = key();
        let l = licence(vec![Feature::Sso]);
        let text = sign(&l, &k);
        assert!(text.starts_with("xmsg-license-v1."));
        assert_eq!(verify_with(&text, &[public_key(&k)]), Ok(l.clone()));
        assert_eq!(
            verify_with(&text, &[public_key(&key())]),
            Err(LicenceError::BadSignature),
            "another vendor key"
        );
        // More users in the payload, same signature.
        let forged = licence(vec![Feature::Sso, Feature::Scim]);
        let forged_payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&forged).unwrap());
        let parts: Vec<&str> = text.split('.').collect();
        let tampered = format!("{}.{forged_payload}.{}", parts[0], parts[2]);
        assert_eq!(
            verify_with(&tampered, &[public_key(&k)]),
            Err(LicenceError::BadSignature)
        );
        assert!(matches!(
            verify_with("nonsense", &[public_key(&k)]),
            Err(LicenceError::Malformed(_))
        ));
    }

    /// ADR-0022's table, row by row.
    #[test]
    fn expiry_follows_the_per_feature_rules() {
        let l = licence(vec![
            Feature::Sso,
            Feature::Scim,
            Feature::Admin,
            Feature::Compliance,
        ]);
        let active = l.expires_at - Duration::days(1);
        let grace = l.expires_at + Duration::days(GRACE_DAYS - 1);
        let expired = l.expires_at + Duration::days(GRACE_DAYS + 1);
        assert_eq!(
            (l.status(active), l.status(grace), l.status(expired)),
            (Status::Active, Status::Grace, Status::Expired)
        );
        for f in [
            Feature::Sso,
            Feature::Scim,
            Feature::Admin,
            Feature::Compliance,
        ] {
            for a in [Action::Use, Action::Configure, Action::Export] {
                assert!(
                    l.allowed(f, a, active) && l.allowed(f, a, grace),
                    "{f:?} {a:?} before expiry"
                );
            }
        }
        // SSO, LDAP and SCIM sign-in keep working; only their configuration is blocked.
        assert!(l.allowed(Feature::Sso, Action::Use, expired));
        assert!(l.allowed(Feature::Scim, Action::Use, expired));
        assert!(!l.allowed(Feature::Sso, Action::Configure, expired));
        // Legal holds and retention stay enforced; editing them and exports are blocked.
        assert!(l.allowed(Feature::Compliance, Action::Use, expired));
        assert!(!l.allowed(Feature::Compliance, Action::Configure, expired));
        assert!(!l.allowed(Feature::Compliance, Action::Export, expired));
        // Admin console: read-only.
        assert!(l.allowed(Feature::Admin, Action::Use, expired));
        assert!(!l.allowed(Feature::Admin, Action::Configure, expired));
        // Features not licensed: never.
        assert!(!licence(vec![Feature::Sso]).allowed(Feature::Scim, Action::Use, active));
    }

    #[test]
    fn the_built_in_keys_are_valid_ed25519_public_keys() {
        for k in VENDOR_KEYS {
            let bytes: [u8; 32] = URL_SAFE_NO_PAD.decode(k).unwrap().try_into().unwrap();
            assert!(VerifyingKey::from_bytes(&bytes).is_ok(), "{k}");
        }
        // A licence signed by someone else's key is still refused.
        let forged = sign(&licence(vec![Feature::Sso]), &key());
        assert_eq!(verify(&forged), Err(LicenceError::BadSignature));
    }

    #[test]
    fn without_a_built_in_key_nothing_verifies() {
        if VENDOR_KEYS.is_empty() && std::env::var("XMSG_EE_LICENSE_TEST_KEY").is_err() {
            assert_eq!(
                verify("xmsg-license-v1.a.b"),
                Err(LicenceError::NoTrustedKey)
            );
        }
    }
}
