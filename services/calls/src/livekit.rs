// SPDX-License-Identifier: AGPL-3.0-only
//! LiveKit's two signed formats, both HS256 JWTs with the installation's API key and
//! secret: the access tokens clients join rooms with, and the `Authorization` header of
//! the webhooks LiveKit sends us (whose `sha256` claim covers the body).

use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub struct Keys {
    pub api_key: String,
    pub api_secret: String,
}

/// What a participant may do in the room: talk, show their camera and share a screen.
const SOURCES: [&str; 4] = ["camera", "microphone", "screen_share", "screen_share_audio"];

fn sign(secret: &str, data: &str) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("any key length");
    mac.update(data.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

impl Keys {
    /// An access token for `identity` (our user ID) to join `room`, valid `ttl_secs`.
    pub fn token(&self, room: &str, identity: &str, now_secs: i64, ttl_secs: i64) -> String {
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
        let claims = json!({
            "iss": self.api_key,
            "sub": identity,
            "nbf": now_secs - 10,
            "exp": now_secs + ttl_secs,
            "video": {
                "room": room,
                "roomJoin": true,
                "canPublish": true,
                "canSubscribe": true,
                "canPublishData": true,
                "canPublishSources": SOURCES,
            },
        });
        let payload = URL_SAFE_NO_PAD.encode(claims.to_string());
        let signed = format!("{header}.{payload}");
        let signature = URL_SAFE_NO_PAD.encode(sign(&self.api_secret, &signed));
        format!("{signed}.{signature}")
    }

    /// The claims of a JWT we (or LiveKit, with the same secret) signed, if the
    /// signature, issuer and lifetime check out.
    pub fn verify(&self, jwt: &str, now_secs: i64) -> Option<Value> {
        let mut parts = jwt
            .trim()
            .strip_prefix("Bearer ")
            .unwrap_or(jwt.trim())
            .split('.');
        let (header, payload, signature) = (parts.next()?, parts.next()?, parts.next()?);
        if parts.next().is_some() {
            return None;
        }
        let header: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(header).ok()?).ok()?;
        if header["alg"] != "HS256" {
            return None;
        }
        let expected = sign(&self.api_secret, &format!("{}.{payload}", jwt_head(jwt)));
        let given = URL_SAFE_NO_PAD.decode(signature).ok()?;
        if !same(&expected, &given) {
            return None;
        }
        let claims: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).ok()?).ok()?;
        let leeway = 60;
        let fresh = claims["exp"]
            .as_i64()
            .is_none_or(|exp| now_secs <= exp + leeway)
            && claims["nbf"]
                .as_i64()
                .is_none_or(|nbf| now_secs + leeway >= nbf);
        (claims["iss"] == self.api_key.as_str() && fresh).then_some(claims)
    }

    /// Checks a webhook: the header's JWT, and that its `sha256` claim is the body's hash.
    pub fn verify_webhook(&self, authorization: &str, body: &[u8], now_secs: i64) -> bool {
        let Some(claims) = self.verify(authorization, now_secs) else {
            return false;
        };
        let hash = STANDARD.encode(Sha256::digest(body));
        claims["sha256"]
            .as_str()
            .is_some_and(|h| same(h.as_bytes(), hash.as_bytes()))
    }
}

/// The header part of a JWT (without a "Bearer " prefix).
fn jwt_head(jwt: &str) -> &str {
    let jwt = jwt.trim();
    let jwt = jwt.strip_prefix("Bearer ").unwrap_or(jwt);
    jwt.split('.').next().unwrap_or("")
}

/// Equal without leaking where they differ through timing.
fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys() -> Keys {
        Keys {
            api_key: "APIkey".into(),
            api_secret: "secret-of-at-least-32-characters!!".into(),
        }
    }

    #[test]
    fn tokens_carry_the_room_and_what_may_be_published() {
        let k = keys();
        let token = k.token("c7-ab", "42", 1_000, 3600);
        let claims = k.verify(&token, 1_000).expect("our own token verifies");
        assert_eq!(claims["sub"], "42");
        assert_eq!(claims["iss"], "APIkey");
        assert_eq!(claims["video"]["room"], "c7-ab");
        assert_eq!(claims["video"]["roomJoin"], true);
        assert_eq!(claims["video"]["canPublishSources"], json!(SOURCES));
        assert_eq!(claims["exp"], 4600);
    }

    #[test]
    fn tampered_expired_and_foreign_tokens_fail() {
        let k = keys();
        let token = k.token("c7-ab", "42", 1_000, 3600);
        assert!(k.verify(&token, 1_000 + 3600 + 61).is_none(), "expired");
        let mut parts: Vec<&str> = token.split('.').collect();
        let forged = URL_SAFE_NO_PAD.encode(r#"{"iss":"APIkey","sub":"1"}"#);
        parts[1] = &forged;
        assert!(
            k.verify(&parts.join("."), 1_000).is_none(),
            "payload changed"
        );
        let other = Keys {
            api_key: "APIkey".into(),
            api_secret: "another-secret-another-secret-1234".into(),
        };
        assert!(other.verify(&token, 1_000).is_none(), "another secret");
        let none = format!(
            "{}.{}.",
            URL_SAFE_NO_PAD.encode(r#"{"alg":"none"}"#),
            token.split('.').nth(1).unwrap()
        );
        assert!(k.verify(&none, 1_000).is_none(), "alg none");
    }

    /// A webhook as LiveKit signs it: a JWT with the body's SHA-256 (standard base64).
    fn webhook_auth(k: &Keys, body: &[u8], now: i64) -> String {
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
        let claims = json!({ "iss": k.api_key, "nbf": now, "exp": now + 300, "sha256": STANDARD.encode(Sha256::digest(body)) });
        let signed = format!("{header}.{}", URL_SAFE_NO_PAD.encode(claims.to_string()));
        format!(
            "{signed}.{}",
            URL_SAFE_NO_PAD.encode(sign(&k.api_secret, &signed))
        )
    }

    #[test]
    fn webhooks_are_checked_against_their_body() {
        let k = keys();
        let body = br#"{"event":"participant_joined"}"#;
        let auth = webhook_auth(&k, body, 1_000);
        assert!(k.verify_webhook(&auth, body, 1_000));
        assert!(k.verify_webhook(&format!("Bearer {auth}"), body, 1_000));
        assert!(
            !k.verify_webhook(&auth, br#"{"event":"room_finished"}"#, 1_000),
            "another body"
        );
        assert!(!k.verify_webhook("nonsense", body, 1_000));
    }
}
