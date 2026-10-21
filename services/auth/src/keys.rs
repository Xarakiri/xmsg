// SPDX-License-Identifier: AGPL-3.0-only
//! Ed25519 access tokens (EdDSA JWTs) and the JWKS the gateway validates them with.

use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use ed25519_dalek::SigningKey;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

pub const ISSUER: &str = "xmsg";
pub const AUDIENCE: &str = "xmsg";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Claims {
    /// User ID, as a string (JWT convention).
    pub sub: String,
    /// Session (device) ID: what revocations and refresh tokens are tied to.
    pub sid: i64,
    pub iat: i64,
    pub exp: i64,
    pub iss: String,
    pub aud: String,
}

pub struct Keys {
    kid: String,
    encoding: EncodingKey,
    decoding: DecodingKey,
    public_x: String,
}

impl Keys {
    /// From a 32-byte Ed25519 seed (base64). The key ID is derived from the public key.
    pub fn from_seed_base64(seed: &str) -> Result<Self, String> {
        let bytes = STANDARD
            .decode(seed.trim())
            .map_err(|e| format!("XMSG_AUTH_KEY is not base64: {e}"))?;
        let seed: [u8; 32] = bytes
            .try_into()
            .map_err(|_| "XMSG_AUTH_KEY must be 32 bytes".to_string())?;
        Self::from_seed(seed)
    }

    pub fn generate() -> Self {
        let mut seed = [0u8; 32];
        getrandom::fill(&mut seed).expect("OS random");
        Self::from_seed(seed).expect("valid seed")
    }

    fn from_seed(seed: [u8; 32]) -> Result<Self, String> {
        let signing = SigningKey::from_bytes(&seed);
        // PKCS#8 v1 for Ed25519 (RFC 8410): fixed 16-byte prefix, then the 32-byte seed.
        let mut der = vec![
            0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22,
            0x04, 0x20,
        ];
        der.extend_from_slice(&seed);
        let public = signing.verifying_key().to_bytes();
        let public_x = URL_SAFE_NO_PAD.encode(public);
        let kid = crate::token_hash(&public_x)[..16].to_string();
        Ok(Keys {
            encoding: EncodingKey::from_ed_der(&der),
            decoding: DecodingKey::from_ed_components(&public_x)
                .map_err(|e| format!("decoding key: {e}"))?,
            kid,
            public_x,
        })
    }

    pub fn issue(&self, user_id: i64, session_id: i64, now: i64, ttl_secs: i64) -> String {
        let claims = Claims {
            sub: user_id.to_string(),
            sid: session_id,
            iat: now,
            exp: now + ttl_secs,
            iss: ISSUER.into(),
            aud: AUDIENCE.into(),
        };
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some(self.kid.clone());
        jsonwebtoken::encode(&header, &claims, &self.encoding).expect("signing")
    }

    pub fn verify(&self, token: &str) -> Result<Claims, String> {
        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.set_issuer(&[ISSUER]);
        validation.set_audience(&[AUDIENCE]);
        validation.leeway = 30;
        jsonwebtoken::decode::<Claims>(token, &self.decoding, &validation)
            .map(|d| d.claims)
            .map_err(|e| e.to_string())
    }

    /// RFC 7517 JWKS with one OKP/Ed25519 key.
    pub fn jwks(&self) -> serde_json::Value {
        serde_json::json!({
            "keys": [{ "kty": "OKP", "crv": "Ed25519", "alg": "EdDSA", "use": "sig", "kid": self.kid, "x": self.public_x }]
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn issued_tokens_verify_and_carry_the_session() {
        let keys = Keys::generate();
        let now = chrono::Utc::now().timestamp();
        let token = keys.issue(42, 7, now, 900);
        let claims = keys.verify(&token).unwrap();
        assert_eq!(
            (claims.sub.as_str(), claims.sid, claims.exp - claims.iat),
            ("42", 7, 900)
        );
    }

    #[test]
    fn expired_tampered_and_foreign_tokens_are_refused() {
        let keys = Keys::generate();
        let now = chrono::Utc::now().timestamp();
        assert!(
            keys.verify(&keys.issue(1, 1, now - 3_600, 900)).is_err(),
            "expired"
        );
        let mut token = keys.issue(1, 1, now, 900);
        token.replace_range(token.len() - 4.., "AAAA");
        assert!(keys.verify(&token).is_err(), "tampered signature");
        let other = Keys::generate();
        assert!(
            keys.verify(&other.issue(1, 1, now, 900)).is_err(),
            "signed by another key"
        );
    }

    #[test]
    fn the_same_seed_gives_the_same_key_id_and_jwks() {
        let seed = STANDARD.encode([7u8; 32]);
        let (a, b) = (
            Keys::from_seed_base64(&seed).unwrap(),
            Keys::from_seed_base64(&seed).unwrap(),
        );
        assert_eq!(a.jwks(), b.jwks());
        assert_eq!(a.jwks()["keys"][0]["crv"], "Ed25519");
        assert!(Keys::from_seed_base64("dG9vIHNob3J0").is_err());
    }
}
