// SPDX-License-Identifier: AGPL-3.0-only
//! Auth service (plan §5.2): sign-up, login, short-lived Ed25519 access tokens,
//! rotating refresh tokens with reuse detection, and a JWKS endpoint so the
//! gateway and Envoy validate tokens locally.

pub mod api;
pub mod config;
pub mod keys;
pub mod password;
pub mod revocation;
pub mod store;

use std::time::{SystemTime, UNIX_EPOCH};

/// Time-ordered 63-bit IDs: milliseconds since 2026-01-01 in the high bits,
/// 22 random bits below (no coordination needed for users, devices, tokens).
pub fn new_id() -> i64 {
    const EPOCH_MS: u64 = 1_767_225_600_000;
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let mut random = [0u8; 4];
    getrandom::fill(&mut random).expect("OS random");
    let low = u32::from_le_bytes(random) as u64 & ((1 << 22) - 1);
    (((ms - EPOCH_MS) << 22) | low) as i64
}

/// Random bytes as base64url (no padding), for opaque tokens.
pub fn random_token(bytes: usize) -> String {
    use base64::Engine;
    let mut buf = vec![0u8; bytes];
    getrandom::fill(&mut buf).expect("OS random");
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(buf)
}

/// Refresh tokens are stored only as SHA-256 hashes.
pub fn token_hash(token: &str) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(token.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_positive_and_increase_over_time() {
        let a = new_id();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let b = new_id();
        assert!(a > 0 && b > a);
    }

    #[test]
    fn tokens_are_random_and_hashes_are_stable() {
        let (a, b) = (random_token(32), random_token(32));
        assert_ne!(a, b);
        assert_eq!(a.len(), 43);
        assert_eq!(token_hash(&a), token_hash(&a));
        assert_ne!(token_hash(&a), token_hash(&b));
        assert_eq!(token_hash(&a).len(), 64);
    }
}
