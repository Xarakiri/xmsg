// SPDX-License-Identifier: AGPL-3.0-only
//! Users service (plan §5.3): profiles, workspaces, workspace roles and invites.
//!
//! The auth service owns sign-up and credentials and adds every new user to the
//! default workspace; this service owns everything else about users and workspaces.
//! Profiles are visible only to people who share a workspace with their owner.

pub mod api;
pub mod config;
pub mod store;

use std::time::{SystemTime, UNIX_EPOCH};

/// Time-ordered 63-bit IDs, the same layout as the auth service's: milliseconds since
/// 2026-01-01 above 22 random bits.
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

/// A fresh invite code: 16 random bytes as base64url (22 characters, URL-safe).
pub fn invite_code() -> String {
    use base64::Engine;
    let mut buf = [0u8; 16];
    getrandom::fill(&mut buf).expect("OS random");
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(buf)
}

/// Invite codes are bearer secrets, stored only as SHA-256 hashes.
pub fn code_hash(code: &str) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(code.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_are_random_and_hashes_are_stable() {
        let (a, b) = (invite_code(), invite_code());
        assert_ne!(a, b);
        assert_eq!(a.len(), 22);
        assert_eq!(code_hash(&a), code_hash(&a));
        assert_ne!(code_hash(&a), code_hash(&b));
    }
}
