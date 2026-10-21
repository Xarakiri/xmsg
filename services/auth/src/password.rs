// SPDX-License-Identifier: AGPL-3.0-only
//! Password hashing with Argon2id (default parameters of the `argon2` crate).

use argon2::Argon2;
use argon2::password_hash::{PasswordHasher, PasswordVerifier};

pub const MIN_LENGTH: usize = 10;
pub const MAX_LENGTH: usize = 256;

pub fn validate(password: &str) -> Result<(), &'static str> {
    let len = password.chars().count();
    if len < MIN_LENGTH {
        return Err("password must be at least 10 characters");
    }
    if len > MAX_LENGTH {
        return Err("password must be at most 256 characters");
    }
    Ok(())
}

pub fn hash(password: &str) -> String {
    Argon2::default()
        .hash_password(password.as_bytes())
        .expect("argon2")
        .to_string()
}

pub fn verify(hash: &str, password: &str) -> bool {
    Argon2::default()
        .verify_password(password.as_bytes(), hash)
        .is_ok()
}

/// A valid hash of a random password, verified against when the user doesn't
/// exist, so login takes the same time either way.
pub fn dummy_hash() -> &'static str {
    use std::sync::OnceLock;
    static DUMMY: OnceLock<String> = OnceLock::new();
    DUMMY.get_or_init(|| hash(&crate::random_token(24)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashes_verify_only_the_right_password() {
        let h = hash("correct horse battery");
        assert!(h.starts_with("$argon2id$"));
        assert!(verify(&h, "correct horse battery"));
        assert!(!verify(&h, "wrong horse battery"));
        assert!(!verify("not a hash", "anything"));
    }

    #[test]
    fn short_and_huge_passwords_are_refused() {
        assert!(validate("short").is_err());
        assert!(validate(&"x".repeat(257)).is_err());
        assert!(validate("long enough pass").is_ok());
    }
}
