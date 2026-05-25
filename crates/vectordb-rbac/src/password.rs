use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use password_hash::{rand_core::OsRng, SaltString};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PasswordError {
    #[error("hash error: {0}")]
    Hash(String),
    #[error("verify error: {0}")]
    Verify(String),
}

/// Hash a secret (password or token) with Argon2id, default parameters.
///
/// The returned string is a PHC-format hash and includes the algorithm, salt,
/// and parameters; it is safe to store and to round-trip.
pub fn hash_secret(secret: &str) -> Result<String, PasswordError> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    let hash = argon2
        .hash_password(secret.as_bytes(), &salt)
        .map_err(|e| PasswordError::Hash(e.to_string()))?;
    Ok(hash.to_string())
}

/// Verify a candidate secret against a stored PHC hash.
///
/// Constant-time within Argon2; an empty `phc` returns `Ok(false)` to support
/// users created with token-only auth.
pub fn verify_secret(secret: &str, phc: &str) -> Result<bool, PasswordError> {
    if phc.is_empty() {
        return Ok(false);
    }
    let parsed = PasswordHash::new(phc).map_err(|e| PasswordError::Verify(e.to_string()))?;
    match Argon2::default().verify_password(secret.as_bytes(), &parsed) {
        Ok(()) => Ok(true),
        Err(password_hash::Error::Password) => Ok(false),
        Err(e) => Err(PasswordError::Verify(e.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let h = hash_secret("hunter2").unwrap();
        assert!(verify_secret("hunter2", &h).unwrap());
        assert!(!verify_secret("wrong", &h).unwrap());
    }

    #[test]
    fn empty_hash_rejects() {
        assert!(!verify_secret("anything", "").unwrap());
    }
}
