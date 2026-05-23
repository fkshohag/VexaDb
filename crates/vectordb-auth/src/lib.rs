//! Shared API-key authentication configuration and validation.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

pub const HEADER_API_KEY: &str = "x-api-key";
pub const HEADER_AUTHORIZATION: &str = "authorization";
/// gRPC metadata key for Raft leader redirect (value: `http://host:port`).
pub const METADATA_LEADER: &str = "x-vectordb-leader";

/// When `required` is false and `keys` is empty, auth is disabled.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Valid API keys (plaintext; load from env/file in production).
    #[serde(default)]
    pub keys: Vec<String>,
    /// Reject requests without a valid key when true.
    #[serde(default)]
    pub required: bool,
}

impl AuthConfig {
    pub fn disabled() -> Self {
        Self::default()
    }

    pub fn from_keys(keys: Vec<String>) -> Self {
        Self {
            keys,
            required: true,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.required && !self.keys.is_empty()
    }

    pub fn key_set(&self) -> HashSet<String> {
        self.keys.iter().cloned().collect()
    }

    /// Validate a raw API key string.
    pub fn validate_key(&self, key: &str) -> bool {
        if !self.required {
            return true;
        }
        if self.keys.is_empty() {
            return !self.required;
        }
        self.keys.iter().any(|k| k == key)
    }

    /// Extract API key from `Authorization: Bearer <key>` or `x-api-key: <key>`.
    pub fn extract_key(authorization: Option<&str>, api_key_header: Option<&str>) -> Option<String> {
        if let Some(v) = api_key_header {
            let k = v.trim();
            if !k.is_empty() {
                return Some(k.to_string());
            }
        }
        if let Some(v) = authorization {
            let v = v.trim();
            if let Some(rest) = v.strip_prefix("Bearer ") {
                let k = rest.trim();
                if !k.is_empty() {
                    return Some(k.to_string());
                }
            }
        }
        None
    }

    pub fn check_headers(
        &self,
        authorization: Option<&str>,
        api_key_header: Option<&str>,
    ) -> Result<(), AuthError> {
        if !self.is_enabled() {
            return Ok(());
        }
        let key = Self::extract_key(authorization, api_key_header)
            .ok_or(AuthError::MissingKey)?;
        if self.validate_key(&key) {
            Ok(())
        } else {
            Err(AuthError::InvalidKey)
        }
    }
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum AuthError {
    #[error("missing API key")]
    MissingKey,
    #[error("invalid API key")]
    InvalidKey,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bearer_and_header() {
        let cfg = AuthConfig::from_keys(vec!["secret".into()]);
        assert!(cfg
            .check_headers(Some("Bearer secret"), None)
            .is_ok());
        assert!(cfg.check_headers(None, Some("secret")).is_ok());
        assert_eq!(
            cfg.check_headers(Some("Bearer wrong"), None),
            Err(AuthError::InvalidKey)
        );
    }

    #[test]
    fn disabled_when_empty() {
        let cfg = AuthConfig::default();
        assert!(cfg.check_headers(None, None).is_ok());
    }
}
