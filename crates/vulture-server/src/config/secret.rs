//! A string that must never reach logs.

use std::fmt;
use std::path::Path;

use serde::Deserialize;

use super::ConfigError;

/// Wrapper for secret values. `Debug` and `Display` print a placeholder; the value is available only
/// through [`Secret::expose`], which makes accidental logging visible in review.
#[derive(Clone, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct Secret(String);

impl Secret {
    /// Wraps a value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Reads a secret from a file, trimming trailing whitespace.
    pub fn from_file(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::SecretFile {
            path: path.to_path_buf(),
            source,
        })?;
        Ok(Self(text.trim().to_string()))
    }

    /// The secret itself. Do not log the result.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

impl fmt::Display for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}
