use crate::error::ServiceError;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;
use std::str::FromStr;

/// Canonical logical identifier of a service (e.g., "files.service", "fieldnet.search").
///
/// A `ServiceId` represents interface semantics and is completely decoupled from
/// physical provider nodes, IP addresses, ports, or network topologies.
///
/// # Naming Rules:
/// - Must consist of lowercase ASCII characters, digits, hyphens, and underscores.
/// - Must contain at least two dot-separated segments (e.g. `files.service` or `fieldnet.search.v1`).
/// - Total length must be between 3 and 128 characters.
/// - Each segment must be non-empty and start/end with an alphanumeric character.
/// - No consecutive dots or leading/trailing dots.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ServiceId(String);

impl ServiceId {
    /// Creates a new `ServiceId`, validating and normalizing to lowercase.
    pub fn new(raw: impl AsRef<str>) -> Result<Self, ServiceError> {
        let s = raw.as_ref().trim().to_ascii_lowercase();

        if s.len() < 3 || s.len() > 128 {
            return Err(ServiceError::InvalidServiceId(format!(
                "Length must be between 3 and 128 characters, got {}",
                s.len()
            )));
        }

        if s.starts_with('.') || s.ends_with('.') || s.contains("..") {
            return Err(ServiceError::InvalidServiceId(
                "Cannot contain leading, trailing, or consecutive dots".to_string(),
            ));
        }

        // Cannot contain colons, slashes, or whitespace (prevent IP:port or path leakage)
        if s.contains(':') || s.contains('/') || s.contains(' ') {
            return Err(ServiceError::InvalidServiceId(
                "Cannot contain colons, slashes, or whitespace".to_string(),
            ));
        }

        let segments: Vec<&str> = s.split('.').collect();
        if segments.len() < 2 {
            return Err(ServiceError::InvalidServiceId(format!(
                "Must contain at least two dot-separated segments (e.g. 'files.service'), got '{}'",
                s
            )));
        }

        for segment in segments {
            if segment.is_empty() {
                return Err(ServiceError::InvalidServiceId(
                    "Contains empty segment".to_string(),
                ));
            }

            // Check valid characters: [a-z0-9_-]
            if !segment
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
            {
                return Err(ServiceError::InvalidServiceId(format!(
                    "Segment '{}' contains invalid characters (allowed: a-z, 0-9, '-', '_')",
                    segment
                )));
            }

            // Segment must not start or end with hyphen/underscore
            if segment.starts_with('-')
                || segment.starts_with('_')
                || segment.ends_with('-')
                || segment.ends_with('_')
            {
                return Err(ServiceError::InvalidServiceId(format!(
                    "Segment '{}' must not start or end with a hyphen or underscore",
                    segment
                )));
            }
        }

        Ok(Self(s))
    }

    /// Returns the inner string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Deref for ServiceId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<str> for ServiceId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ServiceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for ServiceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ServiceId(\"{}\")", self.0)
    }
}

impl FromStr for ServiceId {
    type Err = ServiceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl TryFrom<&str> for ServiceId {
    type Error = ServiceError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

impl TryFrom<String> for ServiceId {
    type Error = ServiceError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

/// Unique identifier distinguishing a specific deployment of a service on a given provider node.
///
/// Encoded as a non-empty alphanumeric or hexadecimal string (8 to 64 characters).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ServiceInstanceId(String);

impl ServiceInstanceId {
    /// Generates a cryptographically random 16-byte (32 hex character) instance ID.
    pub fn generate() -> Self {
        let mut bytes = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut bytes);
        Self(hex::encode(bytes))
    }

    /// Validates and constructs a `ServiceInstanceId` from a string.
    pub fn new(raw: impl AsRef<str>) -> Result<Self, ServiceError> {
        let s = raw.as_ref().trim();

        if s.len() < 8 || s.len() > 64 {
            return Err(ServiceError::InvalidInstanceId(format!(
                "Length must be between 8 and 64 characters, got {}",
                s.len()
            )));
        }

        if !s
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Err(ServiceError::InvalidInstanceId(
                "Must only contain alphanumeric characters, hyphens, or underscores".to_string(),
            ));
        }

        Ok(Self(s.to_string()))
    }

    /// Returns the inner string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Deref for ServiceInstanceId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<str> for ServiceInstanceId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ServiceInstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for ServiceInstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ServiceInstanceId(\"{}\")", self.0)
    }
}

impl FromStr for ServiceInstanceId {
    type Err = ServiceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl TryFrom<&str> for ServiceInstanceId {
    type Error = ServiceError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

impl TryFrom<String> for ServiceInstanceId {
    type Error = ServiceError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}
