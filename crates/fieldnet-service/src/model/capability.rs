use crate::error::ServiceError;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;
use std::str::FromStr;

/// Canonical service capability identifier (e.g., "files.read", "search.query", "ai.inference").
///
/// Capabilities represent fine-grained functional features advertised by an instance
/// and evaluated during service resolution and authorization.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ServiceCapability(String);

impl ServiceCapability {
    /// Creates and validates a new `ServiceCapability`.
    pub fn new(raw: impl AsRef<str>) -> Result<Self, ServiceError> {
        let s = raw.as_ref().trim().to_ascii_lowercase();

        if s.is_empty() || s.len() > 64 {
            return Err(ServiceError::InvalidCapability(format!(
                "Capability identifier length must be between 1 and 64 characters, got {}",
                s.len()
            )));
        }

        if !s.chars().all(|c| {
            c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '_' || c == '-'
        }) {
            return Err(ServiceError::InvalidCapability(format!(
                "Capability '{}' contains invalid characters (allowed: a-z, 0-9, '.', '_', '-')",
                s
            )));
        }

        Ok(Self(s))
    }

    /// Returns the capability string as a slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Deref for ServiceCapability {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<str> for ServiceCapability {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ServiceCapability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for ServiceCapability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ServiceCapability(\"{}\")", self.0)
    }
}

impl FromStr for ServiceCapability {
    type Err = ServiceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl TryFrom<&str> for ServiceCapability {
    type Error = ServiceError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

impl TryFrom<String> for ServiceCapability {
    type Error = ServiceError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

/// Declared access control requirements and authorization prerequisites.
///
/// Manifest permissions declare what the service demands; Phase 03 authorization
/// evaluates whether a caller's credentials or capability tokens satisfy them.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ServicePermissions {
    /// Whether any peer in the mesh can discover and call this service without special roles.
    pub is_public: bool,
    /// Specific sovereign authorization roles required to invoke this service.
    pub required_roles: Vec<String>,
    /// Whether callers must present an explicit cryptographically signed capability token.
    pub requires_capability_token: bool,
}

impl Default for ServicePermissions {
    fn default() -> Self {
        Self {
            is_public: true,
            required_roles: Vec::new(),
            requires_capability_token: false,
        }
    }
}

impl ServicePermissions {
    /// Creates default public permissions.
    pub fn public() -> Self {
        Self::default()
    }

    /// Creates permissions requiring an explicit signed capability token.
    pub fn requires_token() -> Self {
        Self {
            is_public: false,
            required_roles: Vec::new(),
            requires_capability_token: true,
        }
    }

    /// Creates permissions requiring specific roles.
    pub fn with_roles(roles: Vec<String>) -> Self {
        Self {
            is_public: roles.is_empty(),
            required_roles: roles,
            requires_capability_token: false,
        }
    }
}
