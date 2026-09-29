use crate::error::ServiceError;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Canonical semantic versioning conforming to SemVer 2.0.0 (`MAJOR.MINOR.PATCH`).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize)]
pub struct ServiceVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl ServiceVersion {
    /// Creates a new `ServiceVersion`.
    pub const fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    /// Parses a semantic version string (e.g., "1.2.3" or "v1.2.3").
    pub fn parse(s: &str) -> Result<Self, ServiceError> {
        let clean = s.trim().strip_prefix('v').unwrap_or(s.trim());
        let parts: Vec<&str> = clean.split('.').collect();

        if parts.len() != 3 {
            return Err(ServiceError::InvalidVersion(format!(
                "Expected 3 components (MAJOR.MINOR.PATCH), got '{}'",
                s
            )));
        }

        let major = parts[0].parse::<u32>().map_err(|_| {
            ServiceError::InvalidVersion(format!("Invalid major version '{}'", parts[0]))
        })?;

        let minor = parts[1].parse::<u32>().map_err(|_| {
            ServiceError::InvalidVersion(format!("Invalid minor version '{}'", parts[1]))
        })?;

        let patch = parts[2].parse::<u32>().map_err(|_| {
            ServiceError::InvalidVersion(format!("Invalid patch version '{}'", parts[2]))
        })?;

        Ok(Self {
            major,
            minor,
            patch,
        })
    }

    /// Evaluates standard SemVer caret compatibility (`^required`).
    ///
    /// - For `major > 0`: versions are compatible if `self.major == required.major` and
    ///   `self` is at least equal to `required`.
    /// - For `major == 0`: breaking changes can happen on minor versions, so
    ///   `self.minor == required.minor` and `self.patch >= required.patch`.
    pub fn is_compatible_with(&self, required: &ServiceVersion) -> bool {
        if required.major > 0 {
            self.major == required.major
                && (self.minor > required.minor
                    || (self.minor == required.minor && self.patch >= required.patch))
        } else if required.minor > 0 {
            self.major == 0 && self.minor == required.minor && self.patch >= required.patch
        } else {
            // 0.0.x is only compatible with exact patch match
            self.major == 0 && self.minor == 0 && self.patch == required.patch
        }
    }
}

impl fmt::Display for ServiceVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl fmt::Debug for ServiceVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ServiceVersion({}.{}.{})",
            self.major, self.minor, self.patch
        )
    }
}

impl FromStr for ServiceVersion {
    type Err = ServiceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl TryFrom<&str> for ServiceVersion {
    type Error = ServiceError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::parse(s)
    }
}
