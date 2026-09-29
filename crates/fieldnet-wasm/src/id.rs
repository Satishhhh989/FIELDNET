use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::ops::Deref;
use std::str::FromStr;

use crate::error::ApplicationError;

/// Canonical, strongly typed logical identifier of an application (e.g. "fieldnet.chat", "com.example.sensor").
///
/// An `ApplicationId` represents sovereign application identity decoupled from:
/// - physical device or node identity (`NodeId`)
/// - runtime service instance identity (`ServiceInstanceId`)
/// - filesystem installation paths
///
/// # Naming Invariants:
/// - Must consist of lowercase ASCII characters, digits, hyphens, underscores, and dots.
/// - Must contain at least two dot-separated segments (e.g. `fieldnet.chat`).
/// - Total length must be between 3 and 128 characters.
/// - Each segment must be non-empty and start/end with an alphanumeric character.
/// - No consecutive dots or leading/trailing dots.
/// - Cannot contain colons, slashes, or whitespace.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ApplicationId(String);

impl ApplicationId {
    /// Creates and validates a new `ApplicationId`.
    pub fn new(raw: impl AsRef<str>) -> Result<Self, ApplicationError> {
        let s = raw.as_ref().trim().to_ascii_lowercase();

        if s.len() < 3 || s.len() > 128 {
            return Err(ApplicationError::InvalidApplicationId(format!(
                "Length must be between 3 and 128 characters, got {}",
                s.len()
            )));
        }

        if s.starts_with('.') || s.ends_with('.') || s.contains("..") {
            return Err(ApplicationError::InvalidApplicationId(
                "Cannot contain leading, trailing, or consecutive dots".to_string(),
            ));
        }

        if s.contains(':') || s.contains('/') || s.contains('\\') || s.contains(' ') {
            return Err(ApplicationError::InvalidApplicationId(
                "Cannot contain colons, slashes, backslashes, or whitespace".to_string(),
            ));
        }

        let segments: Vec<&str> = s.split('.').collect();
        if segments.len() < 2 {
            return Err(ApplicationError::InvalidApplicationId(format!(
                "Must contain at least two dot-separated segments (e.g. 'fieldnet.chat'), got '{}'",
                s
            )));
        }

        for segment in segments {
            if segment.is_empty() {
                return Err(ApplicationError::InvalidApplicationId(
                    "Contains empty segment".to_string(),
                ));
            }

            if !segment
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
            {
                return Err(ApplicationError::InvalidApplicationId(format!(
                    "Segment '{}' contains invalid characters (allowed: a-z, 0-9, '-', '_')",
                    segment
                )));
            }

            if segment.starts_with('-')
                || segment.starts_with('_')
                || segment.ends_with('-')
                || segment.ends_with('_')
            {
                return Err(ApplicationError::InvalidApplicationId(format!(
                    "Segment '{}' must not start or end with a hyphen or underscore",
                    segment
                )));
            }

            if segment.contains("--")
                || segment.contains("__")
                || segment.contains("-_")
                || segment.contains("_-")
            {
                return Err(ApplicationError::InvalidApplicationId(format!(
                    "Segment '{}' cannot contain consecutive hyphens or underscores",
                    segment
                )));
            }
        }

        Ok(Self(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Deref for ApplicationId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<str> for ApplicationId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ApplicationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for ApplicationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ApplicationId(\"{}\")", self.0)
    }
}

impl FromStr for ApplicationId {
    type Err = ApplicationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

/// Strongly typed Package Identifier (e.g. "pkg:fieldnet.chat:1.0.0:a1b2c3d4").
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PackageId(String);

impl PackageId {
    pub fn new(raw: impl AsRef<str>) -> Result<Self, ApplicationError> {
        let s = raw.as_ref().trim().to_string();
        if s.len() < 3 || s.len() > 128 {
            return Err(ApplicationError::InvalidPackageId(format!(
                "PackageId length must be between 3 and 128 characters, got {}",
                s.len()
            )));
        }

        if !s
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_' || c == ':')
        {
            return Err(ApplicationError::InvalidPackageId(format!(
                "PackageId contains invalid characters: '{}'",
                s
            )));
        }

        Ok(Self(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PackageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for PackageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PackageId(\"{}\")", self.0)
    }
}

impl FromStr for PackageId {
    type Err = ApplicationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

/// Publisher identity derived from a sovereign Ed25519 verifying key.
///
/// Distinct from NodeId (which identifies a network peer) and ServiceId (which identifies a service).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PublisherIdentity([u8; 32]);

impl PublisherIdentity {
    pub const fn from_public_key(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn public_key_bytes(&self) -> [u8; 32] {
        self.0
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    pub fn from_hex(s: &str) -> Result<Self, ApplicationError> {
        let clean = s.trim();
        let stripped = clean
            .strip_prefix("pub:")
            .or_else(|| clean.strip_prefix("ed25519:"))
            .unwrap_or(clean);

        if stripped.len() != 64 {
            return Err(ApplicationError::InvalidPublisher(format!(
                "Invalid publisher public key hex length: expected 64, got {}",
                stripped.len()
            )));
        }

        let mut bytes = [0u8; 32];
        hex::decode_to_slice(stripped, &mut bytes).map_err(|e| {
            ApplicationError::InvalidPublisher(format!("Publisher hex decode error: {}", e))
        })?;

        Ok(Self(bytes))
    }
}

impl fmt::Display for PublisherIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pub:{}", self.to_hex())
    }
}

impl fmt::Debug for PublisherIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PublisherIdentity(\"pub:{}\")", self.to_hex())
    }
}

impl FromStr for PublisherIdentity {
    type Err = ApplicationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_hex(s)
    }
}

impl Serialize for PublisherIdentity {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for PublisherIdentity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        PublisherIdentity::from_str(&s).map_err(serde::de::Error::custom)
    }
}

/// Semantic Version for applications (e.g. "1.2.3" or "1.2.3-beta.1").
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct AppVersion {
    pub major: u64,
    pub minor: u64,
    pub patch: u64,
    pub pre_release: Option<String>,
}

impl AppVersion {
    pub const fn new(major: u64, minor: u64, patch: u64) -> Self {
        Self {
            major,
            minor,
            patch,
            pre_release: None,
        }
    }

    pub fn with_prerelease(major: u64, minor: u64, patch: u64, pre: impl Into<String>) -> Self {
        Self {
            major,
            minor,
            patch,
            pre_release: Some(pre.into()),
        }
    }
}

impl fmt::Display for AppVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.pre_release {
            Some(pre) => write!(f, "{}.{}.{}-{}", self.major, self.minor, self.patch, pre),
            None => write!(f, "{}.{}.{}", self.major, self.minor, self.patch),
        }
    }
}

impl fmt::Debug for AppVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AppVersion({})", self)
    }
}

impl FromStr for AppVersion {
    type Err = ApplicationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let clean = s.trim();
        let (core, pre) = match clean.split_once('-') {
            Some((c, p)) => (c, Some(p.to_string())),
            None => (clean, None),
        };

        let parts: Vec<&str> = core.split('.').collect();
        let (major, minor, patch) = match parts.len() {
            1 => {
                let major = parts[0].parse::<u64>().map_err(|e| {
                    ApplicationError::InvalidVersion(format!("Invalid major version: {}", e))
                })?;
                (major, 0, 0)
            }
            2 => {
                let major = parts[0].parse::<u64>().map_err(|e| {
                    ApplicationError::InvalidVersion(format!("Invalid major version: {}", e))
                })?;
                let minor = parts[1].parse::<u64>().map_err(|e| {
                    ApplicationError::InvalidVersion(format!("Invalid minor version: {}", e))
                })?;
                (major, minor, 0)
            }
            3 => {
                let major = parts[0].parse::<u64>().map_err(|e| {
                    ApplicationError::InvalidVersion(format!("Invalid major version: {}", e))
                })?;
                let minor = parts[1].parse::<u64>().map_err(|e| {
                    ApplicationError::InvalidVersion(format!("Invalid minor version: {}", e))
                })?;
                let patch = parts[2].parse::<u64>().map_err(|e| {
                    ApplicationError::InvalidVersion(format!("Invalid patch version: {}", e))
                })?;
                (major, minor, patch)
            }
            _ => {
                return Err(ApplicationError::InvalidVersion(format!(
                    "Version must contain 1 to 3 numeric parts, got '{}'",
                    s
                )));
            }
        };

        Ok(Self {
            major,
            minor,
            patch,
            pre_release: pre,
        })
    }
}

impl PartialOrd for AppVersion {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for AppVersion {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.major.cmp(&other.major) {
            std::cmp::Ordering::Equal => match self.minor.cmp(&other.minor) {
                std::cmp::Ordering::Equal => match self.patch.cmp(&other.patch) {
                    std::cmp::Ordering::Equal => match (&self.pre_release, &other.pre_release) {
                        (None, None) => std::cmp::Ordering::Equal,
                        (Some(_), None) => std::cmp::Ordering::Less, // 1.0.0-alpha < 1.0.0
                        (None, Some(_)) => std::cmp::Ordering::Greater,
                        (Some(a), Some(b)) => a.cmp(b),
                    },
                    ord => ord,
                },
                ord => ord,
            },
            ord => ord,
        }
    }
}

impl Serialize for AppVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for AppVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        AppVersion::from_str(&s).map_err(serde::de::Error::custom)
    }
}

/// Version requirement matcher for dependencies.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum AppVersionReq {
    /// Exact match: `=1.0.0` or `1.0.0`
    Exact(AppVersion),
    /// Minimum version: `>=1.0.0`
    AtLeast(AppVersion),
    /// Semver compatible (same major version, >= minor.patch): `^1.0.0`
    Compatible(AppVersion),
    /// Tilde requirement (same major & minor version, >= patch): `~2.0`
    Tilde(AppVersion),
    /// Any version: `*`
    Any,
}

impl AppVersionReq {
    pub fn matches(&self, ver: &AppVersion) -> bool {
        match self {
            Self::Any => true,
            Self::Exact(expected) => ver == expected,
            Self::AtLeast(min) => ver >= min,
            Self::Compatible(target) => {
                if target.major > 0 {
                    ver.major == target.major
                        && (ver.minor > target.minor
                            || (ver.minor == target.minor && ver.patch >= target.patch))
                } else if target.minor > 0 {
                    ver.major == 0
                        && ver.minor == target.minor
                        && ver.patch >= target.patch
                } else {
                    ver.major == 0 && ver.minor == 0 && ver.patch == target.patch
                }
            }
            Self::Tilde(target) => {
                ver.major == target.major
                    && ver.minor == target.minor
                    && ver.patch >= target.patch
            }
        }
    }
}

impl FromStr for AppVersionReq {
    type Err = ApplicationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let clean = s.trim();
        if clean == "*" || clean.is_empty() {
            return Ok(Self::Any);
        }

        if let Some(stripped) = clean.strip_prefix(">=") {
            let ver = AppVersion::from_str(stripped.trim())?;
            return Ok(Self::AtLeast(ver));
        }

        if let Some(stripped) = clean.strip_prefix('^') {
            let ver = AppVersion::from_str(stripped.trim())?;
            return Ok(Self::Compatible(ver));
        }

        if let Some(stripped) = clean.strip_prefix('~') {
            let ver = AppVersion::from_str(stripped.trim())?;
            return Ok(Self::Tilde(ver));
        }

        if let Some(stripped) = clean.strip_prefix('=') {
            let ver = AppVersion::from_str(stripped.trim())?;
            return Ok(Self::Exact(ver));
        }

        let ver = AppVersion::from_str(clean)?;
        Ok(Self::Exact(ver))
    }
}

impl fmt::Display for AppVersionReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Any => write!(f, "*"),
            Self::Exact(v) => write!(f, "={}", v),
            Self::AtLeast(v) => write!(f, ">={}", v),
            Self::Compatible(v) => write!(f, "^{}", v),
            Self::Tilde(v) => write!(f, "~{}", v),
        }
    }
}

/// Unique, strongly typed identifier for an active execution instance of an application.
///
/// Distinct from:
/// - `ApplicationId` (sovereign application identity)
/// - `PackageId` (immutable package artifact)
/// - `NodeId` (network node)
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ExecutionId(String);

impl ExecutionId {
    pub fn new(raw: impl AsRef<str>) -> Result<Self, ApplicationError> {
        let s = raw.as_ref().trim();
        if s.is_empty() || s.len() > 128 {
            return Err(ApplicationError::InvalidExecutionId(format!(
                "ExecutionId length must be between 1 and 128 characters, got {}",
                s.len()
            )));
        }

        if !s
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
        {
            return Err(ApplicationError::InvalidExecutionId(format!(
                "ExecutionId contains invalid characters: '{}'",
                s
            )));
        }

        Ok(Self(s.to_string()))
    }

    pub fn generate() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let random_suffix: u64 = rand::random();
        Self(format!("exec-{}-{:08x}", now, random_suffix))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Deref for ExecutionId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<str> for ExecutionId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ExecutionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for ExecutionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ExecutionId(\"{}\")", self.0)
    }
}

impl FromStr for ExecutionId {
    type Err = ApplicationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}
