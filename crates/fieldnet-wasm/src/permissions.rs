use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;

use crate::error::ApplicationError;

/// Declared capability permissions that an application requests in its manifest.
///
/// Note: Permissions in the manifest are DECLARATIONS ONLY. They do not confer actual
/// access until explicitly granted and signed by the node's capability authorization system.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum AppPermission {
    /// Read access to local node key-value/document storage ("storage.read").
    StorageRead,
    /// Write access to local node storage ("storage.write").
    StorageWrite,
    /// Outbound network message transmission to mesh peers ("network.send").
    NetworkSend,
    /// Service catalog discovery and query resolution ("service.discover").
    ServiceDiscover,
    /// Invocations against remote mesh services ("service.invoke").
    ServiceInvoke,
    /// Subscription to runtime event bus notifications ("events.subscribe").
    EventsSubscribe,
    /// Publication of application event bus notifications ("events.publish").
    EventsPublish,
    /// Submission of computation tasks to distributed compute nodes ("compute.submit").
    ComputeSubmit,
    /// Retrieval of objects from Content-Addressed Storage ("content.read").
    ContentRead,
    /// Publication of new objects into Content-Addressed Storage ("content.write").
    ContentWrite,
    /// Custom or non-standard capability declaration. Must be validated explicitly.
    Custom(String),
}

impl AppPermission {
    /// Canonical string representation of the permission.
    pub fn as_str(&self) -> &str {
        match self {
            Self::StorageRead => "storage.read",
            Self::StorageWrite => "storage.write",
            Self::NetworkSend => "network.send",
            Self::ServiceDiscover => "service.discover",
            Self::ServiceInvoke => "service.invoke",
            Self::EventsSubscribe => "events.subscribe",
            Self::EventsPublish => "events.publish",
            Self::ComputeSubmit => "compute.submit",
            Self::ContentRead => "content.read",
            Self::ContentWrite => "content.write",
            Self::Custom(s) => s.as_str(),
        }
    }

    /// Returns true if this is a known standard permission recognized by FIELDNET.
    pub fn is_standard(&self) -> bool {
        !matches!(self, Self::Custom(_))
    }

    /// Validates the permission identifier format.
    ///
    /// Unknown permissions will return an error during strict validation unless explicitly allowed.
    pub fn validate_strict(&self) -> Result<(), ApplicationError> {
        match self {
            Self::Custom(s) => Err(ApplicationError::PermissionError(format!(
                "Unknown or non-standard permission '{}' is not supported in strict mode",
                s
            ))),
            _ => Ok(()),
        }
    }

    /// Validates syntactical format of custom permissions (e.g. `domain.action`).
    pub fn validate_syntax(&self) -> Result<(), ApplicationError> {
        let s = self.as_str();
        if s.len() < 3 || s.len() > 64 {
            return Err(ApplicationError::PermissionError(format!(
                "Permission length must be between 3 and 64 characters, got {}",
                s.len()
            )));
        }

        if !s.chars().all(|c| {
            c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '_' || c == '-'
        }) {
            return Err(ApplicationError::PermissionError(format!(
                "Permission '{}' contains invalid characters (allowed: a-z, 0-9, '.', '_', '-')",
                s
            )));
        }

        if !s.contains('.') {
            return Err(ApplicationError::PermissionError(format!(
                "Permission '{}' must follow namespace.action format",
                s
            )));
        }

        Ok(())
    }
}

impl fmt::Display for AppPermission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for AppPermission {
    type Err = ApplicationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let clean = s.trim().to_ascii_lowercase();
        let perm = match clean.as_str() {
            "storage.read" => Self::StorageRead,
            "storage.write" => Self::StorageWrite,
            "network.send" => Self::NetworkSend,
            "service.discover" => Self::ServiceDiscover,
            "service.invoke" => Self::ServiceInvoke,
            "events.subscribe" => Self::EventsSubscribe,
            "events.publish" => Self::EventsPublish,
            "compute.submit" => Self::ComputeSubmit,
            "content.read" => Self::ContentRead,
            "content.write" => Self::ContentWrite,
            other => Self::Custom(other.to_string()),
        };
        perm.validate_syntax()?;
        Ok(perm)
    }
}

impl Serialize for AppPermission {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for AppPermission {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        AppPermission::from_str(&s).map_err(serde::de::Error::custom)
    }
}
