use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

use crate::error::ApplicationError;

/// Explicit operational lifecycle state of an application on a FIELDNET node.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ApplicationState {
    /// Package has been found or announced, but not yet verified.
    Discovered,

    /// Package content integrity and publisher signature have been verified.
    Verified,

    /// Package has been persisted, manifest registered, and stored on the node.
    Installed,

    /// Capability permissions have been approved by the node security system.
    Authorized,

    /// Runtime execution environment is initializing (reserved for execution build).
    Starting,

    /// Application code is actively running (reserved for execution build).
    Running,

    /// Application process has been paused / execution frozen to conserve resources.
    Suspended,

    /// Application has been terminated cleanly.
    Stopped,

    /// Application aborted unexpectedly due to panic, fuel exhaustion, or runtime failure.
    Crashed,

    /// Application has been quarantined due to security violation or severe corruption.
    Isolated,
}

impl ApplicationState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Discovered => "DISCOVERED",
            Self::Verified => "VERIFIED",
            Self::Installed => "INSTALLED",
            Self::Authorized => "AUTHORIZED",
            Self::Starting => "STARTING",
            Self::Running => "RUNNING",
            Self::Suspended => "SUSPENDED",
            Self::Stopped => "STOPPED",
            Self::Crashed => "CRASHED",
            Self::Isolated => "ISOLATED",
        }
    }

    /// Evaluates whether a direct transition from `current` to `target` is valid.
    pub fn can_transition_to(&self, target: ApplicationState) -> bool {
        if *self == target {
            return true; // Idempotent same-state transition
        }

        match self {
            Self::Discovered => matches!(target, Self::Verified | Self::Isolated),
            Self::Verified => matches!(target, Self::Installed | Self::Isolated),
            Self::Installed => matches!(target, Self::Authorized | Self::Isolated),
            Self::Authorized => {
                matches!(target, Self::Starting | Self::Installed | Self::Isolated)
            }
            Self::Starting => matches!(target, Self::Running | Self::Crashed | Self::Isolated),
            Self::Running => matches!(
                target,
                Self::Suspended | Self::Stopped | Self::Crashed | Self::Isolated
            ),
            Self::Suspended => matches!(target, Self::Running | Self::Stopped | Self::Isolated),
            Self::Stopped => matches!(
                target,
                Self::Starting | Self::Authorized | Self::Installed | Self::Isolated
            ),
            Self::Crashed => matches!(target, Self::Starting | Self::Stopped | Self::Isolated),
            Self::Isolated => matches!(target, Self::Stopped), // Administrative unquarantine
        }
    }

    /// Attempts to validate a state transition, returning an error on invalid transitions.
    pub fn transition_to(&self, target: ApplicationState) -> Result<Self, ApplicationError> {
        if !self.can_transition_to(target) {
            return Err(ApplicationError::InvalidLifecycleTransition {
                from: *self,
                to: target,
            });
        }
        Ok(target)
    }
}

impl fmt::Display for ApplicationState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for ApplicationState {
    type Err = ApplicationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_uppercase().as_str() {
            "DISCOVERED" => Ok(Self::Discovered),
            "VERIFIED" => Ok(Self::Verified),
            "INSTALLED" => Ok(Self::Installed),
            "AUTHORIZED" => Ok(Self::Authorized),
            "STARTING" => Ok(Self::Starting),
            "RUNNING" => Ok(Self::Running),
            "SUSPENDED" => Ok(Self::Suspended),
            "STOPPED" => Ok(Self::Stopped),
            "CRASHED" => Ok(Self::Crashed),
            "ISOLATED" => Ok(Self::Isolated),
            other => Err(ApplicationError::InvalidManifest(format!(
                "Unknown application state: '{}'",
                other
            ))),
        }
    }
}
