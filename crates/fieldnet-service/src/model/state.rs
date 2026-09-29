use crate::error::ServiceError;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Authoritative lifecycle state of a service instance.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub enum ServiceState {
    /// Manifest received from mesh wire or discovery, awaiting cryptographic authentication.
    Discovered,
    /// Ed25519 signature verified and provider identity confirmed.
    Authenticated,
    /// Verified, active, and reachable for client invocation.
    Available,
    /// Reachable but experiencing transient latency spikes, degraded metrics, or missed heartbeats.
    Degraded,
    /// Hosting provider node disconnected, route lost, or explicitly halted.
    Unavailable,
    /// Advertisement TTL exceeded without monotonic refresh.
    Expired,
    /// Explicitly unregistered or garbage-collected from the registry.
    Removed,
}

impl ServiceState {
    /// Checks if a transition from `self` to `target` is valid according to the Phase 08 state machine.
    pub fn can_transition_to(&self, target: ServiceState) -> bool {
        if *self == target {
            return true; // No-op transition
        }

        match self {
            Self::Discovered => matches!(target, Self::Authenticated | Self::Removed),
            Self::Authenticated => {
                matches!(target, Self::Available | Self::Unavailable | Self::Removed)
            }
            Self::Available => matches!(
                target,
                Self::Degraded | Self::Unavailable | Self::Expired | Self::Removed
            ),
            Self::Degraded => matches!(
                target,
                Self::Available | Self::Unavailable | Self::Expired | Self::Removed
            ),
            Self::Unavailable => {
                matches!(target, Self::Available | Self::Expired | Self::Removed)
            }
            Self::Expired => matches!(target, Self::Removed | Self::Available),
            Self::Removed => false, // Terminal state; no transitions allowed
        }
    }

    /// Enforces a state transition, returning an error if the transition is illegal.
    pub fn transition_to(&mut self, target: ServiceState) -> Result<(), ServiceError> {
        if self.can_transition_to(target) {
            *self = target;
            Ok(())
        } else {
            Err(ServiceError::InvalidStateTransition {
                from: self.to_string(),
                to: target.to_string(),
            })
        }
    }

    /// Whether this state is callable by clients (Available or Degraded).
    pub fn is_callable(&self) -> bool {
        matches!(self, Self::Available | Self::Degraded)
    }

    /// Whether this state represents a terminal dead state.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Removed)
    }
}

impl fmt::Display for ServiceState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Discovered => write!(f, "Discovered"),
            Self::Authenticated => write!(f, "Authenticated"),
            Self::Available => write!(f, "Available"),
            Self::Degraded => write!(f, "Degraded"),
            Self::Unavailable => write!(f, "Unavailable"),
            Self::Expired => write!(f, "Expired"),
            Self::Removed => write!(f, "Removed"),
        }
    }
}
