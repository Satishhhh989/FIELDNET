use serde::{Deserialize, Serialize};
use std::fmt;

use crate::error::ContentError;

/// Authoritative state machine for distributed content transfer sessions (Phase 07 Build 04).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TransferState {
    /// Provider has been resolved or candidate discovered.
    Discovered,
    /// Content request dispatched; awaiting provider negotiation/manifest.
    Requested,
    /// Transfer parameters and manifest are being negotiated/validated.
    Negotiating,
    /// Chunks are actively being requested, streamed, and verified.
    Transferring,
    /// All constituent chunks verified; final Merkle root and ContentId hashing in progress.
    Verifying,
    /// Content fully verified, committed to Content-Addressed Storage, and ready for use.
    Complete,
    /// Transfer temporarily paused (network disconnection, route loss, or user pause).
    Paused,
    /// Resuming from a paused, cancelled, or interrupted state; missing chunks recalculated.
    Resuming,
    /// Transfer permanently failed (e.g. max retries exceeded, provider unreachable).
    Failed,
    /// Transfer explicitly cancelled by caller.
    Cancelled,
    /// Transfer expired due to inactivity or session deadline exceeded.
    Expired,
    /// Final ContentId hash mismatch or corrupt chunk unresolvable.
    Corrupted,
}

impl TransferState {
    /// Returns canonical uppercase string representation for persistence and logging.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Discovered => "DISCOVERED",
            Self::Requested => "REQUESTED",
            Self::Negotiating => "NEGOTIATING",
            Self::Transferring => "TRANSFERRING",
            Self::Verifying => "VERIFYING",
            Self::Complete => "COMPLETE",
            Self::Paused => "PAUSED",
            Self::Resuming => "RESUMING",
            Self::Failed => "FAILED",
            Self::Cancelled => "CANCELLED",
            Self::Expired => "EXPIRED",
            Self::Corrupted => "CORRUPTED",
        }
    }

    /// Parses string representation back into `TransferState`.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_uppercase().as_str() {
            "DISCOVERED" => Some(Self::Discovered),
            "REQUESTED" => Some(Self::Requested),
            "NEGOTIATING" => Some(Self::Negotiating),
            "TRANSFERRING" => Some(Self::Transferring),
            "VERIFYING" => Some(Self::Verifying),
            "COMPLETE" => Some(Self::Complete),
            "PAUSED" => Some(Self::Paused),
            "RESUMING" => Some(Self::Resuming),
            "FAILED" => Some(Self::Failed),
            "CANCELLED" => Some(Self::Cancelled),
            "EXPIRED" => Some(Self::Expired),
            "CORRUPTED" => Some(Self::Corrupted),
            _ => None,
        }
    }

    /// Returns true if this state is terminal (cannot transition further unless resumed).
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Complete | Self::Failed | Self::Cancelled | Self::Expired | Self::Corrupted
        )
    }

    /// Returns true if this state is actively performing network or cryptographic work.
    pub fn is_active(&self) -> bool {
        matches!(
            self,
            Self::Requested
                | Self::Negotiating
                | Self::Transferring
                | Self::Verifying
                | Self::Resuming
        )
    }

    /// Validates whether transitioning from `self` to `target` is legally permitted.
    pub fn can_transition_to(&self, target: &Self) -> bool {
        if self == target {
            return true; // Idempotent transitions are safe
        }

        match self {
            Self::Discovered => matches!(target, Self::Requested | Self::Cancelled | Self::Expired),
            Self::Requested => matches!(
                target,
                Self::Negotiating
                    | Self::Paused
                    | Self::Failed
                    | Self::Cancelled
                    | Self::Expired
                    | Self::Corrupted
            ),
            Self::Negotiating => matches!(
                target,
                Self::Transferring
                    | Self::Paused
                    | Self::Failed
                    | Self::Cancelled
                    | Self::Expired
                    | Self::Corrupted
            ),
            Self::Transferring => matches!(
                target,
                Self::Verifying
                    | Self::Paused
                    | Self::Resuming
                    | Self::Failed
                    | Self::Cancelled
                    | Self::Expired
                    | Self::Corrupted
            ),
            Self::Verifying => matches!(target, Self::Complete | Self::Corrupted | Self::Failed),
            Self::Paused => matches!(target, Self::Resuming | Self::Cancelled | Self::Expired),
            Self::Resuming => matches!(
                target,
                Self::Transferring | Self::Paused | Self::Failed | Self::Cancelled | Self::Expired
            ),
            Self::Cancelled | Self::Failed => matches!(target, Self::Resuming),
            Self::Complete | Self::Expired | Self::Corrupted => false,
        }
    }

    /// Enforces the transition, returning `Err(ContentError::InvalidState)` if illegal.
    pub fn validate_transition(&self, target: Self) -> Result<(), ContentError> {
        if self.can_transition_to(&target) {
            Ok(())
        } else {
            Err(ContentError::InvalidState {
                expected: format!("Legal transition from {}", self.as_str()),
                actual: target.as_str().to_string(),
            })
        }
    }
}

impl fmt::Display for TransferState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_forward_transitions() {
        let state = TransferState::Discovered;
        assert!(state.can_transition_to(&TransferState::Requested));

        let state = TransferState::Requested;
        assert!(state.can_transition_to(&TransferState::Negotiating));

        let state = TransferState::Negotiating;
        assert!(state.can_transition_to(&TransferState::Transferring));

        let state = TransferState::Transferring;
        assert!(state.can_transition_to(&TransferState::Verifying));

        let state = TransferState::Verifying;
        assert!(state.can_transition_to(&TransferState::Complete));
    }

    #[test]
    fn test_interruption_and_resume_transitions() {
        let state = TransferState::Transferring;
        assert!(state.can_transition_to(&TransferState::Paused));

        let state = TransferState::Paused;
        assert!(state.can_transition_to(&TransferState::Resuming));

        let state = TransferState::Resuming;
        assert!(state.can_transition_to(&TransferState::Transferring));
    }

    #[test]
    fn test_invalid_transitions() {
        let state = TransferState::Discovered;
        assert!(!state.can_transition_to(&TransferState::Complete));
        assert!(!state.can_transition_to(&TransferState::Verifying));

        let state = TransferState::Complete;
        assert!(!state.can_transition_to(&TransferState::Transferring));
        assert!(!state.can_transition_to(&TransferState::Requested));
    }
}
