//! Synchronization Session State Machine for FIELDNET Phase 06.
//!
//! State transitions:
//! IDLE -> NEGOTIATING -> COMPARING -> REQUESTING -> RECEIVING -> APPLYING -> ACKNOWLEDGING -> CONVERGED
//! Interruption/failure states:
//! PARTIAL, INTERRUPTED, FAILED

use fieldnet_core::{NodeId, Timestamp};
use rand::RngCore;
use serde::{Deserialize, Serialize};

use crate::error::DataError;

/// Lifecycle state machine for an active or suspended synchronization session.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncSessionState {
    /// No synchronization currently in progress.
    Idle,
    /// Protocol version and capabilities handshake in progress (Hello).
    Negotiating,
    /// Exchanging StateSummary and computing missing delta ranges.
    Comparing,
    /// Waiting for requested DeltaResponse from remote peer.
    Requesting,
    /// Inbound DeltaResponse streaming and buffering.
    Receiving,
    /// Committing validated operations to local SQLite transaction.
    Applying,
    /// Issuing SyncAck to confirm received watermark.
    Acknowledging,
    /// All missing operations exchanged; state vectors identical.
    Converged,
    /// Partially completed session interrupted by disconnection.
    Partial,
    /// Connection dropped or timed out; eligible for resumption.
    Interrupted,
    /// Fatal error encountered (incompatible version, unauthorized).
    Failed { reason: String },
}

impl SyncSessionState {
    /// Returns human-readable state label.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Idle => "IDLE",
            Self::Negotiating => "NEGOTIATING",
            Self::Comparing => "COMPARING",
            Self::Requesting => "REQUESTING",
            Self::Receiving => "RECEIVING",
            Self::Applying => "APPLYING",
            Self::Acknowledging => "ACKNOWLEDGING",
            Self::Converged => "CONVERGED",
            Self::Partial => "PARTIAL",
            Self::Interrupted => "INTERRUPTED",
            Self::Failed { .. } => "FAILED",
        }
    }

    /// Checks if a transition from current state to target state is legally valid.
    pub fn can_transition_to(&self, target: &Self) -> bool {
        // Any non-terminal state can transition to Interrupted or Failed
        if matches!(
            target,
            Self::Interrupted | Self::Partial | Self::Failed { .. }
        ) {
            return true;
        }

        match (self, target) {
            (Self::Idle, Self::Negotiating) => true,
            (Self::Negotiating, Self::Comparing) => true,
            (Self::Comparing, Self::Requesting) => true,
            (Self::Comparing, Self::Converged) => true, // Already converged!
            (Self::Requesting, Self::Receiving) => true,
            (Self::Receiving, Self::Applying) => true,
            (Self::Applying, Self::Acknowledging) => true,
            (Self::Acknowledging, Self::Requesting) => true, // More pages remain
            (Self::Acknowledging, Self::Converged) => true,
            (Self::Interrupted | Self::Partial, Self::Comparing) => true, // Resuming from watermark
            (Self::Interrupted | Self::Partial, Self::Negotiating) => true,
            (Self::Converged, Self::Negotiating | Self::Comparing) => true, // New sync pass
            _ => false,
        }
    }
}

/// Persistent and in-memory synchronization session descriptor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncSession {
    /// Randomly generated 16-byte session identifier.
    pub session_id: [u8; 16],
    /// Remote peer's sovereign NodeId.
    pub peer_id: NodeId,
    /// Current state machine progress.
    pub state: SyncSessionState,
    /// Physical creation time.
    pub started_at: Timestamp,
    /// Total bytes transmitted during this session.
    pub bytes_sent: u64,
    /// Total bytes received during this session.
    pub bytes_received: u64,
    /// Total operations sent.
    pub ops_sent: u64,
    /// Total operations received and committed.
    pub ops_received: u64,
    /// Last physical activity timestamp.
    pub last_activity: Timestamp,
}

impl SyncSession {
    /// Creates a new synchronization session targeting a remote peer.
    pub fn new(peer_id: NodeId) -> Self {
        let mut session_id = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut session_id);
        let now = Timestamp::now();

        Self {
            session_id,
            peer_id,
            state: SyncSessionState::Idle,
            started_at: now,
            bytes_sent: 0,
            bytes_received: 0,
            ops_sent: 0,
            ops_received: 0,
            last_activity: now,
        }
    }

    /// Attempts to advance session to a target state, enforcing valid state machine transitions.
    pub fn transition_to(&mut self, next_state: SyncSessionState) -> Result<(), DataError> {
        if !self.state.can_transition_to(&next_state) {
            return Err(DataError::SyncError(format!(
                "Illegal sync session transition from {} to {}",
                self.state.as_str(),
                next_state.as_str()
            )));
        }
        self.state = next_state;
        self.last_activity = Timestamp::now();
        Ok(())
    }

    /// Records outbound traffic.
    pub fn record_sent(&mut self, bytes: usize, ops: usize) {
        self.bytes_sent = self.bytes_sent.saturating_add(bytes as u64);
        self.ops_sent = self.ops_sent.saturating_add(ops as u64);
        self.last_activity = Timestamp::now();
    }

    /// Records inbound traffic.
    pub fn record_received(&mut self, bytes: usize, ops: usize) {
        self.bytes_received = self.bytes_received.saturating_add(bytes as u64);
        self.ops_received = self.ops_received.saturating_add(ops as u64);
        self.last_activity = Timestamp::now();
    }

    /// Checks if this session is converged or in progress.
    pub fn is_active(&self) -> bool {
        matches!(
            self.state,
            SyncSessionState::Negotiating
                | SyncSessionState::Comparing
                | SyncSessionState::Requesting
                | SyncSessionState::Receiving
                | SyncSessionState::Applying
                | SyncSessionState::Acknowledging
        )
    }

    /// Checks if this session can be resumed from stored watermarks.
    pub fn is_resumable(&self) -> bool {
        matches!(
            self.state,
            SyncSessionState::Interrupted | SyncSessionState::Partial
        )
    }
}
