use fieldnet_content::identity::ContentId;
use fieldnet_core::{NodeId, Timestamp};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::error::ApplicationError;
use crate::id::{AppVersion, ApplicationId};

/// Explicit state machine states for package transfer sessions.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferStatus {
    /// Initial request has been issued or received.
    Requested,
    /// Request accepted by provider, transfer metadata acknowledged.
    Accepted,
    /// Active chunk transmission in progress.
    Transferring,
    /// Transfer paused by administrative action or congestion control.
    Paused,
    /// Transfer interrupted due to network disconnect or node restart.
    Interrupted,
    /// All chunks received, assembling bundle.
    Completing,
    /// Assembled package undergoing integrity, hash, and signature verification.
    Verifying,
    /// Transfer and verification successfully completed.
    Completed,
    /// Transfer failed irreversibly.
    Failed,
    /// Transfer explicitly cancelled by requester.
    Cancelled,
}

impl TransferStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::Accepted => "accepted",
            Self::Transferring => "transferring",
            Self::Paused => "paused",
            Self::Interrupted => "interrupted",
            Self::Completing => "completing",
            Self::Verifying => "verifying",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    pub fn can_resume(&self) -> bool {
        matches!(self, Self::Paused | Self::Interrupted | Self::Transferring)
    }
}

/// Tracks the full progress, chunk bitmap, and metadata of a package transfer session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PackageTransferSession {
    /// Unique session / transfer identifier.
    pub transfer_id: String,

    /// Authoritative content hash of the package being transferred.
    pub package_hash: ContentId,

    /// Application identifier.
    pub application_id: ApplicationId,

    /// Application semantic version.
    pub version: AppVersion,

    /// Source peer providing package chunks.
    pub source_node: NodeId,

    /// Destination peer receiving package chunks.
    pub destination_node: NodeId,

    /// Total number of chunks in this package transfer.
    pub total_chunks: u32,

    /// Standard size of each chunk in bytes.
    pub chunk_size: u32,

    /// Total size of the complete package bundle in bytes.
    pub package_size: u64,

    /// Set of chunk indices whose bytes have been received and verified.
    pub received_chunks: HashSet<u32>,

    /// Ordered expected cryptographic hashes for each chunk index.
    pub chunk_hashes: Vec<ContentId>,

    /// Current state machine status.
    pub status: TransferStatus,

    /// Timestamp when session was initiated.
    pub created_at: Timestamp,

    /// Timestamp of most recent progress or state change.
    pub updated_at: Timestamp,

    /// Timestamp after which session expires and is abandoned.
    pub expires_at: Timestamp,

    /// Failure reason description if session failed.
    pub failure_reason: Option<String>,
}

impl PackageTransferSession {
    /// Constructs a new inbound transfer session in `Requested` state.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        transfer_id: String,
        package_hash: ContentId,
        application_id: ApplicationId,
        version: AppVersion,
        source_node: NodeId,
        destination_node: NodeId,
        ttl_ms: u64,
        now: Timestamp,
    ) -> Self {
        Self {
            transfer_id,
            package_hash,
            application_id,
            version,
            source_node,
            destination_node,
            total_chunks: 0,
            chunk_size: 0,
            package_size: 0,
            received_chunks: HashSet::new(),
            chunk_hashes: Vec::new(),
            status: TransferStatus::Requested,
            created_at: now,
            updated_at: now,
            expires_at: Timestamp(now.0 + ttl_ms),
            failure_reason: None,
        }
    }

    /// Evaluates whether all expected chunks have been received and verified.
    pub fn is_complete(&self) -> bool {
        self.total_chunks > 0 && self.received_chunks.len() == self.total_chunks as usize
    }

    /// Computes percentage of chunks received (0.0 to 100.0).
    pub fn progress_percentage(&self) -> f64 {
        if self.total_chunks == 0 {
            0.0
        } else {
            (self.received_chunks.len() as f64 / self.total_chunks as f64) * 100.0
        }
    }

    /// Returns a list of chunk indices that have not yet been received.
    pub fn missing_chunks(&self) -> Vec<u32> {
        (0..self.total_chunks)
            .filter(|idx| !self.received_chunks.contains(idx))
            .collect()
    }

    /// Records receipt and verification of a chunk index.
    pub fn record_chunk(&mut self, chunk_index: u32, now: Timestamp) {
        self.received_chunks.insert(chunk_index);
        self.updated_at = now;
        if self.status == TransferStatus::Accepted || self.status == TransferStatus::Interrupted {
            self.status = TransferStatus::Transferring;
        }
    }

    /// Safely transitions the session state according to the transfer state machine.
    pub fn transition(
        &mut self,
        new_status: TransferStatus,
        now: Timestamp,
        reason: Option<String>,
    ) -> Result<(), ApplicationError> {
        // Enforce valid transitions
        match (self.status, new_status) {
            // Self transitions are idempotent
            (curr, next) if curr == next => Ok(()),

            // From Requested
            (TransferStatus::Requested, TransferStatus::Accepted)
            | (TransferStatus::Requested, TransferStatus::Transferring)
            | (TransferStatus::Requested, TransferStatus::Failed)
            | (TransferStatus::Requested, TransferStatus::Cancelled) => {
                self.status = new_status;
                self.updated_at = now;
                if let Some(r) = reason {
                    self.failure_reason = Some(r);
                }
                Ok(())
            }

            // From Accepted / Transferring
            (TransferStatus::Accepted, TransferStatus::Transferring)
            | (TransferStatus::Accepted, TransferStatus::Failed)
            | (TransferStatus::Accepted, TransferStatus::Cancelled)
            | (TransferStatus::Transferring, TransferStatus::Paused)
            | (TransferStatus::Transferring, TransferStatus::Interrupted)
            | (TransferStatus::Transferring, TransferStatus::Completing)
            | (TransferStatus::Transferring, TransferStatus::Failed)
            | (TransferStatus::Transferring, TransferStatus::Cancelled) => {
                self.status = new_status;
                self.updated_at = now;
                if let Some(r) = reason {
                    self.failure_reason = Some(r);
                }
                Ok(())
            }

            // From Paused / Interrupted
            (TransferStatus::Paused, TransferStatus::Transferring)
            | (TransferStatus::Paused, TransferStatus::Cancelled)
            | (TransferStatus::Paused, TransferStatus::Failed)
            | (TransferStatus::Interrupted, TransferStatus::Transferring)
            | (TransferStatus::Interrupted, TransferStatus::Cancelled)
            | (TransferStatus::Interrupted, TransferStatus::Failed) => {
                self.status = new_status;
                self.updated_at = now;
                if let Some(r) = reason {
                    self.failure_reason = Some(r);
                }
                Ok(())
            }

            // From Completing / Verifying
            (TransferStatus::Completing, TransferStatus::Verifying)
            | (TransferStatus::Completing, TransferStatus::Failed)
            | (TransferStatus::Verifying, TransferStatus::Completed)
            | (TransferStatus::Verifying, TransferStatus::Failed) => {
                self.status = new_status;
                self.updated_at = now;
                if let Some(r) = reason {
                    self.failure_reason = Some(r);
                }
                Ok(())
            }

            // Terminal states cannot transition
            (TransferStatus::Completed, _)
            | (TransferStatus::Failed, _)
            | (TransferStatus::Cancelled, _) => Err(ApplicationError::TransferError(format!(
                "Cannot transition transfer {} from terminal state '{}' to '{}'",
                self.transfer_id,
                self.status.as_str(),
                new_status.as_str()
            ))),

            // Invalid jump
            (from, to) => Err(ApplicationError::TransferError(format!(
                "Invalid transfer state transition from '{}' to '{}'",
                from.as_str(),
                to.as_str()
            ))),
        }
    }
}
