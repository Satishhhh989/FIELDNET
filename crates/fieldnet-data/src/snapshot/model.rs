//! Models for Document Snapshots, Compaction Records, and Safety Reports.

use fieldnet_core::Timestamp;
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::causality::{HybridLogicalClock, StateVector};
use crate::document::DocumentId;
use crate::error::DataError;

/// Globally unique 32-byte cryptographic identifier for a document snapshot.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SnapshotId([u8; 32]);

impl SnapshotId {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    pub fn generate() -> Self {
        let mut bytes = [0u8; 32];
        rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut bytes);
        Self(bytes)
    }
}

impl fmt::Display for SnapshotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(self.0))
    }
}

impl fmt::Debug for SnapshotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SnapshotId({})", hex::encode(self.0))
    }
}

impl AsRef<[u8]> for SnapshotId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Durable, immutable point-in-time snapshot representing materialized document state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentSnapshot {
    /// Cryptographic identifier for this snapshot.
    pub snapshot_id: SnapshotId,
    /// Target document identifier.
    pub document_id: DocumentId,
    /// Monotonically increasing snapshot generation number (1, 2, ...).
    pub snapshot_version: u64,
    /// Highest operation sequence covered by this snapshot.
    pub compacted_up_to_seq: u64,
    /// Exact causal state vector covered by this snapshot.
    pub state_vector: StateVector,
    /// Hybrid logical clock timestamp at time of snapshot creation.
    pub hlc: HybridLogicalClock,
    /// CRDT type family (e.g. "pn_counter", "or_set", "lww_register", "crdt_map", "document").
    pub crdt_type: String,
    /// Serialized materialized document state payload.
    pub state: Vec<u8>,
    /// Deterministic BLAKE3 state hash over payload and causal coverage metadata.
    pub state_hash: [u8; 32],
    /// Schema version for format compatibility.
    pub schema_version: u32,
    /// Creation physical timestamp.
    pub created_at: Timestamp,
}

impl DocumentSnapshot {
    /// Computes deterministic 32-byte BLAKE3 hash over snapshot payload and causal coverage metadata.
    pub fn compute_state_hash(
        document_id: &DocumentId,
        snapshot_version: u64,
        compacted_up_to_seq: u64,
        state_vector: &StateVector,
        hlc: &HybridLogicalClock,
        crdt_type: &str,
        state: &[u8],
        schema_version: u32,
    ) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"FIELDNET_SNAPSHOT_V1");
        hasher.update(document_id.as_bytes());
        hasher.update(&snapshot_version.to_be_bytes());
        hasher.update(&compacted_up_to_seq.to_be_bytes());
        hasher.update(&state_vector.to_bytes());
        hasher.update(&hlc.to_bytes());
        hasher.update(crdt_type.as_bytes());
        hasher.update(&(state.len() as u64).to_be_bytes());
        hasher.update(state);
        hasher.update(&schema_version.to_be_bytes());
        *hasher.finalize().as_bytes()
    }

    /// Validates the integrity of this snapshot against its recorded state hash.
    pub fn validate_integrity(&self) -> Result<(), DataError> {
        let expected = Self::compute_state_hash(
            &self.document_id,
            self.snapshot_version,
            self.compacted_up_to_seq,
            &self.state_vector,
            &self.hlc,
            &self.crdt_type,
            &self.state,
            self.schema_version,
        );

        if self.state_hash != expected {
            return Err(DataError::CorruptedSnapshot {
                snapshot_id: self.snapshot_id.to_string(),
                reason: format!(
                    "State hash mismatch: expected {}, recorded {}",
                    hex::encode(expected),
                    hex::encode(self.state_hash)
                ),
            });
        }

        Ok(())
    }
}

/// Metadata record generated upon completing a safe compaction cycle.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionRecord {
    /// Unique compaction execution identifier.
    pub compaction_id: [u8; 16],
    /// Target document identifier.
    pub document_id: DocumentId,
    /// Baseline snapshot identifier anchoring this compaction.
    pub snapshot_id: SnapshotId,
    /// Highest operation sequence pruned.
    pub compacted_up_to_seq: u64,
    /// Number of operations pruned from storage.
    pub ops_removed: usize,
    /// Approximate bytes reclaimed from storage.
    pub bytes_reclaimed: usize,
    /// Physical execution timestamp.
    pub compacted_at: Timestamp,
    /// Optional compaction rationale or trigger notes.
    pub reason: Option<String>,
}

/// Assessment report determining whether compaction is legally and causally safe.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionSafetyReport {
    /// True if all safety invariants are met and compaction may proceed.
    pub is_safe: bool,
    /// Highest sequence number safe to prune.
    pub max_safe_sequence: u64,
    /// Total eligible operations that would be pruned.
    pub eligible_operations_count: usize,
    /// Latest snapshot sequence coverage.
    pub latest_snapshot_seq: u64,
    /// Minimum sequence acknowledged by active peers, if any.
    pub min_peer_synced_seq: Option<u64>,
    /// Reasons explaining why compaction is unsafe or was skipped.
    pub reasons_unsafe: Vec<String>,
}
