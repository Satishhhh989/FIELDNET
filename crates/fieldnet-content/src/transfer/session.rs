use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use fieldnet_core::{NodeId, Priority, Timestamp};

use super::protocol::TransferId;
use super::state::TransferState;
use crate::error::ContentError;
use crate::identity::ContentId;
use crate::manifest::id::ManifestId;
use crate::manifest::model::ContentManifest;

/// Represents the in-memory and persistent state of a single content transfer session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransferSession {
    pub transfer_id: TransferId,
    pub content_id: ContentId,
    pub manifest_id: Option<ManifestId>,
    pub requester_node_id: NodeId,
    pub provider_node_id: NodeId,
    pub status: TransferState,
    pub priority: Priority,
    pub manifest: Option<ContentManifest>,
    /// Set of chunk indices whose bytes have been received, cryptographically verified, and stored.
    pub verified_chunks: HashSet<u32>,
    /// Set of chunk indices currently requested and in-flight over the network.
    pub in_flight_chunks: HashSet<u32>,
    /// Map of chunk index to consecutive failure / timeout count.
    pub failed_chunks: HashMap<u32, u8>,
    pub total_chunks: Option<u32>,
    pub total_bytes: Option<u64>,
    pub verified_bytes: u64,
    pub retry_count: u32,
    pub created_at: u64,
    pub updated_at: u64,
    pub expires_at: u64,
    pub last_activity_at: u64,
    pub error_message: Option<String>,
}

impl TransferSession {
    /// Creates a new transfer session in `Discovered` state.
    pub fn new(
        transfer_id: TransferId,
        content_id: ContentId,
        requester_node_id: NodeId,
        provider_node_id: NodeId,
        priority: Priority,
        ttl_secs: u64,
    ) -> Self {
        let now = Timestamp::now().as_millis();
        let expires_at = now + (ttl_secs * 1000);

        Self {
            transfer_id,
            content_id,
            manifest_id: None,
            requester_node_id,
            provider_node_id,
            status: TransferState::Discovered,
            priority,
            manifest: None,
            verified_chunks: HashSet::new(),
            in_flight_chunks: HashSet::new(),
            failed_chunks: HashMap::new(),
            total_chunks: None,
            total_bytes: None,
            verified_bytes: 0,
            retry_count: 0,
            created_at: now,
            updated_at: now,
            expires_at,
            last_activity_at: now,
            error_message: None,
        }
    }

    /// Transitions session to a new state enforcing the legal state machine rules.
    pub fn transition_to(&mut self, new_state: TransferState) -> Result<(), ContentError> {
        self.status.validate_transition(new_state)?;
        self.status = new_state;
        let now = Timestamp::now().as_millis();
        self.updated_at = now;
        self.last_activity_at = now;
        Ok(())
    }

    /// Associates a validated manifest with the transfer session.
    pub fn set_manifest(&mut self, manifest: ContentManifest) {
        self.manifest_id = Some(manifest.manifest_id);
        self.total_chunks = Some(manifest.chunk_count);
        self.total_bytes = Some(manifest.total_size);
        self.manifest = Some(manifest);
        let now = Timestamp::now().as_millis();
        self.updated_at = now;
        self.last_activity_at = now;
    }

    /// Marks a chunk as in-flight over the network.
    pub fn mark_chunk_in_flight(&mut self, chunk_index: u32) {
        self.in_flight_chunks.insert(chunk_index);
        let now = Timestamp::now().as_millis();
        self.last_activity_at = now;
    }

    /// Removes a chunk from in-flight without marking verified (e.g. timeout or drop).
    pub fn unmark_chunk_in_flight(&mut self, chunk_index: u32) {
        self.in_flight_chunks.remove(&chunk_index);
    }

    /// Marks a chunk verified and committed to storage.
    pub fn mark_chunk_verified(&mut self, chunk_index: u32, size_bytes: u32) {
        self.in_flight_chunks.remove(&chunk_index);
        self.failed_chunks.remove(&chunk_index);
        if self.verified_chunks.insert(chunk_index) {
            self.verified_bytes += size_bytes as u64;
        }
        let now = Timestamp::now().as_millis();
        self.updated_at = now;
        self.last_activity_at = now;
    }

    /// Marks a chunk attempt as failed and increments its individual failure counter.
    pub fn mark_chunk_failed(&mut self, chunk_index: u32) -> u8 {
        self.in_flight_chunks.remove(&chunk_index);
        let count = self.failed_chunks.entry(chunk_index).or_insert(0);
        *count = count.saturating_add(1);
        let now = Timestamp::now().as_millis();
        self.updated_at = now;
        self.last_activity_at = now;
        *count
    }

    /// Returns the sorted list of missing chunk indices that are not yet verified.
    pub fn missing_chunks(&self) -> Vec<u32> {
        if let Some(total) = self.total_chunks {
            (0..total)
                .filter(|idx| !self.verified_chunks.contains(idx))
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Returns true if all chunks defined in the manifest are verified.
    pub fn is_complete(&self) -> bool {
        if let Some(total) = self.total_chunks {
            self.verified_chunks.len() as u32 == total
        } else {
            false
        }
    }

    /// Checks if the transfer has expired past its absolute deadline.
    pub fn is_expired(&self, now: u64) -> bool {
        now >= self.expires_at
    }

    /// Checks if the transfer has exceeded its maximum idle inactivity window.
    pub fn is_idle_timeout(&self, now: u64, idle_limit_secs: u64) -> bool {
        let idle_ms = idle_limit_secs * 1000;
        now.saturating_sub(self.last_activity_at) >= idle_ms
    }

    /// Returns the completion ratio between 0.0 and 1.0.
    pub fn progress_ratio(&self) -> f64 {
        match self.total_chunks {
            Some(0) => 1.0,
            Some(total) => (self.verified_chunks.len() as f64 / total as f64).min(1.0),
            None => 0.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_lifecycle_and_progress() {
        let tid = TransferId::random();
        let cid = ContentId::from_bytes([5u8; 32]);
        let req = NodeId::from_bytes([1u8; 32]);
        let prov = NodeId::from_bytes([2u8; 32]);

        let mut session = TransferSession::new(tid, cid, req, prov, Priority::Normal, 300);
        assert_eq!(session.status, TransferState::Discovered);
        assert_eq!(session.progress_ratio(), 0.0);

        session.transition_to(TransferState::Requested).unwrap();
        assert_eq!(session.status, TransferState::Requested);

        session.total_chunks = Some(4);
        assert_eq!(session.missing_chunks(), vec![0, 1, 2, 3]);

        session.mark_chunk_in_flight(0);
        assert!(session.in_flight_chunks.contains(&0));

        session.mark_chunk_verified(0, 1024);
        assert!(!session.in_flight_chunks.contains(&0));
        assert!(session.verified_chunks.contains(&0));
        assert_eq!(session.verified_bytes, 1024);
        assert_eq!(session.progress_ratio(), 0.25);
        assert_eq!(session.missing_chunks(), vec![1, 2, 3]);
        assert!(!session.is_complete());

        session.mark_chunk_verified(1, 1024);
        session.mark_chunk_verified(2, 1024);
        session.mark_chunk_verified(3, 1024);
        assert!(session.is_complete());
        assert_eq!(session.progress_ratio(), 1.0);
    }
}
