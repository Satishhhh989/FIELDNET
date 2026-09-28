use serde::{Deserialize, Serialize};

use super::classifier::StorageTier;
use crate::identity::ContentId;

/// An identified candidate object for garbage collection evaluation.
#[derive(Clone, Debug)]
pub struct EvictionCandidate {
    pub content_id: ContentId,
    pub tier: StorageTier,
    pub size_bytes: u64,
    pub last_accessed_at: u64,
    pub is_pinned: bool,
}

/// Telemetry and summary report returned upon completion of a garbage collection sweep.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcReport {
    pub candidates: usize,
    pub protected_objects: usize,
    pub evicted_objects: usize,
    pub evicted_chunks: usize,
    pub bytes_reclaimed: u64,
    pub failed_evictions: usize,
}
