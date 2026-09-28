use serde::{Deserialize, Serialize};

/// Storage tier classifications recognized by the Content Garbage Collector (Phase 07 BUILD 06).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StorageTier {
    /// Pinned by user or policy — 100% immune from garbage collection and eviction.
    Pinned,
    /// Chunks or content locked by an active, in-flight transfer session.
    ActiveTransfer,
    /// Replicated content copies maintained to satisfy replication factor requirements.
    Replicated,
    /// Verified, unpinned content stored locally — evictable via LRU under storage pressure.
    Cached,
    /// Temporary files, aborted downloads, and expired sessions — evictable.
    Temporary,
    /// Pre-verification ingestion buffers and incoming parts.
    Staging,
    /// Confirmed corrupted chunks or objects — highest priority for immediate removal or quarantine.
    Corrupted,
}

impl StorageTier {
    /// Returns true if content in this tier is protected from normal automated eviction.
    pub fn is_protected(&self) -> bool {
        matches!(self, Self::Pinned | Self::ActiveTransfer)
    }

    /// Returns the eviction priority rank (lower number = evicted first).
    pub fn eviction_priority(&self) -> u8 {
        match self {
            Self::Corrupted => 0,
            Self::Temporary => 1,
            Self::Staging => 2,
            Self::Cached => 3,
            Self::Replicated => 4,
            Self::ActiveTransfer => 254,
            Self::Pinned => 255,
        }
    }
}
