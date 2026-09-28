//! Configuration for Document Snapshots and Background Compaction.

use crate::schema::{DEFAULT_PEER_GRACE_PERIOD_DAYS, DEFAULT_SNAPSHOT_RETENTION};
use serde::{Deserialize, Serialize};

/// Configuration governing snapshot retention, compaction thresholds, and peer grace periods.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactionConfig {
    /// Whether background automatic compaction should run.
    pub auto_compaction_enabled: bool,
    /// Interval in seconds between background compaction sweeps.
    pub interval_seconds: u64,
    /// Number of operations accumulated before a document is eligible for auto-snapshotting.
    pub operation_threshold: usize,
    /// Number of valid snapshot generations to retain per document for safe recovery (minimum 2).
    pub snapshot_retention_count: usize,
    /// Inactivity grace period in days before an uncommunicative peer is treated as offline.
    pub peer_grace_period_days: u64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            auto_compaction_enabled: true,
            interval_seconds: 60,
            operation_threshold: 50,
            snapshot_retention_count: DEFAULT_SNAPSHOT_RETENTION,
            peer_grace_period_days: DEFAULT_PEER_GRACE_PERIOD_DAYS,
        }
    }
}
