use serde::{Deserialize, Serialize};

/// Comprehensive configurable resource limits preventing resource exhaustion (Phase 07 BUILD 06).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResourceLimits {
    /// Maximum size of an individual content object in bytes (default: 100 GB).
    pub max_object_size: u64,
    /// Maximum size of an individual chunk in bytes (default: 16 MB).
    pub max_chunk_size: u32,
    /// Maximum serialized manifest size in bytes (default: 16 MB).
    pub max_manifest_size: u32,
    /// Maximum number of chunks allowed per content manifest (default: 500,000).
    pub max_chunks_per_manifest: u32,
    /// Maximum concurrent active transfer sessions (default: 32).
    pub max_concurrent_transfers: usize,
    /// Maximum provider records tracked in ProviderRegistry (default: 10,000).
    pub max_provider_records: usize,
    /// Maximum background replication jobs (default: 64).
    pub max_replication_jobs: usize,
    /// Maximum queued content requests in incoming queue (default: 1,024).
    pub max_queued_content_requests: usize,
    /// Maximum chunk request retries before failing (default: 5).
    pub max_retries: u32,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_object_size: 100 * 1024 * 1024 * 1024, // 100 GB
            max_chunk_size: 16 * 1024 * 1024,          // 16 MB
            max_manifest_size: 16 * 1024 * 1024,       // 16 MB
            max_chunks_per_manifest: 500_000,
            max_concurrent_transfers: 32,
            max_provider_records: 10_000,
            max_replication_jobs: 64,
            max_queued_content_requests: 1_024,
            max_retries: 5,
        }
    }
}

/// Storage quota allocation configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuotaConfig {
    /// Maximum total storage quota in bytes (default: 10 GB).
    pub total_storage_quota: u64,
    /// Maximum storage quota allocated for unpinned cache in bytes (default: 5 GB).
    pub cache_quota: u64,
    /// Maximum storage quota allocated for staging/temporary transfers (default: 2 GB).
    pub temp_quota: u64,
    /// Warning threshold percentage (e.g. 80.0%).
    pub warning_threshold_percent: f64,
}

impl Default for QuotaConfig {
    fn default() -> Self {
        Self {
            total_storage_quota: 10 * 1024 * 1024 * 1024, // 10 GB
            cache_quota: 5 * 1024 * 1024 * 1024,          // 5 GB
            temp_quota: 2 * 1024 * 1024 * 1024,           // 2 GB
            warning_threshold_percent: 80.0,
        }
    }
}
