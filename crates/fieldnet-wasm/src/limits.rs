use serde::{Deserialize, Serialize};

use crate::error::ApplicationError;

/// Sane default maximum resource limits.
pub const DEFAULT_MAX_MEMORY_BYTES: u64 = 64 * 1024 * 1024; // 64 MB
pub const DEFAULT_MAX_EXECUTION_TIME_MS: u64 = 30_000; // 30 seconds
pub const DEFAULT_MAX_CPU_FUEL: u64 = 1_000_000_000; // 1 billion instructions
pub const DEFAULT_MAX_STORAGE_BYTES: u64 = 100 * 1024 * 1024; // 100 MB
pub const DEFAULT_MAX_NETWORK_BANDWIDTH_BPS: u64 = 10 * 1024 * 1024; // 10 MB/s
pub const DEFAULT_MAX_CONCURRENCY: u32 = 4; // 4 concurrent tasks

/// Absolute maximum caps to protect system safety.
pub const HARD_MAX_MEMORY_BYTES: u64 = 1024 * 1024 * 1024; // 1 GB
pub const HARD_MAX_EXECUTION_TIME_MS: u64 = 600_000; // 10 minutes
pub const HARD_MAX_STORAGE_BYTES: u64 = 10 * 1024 * 1024 * 1024; // 10 GB
pub const HARD_MAX_CONCURRENCY: u32 = 64;

/// Resource limit configurations declared in an application manifest.
///
/// These structures represent configuration and data boundaries for future sandboxed execution.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ResourceLimits {
    /// Maximum linear memory allocation in bytes.
    #[serde(default = "default_memory")]
    pub max_memory_bytes: u64,

    /// Maximum wall-clock execution time per invocation in milliseconds.
    #[serde(default = "default_execution_time")]
    pub max_execution_time_ms: u64,

    /// Maximum WebAssembly instruction fuel consumption.
    #[serde(default = "default_cpu_fuel")]
    pub max_cpu_fuel: u64,

    /// Maximum isolated disk storage quota in bytes.
    #[serde(default = "default_storage")]
    pub max_storage_bytes: u64,

    /// Maximum outbound network transmission rate in bytes per second.
    #[serde(default = "default_bandwidth")]
    pub max_network_bandwidth_bps: u64,

    /// Maximum concurrent background execution threads/tasks.
    #[serde(default = "default_concurrency")]
    pub max_concurrency: u32,
}

fn default_memory() -> u64 {
    DEFAULT_MAX_MEMORY_BYTES
}
fn default_execution_time() -> u64 {
    DEFAULT_MAX_EXECUTION_TIME_MS
}
fn default_cpu_fuel() -> u64 {
    DEFAULT_MAX_CPU_FUEL
}
fn default_storage() -> u64 {
    DEFAULT_MAX_STORAGE_BYTES
}
fn default_bandwidth() -> u64 {
    DEFAULT_MAX_NETWORK_BANDWIDTH_BPS
}
fn default_concurrency() -> u32 {
    DEFAULT_MAX_CONCURRENCY
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_memory_bytes: DEFAULT_MAX_MEMORY_BYTES,
            max_execution_time_ms: DEFAULT_MAX_EXECUTION_TIME_MS,
            max_cpu_fuel: DEFAULT_MAX_CPU_FUEL,
            max_storage_bytes: DEFAULT_MAX_STORAGE_BYTES,
            max_network_bandwidth_bps: DEFAULT_MAX_NETWORK_BANDWIDTH_BPS,
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
        }
    }
}

impl ResourceLimits {
    /// Validates declared resource limits against sanity bounds.
    pub fn validate(&self) -> Result<(), ApplicationError> {
        if self.max_memory_bytes == 0 || self.max_memory_bytes > HARD_MAX_MEMORY_BYTES {
            return Err(ApplicationError::ResourceLimitsError(format!(
                "Memory limit {} bytes is invalid (allowed: 1..={})",
                self.max_memory_bytes, HARD_MAX_MEMORY_BYTES
            )));
        }

        if self.max_execution_time_ms == 0
            || self.max_execution_time_ms > HARD_MAX_EXECUTION_TIME_MS
        {
            return Err(ApplicationError::ResourceLimitsError(format!(
                "Execution time limit {} ms is invalid (allowed: 1..={})",
                self.max_execution_time_ms, HARD_MAX_EXECUTION_TIME_MS
            )));
        }

        if self.max_cpu_fuel == 0 {
            return Err(ApplicationError::ResourceLimitsError(
                "CPU fuel limit must be greater than zero".into(),
            ));
        }

        if self.max_storage_bytes > HARD_MAX_STORAGE_BYTES {
            return Err(ApplicationError::ResourceLimitsError(format!(
                "Storage limit {} bytes exceeds hard cap of {}",
                self.max_storage_bytes, HARD_MAX_STORAGE_BYTES
            )));
        }

        if self.max_concurrency == 0 || self.max_concurrency > HARD_MAX_CONCURRENCY {
            return Err(ApplicationError::ResourceLimitsError(format!(
                "Concurrency limit {} is invalid (allowed: 1..={})",
                self.max_concurrency, HARD_MAX_CONCURRENCY
            )));
        }

        Ok(())
    }
}
