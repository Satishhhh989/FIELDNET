/// Configuration options and capacity bounds for the `ServiceRegistry`.
#[derive(Clone, Debug)]
pub struct RegistryConfig {
    /// Maximum number of active instances allowed for any single `ServiceId` (default: 32).
    pub max_instances_per_service: usize,
    /// Maximum total service instances stored in memory across all services (default: 10,000).
    pub max_total_instances: usize,
    /// Maximum number of instances allowed from any single provider node (default: 500).
    pub max_instances_per_provider: usize,
    /// Number of consecutive RPC failures before an instance transitions to Unavailable (default: 3).
    pub max_consecutive_failures_before_unavailable: u32,
    /// Default time-to-live for advertisements in seconds (default: 120s).
    pub default_ttl_seconds: u32,
    /// Frequency of background TTL expiration sweeps in seconds (default: 5s).
    pub sweep_interval_seconds: u64,
}

impl Default for RegistryConfig {
    fn default() -> Self {
        Self {
            max_instances_per_service: 32,
            max_total_instances: 10_000,
            max_instances_per_provider: 500,
            max_consecutive_failures_before_unavailable: 3,
            default_ttl_seconds: 120,
            sweep_interval_seconds: 5,
        }
    }
}
