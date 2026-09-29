use fieldnet_core::Timestamp;
use wasmtime::{StoreLimits, StoreLimitsBuilder};

use crate::host::context::HostContext;
use crate::id::{ApplicationId, ExecutionId, PackageId};
use crate::limits::ResourceLimits;

/// Contextual data associated with each active Wasmtime `Store`.
///
/// Encapsulates execution tracking identifiers, resource limits, and execution start timing.
pub struct StoreData {
    pub execution_id: ExecutionId,
    pub application_id: ApplicationId,
    pub package_id: PackageId,
    pub limits: StoreLimits,
    pub initial_fuel: u64,
    pub max_execution_time_ms: u64,
    pub start_time: Timestamp,
    pub host_context: Option<HostContext>,
}

impl StoreData {
    /// Creates a new `StoreData` container configured according to the application's declared resource limits.
    pub fn new(
        execution_id: ExecutionId,
        application_id: ApplicationId,
        package_id: PackageId,
        limits_config: &ResourceLimits,
    ) -> Self {
        let store_limits = StoreLimitsBuilder::new()
            .memory_size(limits_config.max_memory_bytes as usize)
            .instances(1)
            .memories(1)
            .tables(10)
            .build();

        Self {
            execution_id,
            application_id,
            package_id,
            limits: store_limits,
            initial_fuel: limits_config.max_cpu_fuel,
            max_execution_time_ms: limits_config.max_execution_time_ms,
            start_time: Timestamp::now(),
            host_context: None,
        }
    }

    /// Attaches an execution-scoped HostContext to this store.
    pub fn with_host_context(mut self, ctx: HostContext) -> Self {
        self.host_context = Some(ctx);
        self
    }
}
