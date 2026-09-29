use crate::host::events::HostEvents;
use crate::host::network::HostNetwork;
use crate::host::permissions::CapabilityContext;
use crate::host::services::HostServices;
use crate::host::storage::HostStorage;
use crate::id::{ApplicationId, ExecutionId};

/// Execution-scoped host context serving as the strict capability and security boundary
/// between guest WASM execution and FIELDNET subsystems.
#[derive(Clone)]
pub struct HostContext {
    pub application_id: ApplicationId,
    pub execution_id: ExecutionId,
    pub capabilities: CapabilityContext,
    pub storage: HostStorage,
    pub services: Option<HostServices>,
    pub network: Option<HostNetwork>,
    pub events: HostEvents,
    pub tokio_handle: tokio::runtime::Handle,
}

impl HostContext {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        application_id: ApplicationId,
        execution_id: ExecutionId,
        capabilities: CapabilityContext,
        storage: HostStorage,
        services: Option<HostServices>,
        network: Option<HostNetwork>,
        events: HostEvents,
        tokio_handle: tokio::runtime::Handle,
    ) -> Self {
        Self {
            application_id,
            execution_id,
            capabilities,
            storage,
            services,
            network,
            events,
            tokio_handle,
        }
    }
}
