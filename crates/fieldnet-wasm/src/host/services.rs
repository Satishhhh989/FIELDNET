use fieldnet_service::model::{ServiceCapability, ServiceId, ServiceVersion};
use fieldnet_service::resolver::{ServiceQuery, ServiceResolver};
use std::sync::Arc;
use tracing::{debug, info};

use crate::host::abi::{ServiceDescription, ServiceResolveRequest};
use crate::host::errors::HostError;
use crate::host::permissions::CapabilityContext;
use crate::id::ApplicationId;
use crate::permissions::AppPermission;

/// Service interface bridging WASM host calls to the FIELDNET ServiceResolver subsystem.
#[derive(Clone)]
pub struct HostServices {
    application_id: ApplicationId,
    resolver: Arc<ServiceResolver>,
    tokio_handle: tokio::runtime::Handle,
}

impl HostServices {
    pub fn new(
        application_id: ApplicationId,
        resolver: Arc<ServiceResolver>,
        tokio_handle: tokio::runtime::Handle,
    ) -> Self {
        Self {
            application_id,
            resolver,
            tokio_handle,
        }
    }

    /// Resolves a logical service query to an authoritative provider description.
    ///
    /// Requires capability: `service.discover`
    pub fn resolve(
        &self,
        caps: &CapabilityContext,
        req: &ServiceResolveRequest,
    ) -> Result<ServiceDescription, HostError> {
        caps.check_permission(AppPermission::ServiceDiscover)?;

        let service_id = ServiceId::new(&req.service_id)
            .map_err(|e| HostError::invalid_id(format!("Invalid service_id: {}", e)))?;

        let mut query = ServiceQuery::new(service_id);

        if let Some(ref min_ver) = req.min_version {
            let ver: ServiceVersion = min_ver
                .parse()
                .map_err(|e| HostError::invalid_request(format!("Invalid min_version: {}", e)))?;
            query.min_version = Some(ver);
        }

        if let Some(ref req_caps) = req.required_capabilities {
            let mut parsed_caps = Vec::new();
            for c in req_caps {
                let cap = ServiceCapability::new(c).map_err(|e| {
                    HostError::invalid_request(format!("Invalid capability '{}': {}", c, e))
                })?;
                parsed_caps.push(cap);
            }
            query.required_capabilities = parsed_caps;
        }

        let resolution = self
            .tokio_handle
            .block_on(async { self.resolver.resolve(&query, None).await })
            .map_err(|e| {
                debug!(
                    application_id = %self.application_id,
                    service_id = %req.service_id,
                    error = %e,
                    "Service resolution failed"
                );
                HostError::service_unavailable(format!(
                    "Service '{}' resolution failed: {}",
                    req.service_id, e
                ))
            })?;

        info!(
            application_id = %self.application_id,
            service_id = %resolution.service_id,
            instance_id = %resolution.instance_id,
            provider_node = %resolution.provider_node_id,
            "Service successfully resolved for application"
        );

        let desc = ServiceDescription {
            service_id: resolution.service_id.to_string(),
            instance_id: resolution.instance_id.to_string(),
            provider_node: resolution.provider_node_id.to_string(),
            version: resolution.version().to_string(),
            capabilities: resolution
                .selected_instance
                .manifest
                .capabilities
                .iter()
                .map(|c| c.to_string())
                .collect(),
            protocol: format!("{:?}", resolution.protocol()),
            endpoint: format!("{:?}", resolution.endpoint()),
            status: format!("{:?}", resolution.selected_instance.health),
        };

        Ok(desc)
    }

    /// Describes a service by resolving its default active instance.
    ///
    /// Requires capability: `service.discover`
    pub fn describe(
        &self,
        caps: &CapabilityContext,
        service_id_str: &str,
    ) -> Result<ServiceDescription, HostError> {
        let req = ServiceResolveRequest {
            service_id: service_id_str.to_string(),
            min_version: None,
            required_capabilities: None,
        };
        self.resolve(caps, &req)
    }
}
