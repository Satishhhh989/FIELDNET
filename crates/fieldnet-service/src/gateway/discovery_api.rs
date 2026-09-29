use fieldnet_core::{NodeId, Timestamp};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::model::ServiceState;
use crate::registry::ServiceRegistry;

/// Browser-safe service item metadata (redacted of internal private keys, paths, and raw routing).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BrowserServiceItem {
    pub service_id: String,
    pub display_name: String,
    pub description: String,
    pub version: String,
    pub mount_path: String,
    pub status: String,
    pub capabilities: Vec<String>,
    pub protocols: Vec<String>,
    pub is_public: bool,
}

/// Browser-safe service catalog document emitted by `GET /api/v1/services`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BrowserServiceCatalog {
    pub schema_version: u32,
    pub gateway_node: String,
    pub timestamp: u64,
    pub services: Vec<BrowserServiceItem>,
}

impl BrowserServiceCatalog {
    /// Generates a sanitized browser service catalog from the live `ServiceRegistry`.
    pub async fn from_registry(registry: &Arc<ServiceRegistry>, gateway_node: NodeId) -> Self {
        let records = registry.list_all_instances().await;
        let mut services = Vec::with_capacity(records.len());

        for record in records {
            let manifest = &record.manifest;
            let status = match record.state() {
                ServiceState::Available => "Available",
                ServiceState::Degraded => "Degraded",
                ServiceState::Unavailable => "Unavailable",
                ServiceState::Expired => "Expired",
                ServiceState::Removed => "Removed",
                ServiceState::Discovered => "Discovered",
                ServiceState::Authenticated => "Authenticated",
            };

            services.push(BrowserServiceItem {
                service_id: manifest.service_id.to_string(),
                display_name: manifest.display_name.clone(),
                description: manifest.description.clone(),
                version: manifest.version.to_string(),
                mount_path: format!("/services/{}", manifest.service_id),
                status: status.to_string(),
                capabilities: manifest
                    .capabilities
                    .iter()
                    .map(|c| c.as_str().to_string())
                    .collect(),
                protocols: vec![manifest.protocol.to_string()],
                is_public: manifest.permissions.is_public,
            });
        }

        Self {
            schema_version: 1,
            gateway_node: gateway_node.to_string(),
            timestamp: Timestamp::now().as_millis(),
            services,
        }
    }
}
