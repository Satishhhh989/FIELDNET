use fieldnet_core::Timestamp;
use fieldnet_identity::NodeIdentity;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, info};

use crate::error::ServiceError;
use crate::events::ServiceEvent;
use crate::model::{ServiceInstanceId, ServiceManifest, ServiceManifestBuilder};
use crate::registry::ServiceRegistry;
use crate::wire::ServiceWithdrawal;

/// Authoritative manager for declaring, updating, signing, and refreshing
/// locally provided service advertisements on the FIELDNET mesh.
pub struct ServiceAdvertiser {
    registry: Arc<ServiceRegistry>,
    identity: Arc<NodeIdentity>,
    event_tx: broadcast::Sender<ServiceEvent>,
    active_local_manifests: Arc<RwLock<HashMap<ServiceInstanceId, ServiceManifest>>>,
    default_ttl_seconds: u32,
}

impl ServiceAdvertiser {
    /// Creates a new `ServiceAdvertiser`.
    pub fn new(
        registry: Arc<ServiceRegistry>,
        identity: Arc<NodeIdentity>,
        event_tx: broadcast::Sender<ServiceEvent>,
        default_ttl_seconds: u32,
    ) -> Self {
        Self {
            registry,
            identity,
            event_tx,
            active_local_manifests: Arc::new(RwLock::new(HashMap::new())),
            default_ttl_seconds,
        }
    }

    /// Returns the default advertisement TTL in seconds.
    pub fn default_ttl_seconds(&self) -> u32 {
        self.default_ttl_seconds
    }

    /// Accessor for the underlying ServiceRegistry.
    pub fn registry(&self) -> &Arc<ServiceRegistry> {
        &self.registry
    }

    /// Declares, cryptographically signs, and registers a local service instance.
    ///
    /// The signed manifest is placed in the local `ServiceRegistry` and tracked
    /// for automatic keepalive refresh and network advertisement.
    pub async fn register_and_advertise(
        &self,
        builder: ServiceManifestBuilder,
    ) -> Result<ServiceManifest, ServiceError> {
        let manifest = builder.build_and_sign(&self.identity)?;
        let instance_id = manifest.instance_id.clone();
        let service_id = manifest.service_id.clone();

        // 1. Authoritative local registration
        self.registry.register_local(manifest.clone()).await?;

        // 2. Track in active local services
        {
            let mut active = self.active_local_manifests.write().await;
            active.insert(instance_id.clone(), manifest.clone());
        }

        info!(
            "Registered and advertised local service '{}' (instance: '{}')",
            service_id, instance_id
        );

        Ok(manifest)
    }

    /// Mutates an active local service, increments its sequence, re-signs,
    /// and updates the local `ServiceRegistry`.
    pub async fn update_service<F>(
        &self,
        instance_id: &ServiceInstanceId,
        mutate: F,
    ) -> Result<ServiceManifest, ServiceError>
    where
        F: FnOnce(&mut ServiceManifest),
    {
        let mut manifest = {
            let active = self.active_local_manifests.read().await;
            active
                .get(instance_id)
                .cloned()
                .ok_or_else(|| ServiceError::InstanceNotFound(instance_id.to_string()))?
        };

        // Increment sequence and apply mutations
        manifest.sequence = manifest.sequence.saturating_add(1);
        let now = Timestamp::now();
        manifest.created_at = now;
        manifest.expires_at = now.add_seconds(manifest.ttl_seconds as u64);
        mutate(&mut manifest);

        // Cryptographically re-sign
        manifest.sign(&self.identity)?;

        // Update local registry
        self.registry.register_local(manifest.clone()).await?;

        // Update local tracking
        {
            let mut active = self.active_local_manifests.write().await;
            active.insert(instance_id.clone(), manifest.clone());
        }

        info!(
            "Updated and republished local service '{}' (sequence: {})",
            manifest.service_id, manifest.sequence
        );

        Ok(manifest)
    }

    /// Explicitly withdraws an active local service instance from the mesh.
    ///
    /// Removes the service from the local `ServiceRegistry` and creates a signed
    /// `ServiceWithdrawal` message for network propagation.
    pub async fn withdraw(
        &self,
        instance_id: &ServiceInstanceId,
    ) -> Result<ServiceWithdrawal, ServiceError> {
        let manifest = {
            let mut active = self.active_local_manifests.write().await;
            active
                .remove(instance_id)
                .ok_or_else(|| ServiceError::InstanceNotFound(instance_id.to_string()))?
        };

        // 1. Unregister from local registry
        self.registry.unregister(instance_id).await?;

        // 2. Create signed withdrawal notice with incremented sequence
        let mut withdrawal = ServiceWithdrawal {
            service_id: manifest.service_id.clone(),
            instance_id: instance_id.clone(),
            provider_node_id: self.identity.node_id(),
            sequence: manifest.sequence.saturating_add(1),
            timestamp: Timestamp::now(),
            signature: [0u8; 64],
        };
        withdrawal.sign(&self.identity);

        // 3. Emit Withdrawn event
        let _ = self.event_tx.send(ServiceEvent::Withdrawn {
            service_id: manifest.service_id,
            instance_id: instance_id.clone(),
            provider_node_id: self.identity.node_id(),
        });

        info!("Explicitly withdrawn service instance '{}'", instance_id);

        Ok(withdrawal)
    }

    /// Returns a snapshot of all active local service manifests (e.g. for peer sync).
    pub async fn get_active_advertisements(&self) -> Vec<ServiceManifest> {
        let active = self.active_local_manifests.read().await;
        active.values().cloned().collect()
    }

    /// Restores active local advertisements from the underlying registry (e.g. after persistent restart).
    pub async fn restore_from_registry(&self) {
        let local_records = self.registry.list_local_instances().await;
        let mut active = self.active_local_manifests.write().await;
        for record in local_records {
            active.insert(record.manifest.instance_id.clone(), record.manifest);
        }
    }

    /// Evaluates all active local services and refreshes any whose remaining TTL
    /// has elapsed beyond half of their total duration.
    ///
    /// Returns the subset of manifests that were refreshed and need transmission.
    pub async fn refresh_due_manifests(
        &self,
        now: Timestamp,
    ) -> Result<Vec<ServiceManifest>, ServiceError> {
        let mut due_instances = Vec::new();

        {
            let active = self.active_local_manifests.read().await;
            for (id, manifest) in active.iter() {
                let half_ttl_millis = (manifest.ttl_seconds as u64 * 1000) / 2;
                let refresh_threshold = manifest
                    .expires_at
                    .as_millis()
                    .saturating_sub(half_ttl_millis);

                if now.as_millis() >= refresh_threshold {
                    due_instances.push(id.clone());
                }
            }
        }

        let mut refreshed = Vec::new();
        for id in due_instances {
            match self.update_service(&id, |_| {}).await {
                Ok(m) => {
                    debug!(
                        "Refreshed keepalive for service '{}' ({})",
                        m.service_id, m.instance_id
                    );
                    refreshed.push(m);
                }
                Err(e) => {
                    debug!("Failed to refresh service '{}': {}", id, e);
                }
            }
        }

        Ok(refreshed)
    }
}
