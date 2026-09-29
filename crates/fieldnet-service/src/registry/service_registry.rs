use fieldnet_core::{NodeId, Timestamp};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, error, info, warn};

use super::config::RegistryConfig;
use super::store::ServiceStore;
use crate::error::ServiceError;
use crate::events::ServiceEvent;
use crate::model::{ServiceId, ServiceInstanceId, ServiceManifest, ServiceRecord, ServiceState};

struct RegistryInner {
    /// ServiceId -> (ServiceInstanceId -> ServiceRecord)
    by_service: HashMap<ServiceId, HashMap<ServiceInstanceId, ServiceRecord>>,
    /// Provider NodeId -> Set of ServiceInstanceIds
    by_provider: HashMap<NodeId, HashSet<ServiceInstanceId>>,
    /// InstanceId -> ServiceId reverse index
    instance_to_service: HashMap<ServiceInstanceId, ServiceId>,
    /// Total count of records across all services
    total_records: usize,
}

impl RegistryInner {
    fn new() -> Self {
        Self {
            by_service: HashMap::new(),
            by_provider: HashMap::new(),
            instance_to_service: HashMap::new(),
            total_records: 0,
        }
    }

    fn insert_or_update(&mut self, record: ServiceRecord) -> Option<ServiceRecord> {
        let service_id = record.service_id().clone();
        let instance_id = record.instance_id().clone();
        let provider_id = *record.provider_node_id();

        // Index by provider
        self.by_provider
            .entry(provider_id)
            .or_default()
            .insert(instance_id.clone());

        // Reverse index
        self.instance_to_service
            .insert(instance_id.clone(), service_id.clone());

        // Index by service
        let service_map = self.by_service.entry(service_id).or_default();
        let old = service_map.insert(instance_id, record);

        if old.is_none() {
            self.total_records += 1;
        }

        old
    }

    fn remove(&mut self, instance_id: &ServiceInstanceId) -> Option<ServiceRecord> {
        let service_id = self.instance_to_service.remove(instance_id)?;

        let removed = if let Some(instances) = self.by_service.get_mut(&service_id) {
            let record = instances.remove(instance_id);
            if instances.is_empty() {
                self.by_service.remove(&service_id);
            }
            record
        } else {
            None
        };

        if let Some(ref rec) = removed {
            let provider_id = rec.provider_node_id();
            if let Some(set) = self.by_provider.get_mut(provider_id) {
                set.remove(instance_id);
                if set.is_empty() {
                    self.by_provider.remove(provider_id);
                }
            }
            self.total_records = self.total_records.saturating_sub(1);
        }

        removed
    }
}

/// Central registry tracking and managing all local and mesh service instances.
pub struct ServiceRegistry {
    inner: Arc<RwLock<RegistryInner>>,
    store: Option<Arc<ServiceStore>>,
    event_tx: broadcast::Sender<ServiceEvent>,
    config: RegistryConfig,
}

impl ServiceRegistry {
    /// Opens or initializes a new `ServiceRegistry`.
    ///
    /// If `db_path` is provided, persistent SQLite storage is loaded, restoring local manifests
    /// and unexpired cached remote manifests.
    pub fn open(
        config: RegistryConfig,
        db_path: Option<PathBuf>,
        event_tx: broadcast::Sender<ServiceEvent>,
    ) -> Result<Arc<Self>, ServiceError> {
        let store = match db_path {
            Some(ref path) => Some(ServiceStore::open(path)?),
            None => None,
        };

        let mut inner = RegistryInner::new();

        // If persistence is enabled, reload persisted records
        if let Some(ref store) = store {
            let now = Timestamp::now();

            // 1. Load local manifests (always trusted, local provider)
            match store.load_all_local_manifests() {
                Ok(local_manifests) => {
                    info!(
                        "Loaded {} local service manifests from SQLite",
                        local_manifests.len()
                    );
                    for manifest in local_manifests {
                        let record = ServiceRecord::new_local(manifest, now);
                        inner.insert_or_update(record);
                    }
                }
                Err(e) => {
                    error!("Failed to reload local manifests from SQLite: {}", e);
                }
            }

            // 2. Load cached remote manifests (verify signature and check expiration)
            match store.load_all_cached_manifests() {
                Ok(cached_manifests) => {
                    let mut loaded_remote = 0;
                    for manifest in cached_manifests {
                        if manifest.is_expired(now) {
                            debug!(
                                "Skipping expired cached manifest for {}",
                                manifest.instance_id
                            );
                            continue;
                        }

                        if let Err(e) = manifest.verify_signature() {
                            warn!(
                                "Discarding cached manifest with invalid signature ({}): {}",
                                manifest.instance_id, e
                            );
                            continue;
                        }

                        let record = ServiceRecord::new_remote(manifest, now);
                        inner.insert_or_update(record);
                        loaded_remote += 1;
                    }
                    info!(
                        "Loaded {} valid cached remote service manifests from SQLite",
                        loaded_remote
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to reload cached remote manifests from SQLite: {}",
                        e
                    );
                }
            }
        }

        let registry = Arc::new(Self {
            inner: Arc::new(RwLock::new(inner)),
            store,
            event_tx,
            config,
        });

        Ok(registry)
    }

    /// Creates an in-memory `ServiceRegistry` with SQLite backing (useful for testing).
    pub fn open_in_memory(
        config: RegistryConfig,
        event_tx: broadcast::Sender<ServiceEvent>,
    ) -> Result<Arc<Self>, ServiceError> {
        let store = ServiceStore::open_in_memory()?;
        Ok(Arc::new(Self {
            inner: Arc::new(RwLock::new(RegistryInner::new())),
            store: Some(store),
            event_tx,
            config,
        }))
    }

    /// Registers a locally declared and hosted service instance.
    pub async fn register_local(&self, manifest: ServiceManifest) -> Result<(), ServiceError> {
        let now = Timestamp::now();

        // 1. Validate manifest structure
        manifest.validate_basics(now)?;

        // 2. Verify signature
        manifest.verify_signature()?;

        let instance_id = manifest.instance_id.clone();
        let service_id = manifest.service_id.clone();
        let provider_node_id = manifest.provider_node_id;

        // 3. Check duplicate handling
        {
            let inner = self.inner.read().await;
            if let Some(service_map) = inner.by_service.get(&service_id) {
                if let Some(existing) = service_map.get(&instance_id) {
                    if existing.manifest.provider_node_id != provider_node_id {
                        return Err(ServiceError::ConflictingProvider {
                            instance_id: instance_id.to_string(),
                            existing_provider: existing.manifest.provider_node_id,
                            claimed_provider: provider_node_id,
                        });
                    }

                    if manifest.sequence < existing.manifest.sequence {
                        return Err(ServiceError::StaleSequence {
                            instance_id: instance_id.to_string(),
                            current_sequence: existing.manifest.sequence,
                            incoming_sequence: manifest.sequence,
                        });
                    }
                }
            }
        }

        // 4. Persist to local_services table
        if let Some(ref store) = self.store {
            store.save_local_manifest(&manifest, now)?;
        }

        // 5. Insert into memory index
        let record = ServiceRecord::new_local(manifest, now);
        let mut inner = self.inner.write().await;
        let is_update = inner.insert_or_update(record).is_some();

        // 6. Emit events
        if is_update {
            let _ = self.event_tx.send(ServiceEvent::Updated {
                service_id: service_id.clone(),
                instance_id: instance_id.clone(),
                sequence: 1,
            });
        } else {
            let _ = self.event_tx.send(ServiceEvent::Registered {
                service_id: service_id.clone(),
                instance_id: instance_id.clone(),
                provider_node_id,
                is_local: true,
            });
        }

        let _ = self.event_tx.send(ServiceEvent::Available {
            service_id,
            instance_id,
            provider_node_id,
        });

        Ok(())
    }

    /// Ingests and registers an incoming remote service manifest discovered from the mesh.
    pub async fn register_remote(
        &self,
        manifest: ServiceManifest,
        now: Timestamp,
    ) -> Result<(), ServiceError> {
        // 1. Structural and timestamp validation
        manifest.validate_basics(now)?;

        // 2. Cryptographic signature verification
        manifest.verify_signature()?;

        let instance_id = manifest.instance_id.clone();
        let service_id = manifest.service_id.clone();
        let provider_node_id = manifest.provider_node_id;
        let sequence = manifest.sequence;

        // 3. Duplicate and sequence checking
        let mut is_update = false;
        {
            let inner = self.inner.read().await;

            // Check capacity limits
            if inner.total_records >= self.config.max_total_instances {
                return Err(ServiceError::CapacityExceeded(format!(
                    "Total instances limit reached: {}",
                    self.config.max_total_instances
                )));
            }

            if let Some(instances) = inner.by_service.get(&service_id) {
                if instances.len() >= self.config.max_instances_per_service
                    && !instances.contains_key(&instance_id)
                {
                    return Err(ServiceError::CapacityExceeded(format!(
                        "Instances per service limit reached for '{}': {}",
                        service_id, self.config.max_instances_per_service
                    )));
                }

                if let Some(existing) = instances.get(&instance_id) {
                    if existing.manifest.provider_node_id != provider_node_id {
                        return Err(ServiceError::ConflictingProvider {
                            instance_id: instance_id.to_string(),
                            existing_provider: existing.manifest.provider_node_id,
                            claimed_provider: provider_node_id,
                        });
                    }

                    if sequence < existing.manifest.sequence {
                        return Err(ServiceError::StaleSequence {
                            instance_id: instance_id.to_string(),
                            current_sequence: existing.manifest.sequence,
                            incoming_sequence: sequence,
                        });
                    }

                    if sequence == existing.manifest.sequence {
                        // Idempotent refresh: same sequence from same provider
                        debug!("Idempotent refresh for instance {}", instance_id);
                        return Ok(());
                    }

                    is_update = true;
                }
            }

            if let Some(provider_instances) = inner.by_provider.get(&provider_node_id) {
                if provider_instances.len() >= self.config.max_instances_per_provider
                    && !provider_instances.contains(&instance_id)
                {
                    return Err(ServiceError::CapacityExceeded(format!(
                        "Instances per provider limit reached for '{}': {}",
                        provider_node_id, self.config.max_instances_per_provider
                    )));
                }
            }
        }

        // 4. Persist to cached_remote_manifests
        if let Some(ref store) = self.store {
            store.save_cached_manifest(&manifest)?;
        }

        // 5. Insert into memory index
        let record = ServiceRecord::new_remote(manifest, now);
        let mut inner = self.inner.write().await;
        inner.insert_or_update(record);

        // 6. Emit events
        let _ = self.event_tx.send(ServiceEvent::Authenticated {
            service_id: service_id.clone(),
            instance_id: instance_id.clone(),
        });

        if is_update {
            let _ = self.event_tx.send(ServiceEvent::Updated {
                service_id: service_id.clone(),
                instance_id: instance_id.clone(),
                sequence,
            });
        } else {
            let _ = self.event_tx.send(ServiceEvent::Registered {
                service_id: service_id.clone(),
                instance_id: instance_id.clone(),
                provider_node_id,
                is_local: false,
            });
        }

        let _ = self.event_tx.send(ServiceEvent::Available {
            service_id,
            instance_id,
            provider_node_id,
        });

        Ok(())
    }

    /// Explicitly unregisters and removes a service instance.
    pub async fn unregister(&self, instance_id: &ServiceInstanceId) -> Result<(), ServiceError> {
        let removed = {
            let mut inner = self.inner.write().await;
            inner.remove(instance_id)
        };

        if let Some(record) = removed {
            if let Some(ref store) = self.store {
                if record.is_local {
                    store.delete_local_manifest(instance_id)?;
                } else {
                    store.delete_cached_manifest(instance_id)?;
                }
            }

            let _ = self.event_tx.send(ServiceEvent::Removed {
                service_id: record.manifest.service_id,
                instance_id: instance_id.clone(),
                provider_node_id: record.manifest.provider_node_id,
            });

            Ok(())
        } else {
            Err(ServiceError::InstanceNotFound(instance_id.to_string()))
        }
    }

    /// Looks up a specific service record by instance ID.
    pub async fn get_instance(&self, instance_id: &ServiceInstanceId) -> Option<ServiceRecord> {
        let inner = self.inner.read().await;
        let service_id = inner.instance_to_service.get(instance_id)?;
        inner.by_service.get(service_id)?.get(instance_id).cloned()
    }

    /// Lists all instances for a given `ServiceId`.
    pub async fn get_service_instances(&self, service_id: &ServiceId) -> Vec<ServiceRecord> {
        let inner = self.inner.read().await;
        inner
            .by_service
            .get(service_id)
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Lists all callable (`Available` or `Degraded`) instances for a given `ServiceId`.
    pub async fn list_callable_instances(&self, service_id: &ServiceId) -> Vec<ServiceRecord> {
        let inner = self.inner.read().await;
        inner
            .by_service
            .get(service_id)
            .map(|m| m.values().filter(|r| r.is_callable()).cloned().collect())
            .unwrap_or_default()
    }

    /// Lists all distinct known `ServiceId`s.
    pub async fn list_all_services(&self) -> Vec<ServiceId> {
        let inner = self.inner.read().await;
        inner.by_service.keys().cloned().collect()
    }

    /// Returns all tracked instances across all services.
    pub async fn list_all_instances(&self) -> Vec<ServiceRecord> {
        let inner = self.inner.read().await;
        inner
            .by_service
            .values()
            .flat_map(|m| m.values().cloned())
            .collect()
    }

    /// Returns all local service instances declared and hosted on this node.
    pub async fn list_local_instances(&self) -> Vec<ServiceRecord> {
        let inner = self.inner.read().await;
        inner
            .by_service
            .values()
            .flat_map(|m| m.values().filter(|r| r.is_local).cloned())
            .collect()
    }

    /// Sweeps and evicts all expired remote service instances.
    ///
    /// Returns the number of evicted instances.
    pub async fn sweep_expired(&self, now: Timestamp) -> usize {
        let expired_instances: Vec<(ServiceInstanceId, ServiceId, NodeId)> = {
            let inner = self.inner.read().await;
            inner
                .by_service
                .values()
                .flat_map(|m| m.values())
                .filter(|r| !r.is_local && r.is_expired(now))
                .map(|r| {
                    (
                        r.instance_id().clone(),
                        r.service_id().clone(),
                        *r.provider_node_id(),
                    )
                })
                .collect()
        };

        if expired_instances.is_empty() {
            return 0;
        }

        let mut count = 0;
        for (instance_id, service_id, provider_id) in expired_instances {
            let mut inner = self.inner.write().await;
            if let Some(mut record) = inner.remove(&instance_id) {
                let _ = record.health.state.transition_to(ServiceState::Expired);
                let _ = self.event_tx.send(ServiceEvent::Expired {
                    service_id,
                    instance_id: instance_id.clone(),
                    provider_node_id: provider_id,
                });

                if let Some(ref store) = self.store {
                    let _ = store.delete_cached_manifest(&instance_id);
                }

                count += 1;
            }
        }

        count
    }

    /// Marks all instances hosted by `provider_node_id` as `Unavailable` (e.g. upon peer disconnect).
    ///
    /// Does NOT remove the instances, allowing them to recover if the peer reconnects.
    pub async fn mark_provider_unavailable(
        &self,
        provider_node_id: &NodeId,
        reason: &str,
    ) -> usize {
        let mut affected = Vec::new();
        {
            let mut inner = self.inner.write().await;
            if let Some(instance_ids) = inner.by_provider.get(provider_node_id).cloned() {
                for instance_id in instance_ids {
                    if let Some(service_id) = inner.instance_to_service.get(&instance_id).cloned() {
                        if let Some(service_map) = inner.by_service.get_mut(&service_id) {
                            if let Some(record) = service_map.get_mut(&instance_id) {
                                if record.health.state != ServiceState::Unavailable {
                                    let _ = record
                                        .health
                                        .state
                                        .transition_to(ServiceState::Unavailable);
                                    affected.push((
                                        service_id.clone(),
                                        instance_id.clone(),
                                        *provider_node_id,
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }

        let count = affected.len();
        for (service_id, instance_id, provider_id) in affected {
            let _ = self.event_tx.send(ServiceEvent::Unavailable {
                service_id,
                instance_id,
                provider_node_id: provider_id,
                reason: reason.to_string(),
            });
        }

        count
    }

    /// Marks all unexpired instances hosted by `provider_node_id` as `Available` (e.g. upon peer reconnect).
    pub async fn mark_provider_available(&self, provider_node_id: &NodeId) -> usize {
        let mut affected = Vec::new();
        {
            let mut inner = self.inner.write().await;
            if let Some(instance_ids) = inner.by_provider.get(provider_node_id).cloned() {
                for instance_id in instance_ids {
                    if let Some(service_id) = inner.instance_to_service.get(&instance_id).cloned() {
                        if let Some(service_map) = inner.by_service.get_mut(&service_id) {
                            if let Some(record) = service_map.get_mut(&instance_id) {
                                if record.health.state == ServiceState::Unavailable {
                                    let _ =
                                        record.health.state.transition_to(ServiceState::Available);
                                    affected.push((
                                        service_id.clone(),
                                        instance_id.clone(),
                                        *provider_node_id,
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }

        let count = affected.len();
        for (service_id, instance_id, provider_id) in affected {
            let _ = self.event_tx.send(ServiceEvent::Available {
                service_id,
                instance_id,
                provider_node_id: provider_id,
            });
        }

        count
    }

    /// Records a successful interaction with an instance, updating health and latency telemetry.
    pub async fn record_call_success(
        &self,
        instance_id: &ServiceInstanceId,
        latency_ms: u32,
        now: Timestamp,
    ) {
        let mut inner = self.inner.write().await;
        if let Some(service_id) = inner.instance_to_service.get(instance_id).cloned() {
            if let Some(service_map) = inner.by_service.get_mut(&service_id) {
                if let Some(record) = service_map.get_mut(instance_id) {
                    record.health.record_success(latency_ms, now);
                }
            }
        }
    }

    /// Records an interaction failure with an instance, potentially triggering a state transition.
    pub async fn record_call_failure(&self, instance_id: &ServiceInstanceId, now: Timestamp) {
        let mut transition_event = None;
        {
            let mut inner = self.inner.write().await;
            if let Some(service_id) = inner.instance_to_service.get(instance_id).cloned() {
                if let Some(service_map) = inner.by_service.get_mut(&service_id) {
                    if let Some(record) = service_map.get_mut(instance_id) {
                        let provider_id = *record.provider_node_id();
                        if let Some(new_state) = record.health.record_failure(
                            now,
                            self.config.max_consecutive_failures_before_unavailable,
                        ) {
                            match new_state {
                                ServiceState::Degraded => {
                                    transition_event = Some(ServiceEvent::Degraded {
                                        service_id: service_id.clone(),
                                        instance_id: instance_id.clone(),
                                        reason: "Consecutive RPC failure".to_string(),
                                    });
                                }
                                ServiceState::Unavailable => {
                                    transition_event = Some(ServiceEvent::Unavailable {
                                        service_id: service_id.clone(),
                                        instance_id: instance_id.clone(),
                                        provider_node_id: provider_id,
                                        reason: format!(
                                            "Exceeded max consecutive failures ({})",
                                            self.config.max_consecutive_failures_before_unavailable
                                        ),
                                    });
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }

        if let Some(event) = transition_event {
            let _ = self.event_tx.send(event);
        }
    }

    /// Subscribes to service lifecycle events emitted by this registry.
    pub fn subscribe_events(&self) -> broadcast::Receiver<ServiceEvent> {
        self.event_tx.subscribe()
    }

    /// Returns the total number of tracked service instances.
    pub async fn count_instances(&self) -> usize {
        let inner = self.inner.read().await;
        inner.total_records
    }

    /// Returns the total number of unique registered services.
    pub async fn count_services(&self) -> usize {
        let inner = self.inner.read().await;
        inner.by_service.len()
    }

    /// Spawns a background periodic task that automatically reaps expired service instances.
    pub fn start_expiration_task(
        self: &Arc<Self>,
        interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        let registry = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            loop {
                interval_timer.tick().await;
                let now = Timestamp::now();
                let swept = registry.sweep_expired(now).await;
                if swept > 0 {
                    debug!("Periodic sweep evicted {} expired service instances", swept);
                }
            }
        })
    }
}
