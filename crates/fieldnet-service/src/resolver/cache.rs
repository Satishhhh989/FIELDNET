use fieldnet_core::{NodeId, Timestamp};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::query::ServiceQuery;
use super::resolution::ServiceResolution;
use crate::model::{ServiceId, ServiceInstanceId};

#[derive(Clone)]
struct CacheEntry {
    resolution: ServiceResolution,
    expires_at: Timestamp,
}

/// Thread-safe resolution cache with TTL and event-driven invalidation hooks.
pub struct ResolutionCache {
    entries: Arc<RwLock<HashMap<ServiceQuery, CacheEntry>>>,
    ttl_millis: u64,
}

impl ResolutionCache {
    /// Creates a new `ResolutionCache` with the specified TTL.
    pub fn new(ttl_millis: u64) -> Self {
        Self {
            entries: Arc::new(RwLock::new(HashMap::new())),
            ttl_millis,
        }
    }

    /// Retrieves an unexpired resolution for the given query.
    pub async fn get(&self, query: &ServiceQuery, now: Timestamp) -> Option<ServiceResolution> {
        let entries = self.entries.read().await;
        if let Some(entry) = entries.get(query) {
            if now.as_millis() < entry.expires_at.as_millis() {
                return Some(entry.resolution.clone());
            }
        }
        None
    }

    /// Stores a resolution in the cache.
    pub async fn put(&self, query: ServiceQuery, resolution: ServiceResolution, now: Timestamp) {
        let mut entries = self.entries.write().await;
        let expires_at = now.add_seconds(self.ttl_millis / 1000);
        entries.insert(
            query,
            CacheEntry {
                resolution,
                expires_at,
            },
        );
    }

    /// Invalidates all cached queries resolving to a specific `ServiceInstanceId`.
    pub async fn invalidate_instance(&self, instance_id: &ServiceInstanceId) {
        let mut entries = self.entries.write().await;
        entries.retain(|_, entry| entry.resolution.instance_id() != instance_id);
    }

    /// Invalidates all cached queries for a given `ServiceId`.
    pub async fn invalidate_service(&self, service_id: &ServiceId) {
        let mut entries = self.entries.write().await;
        entries.retain(|query, _| &query.service_id != service_id);
    }

    /// Invalidates all cached queries hosted by `provider_node_id`.
    pub async fn invalidate_provider(&self, provider_node_id: &NodeId) {
        let mut entries = self.entries.write().await;
        entries.retain(|_, entry| entry.resolution.provider_node_id() != provider_node_id);
    }

    /// Clears the entire cache.
    pub async fn clear(&self) {
        let mut entries = self.entries.write().await;
        entries.clear();
    }
}
