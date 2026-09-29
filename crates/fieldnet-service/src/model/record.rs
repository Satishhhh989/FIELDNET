use fieldnet_core::{NodeId, Timestamp};
use serde::{Deserialize, Serialize};

use super::health::ServiceHealth;
use super::id::{ServiceId, ServiceInstanceId};
use super::manifest::ServiceManifest;
use super::state::ServiceState;

/// Full registry record combining authoritative manifest metadata with dynamic telemetry.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ServiceRecord {
    /// Authoritative signed manifest.
    pub manifest: ServiceManifest,
    /// Dynamic health, state, and telemetry.
    pub health: ServiceHealth,
    /// Whether this service instance is hosted directly on this local node.
    pub is_local: bool,
    /// Timestamp of most recent manifest ingestion or keepalive refresh.
    pub last_refreshed_at: Timestamp,
}

impl ServiceRecord {
    /// Creates a new `ServiceRecord` for a locally hosted service.
    pub fn new_local(manifest: ServiceManifest, now: Timestamp) -> Self {
        Self {
            manifest,
            health: ServiceHealth::new(ServiceState::Available),
            is_local: true,
            last_refreshed_at: now,
        }
    }

    /// Creates a new `ServiceRecord` for a discovered remote mesh service.
    pub fn new_remote(manifest: ServiceManifest, now: Timestamp) -> Self {
        Self {
            manifest,
            health: ServiceHealth::new(ServiceState::Available),
            is_local: false,
            last_refreshed_at: now,
        }
    }

    /// Returns the logical `ServiceId`.
    pub fn service_id(&self) -> &ServiceId {
        &self.manifest.service_id
    }

    /// Returns the unique `ServiceInstanceId`.
    pub fn instance_id(&self) -> &ServiceInstanceId {
        &self.manifest.instance_id
    }

    /// Returns the provider's `NodeId`.
    pub fn provider_node_id(&self) -> &NodeId {
        &self.manifest.provider_node_id
    }

    /// Returns the current lifecycle state.
    pub fn state(&self) -> ServiceState {
        self.health.state
    }

    /// Returns true if this record has expired.
    pub fn is_expired(&self, now: Timestamp) -> bool {
        self.manifest.is_expired(now)
    }

    /// Returns true if this record is currently callable.
    pub fn is_callable(&self) -> bool {
        self.health.is_callable()
    }
}
