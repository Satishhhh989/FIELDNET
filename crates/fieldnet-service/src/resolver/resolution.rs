use fieldnet_core::{NodeId, Timestamp};
use serde::{Deserialize, Serialize};

use super::query::LocalityLevel;
use crate::model::{
    ServiceCapability, ServiceEndpoint, ServiceId, ServiceInstanceId, ServiceProtocol,
    ServiceRecord, ServiceVersion,
};

/// Structured, safe explanation of why a provider instance was selected.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ServiceResolutionReason {
    /// ServiceId matched.
    pub matched_service: bool,
    /// Compatible SemVer version matched.
    pub compatible_version: ServiceVersion,
    /// Required capabilities satisfied.
    pub capabilities_satisfied: Vec<ServiceCapability>,
    /// Protocol satisfied.
    pub protocol_satisfied: ServiceProtocol,
    /// Cryptographic manifest signature verified.
    pub provider_authenticated: bool,
    /// Caller access control permissions verified.
    pub provider_authorized: bool,
    /// Authoritative lifecycle state is callable.
    pub service_available: bool,
    /// Provider is topologically reachable on the overlay network.
    pub route_available: bool,
    /// Evaluated locality level.
    pub locality: LocalityLevel,
    /// Health, reliability, and latency telemetry summary.
    pub health_status: String,
    /// Whether deterministic tie-breaking was applied between equivalent candidates.
    pub deterministic_tiebreak: bool,
    /// Human-readable explanation text.
    pub explanation_text: String,
}

/// Detailed resolution audit report.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ResolutionExplanation {
    /// Total count of instances examined in the registry for this ServiceId.
    pub candidates_evaluated: usize,
    /// Count of candidates that satisfied all hard constraints.
    pub candidates_passed_hard_filters: usize,
    /// Winning instance ID.
    pub selected_instance_id: ServiceInstanceId,
    /// Winning provider NodeId.
    pub selected_provider_node_id: NodeId,
    /// Detailed reason.
    pub reason: ServiceResolutionReason,
}

/// Authoritative result of resolving a `ServiceQuery`.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ServiceResolution {
    /// Requested service identifier.
    pub service_id: ServiceId,
    /// Unique instance identifier of the selected provider.
    pub instance_id: ServiceInstanceId,
    /// Sovereign node ID of the hosting provider.
    pub provider_node_id: NodeId,
    /// Complete authoritative record of the selected instance.
    pub selected_instance: ServiceRecord,
    /// Structured explanation of selection.
    pub reason: ServiceResolutionReason,
    /// Timestamp at which resolution succeeded.
    pub resolved_at: Timestamp,
    /// Network hop count to provider (0 for local, None if DTN/unrouted).
    pub route_hops: Option<u8>,
    /// Optional correlation identifier.
    pub correlation_id: Option<String>,
}

impl ServiceResolution {
    /// Returns the instance ID of the selected service.
    pub fn instance_id(&self) -> &ServiceInstanceId {
        &self.instance_id
    }

    /// Returns the provider's sovereign NodeId.
    pub fn provider_node_id(&self) -> &NodeId {
        &self.provider_node_id
    }

    /// Returns the SemVer version of the selected service.
    pub fn version(&self) -> &ServiceVersion {
        &self.selected_instance.manifest.version
    }

    /// Returns the reachability endpoint of the selected service.
    pub fn endpoint(&self) -> &ServiceEndpoint {
        &self.selected_instance.manifest.endpoint
    }

    /// Returns the communication protocol dialect of the selected service.
    pub fn protocol(&self) -> ServiceProtocol {
        self.selected_instance.manifest.protocol
    }

    /// Returns whether the selected instance is hosted directly on this local node.
    pub fn is_local(&self) -> bool {
        self.selected_instance.is_local
    }

    /// Returns a full `ResolutionExplanation`.
    pub fn explanation(&self, evaluated: usize, passed: usize) -> ResolutionExplanation {
        ResolutionExplanation {
            candidates_evaluated: evaluated,
            candidates_passed_hard_filters: passed,
            selected_instance_id: self.instance_id.clone(),
            selected_provider_node_id: self.provider_node_id,
            reason: self.reason.clone(),
        }
    }
}
