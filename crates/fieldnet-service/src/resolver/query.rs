use serde::{Deserialize, Serialize};

use crate::error::ServiceError;
use crate::model::{ServiceCapability, ServiceId, ServiceProtocol, ServiceVersion};

/// Client preference for network locality when selecting candidate service providers.
#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default, Serialize, Deserialize,
)]
pub enum LocalityPreference {
    /// Any available candidate regardless of locality.
    #[default]
    Any,
    /// Prefer local node; if none available, fallback to direct/nearby/remote peers.
    PreferLocal,
    /// Hard requirement: only select instances hosted directly on the local node.
    LocalOnly,
    /// Prefer offloading to remote peers; if none available, fallback to local node.
    PreferRemote,
}

/// Evaluated topological locality level of a provider node.
#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default, Serialize, Deserialize,
)]
pub enum LocalityLevel {
    /// Hosted directly on this node (0 hops, `is_local == true`).
    LocalNode = 0,
    /// Direct one-hop peer on the overlay transport.
    DirectPeer = 1,
    /// Nearby peer (2-3 hops away).
    NearbyPeer = 2,
    /// Multi-hop remote peer (4+ hops away).
    Remote = 3,
    /// Locality cannot be determined or route is unavailable.
    #[default]
    Unknown = 4,
}

impl LocalityLevel {
    /// Maps a hop count to a canonical locality level.
    pub fn from_hops(hops: u8, is_local: bool) -> Self {
        if is_local || hops == 0 {
            Self::LocalNode
        } else if hops == 1 {
            Self::DirectPeer
        } else if hops <= 3 {
            Self::NearbyPeer
        } else {
            Self::Remote
        }
    }
}

/// Explicit cryptographic trust requirements enforced during resolution.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct TrustRequirement {
    /// Whether the candidate manifest must have a verified Ed25519 signature
    /// and a public key matching its claimed provider `NodeId`.
    pub require_authenticated_provider: bool,
    /// Whether the calling context must satisfy the candidate manifest's access permissions.
    pub require_authorized_provider: bool,
}

impl Default for TrustRequirement {
    fn default() -> Self {
        Self {
            require_authenticated_provider: true,
            require_authorized_provider: true,
        }
    }
}

impl TrustRequirement {
    /// Default trust requirements (authenticated and authorized).
    pub fn standard() -> Self {
        Self::default()
    }

    /// Relaxed requirements (e.g. for discovery probes or testing).
    pub fn unauthenticated() -> Self {
        Self {
            require_authenticated_provider: false,
            require_authorized_provider: false,
        }
    }
}

/// Optional hardware or resource capacity prerequisites requested by a client.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct ResourceRequirement {
    /// Minimum available host RAM in megabytes.
    pub min_memory_mb: Option<u64>,
    /// Minimum CPU execution cores.
    pub min_cpu_cores: Option<u32>,
}

/// Canonical query submitted to the `ServiceResolver` to discover and select a provider.
#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct ServiceQuery {
    /// Target service identifier to resolve (e.g. "files.service", "search.service").
    pub service_id: ServiceId,
    /// Optional minimum compatible SemVer version.
    pub min_version: Option<ServiceVersion>,
    /// Optional maximum compatible SemVer version.
    pub max_version: Option<ServiceVersion>,
    /// Fine-grained capabilities that the candidate MUST advertise.
    pub required_capabilities: Vec<ServiceCapability>,
    /// Specific protocol dialect required by the client (e.g. NativeFieldnetRpc, Http1).
    pub required_protocol: Option<ServiceProtocol>,
    /// Locality preference policy.
    pub locality_preference: LocalityPreference,
    /// Cryptographic trust and authorization rules.
    pub trust_requirement: TrustRequirement,
    /// Optional hardware resource constraints.
    pub resource_requirement: Option<ResourceRequirement>,
    /// Whether unreachable providers may be returned for delay-tolerant store-and-forward operations.
    pub allow_dtn: bool,
    /// Optional correlation/request identifier for audit trails and telemetry.
    pub correlation_id: Option<String>,
}

impl ServiceQuery {
    /// Creates a minimal `ServiceQuery` requesting any compatible instance of `service_id`.
    pub fn new(service_id: ServiceId) -> Self {
        Self {
            service_id,
            min_version: None,
            max_version: None,
            required_capabilities: Vec::new(),
            required_protocol: None,
            locality_preference: LocalityPreference::default(),
            trust_requirement: TrustRequirement::default(),
            resource_requirement: None,
            allow_dtn: false,
            correlation_id: None,
        }
    }

    /// Returns a new fluent builder for constructing `ServiceQuery`.
    pub fn builder(service_id: ServiceId) -> ServiceQueryBuilder {
        ServiceQueryBuilder::new(service_id)
    }
}

/// Fluent builder for constructing and validating a `ServiceQuery`.
pub struct ServiceQueryBuilder {
    service_id: ServiceId,
    min_version: Option<ServiceVersion>,
    max_version: Option<ServiceVersion>,
    required_capabilities: Vec<ServiceCapability>,
    required_protocol: Option<ServiceProtocol>,
    locality_preference: LocalityPreference,
    trust_requirement: TrustRequirement,
    resource_requirement: Option<ResourceRequirement>,
    allow_dtn: bool,
    correlation_id: Option<String>,
}

impl ServiceQueryBuilder {
    pub fn new(service_id: ServiceId) -> Self {
        Self {
            service_id,
            min_version: None,
            max_version: None,
            required_capabilities: Vec::new(),
            required_protocol: None,
            locality_preference: LocalityPreference::default(),
            trust_requirement: TrustRequirement::default(),
            resource_requirement: None,
            allow_dtn: false,
            correlation_id: None,
        }
    }

    pub fn min_version(mut self, version: ServiceVersion) -> Self {
        self.min_version = Some(version);
        self
    }

    pub fn max_version(mut self, version: ServiceVersion) -> Self {
        self.max_version = Some(version);
        self
    }

    pub fn capability(mut self, cap: ServiceCapability) -> Self {
        self.required_capabilities.push(cap);
        self
    }

    pub fn capabilities(mut self, caps: Vec<ServiceCapability>) -> Self {
        self.required_capabilities = caps;
        self
    }

    pub fn protocol(mut self, protocol: ServiceProtocol) -> Self {
        self.required_protocol = Some(protocol);
        self
    }

    pub fn locality(mut self, preference: LocalityPreference) -> Self {
        self.locality_preference = preference;
        self
    }

    pub fn trust(mut self, requirement: TrustRequirement) -> Self {
        self.trust_requirement = requirement;
        self
    }

    pub fn resource(mut self, resource: ResourceRequirement) -> Self {
        self.resource_requirement = Some(resource);
        self
    }

    pub fn allow_dtn(mut self, allow: bool) -> Self {
        self.allow_dtn = allow;
        self
    }

    pub fn correlation_id(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = Some(id.into());
        self
    }

    /// Validates constraints and builds the `ServiceQuery`.
    pub fn build(self) -> Result<ServiceQuery, ServiceError> {
        if let (Some(min), Some(max)) = (self.min_version, self.max_version) {
            if min > max {
                return Err(ServiceError::InvalidQuery(format!(
                    "min_version ({}) cannot exceed max_version ({})",
                    min, max
                )));
            }
        }

        Ok(ServiceQuery {
            service_id: self.service_id,
            min_version: self.min_version,
            max_version: self.max_version,
            required_capabilities: self.required_capabilities,
            required_protocol: self.required_protocol,
            locality_preference: self.locality_preference,
            trust_requirement: self.trust_requirement,
            resource_requirement: self.resource_requirement,
            allow_dtn: self.allow_dtn,
            correlation_id: self.correlation_id,
        })
    }
}
