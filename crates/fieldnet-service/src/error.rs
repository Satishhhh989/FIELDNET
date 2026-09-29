use fieldnet_core::NodeId;
use thiserror::Error;

/// Typed error representation for the FIELDNET Service subsystem (Phase 08).
#[derive(Debug, Error)]
pub enum ServiceError {
    #[error("Invalid ServiceId: {0}")]
    InvalidServiceId(String),

    #[error("Invalid ServiceInstanceId: {0}")]
    InvalidInstanceId(String),

    #[error("Invalid ServiceVersion: {0}")]
    InvalidVersion(String),

    #[error("Invalid ServiceEndpoint: {0}")]
    InvalidEndpoint(String),

    #[error("Invalid ServiceCapability: {0}")]
    InvalidCapability(String),

    #[error("Manifest is expired: now={now}, expires_at={expires_at}")]
    InvalidExpiration { now: u64, expires_at: u64 },

    #[error("Invalid manifest signature: {0}")]
    InvalidSignature(String),

    #[error("Unsupported manifest schema version: {0}")]
    UnsupportedSchemaVersion(u32),

    #[error("Provider public key does not match provider NodeId (expected {expected}, got {got})")]
    MismatchedProviderKey { expected: NodeId, got: NodeId },

    #[error(
        "Conflicting provider for instance '{instance_id}': already owned by {existing_provider}, claimed by {claimed_provider}"
    )]
    ConflictingProvider {
        instance_id: String,
        existing_provider: NodeId,
        claimed_provider: NodeId,
    },

    #[error(
        "Stale or duplicate sequence for instance '{instance_id}': current={current_sequence}, incoming={incoming_sequence}"
    )]
    StaleSequence {
        instance_id: String,
        current_sequence: u64,
        incoming_sequence: u64,
    },

    #[error("Illegal service state transition: from '{from}' to '{to}'")]
    InvalidStateTransition { from: String, to: String },

    #[error("Service instance '{0}' not found in registry")]
    InstanceNotFound(String),

    #[error("Service '{0}' not found in registry")]
    ServiceNotFound(String),

    #[error("No compatible version for service '{service_id}': requested min={requested_min:?}, max={requested_max:?}")]
    NoCompatibleVersion {
        service_id: String,
        requested_min: Option<String>,
        requested_max: Option<String>,
    },

    #[error("Missing required capabilities for service '{service_id}': missing {missing:?}")]
    MissingCapability {
        service_id: String,
        missing: Vec<String>,
    },

    #[error("Unsupported protocol for service '{service_id}': requested '{requested}'")]
    UnsupportedProtocol {
        service_id: String,
        requested: String,
    },

    #[error("Unauthorized access to service '{service_id}': {reason}")]
    Unauthorized { service_id: String, reason: String },

    #[error("No reachable provider route for service '{service_id}' ({candidate_count} candidate(s) unreachable)")]
    NoReachableProvider {
        service_id: String,
        candidate_count: usize,
    },

    #[error(
        "All candidate providers for service '{service_id}' are unavailable ({count} instance(s))"
    )]
    AllProvidersUnavailable { service_id: String, count: usize },

    #[error("Invalid service query: {0}")]
    InvalidQuery(String),

    #[error("Service resolution failed: {0}")]
    ResolutionFailed(String),

    #[error("Registry capacity limit exceeded: {0}")]
    CapacityExceeded(String),

    #[error("Persistence error: {0}")]
    PersistenceError(String),

    #[error("Internal concurrency lock error: {0}")]
    LockError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("SQLite database error: {0}")]
    DatabaseError(#[from] rusqlite::Error),

    #[error("JSON serialization/deserialization error: {0}")]
    JsonError(#[from] serde_json::Error),
}
