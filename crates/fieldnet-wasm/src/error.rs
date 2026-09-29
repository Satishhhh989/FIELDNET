use thiserror::Error;

use crate::lifecycle::ApplicationState;

/// Errors arising across the FIELDNET Application, Package, and Runtime subsystem.
#[derive(Debug, Error)]
pub enum ApplicationError {
    #[error("Invalid application ID: {0}")]
    InvalidApplicationId(String),

    #[error("Invalid package ID: {0}")]
    InvalidPackageId(String),

    #[error("Invalid publisher identity: {0}")]
    InvalidPublisher(String),

    #[error("Invalid version string: {0}")]
    InvalidVersion(String),

    #[error("Invalid version requirement: {0}")]
    InvalidVersionRequirement(String),

    #[error("Invalid manifest: {0}")]
    InvalidManifest(String),

    #[error("Package integrity violation: {0}")]
    IntegrityError(String),

    #[error("Package signature verification failed: {0}")]
    SignatureError(String),

    #[error("Application permission error: {0}")]
    PermissionError(String),

    #[error("Resource limits error: {0}")]
    ResourceLimitsError(String),

    #[error("Application dependency unsatisfied: {0}")]
    DependencyError(String),

    #[error("Invalid lifecycle transition from {from:?} to {to:?}")]
    InvalidLifecycleTransition {
        from: ApplicationState,
        to: ApplicationState,
    },

    #[error("Application already installed: {0}")]
    AlreadyInstalled(String),

    #[error("Application not found: {0}")]
    NotFound(String),

    #[error("Package rollback error: {0}")]
    RollbackError(String),

    #[error("Application registry database error: {0}")]
    DatabaseError(String),

    #[error("Content-addressed storage error: {0}")]
    ContentError(String),

    #[error("Serialization / JSON error: {0}")]
    SerializationError(String),

    #[error("I/O error: {0}")]
    IoError(String),

    #[error("Invalid execution ID: {0}")]
    InvalidExecutionId(String),

    #[error("Application execution error: {0}")]
    ExecutionError(String),

    #[error("WebAssembly trap: {trap} (reason: {reason})")]
    TrapError { trap: String, reason: String },

    #[error("CPU fuel limit exhausted")]
    FuelExhausted,

    #[error("Memory limit exceeded: {0}")]
    MemoryLimitExceeded(String),

    #[error("Execution timed out after {max_ms} ms")]
    ExecutionTimeout { max_ms: u64 },

    #[error("Execution cancelled: {0}")]
    ExecutionCancelled(String),

    #[error("Application concurrency limit reached (current: {current}, max: {max})")]
    ConcurrencyLimitReached { current: usize, max: usize },

    #[error("WASM entrypoint '{0}' not found in module exports")]
    EntrypointNotFound(String),

    #[error("Application is not authorized to execute (current state: {current_state})")]
    ApplicationNotAuthorized { current_state: ApplicationState },

    #[error("WASM module compilation error: {0}")]
    CompilationError(String),

    #[error("Execution instance '{0}' not found")]
    ExecutionNotFound(String),
}

impl From<rusqlite::Error> for ApplicationError {
    fn from(err: rusqlite::Error) -> Self {
        Self::DatabaseError(err.to_string())
    }
}

impl From<serde_json::Error> for ApplicationError {
    fn from(err: serde_json::Error) -> Self {
        Self::SerializationError(err.to_string())
    }
}

impl From<std::io::Error> for ApplicationError {
    fn from(err: std::io::Error) -> Self {
        Self::IoError(err.to_string())
    }
}

impl From<fieldnet_content::error::ContentError> for ApplicationError {
    fn from(err: fieldnet_content::error::ContentError) -> Self {
        Self::ContentError(err.to_string())
    }
}
