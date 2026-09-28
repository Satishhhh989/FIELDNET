use crate::document::DocumentId;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DataError {
    #[error("Database unavailable: {0}")]
    DatabaseUnavailable(String),

    #[error("Migration failed at version {version}: {reason}")]
    MigrationFailed { version: u32, reason: String },

    #[error(
        "Migration checksum mismatch at version {version}: expected {expected}, found {actual}"
    )]
    MigrationChecksumMismatch {
        version: u32,
        expected: String,
        actual: String,
    },

    #[error("Document not found: {0}")]
    DocumentNotFound(DocumentId),

    #[error("Document is marked as deleted (tombstone): {0}")]
    DocumentDeleted(DocumentId),

    #[error("Document already exists: {0}")]
    DocumentAlreadyExists(DocumentId),

    #[error("Invalid document ID: {0}")]
    InvalidDocumentId(String),

    #[error("Invalid document type: {0}")]
    InvalidDocumentType(String),

    #[error("Invalid document state: {0}")]
    InvalidDocumentState(String),

    #[error("Document size {size} bytes exceeds maximum limit of {max} bytes")]
    DocumentTooLarge { size: usize, max: usize },

    #[error("Serialization failure: {0}")]
    SerializationFailure(String),

    #[error("Transaction failure: {0}")]
    TransactionFailure(String),

    #[error("Storage failure: {0}")]
    StorageFailure(String),

    #[error("Lock error: {0}")]
    LockError(String),

    #[error("Corrupted database or record: {0}")]
    CorruptedData(String),

    #[error("Missing causal dependency operation: {0}")]
    MissingDependency(crate::operation::OperationId),

    #[error("CRDT error: {0}")]
    CrdtError(String),

    #[error("CRDT type mismatch: expected {expected}, found {found}")]
    TypeMismatch { expected: String, found: String },

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("Incompatible sync protocol version: expected {expected}, found {actual}")]
    IncompatibleProtocolVersion { expected: u32, actual: u32 },

    #[error("Unauthorized data operation: {0}")]
    Unauthorized(String),

    #[error("Sync protocol error: {0}")]
    SyncError(String),

    #[error("Sync session not found: {0}")]
    SessionNotFound(String),

    #[error("Sync batch size {size} exceeds limit of {max}")]
    BatchSizeExceeded { size: usize, max: usize },

    #[error("Snapshot not found: {0}")]
    SnapshotNotFound(String),

    #[error("Corrupted snapshot {snapshot_id}: {reason}")]
    CorruptedSnapshot { snapshot_id: String, reason: String },

    #[error("Compaction safety violation: {0}")]
    CompactionSafetyViolation(String),

    #[error("Recovery failed for document {0}: {1}")]
    RecoveryFailed(DocumentId, String),

    #[error("SQLite database error: {0}")]
    Sqlite(String),

    #[error("I/O error: {0}")]
    Io(String),
}

impl From<rusqlite::Error> for DataError {
    fn from(err: rusqlite::Error) -> Self {
        DataError::Sqlite(err.to_string())
    }
}

impl From<serde_json::Error> for DataError {
    fn from(err: serde_json::Error) -> Self {
        DataError::SerializationFailure(err.to_string())
    }
}

impl From<std::io::Error> for DataError {
    fn from(err: std::io::Error) -> Self {
        DataError::Io(err.to_string())
    }
}
