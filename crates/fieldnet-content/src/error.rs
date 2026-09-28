use crate::identity::ContentId;
use crate::transfer::TransferId;
use fieldnet_core::NodeId;
use thiserror::Error;

/// Authoritative error model for FIELDNET Content Storage and Identity (Phase 07).
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum ContentError {
    #[error("Content not found: {0}")]
    ContentNotFound(ContentId),

    #[error("Content already exists: {0}")]
    AlreadyExists(ContentId),

    #[error("Invalid ContentId: {0}")]
    InvalidContentId(String),

    #[error("Integrity check failed for {expected}: actual computed hash was {actual}")]
    IntegrityFailure {
        expected: ContentId,
        actual: ContentId,
    },

    #[error("Storage failure: {0}")]
    StorageFailure(String),

    #[error("Metadata failure: {0}")]
    MetadataFailure(String),

    #[error("Invalid state transition: expected {expected}, actual {actual}")]
    InvalidState { expected: String, actual: String },

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Content corrupted: {id} - {reason}")]
    ContentCorrupted { id: ContentId, reason: String },

    #[error("Cannot delete pinned content: {0}")]
    PinnedContent(ContentId),

    #[error("Cannot delete content with active references: {0}")]
    ActiveReference(ContentId),

    #[error("Chunk not found: {0}")]
    ChunkNotFound(crate::chunks::ChunkId),

    #[error("Chunk corrupted: {id} - {reason}")]
    ChunkCorrupted {
        id: crate::chunks::ChunkId,
        reason: String,
    },

    #[error("Chunk integrity failure for {expected}: actual computed hash was {actual}")]
    ChunkIntegrityFailure {
        expected: crate::chunks::ChunkId,
        actual: crate::chunks::ChunkId,
    },

    #[error("Manifest not found: {0}")]
    ManifestNotFound(crate::manifest::ManifestId),

    #[error("Manifest invalid: {0}")]
    ManifestInvalid(String),

    #[error("Unsupported chunking algorithm: {0}")]
    UnsupportedChunking(String),

    #[error("Missing chunk at index {index}: {chunk_id}")]
    MissingChunk {
        index: u32,
        chunk_id: crate::chunks::ChunkId,
    },

    #[error("Reconstruction failed: {0}")]
    ReconstructionFailed(String),

    #[error("Manifest integrity failure: {0}")]
    ManifestIntegrityFailure(String),

    #[error("Provider not found for content {content_id}: {provider}")]
    ProviderNotFound {
        content_id: ContentId,
        provider: NodeId,
    },

    #[error("Provider expired: {content_id} at {provider}")]
    ProviderExpired {
        content_id: ContentId,
        provider: NodeId,
    },

    #[error("Provider banned: {content_id} at {provider}")]
    ProviderBanned {
        content_id: ContentId,
        provider: NodeId,
    },

    #[error("Invalid advertisement: {0}")]
    InvalidAdvertisement(String),

    #[error("Advertisement expired: created {created_at}, expires {expires_at}, now {now}")]
    AdvertisementExpired {
        created_at: u64,
        expires_at: u64,
        now: u64,
    },

    #[error("Query expired: created {created_at}, expires {expires_at}, now {now}")]
    QueryExpired {
        created_at: u64,
        expires_at: u64,
        now: u64,
    },

    #[error("Registry capacity exceeded: {0}")]
    RegistryCapacityExceeded(String),

    // BUILD 04: Transfer Engine Errors
    #[error("Transfer not found: {0}")]
    TransferNotFound(TransferId),

    #[error("Transfer cancelled: {0}")]
    TransferCancelled(TransferId),

    #[error("Transfer expired: {0}")]
    TransferExpired(TransferId),

    #[error("Transfer corrupted: {transfer_id} - {reason}")]
    TransferCorrupted {
        transfer_id: TransferId,
        reason: String,
    },

    #[error("Chunk verification failed for transfer {transfer_id}, chunk {chunk_index}: expected {expected}, actual {actual}")]
    ChunkVerificationFailed {
        transfer_id: TransferId,
        chunk_index: u32,
        expected: crate::chunks::ChunkId,
        actual: crate::chunks::ChunkId,
    },

    #[error("Transfer capacity exceeded: {0}")]
    TransferCapacityExceeded(String),

    #[error("Transfer timeout: {0}")]
    TransferTimeout(String),

    #[error("I/O error: {0}")]
    IOError(String),

    #[error("Database error: {0}")]
    DatabaseError(String),

    #[error("Lock error: {0}")]
    LockError(String),

    // BUILD 06: Content Integrity, Authorization, Garbage Collection & Resilience Errors
    #[error("Authorization denied: {0}")]
    AuthorizationDenied(String),

    #[error("Capability token expired: expired at {expired_at}, now {now}")]
    TokenExpired { expired_at: u64, now: u64 },

    #[error("Invalid capability token: {0}")]
    InvalidToken(String),

    #[error("Storage quota exceeded: requested {requested_bytes} bytes, available {available_bytes} bytes, quota {quota_bytes} bytes")]
    QuotaExceeded {
        requested_bytes: u64,
        available_bytes: u64,
        quota_bytes: u64,
    },

    #[error("Storage full: {0}")]
    StorageFull(String),

    #[error("Orphan detected: {id} - {reason}")]
    OrphanDetected { id: String, reason: String },

    #[error("Integrity audit failed: {0}")]
    AuditFailed(String),

    #[error("Resource limit exceeded: {0}")]
    ResourceLimitExceeded(String),
}

impl From<std::io::Error> for ContentError {
    fn from(err: std::io::Error) -> Self {
        ContentError::IOError(err.to_string())
    }
}

impl From<rusqlite::Error> for ContentError {
    fn from(err: rusqlite::Error) -> Self {
        ContentError::DatabaseError(err.to_string())
    }
}

impl From<serde_json::Error> for ContentError {
    fn from(err: serde_json::Error) -> Self {
        ContentError::MetadataFailure(err.to_string())
    }
}

impl From<hex::FromHexError> for ContentError {
    fn from(err: hex::FromHexError) -> Self {
        ContentError::InvalidContentId(format!("Hex decode error: {}", err))
    }
}
