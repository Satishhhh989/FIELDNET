//! FIELDNET Content-Addressed Storage, Chunk Store, Manifests & Verification (Phase 07).
//!
//! Provides content identity (`ContentId`), chunk identity (`ChunkId`), manifest identity (`ManifestId`),
//! streaming SHA-256 hashing, persistent sharded CAS (`ContentStore`), chunk storage (`ChunkStore`),
//! content manifests (`ContentManifest`, `ManifestStore`), Merkle tree verification,
//! partial availability bitmaps (`ChunkAvailabilityBitmap`), content reconstruction,
//! provider discovery (`ProviderRegistry`), and distributed transfer engine (`TransferManager`).

pub mod auth;
pub mod chunks;
pub mod error;
pub mod events;
pub mod gc;
pub mod hashing;
pub mod identity;
pub mod integrity;
pub mod manifest;
pub mod metadata;
pub mod providers;
pub mod quota;
pub mod reconstruction;
pub mod recovery;
pub mod store;
pub mod transfer;

pub use auth::{CapabilityToken, ContentAction, ContentAuthorizer};
pub use chunks::{
    ChunkDescriptor, ChunkId, ChunkMetadata, ChunkStorageState, ChunkStore, Chunker,
    FixedSizeChunker, CHUNKING_ALGORITHM_FIXED_256K, CHUNK_ID_BYTES_LEN, CHUNK_ID_HEX_LEN,
    DEFAULT_CHUNK_SIZE, MAX_CHUNK_SIZE, MIN_CHUNK_SIZE,
};
pub use error::ContentError;
pub use events::ContentEvent;
pub use gc::{
    ActiveTransferProtector, ContentGarbageCollector, EmptyTransferProtector, EvictionCandidate,
    GcReport, StorageTier,
};
pub use hashing::{
    hash_and_copy_stream, hash_bytes, hash_reader, hash_stream, ContentHasher, STREAM_BUFFER_SIZE,
};
pub use identity::{ContentId, CONTENT_ID_BYTES_LEN, CONTENT_ID_HEX_LEN};
pub use integrity::{
    ContentAuditReport, ContentVerifier, CorruptionRecoveryEngine, IntegrityAuditor,
};
pub use manifest::{
    compute_merkle_root, ChunkAvailabilityBitmap, ContentManifest, ManifestId, ManifestStore,
    ManifestValidator, CURRENT_MANIFEST_SCHEMA_VERSION, MANIFEST_ID_BYTES_LEN, MANIFEST_ID_HEX_LEN,
};
pub use metadata::{ContentMetadata, ContentStorageState};
pub use providers::{
    ContentAdvertise, ContentMessage, ContentQuery, ContentResolution, ContentResolver,
    ProviderRecord, ProviderRegistry, ProviderRegistryConfig, ProviderState, TrustTier,
    MAX_ADVERTISEMENT_BYTES, MAX_CONTENT_MESSAGE_SIZE, MAX_QUERY_BYTES,
};
pub use quota::{
    QuotaConfig, QuotaStatus, ResourceLimits, StorageQuotaManager, StorageReservation,
};
pub use reconstruction::ContentReconstructor;
pub use recovery::{
    CrashRecoveryEngine, CrashRecoveryReport, OrphanClassification, OrphanDetector, OrphanItem,
};
pub use store::{ContentLock, ContentStore, PutOptions, PutResult};
pub use transfer::{
    AckStatus, ChunkScheduler, ContentAck, ContentChunk, ContentRequest, ContentResponse,
    ProviderResponse, SchedulerConfig, TransferCancel, TransferComplete, TransferConfig,
    TransferError, TransferErrorCode, TransferId, TransferManager, TransferPersistence,
    TransferProviderService, TransferResponseStatus, TransferSession, TransferState,
};
