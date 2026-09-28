use crate::chunks::id::ChunkId;
use crate::identity::ContentId;
use crate::manifest::id::ManifestId;
use crate::providers::ProviderState;
use crate::transfer::TransferId;
use fieldnet_core::{MessageId, NodeId};

/// Authoritative event model emitted by the local Content Subsystem (Phase 07).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContentEvent {
    /// Content object was freshly written and committed to local storage.
    Created { id: ContentId, size_bytes: u64 },
    /// Content object has been verified and is available for retrieval and serving.
    Available { id: ContentId, size_bytes: u64 },
    /// Content object was deleted from local storage.
    Deleted { id: ContentId },
    /// Content object failed cryptographic integrity verification.
    IntegrityFailed {
        id: ContentId,
        expected: ContentId,
        actual: ContentId,
    },
    /// Content object on disk was detected as missing or corrupted.
    Corrupted { id: ContentId, reason: String },
    /// Content was marked pinned.
    Pinned { id: ContentId },
    /// Content was unpinned.
    Unpinned { id: ContentId },

    // BUILD 02: Chunk Events
    /// A chunk was created and written to local storage.
    ChunkCreated { chunk_id: ChunkId, size_bytes: u32 },
    /// A chunk was cryptographically verified.
    ChunkVerified { chunk_id: ChunkId, size_bytes: u32 },
    /// A chunk was rejected due to hash mismatch or corruption.
    ChunkRejected { chunk_id: ChunkId, reason: String },

    // BUILD 02: Manifest Events
    /// A content manifest was created and stored.
    ManifestCreated {
        manifest_id: ManifestId,
        content_id: ContentId,
        chunk_count: u32,
    },
    /// A manifest was fully verified (all constituent chunks present and verified).
    ManifestVerified {
        manifest_id: ManifestId,
        content_id: ContentId,
    },

    // BUILD 02: Reconstruction Events
    /// Content reconstruction was initiated.
    ReconstructionStarted {
        content_id: ContentId,
        manifest_id: ManifestId,
    },
    /// Content reconstruction succeeded and was cryptographically verified.
    ReconstructionCompleted {
        content_id: ContentId,
        manifest_id: ManifestId,
        total_bytes: u64,
    },
    /// Content reconstruction failed.
    ReconstructionFailed {
        content_id: ContentId,
        reason: String,
    },

    // BUILD 03: Provider Discovery Events
    /// A remote provider was discovered for a content object.
    ProviderDiscovered {
        content_id: ContentId,
        provider_node_id: NodeId,
        has_full_content: bool,
    },
    /// A provider's operational state or reliability was updated.
    ProviderUpdated {
        content_id: ContentId,
        provider_node_id: NodeId,
        state: ProviderState,
    },
    /// A provider record expired past its TTL.
    ProviderExpired {
        content_id: ContentId,
        provider_node_id: NodeId,
    },
    /// A provider record was explicitly invalidated or banned.
    ProviderInvalidated {
        content_id: ContentId,
        provider_node_id: NodeId,
        reason: String,
    },
    /// A content query was broadcast or dispatched to the network.
    QueryStarted {
        query_id: MessageId,
        content_id: ContentId,
    },
    /// A content query completed with discovered provider count.
    QueryCompleted {
        query_id: MessageId,
        content_id: ContentId,
        providers_found: usize,
    },
    /// A content query failed or timed out.
    QueryFailed {
        query_id: MessageId,
        content_id: ContentId,
        reason: String,
    },

    // BUILD 04: Content Transfer Engine & Resumable Transfers Events
    /// Transfer session created and registered.
    TransferCreated {
        transfer_id: TransferId,
        content_id: ContentId,
        provider: NodeId,
    },
    /// Content request dispatched to candidate provider.
    TransferRequested {
        transfer_id: TransferId,
        content_id: ContentId,
        requester: NodeId,
    },
    /// Transfer parameters and manifest negotiated successfully.
    TransferNegotiated {
        transfer_id: TransferId,
        content_id: ContentId,
        total_chunks: u32,
        total_bytes: u64,
    },
    /// Transfer active and chunk pipeline started.
    TransferStarted {
        transfer_id: TransferId,
        content_id: ContentId,
    },
    /// An individual chunk was requested from provider.
    ChunkRequested {
        transfer_id: TransferId,
        content_id: ContentId,
        chunk_index: u32,
        chunk_id: ChunkId,
    },
    /// An individual chunk payload was received over transport.
    ChunkReceived {
        transfer_id: TransferId,
        content_id: ContentId,
        chunk_index: u32,
        chunk_id: ChunkId,
        size_bytes: u32,
    },
    /// An individual chunk payload was cryptographically verified and committed.
    ChunkTransferVerified {
        transfer_id: TransferId,
        content_id: ContentId,
        chunk_index: u32,
        chunk_id: ChunkId,
    },
    /// Transfer temporarily paused.
    TransferPaused {
        transfer_id: TransferId,
        reason: String,
    },
    /// Transfer resumed from paused/interrupted state.
    TransferResumed {
        transfer_id: TransferId,
        remaining_chunks: u32,
    },
    /// Transfer explicitly cancelled.
    TransferCancelled {
        transfer_id: TransferId,
        reason: String,
    },
    /// Transfer permanently failed.
    TransferFailed {
        transfer_id: TransferId,
        reason: String,
    },
    /// Transfer successfully completed, reconstructed, and committed to CAS.
    TransferCompleted {
        transfer_id: TransferId,
        content_id: ContentId,
        total_bytes: u64,
    },
    /// Transfer expired due to inactivity or deadline.
    TransferExpired { transfer_id: TransferId },
    /// Content corrupted or final ContentId verification failed.
    TransferCorrupted {
        transfer_id: TransferId,
        content_id: ContentId,
        reason: String,
    },

    // BUILD 06: Content Integrity, Authorization, Garbage Collection & Resilience Events
    /// Content authorization was denied for a peer and action.
    AuthorizationDenied {
        content_id: ContentId,
        peer: NodeId,
        action: String,
        reason: String,
    },
    /// An integrity audit was started.
    IntegrityAuditStarted {
        content_id: Option<ContentId>,
        scope: String,
    },
    /// An integrity audit failed for a content object or chunk.
    IntegrityAuditFailed {
        content_id: ContentId,
        chunk_id: Option<ChunkId>,
        reason: String,
    },
    /// Content or chunk corruption was detected.
    CorruptionDetected {
        content_id: ContentId,
        chunk_id: Option<ChunkId>,
        reason: String,
    },
    /// Corrupted content or chunk was successfully recovered.
    CorruptionRecovered {
        content_id: ContentId,
        chunk_id: Option<ChunkId>,
        provider: Option<NodeId>,
    },
    /// Garbage collection cycle started.
    GcStarted { total_candidates: usize },
    /// An object or chunk was evicted by garbage collection.
    GcEvicted {
        content_id: Option<ContentId>,
        chunk_id: Option<ChunkId>,
        bytes_reclaimed: u64,
    },
    /// An object or chunk was skipped during garbage collection (protected/pinned).
    GcSkipped {
        content_id: Option<ContentId>,
        chunk_id: Option<ChunkId>,
        reason: String,
    },
    /// Storage usage reached warning threshold.
    QuotaWarning {
        used_bytes: u64,
        quota_bytes: u64,
        usage_permille: u32,
    },
    /// Storage quota was exceeded.
    QuotaExceeded {
        requested_bytes: u64,
        available_bytes: u64,
        quota_bytes: u64,
    },
    /// Storage space was recovered via GC or manual deletion.
    StorageRecovered { bytes_reclaimed: u64 },
    /// Startup crash recovery completed.
    RecoveryCompleted {
        recovered_objects: usize,
        recovered_chunks: usize,
        recovered_sessions: usize,
    },
}

impl ContentEvent {
    /// Returns the canonical event type topic string.
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::Created { .. } => "content.created",
            Self::Available { .. } => "content.available",
            Self::Deleted { .. } => "content.deleted",
            Self::IntegrityFailed { .. } => "content.integrity_failed",
            Self::Corrupted { .. } => "content.corrupted",
            Self::Pinned { .. } => "content.pinned",
            Self::Unpinned { .. } => "content.unpinned",
            Self::ChunkCreated { .. } => "content.chunk.created",
            Self::ChunkVerified { .. } => "content.chunk.verified",
            Self::ChunkRejected { .. } => "content.chunk.rejected",
            Self::ManifestCreated { .. } => "content.manifest.created",
            Self::ManifestVerified { .. } => "content.manifest.verified",
            Self::ReconstructionStarted { .. } => "content.reconstruction.started",
            Self::ReconstructionCompleted { .. } => "content.reconstruction.completed",
            Self::ReconstructionFailed { .. } => "content.reconstruction.failed",
            Self::ProviderDiscovered { .. } => "content.provider.discovered",
            Self::ProviderUpdated { .. } => "content.provider.updated",
            Self::ProviderExpired { .. } => "content.provider.expired",
            Self::ProviderInvalidated { .. } => "content.provider.invalidated",
            Self::QueryStarted { .. } => "content.query.started",
            Self::QueryCompleted { .. } => "content.query.completed",
            Self::QueryFailed { .. } => "content.query.failed",
            Self::TransferCreated { .. } => "content.transfer.created",
            Self::TransferRequested { .. } => "content.transfer.requested",
            Self::TransferNegotiated { .. } => "content.transfer.negotiated",
            Self::TransferStarted { .. } => "content.transfer.started",
            Self::ChunkRequested { .. } => "content.chunk.requested",
            Self::ChunkReceived { .. } => "content.chunk.received",
            Self::ChunkTransferVerified { .. } => "content.chunk.verified",
            Self::TransferPaused { .. } => "content.transfer.paused",
            Self::TransferResumed { .. } => "content.transfer.resumed",
            Self::TransferCancelled { .. } => "content.transfer.cancelled",
            Self::TransferFailed { .. } => "content.transfer.failed",
            Self::TransferCompleted { .. } => "content.transfer.completed",
            Self::TransferExpired { .. } => "content.transfer.expired",
            Self::TransferCorrupted { .. } => "content.transfer.corrupted",
            Self::AuthorizationDenied { .. } => "content.authorization.denied",
            Self::IntegrityAuditStarted { .. } => "content.integrity.audit_started",
            Self::IntegrityAuditFailed { .. } => "content.integrity.failed",
            Self::CorruptionDetected { .. } => "content.corruption.detected",
            Self::CorruptionRecovered { .. } => "content.corruption.recovered",
            Self::GcStarted { .. } => "content.gc.started",
            Self::GcEvicted { .. } => "content.gc.evicted",
            Self::GcSkipped { .. } => "content.gc.skipped",
            Self::QuotaWarning { .. } => "content.quota.warning",
            Self::QuotaExceeded { .. } => "content.quota.exceeded",
            Self::StorageRecovered { .. } => "content.storage.recovered",
            Self::RecoveryCompleted { .. } => "content.recovery.completed",
        }
    }
}
