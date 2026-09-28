use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, error, info, warn};

use fieldnet_core::{NodeId, Priority, Timestamp};

use super::persistence::TransferPersistence;
use super::protocol::{
    AckStatus, ContentAck, ContentChunk, ContentRequest, ContentResponse, TransferId,
    TransferResponseStatus,
};
use super::provider::TransferProviderService;
use super::scheduler::{ChunkScheduler, SchedulerConfig};
use super::session::TransferSession;
use super::state::TransferState;
use crate::chunks::store::ChunkStore;
use crate::error::ContentError;
use crate::events::ContentEvent;
use crate::hashing::hash_bytes;
use crate::identity::ContentId;
use crate::manifest::store::ManifestStore;
use crate::manifest::validator::ManifestValidator;
use crate::metadata::ContentMetadata;
use crate::providers::ProviderRegistry;
use crate::reconstruction::ContentReconstructor;
use crate::store::{ContentStore, PutOptions};

/// Configuration options for the content transfer manager.
#[derive(Clone, Debug)]
pub struct TransferConfig {
    /// Maximum concurrent transfer sessions.
    pub max_concurrent_transfers: usize,
    /// Maximum in-flight chunks per session (backpressure window).
    pub max_in_flight_chunks: usize,
    /// Maximum in-flight bytes per session.
    pub max_in_flight_bytes: usize,
    /// Per-chunk timeout in seconds.
    pub chunk_timeout_secs: u64,
    /// Overall transfer session timeout in seconds.
    pub session_timeout_secs: u64,
    /// Idle timeout in seconds before pausing or failing an inactive transfer.
    pub idle_timeout_secs: u64,
    /// Maximum transfer-level retry attempts before marking FAILED.
    pub max_retries: u32,
    /// Maximum retry attempts for a single chunk before failing.
    pub max_chunk_retries: u8,
    /// Maximum chunks served per request on the provider side.
    pub max_serving_chunks_per_req: usize,
}

impl Default for TransferConfig {
    fn default() -> Self {
        Self {
            max_concurrent_transfers: 16,
            max_in_flight_chunks: 4,
            max_in_flight_bytes: 1024 * 1024,
            chunk_timeout_secs: 10,
            session_timeout_secs: 300,
            idle_timeout_secs: 60,
            max_retries: 3,
            max_chunk_retries: 3,
            max_serving_chunks_per_req: 8,
        }
    }
}

/// Central distributed content transfer engine for FIELDNET (Phase 07 Build 04).
pub struct TransferManager {
    local_node_id: NodeId,
    content_store: Arc<ContentStore>,
    chunk_store: Arc<ChunkStore>,
    manifest_store: Arc<ManifestStore>,
    provider_registry: Arc<ProviderRegistry>,
    persistence: Arc<TransferPersistence>,
    scheduler: ChunkScheduler,
    provider_service: TransferProviderService,
    sessions: Arc<RwLock<HashMap<TransferId, TransferSession>>>,
    event_tx: broadcast::Sender<ContentEvent>,
    config: TransferConfig,
}

impl TransferManager {
    /// Initializes and opens the `TransferManager`, recovering active sessions from SQLite.
    pub fn open(
        local_node_id: NodeId,
        content_store: Arc<ContentStore>,
        conn: Arc<Mutex<rusqlite::Connection>>,
        event_tx: broadcast::Sender<ContentEvent>,
        config: TransferConfig,
    ) -> Result<Arc<Self>, ContentError> {
        let persistence = Arc::new(TransferPersistence::new(conn)?);
        let chunk_store = Arc::clone(content_store.chunk_store());
        let manifest_store = Arc::clone(content_store.manifest_store());
        let provider_registry = Arc::clone(content_store.provider_registry());

        let scheduler = ChunkScheduler::new(SchedulerConfig {
            max_in_flight_chunks: config.max_in_flight_chunks,
            max_in_flight_bytes: config.max_in_flight_bytes,
            max_chunk_retries: config.max_chunk_retries,
        });

        let provider_service = TransferProviderService::new(
            Arc::clone(&content_store),
            Arc::clone(&chunk_store),
            Arc::clone(&manifest_store),
            config.max_serving_chunks_per_req,
        );

        // Recover active (incomplete) transfer sessions from persistent storage
        let active_sessions = persistence.load_active()?;
        let mut session_map = HashMap::new();
        for mut session in active_sessions {
            // Restore cached manifest if present
            if let Some(mid) = session.manifest_id {
                if let Ok(manifest) = manifest_store.get(&mid) {
                    session.manifest = Some(manifest);
                }
            }
            info!(
                transfer_id = %session.transfer_id,
                content_id = %session.content_id,
                status = %session.status,
                verified_chunks = session.verified_chunks.len(),
                "Recovered transfer session from persistent storage"
            );
            session_map.insert(session.transfer_id, session);
        }

        Ok(Arc::new(Self {
            local_node_id,
            content_store,
            chunk_store,
            manifest_store,
            provider_registry,
            persistence,
            scheduler,
            provider_service,
            sessions: Arc::new(RwLock::new(session_map)),
            event_tx,
            config,
        }))
    }

    /// Accessor for the provider service.
    pub fn provider_service(&self) -> &TransferProviderService {
        &self.provider_service
    }

    /// Accessor for the underlying ContentStore.
    pub fn content_store(&self) -> &Arc<ContentStore> {
        &self.content_store
    }

    /// Accessor for the underlying ChunkStore.
    pub fn chunk_store(&self) -> &Arc<ChunkStore> {
        &self.chunk_store
    }

    /// Accessor for the underlying ManifestStore.
    pub fn manifest_store(&self) -> &Arc<ManifestStore> {
        &self.manifest_store
    }

    /// Initiates a content transfer request for `content_id`.
    ///
    /// Resolves provider from `ProviderRegistry` if not explicitly supplied.
    pub async fn create_transfer(
        &self,
        content_id: ContentId,
        target_provider: Option<NodeId>,
        priority: Priority,
    ) -> Result<(TransferId, NodeId, ContentRequest), ContentError> {
        // Check if content already exists locally
        if self.content_store.has(&content_id)? {
            return Err(ContentError::AlreadyExists(content_id));
        }

        // Enforce concurrency limit
        {
            let sessions = self.sessions.read().await;
            let active_count = sessions.values().filter(|s| s.status.is_active()).count();
            if active_count >= self.config.max_concurrent_transfers {
                return Err(ContentError::TransferCapacityExceeded(format!(
                    "Active transfers limit exceeded: {} >= {}",
                    active_count, self.config.max_concurrent_transfers
                )));
            }
        }

        // Resolve candidate provider
        let provider_node_id = match target_provider {
            Some(p) => p,
            None => {
                let candidates = self.provider_registry.find_providers(&content_id);
                let Some(best) = candidates.first() else {
                    return Err(ContentError::ProviderNotFound {
                        content_id,
                        provider: self.local_node_id,
                    });
                };
                best.provider_node_id
            }
        };

        let transfer_id = TransferId::random();
        let mut session = TransferSession::new(
            transfer_id,
            content_id,
            self.local_node_id,
            provider_node_id,
            priority,
            self.config.session_timeout_secs,
        );

        session.transition_to(TransferState::Requested)?;
        self.persistence.save(&session)?;

        {
            let mut sessions = self.sessions.write().await;
            sessions.insert(transfer_id, session.clone());
        }

        let _ = self.event_tx.send(ContentEvent::TransferCreated {
            transfer_id,
            content_id,
            provider: provider_node_id,
        });
        let _ = self.event_tx.send(ContentEvent::TransferRequested {
            transfer_id,
            content_id,
            requester: self.local_node_id,
        });

        info!(
            transfer_id = %transfer_id,
            content_id = %content_id,
            provider = %provider_node_id,
            "Created and dispatched content transfer request"
        );

        let now = Timestamp::now().as_millis();
        let request = ContentRequest {
            transfer_id,
            content_id,
            manifest_id: None,
            requested_chunks: Vec::new(), // Initial request for manifest
            requester_node_id: self.local_node_id,
            max_window_bytes: self.config.max_in_flight_bytes as u32,
            priority: priority_to_u8(priority),
            created_at: now,
            expires_at: now + (self.config.session_timeout_secs * 1000),
            auth_token: None,
        };

        Ok((transfer_id, provider_node_id, request))
    }

    /// Handles an incoming `ContentResponse` (manifest negotiation response from provider).
    pub async fn handle_response(
        &self,
        resp: ContentResponse,
    ) -> Result<Option<ContentRequest>, ContentError> {
        let mut sessions = self.sessions.write().await;
        let Some(session) = sessions.get_mut(&resp.transfer_id) else {
            debug!(
                transfer_id = %resp.transfer_id,
                "Received ContentResponse for unknown or terminated transfer"
            );
            return Ok(None);
        };

        match resp.status {
            TransferResponseStatus::Accepted => {
                let Some(manifest) = resp.manifest else {
                    let err_msg = "Accepted ContentResponse missing manifest".to_string();
                    session.transition_to(TransferState::Failed)?;
                    session.error_message = Some(err_msg.clone());
                    self.persistence.save(session)?;
                    let _ = self.event_tx.send(ContentEvent::TransferFailed {
                        transfer_id: resp.transfer_id,
                        reason: err_msg,
                    });
                    return Err(ContentError::ManifestInvalid(
                        "Missing manifest in response".to_string(),
                    ));
                };

                // Validate manifest cryptographically
                if let Err(e) = ManifestValidator::validate(&manifest) {
                    warn!(
                        transfer_id = %resp.transfer_id,
                        content_id = %manifest.content_id,
                        error = %e,
                        "Received invalid manifest from provider"
                    );
                    session.transition_to(TransferState::Corrupted)?;
                    session.error_message = Some(e.to_string());
                    self.persistence.save(session)?;
                    let _ = self.event_tx.send(ContentEvent::TransferCorrupted {
                        transfer_id: resp.transfer_id,
                        content_id: manifest.content_id,
                        reason: e.to_string(),
                    });
                    return Err(e);
                }

                // Store manifest locally
                let _ = self.manifest_store.put(&manifest);

                // Update session state
                let _ = session.transition_to(TransferState::Negotiating);
                session.set_manifest(manifest.clone());
                session.transition_to(TransferState::Transferring)?;

                // Check if any chunks are already present locally in ChunkStore
                for chunk_desc in &manifest.chunks {
                    if self.chunk_store.has(&chunk_desc.chunk_id)? {
                        session.mark_chunk_verified(chunk_desc.index, chunk_desc.size);
                    }
                }

                self.persistence.save(session)?;

                let _ = self.event_tx.send(ContentEvent::TransferNegotiated {
                    transfer_id: session.transfer_id,
                    content_id: session.content_id,
                    total_chunks: manifest.chunk_count,
                    total_bytes: manifest.total_size,
                });
                let _ = self.event_tx.send(ContentEvent::TransferStarted {
                    transfer_id: session.transfer_id,
                    content_id: session.content_id,
                });

                // If already complete (all chunks deduplicated!), complete immediately
                if session.is_complete() {
                    info!(
                        transfer_id = %session.transfer_id,
                        "All chunks already present locally (deduplication applied); completing transfer"
                    );
                    drop(sessions);
                    self.finalize_complete_transfer(resp.transfer_id).await?;
                    return Ok(None);
                }

                // Schedule first batch of chunks within backpressure window
                let next_chunks = self.scheduler.schedule_next_chunks(session);
                for &idx in &next_chunks {
                    session.mark_chunk_in_flight(idx);
                }
                self.persistence.save(session)?;

                if !next_chunks.is_empty() {
                    for &idx in &next_chunks {
                        if let Some(desc) = manifest.chunks.iter().find(|c| c.index == idx) {
                            let _ = self.event_tx.send(ContentEvent::ChunkRequested {
                                transfer_id: session.transfer_id,
                                content_id: session.content_id,
                                chunk_index: idx,
                                chunk_id: desc.chunk_id,
                            });
                        }
                    }

                    let now = Timestamp::now().as_millis();
                    let chunk_req = ContentRequest {
                        transfer_id: session.transfer_id,
                        content_id: session.content_id,
                        manifest_id: Some(manifest.manifest_id),
                        requested_chunks: next_chunks,
                        requester_node_id: self.local_node_id,
                        max_window_bytes: self.config.max_in_flight_bytes as u32,
                        priority: priority_to_u8(session.priority),
                        created_at: now,
                        expires_at: session.expires_at,
                        auth_token: None,
                    };
                    return Ok(Some(chunk_req));
                }

                Ok(None)
            }
            TransferResponseStatus::NotFound
            | TransferResponseStatus::Unauthorized
            | TransferResponseStatus::Rejected => {
                let reason = resp
                    .error_message
                    .unwrap_or_else(|| format!("Provider returned status {:?}", resp.status));
                warn!(
                    transfer_id = %resp.transfer_id,
                    reason = %reason,
                    "Content transfer rejected by provider"
                );
                session.transition_to(TransferState::Failed)?;
                session.error_message = Some(reason.clone());
                self.persistence.save(session)?;
                let _ = self.event_tx.send(ContentEvent::TransferFailed {
                    transfer_id: resp.transfer_id,
                    reason: reason.clone(),
                });
                Err(ContentError::PermissionDenied(reason))
            }
            TransferResponseStatus::CapacityExceeded => {
                warn!(
                    transfer_id = %resp.transfer_id,
                    "Provider capacity exceeded; pausing transfer"
                );
                session.transition_to(TransferState::Paused)?;
                self.persistence.save(session)?;
                let _ = self.event_tx.send(ContentEvent::TransferPaused {
                    transfer_id: resp.transfer_id,
                    reason: "Provider capacity exceeded".to_string(),
                });
                Ok(None)
            }
        }
    }

    /// Handles an incoming `ContentChunk` from a provider.
    ///
    /// Verifies cryptographic integrity: `hash_bytes(&chunk.data) == chunk.chunk_id`.
    /// If valid, commits chunk to `ChunkStore`, updates persistent state, and schedules next chunks.
    pub async fn handle_chunk(
        &self,
        chunk: ContentChunk,
    ) -> Result<(ContentAck, Option<ContentRequest>, bool), ContentError> {
        let mut sessions = self.sessions.write().await;
        let Some(session) = sessions.get_mut(&chunk.transfer_id) else {
            let ack = ContentAck {
                transfer_id: chunk.transfer_id,
                chunk_index: chunk.chunk_index,
                chunk_id: chunk.chunk_id,
                status: AckStatus::Failed,
                bytes_received: 0,
            };
            return Ok((ack, None, false));
        };

        if session.status != TransferState::Transferring
            && session.status != TransferState::Negotiating
        {
            debug!(
                transfer_id = %chunk.transfer_id,
                chunk_index = chunk.chunk_index,
                status = ?session.status,
                "Ignoring chunk for non-transferring session"
            );
            let ack = ContentAck {
                transfer_id: chunk.transfer_id,
                chunk_index: chunk.chunk_index,
                chunk_id: chunk.chunk_id,
                status: AckStatus::Failed,
                bytes_received: 0,
            };
            return Ok((ack, None, false));
        }

        // Cryptographic integrity verification: verify bytes match expected ChunkId
        let computed_chunk_id =
            crate::chunks::ChunkId::from_bytes(*hash_bytes(&chunk.data).as_bytes());
        if computed_chunk_id != chunk.chunk_id {
            warn!(
                transfer_id = %chunk.transfer_id,
                chunk_index = chunk.chunk_index,
                expected = %chunk.chunk_id,
                computed = %computed_chunk_id,
                "Chunk cryptographic verification failed; discarding chunk"
            );
            session.mark_chunk_failed(chunk.chunk_index);
            self.persistence.save(session)?;
            let _ = self.event_tx.send(ContentEvent::ChunkRejected {
                chunk_id: chunk.chunk_id,
                reason: format!(
                    "Hash mismatch: expected {}, got {}",
                    chunk.chunk_id, computed_chunk_id
                ),
            });

            let ack = ContentAck {
                transfer_id: chunk.transfer_id,
                chunk_index: chunk.chunk_index,
                chunk_id: chunk.chunk_id,
                status: AckStatus::Failed,
                bytes_received: chunk.data.len() as u32,
            };
            return Ok((ack, None, false));
        }

        // Commit verified chunk to ChunkStore
        self.chunk_store.put(&chunk.data).await?;

        // Update session progress
        session.mark_chunk_verified(chunk.chunk_index, chunk.data.len() as u32);
        self.persistence.save(session)?;

        let _ = self.event_tx.send(ContentEvent::ChunkReceived {
            transfer_id: session.transfer_id,
            content_id: session.content_id,
            chunk_index: chunk.chunk_index,
            chunk_id: chunk.chunk_id,
            size_bytes: chunk.data.len() as u32,
        });
        let _ = self.event_tx.send(ContentEvent::ChunkTransferVerified {
            transfer_id: session.transfer_id,
            content_id: session.content_id,
            chunk_index: chunk.chunk_index,
            chunk_id: chunk.chunk_id,
        });

        let ack = ContentAck {
            transfer_id: session.transfer_id,
            chunk_index: chunk.chunk_index,
            chunk_id: chunk.chunk_id,
            status: AckStatus::Verified,
            bytes_received: chunk.data.len() as u32,
        };

        // Check if all chunks have been received and verified
        if session.is_complete() {
            info!(
                transfer_id = %session.transfer_id,
                content_id = %session.content_id,
                verified_chunks = session.verified_chunks.len(),
                "All chunks verified; beginning final content reconstruction"
            );
            drop(sessions);
            self.finalize_complete_transfer(chunk.transfer_id).await?;
            return Ok((ack, None, true));
        }

        // Schedule next chunks within sliding window
        let next_chunks = self.scheduler.schedule_next_chunks(session);
        for &idx in &next_chunks {
            session.mark_chunk_in_flight(idx);
        }
        self.persistence.save(session)?;

        let next_req = if !next_chunks.is_empty() {
            if let Some(manifest) = &session.manifest {
                for &idx in &next_chunks {
                    if let Some(desc) = manifest.chunks.iter().find(|c| c.index == idx) {
                        let _ = self.event_tx.send(ContentEvent::ChunkRequested {
                            transfer_id: session.transfer_id,
                            content_id: session.content_id,
                            chunk_index: idx,
                            chunk_id: desc.chunk_id,
                        });
                    }
                }
            }

            let now = Timestamp::now().as_millis();
            Some(ContentRequest {
                transfer_id: session.transfer_id,
                content_id: session.content_id,
                manifest_id: session.manifest_id,
                requested_chunks: next_chunks,
                requester_node_id: self.local_node_id,
                max_window_bytes: self.config.max_in_flight_bytes as u32,
                priority: priority_to_u8(session.priority),
                created_at: now,
                expires_at: session.expires_at,
                auth_token: None,
            })
        } else {
            None
        };

        Ok((ack, next_req, false))
    }

    /// Finalizes a completed transfer: reconstructs full content, validates final `ContentId`, commits to CAS.
    pub async fn finalize_complete_transfer(
        &self,
        transfer_id: TransferId,
    ) -> Result<ContentMetadata, ContentError> {
        let (content_id, manifest) = {
            let mut sessions = self.sessions.write().await;
            let session = sessions
                .get_mut(&transfer_id)
                .ok_or(ContentError::TransferNotFound(transfer_id))?;
            session.transition_to(TransferState::Verifying)?;
            self.persistence.save(session)?;
            let manifest = session.manifest.clone().ok_or_else(|| {
                ContentError::ReconstructionFailed(
                    "Manifest missing during finalization".to_string(),
                )
            })?;
            (session.content_id, manifest)
        };

        // Reconstruct content bytes and verify Merkle root and constituent chunks
        let reconstructed_bytes = match ContentReconstructor::reconstruct_to_bytes(
            &manifest,
            &self.chunk_store,
            Some(&self.event_tx),
        )
        .await
        {
            Ok(bytes) => bytes,
            Err(e) => {
                error!(
                    transfer_id = %transfer_id,
                    content_id = %content_id,
                    error = %e,
                    "Final content reconstruction failed"
                );
                let mut sessions = self.sessions.write().await;
                if let Some(session) = sessions.get_mut(&transfer_id) {
                    session.transition_to(TransferState::Corrupted)?;
                    session.error_message = Some(e.to_string());
                    let _ = self.persistence.save(session);
                }
                let _ = self.event_tx.send(ContentEvent::TransferCorrupted {
                    transfer_id,
                    content_id,
                    reason: e.to_string(),
                });
                return Err(e);
            }
        };

        // Verify computed ContentId matches expected
        let computed_content_id = hash_bytes(&reconstructed_bytes);
        if computed_content_id != content_id {
            error!(
                transfer_id = %transfer_id,
                expected = %content_id,
                computed = %computed_content_id,
                "Final ContentId cryptographic mismatch on reconstructed object"
            );
            let mut sessions = self.sessions.write().await;
            if let Some(session) = sessions.get_mut(&transfer_id) {
                session.transition_to(TransferState::Corrupted)?;
                let _ = self.persistence.save(session);
            }
            let _ = self.event_tx.send(ContentEvent::TransferCorrupted {
                transfer_id,
                content_id,
                reason: format!(
                    "Final hash mismatch: expected {}, got {}",
                    content_id, computed_content_id
                ),
            });
            return Err(ContentError::IntegrityFailure {
                expected: content_id,
                actual: computed_content_id,
            });
        }

        // Commit content to ContentStore CAS
        let put_result = self
            .content_store
            .put(&reconstructed_bytes, Some(PutOptions::default()))
            .await?;

        // Update session to Complete
        {
            let mut sessions = self.sessions.write().await;
            if let Some(session) = sessions.get_mut(&transfer_id) {
                session.transition_to(TransferState::Complete)?;
                self.persistence.save(session)?;
            }
        }

        let _ = self.event_tx.send(ContentEvent::TransferCompleted {
            transfer_id,
            content_id,
            total_bytes: reconstructed_bytes.len() as u64,
        });

        info!(
            transfer_id = %transfer_id,
            content_id = %content_id,
            size = reconstructed_bytes.len(),
            "Content transfer successfully verified and committed to CAS"
        );

        Ok(put_result.metadata().clone())
    }

    /// Pauses an active transfer session.
    pub async fn pause_transfer(&self, transfer_id: &TransferId) -> Result<(), ContentError> {
        let mut sessions = self.sessions.write().await;
        let session = sessions
            .get_mut(transfer_id)
            .ok_or(ContentError::TransferNotFound(*transfer_id))?;

        session.transition_to(TransferState::Paused)?;
        session.in_flight_chunks.clear();
        self.persistence.save(session)?;

        let _ = self.event_tx.send(ContentEvent::TransferPaused {
            transfer_id: *transfer_id,
            reason: "Transfer paused by request".to_string(),
        });
        Ok(())
    }

    /// Resumes a paused, cancelled, or interrupted transfer session.
    ///
    /// Evaluates missing chunks, preserving all previously verified chunks,
    /// and generates chunk requests for only the remaining missing chunks.
    pub async fn resume_transfer(
        &self,
        transfer_id: &TransferId,
    ) -> Result<Option<ContentRequest>, ContentError> {
        let mut sessions = self.sessions.write().await;
        let session = sessions
            .get_mut(transfer_id)
            .ok_or(ContentError::TransferNotFound(*transfer_id))?;

        // Transition through Resuming -> Transferring
        session.transition_to(TransferState::Resuming)?;
        session.transition_to(TransferState::Transferring)?;
        session.in_flight_chunks.clear();

        // Verify that verified chunks actually still exist in ChunkStore
        let verified_snapshot: Vec<u32> = session.verified_chunks.iter().copied().collect();
        if let Some(manifest) = &session.manifest {
            for &idx in &verified_snapshot {
                if let Some(desc) = manifest.chunks.iter().find(|c| c.index == idx) {
                    if !self.chunk_store.has(&desc.chunk_id)? {
                        session.verified_chunks.remove(&idx);
                    }
                }
            }
        }

        self.persistence.save(session)?;

        let remaining = session.missing_chunks();
        let _ = self.event_tx.send(ContentEvent::TransferResumed {
            transfer_id: *transfer_id,
            remaining_chunks: remaining.len() as u32,
        });

        info!(
            transfer_id = %transfer_id,
            content_id = %session.content_id,
            verified = session.verified_chunks.len(),
            remaining = remaining.len(),
            "Resumed transfer; only requesting missing chunks"
        );

        // If no manifest yet, request manifest again
        if session.manifest.is_none() {
            let now = Timestamp::now().as_millis();
            let req = ContentRequest {
                transfer_id: *transfer_id,
                content_id: session.content_id,
                manifest_id: None,
                requested_chunks: Vec::new(),
                requester_node_id: self.local_node_id,
                max_window_bytes: self.config.max_in_flight_bytes as u32,
                priority: priority_to_u8(session.priority),
                created_at: now,
                expires_at: session.expires_at,
                auth_token: None,
            };
            return Ok(Some(req));
        }

        // If all chunks are already verified upon resume, immediately finalize and commit to CAS
        if session.manifest.is_some() && remaining.is_empty() {
            drop(sessions);
            let _ = self.finalize_complete_transfer(*transfer_id).await?;
            return Ok(None);
        }

        // Schedule first batch of missing chunks
        let next_chunks = self.scheduler.schedule_next_chunks(session);
        for &idx in &next_chunks {
            session.mark_chunk_in_flight(idx);
        }
        self.persistence.save(session)?;

        if !next_chunks.is_empty() {
            let now = Timestamp::now().as_millis();
            let req = ContentRequest {
                transfer_id: *transfer_id,
                content_id: session.content_id,
                manifest_id: session.manifest_id,
                requested_chunks: next_chunks,
                requester_node_id: self.local_node_id,
                max_window_bytes: self.config.max_in_flight_bytes as u32,
                priority: priority_to_u8(session.priority),
                created_at: now,
                expires_at: session.expires_at,
                auth_token: None,
            };
            return Ok(Some(req));
        }

        Ok(None)
    }

    /// Explicitly cancels an active transfer session.
    pub async fn cancel_transfer(
        &self,
        transfer_id: &TransferId,
        reason: &str,
    ) -> Result<(), ContentError> {
        let mut sessions = self.sessions.write().await;
        let session = sessions
            .get_mut(transfer_id)
            .ok_or(ContentError::TransferNotFound(*transfer_id))?;

        session.transition_to(TransferState::Cancelled)?;
        session.in_flight_chunks.clear();
        session.error_message = Some(reason.to_string());
        self.persistence.save(session)?;

        let _ = self.event_tx.send(ContentEvent::TransferCancelled {
            transfer_id: *transfer_id,
            reason: reason.to_string(),
        });
        info!(transfer_id = %transfer_id, reason = %reason, "Transfer cancelled");
        Ok(())
    }

    /// Explicitly fails an active transfer session upon receiving an error or unrecoverable condition.
    pub async fn fail_transfer(
        &self,
        transfer_id: &TransferId,
        reason: &str,
    ) -> Result<(), ContentError> {
        let mut sessions = self.sessions.write().await;
        let session = sessions
            .get_mut(transfer_id)
            .ok_or(ContentError::TransferNotFound(*transfer_id))?;

        session.transition_to(TransferState::Failed)?;
        session.in_flight_chunks.clear();
        session.error_message = Some(reason.to_string());
        self.persistence.save(session)?;

        let _ = self.event_tx.send(ContentEvent::TransferFailed {
            transfer_id: *transfer_id,
            reason: reason.to_string(),
        });
        warn!(transfer_id = %transfer_id, reason = %reason, "Transfer failed");
        Ok(())
    }

    /// Checks active transfers for expiration and idle timeouts, triggering retries or fails.
    pub async fn check_timeouts_and_retries(
        &self,
    ) -> Result<Vec<(TransferId, ContentRequest)>, ContentError> {
        let now = Timestamp::now().as_millis();
        let mut retry_requests = Vec::new();

        let mut sessions = self.sessions.write().await;
        for session in sessions.values_mut() {
            if !session.status.is_active() {
                continue;
            }

            // 1. Check absolute expiration
            if session.is_expired(now) {
                warn!(transfer_id = %session.transfer_id, "Transfer session expired past deadline");
                let _ = session.transition_to(TransferState::Expired);
                let _ = self.persistence.save(session);
                let _ = self.event_tx.send(ContentEvent::TransferExpired {
                    transfer_id: session.transfer_id,
                });
                continue;
            }

            // 2. Check idle timeout
            if session.is_idle_timeout(now, self.config.idle_timeout_secs) {
                session.retry_count += 1;
                if session.retry_count >= self.config.max_retries {
                    warn!(
                        transfer_id = %session.transfer_id,
                        retries = session.retry_count,
                        "Transfer exceeded max retries due to idle timeout; marking FAILED"
                    );
                    let _ = session.transition_to(TransferState::Failed);
                    session.error_message = Some("Max retries exceeded due to timeout".to_string());
                    let _ = self.persistence.save(session);
                    let _ = self.event_tx.send(ContentEvent::TransferFailed {
                        transfer_id: session.transfer_id,
                        reason: "Max retries exceeded".to_string(),
                    });
                    continue;
                }

                // Reschedule in-flight chunks that timed out
                session.in_flight_chunks.clear();
                let next_chunks = self.scheduler.schedule_next_chunks(session);
                for &idx in &next_chunks {
                    session.mark_chunk_in_flight(idx);
                }
                let _ = self.persistence.save(session);

                if !next_chunks.is_empty() {
                    let req = ContentRequest {
                        transfer_id: session.transfer_id,
                        content_id: session.content_id,
                        manifest_id: session.manifest_id,
                        requested_chunks: next_chunks,
                        requester_node_id: self.local_node_id,
                        max_window_bytes: self.config.max_in_flight_bytes as u32,
                        priority: priority_to_u8(session.priority),
                        created_at: now,
                        expires_at: session.expires_at,
                        auth_token: None,
                    };
                    retry_requests.push((session.transfer_id, req));
                }
            }
        }

        Ok(retry_requests)
    }

    /// Retrieves a transfer session snapshot by `TransferId`.
    pub async fn get_session(&self, transfer_id: &TransferId) -> Option<TransferSession> {
        let sessions = self.sessions.read().await;
        sessions.get(transfer_id).cloned()
    }

    /// Lists all transfer session snapshots.
    pub async fn list_sessions(&self) -> Vec<TransferSession> {
        let sessions = self.sessions.read().await;
        sessions.values().cloned().collect()
    }

    /// Subscribes to content transfer events.
    pub fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<ContentEvent> {
        self.event_tx.subscribe()
    }
}

fn priority_to_u8(p: Priority) -> u8 {
    match p {
        Priority::Low => 1,
        Priority::Normal => 5,
        Priority::High => 8,
        Priority::Critical => 10,
    }
}

impl crate::gc::ActiveTransferProtector for TransferManager {
    fn active_chunk_ids(&self) -> std::collections::HashSet<crate::chunks::ChunkId> {
        let mut set = std::collections::HashSet::new();
        if let Ok(sessions) = self.sessions.try_read() {
            for s in sessions.values() {
                if s.status.is_active() {
                    if let Some(ref m) = s.manifest {
                        for c in &m.chunks {
                            set.insert(c.chunk_id);
                        }
                    }
                }
            }
        }
        set
    }
}
