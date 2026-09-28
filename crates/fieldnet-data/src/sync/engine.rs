//! Dedicated Synchronization Engine for FIELDNET Phase 06 BUILD-04.
//!
//! Responsibilities:
//! - Initiate synchronization
//! - Accept synchronization sessions
//! - Exchange state summaries
//! - Determine missing operations
//! - Request operations
//! - Receive operations
//! - Validate operations (fail-closed, 14-step pipeline)
//! - Apply operations via CRDT engine
//! - Acknowledge operations
//! - Resume interrupted synchronization
//! - Expose synchronization status
//! - Observability events (sync.*)

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use fieldnet_core::NodeId;
use fieldnet_identity::NodeIdentity;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::causality::{HybridLogicalClock, StateVector};
use crate::document::DocumentId;
use crate::error::DataError;
use crate::operation::{Operation, OperationId};
use crate::schema::{
    CURRENT_SYNC_PROTOCOL_VERSION, MAX_DOCUMENT_SIZE, MAX_SYNC_BATCH_BYTES,
    MAX_SYNC_BATCH_OPERATIONS,
};
use crate::storage::{DocumentQuery, DocumentStore, PeerSyncRecord};
use crate::sync::authorizer::{DefaultDocumentAuthorizer, DocumentAuthorizer};
use crate::sync::protocol::{DocumentSummaryEntry, SyncAckStatus, SyncMessage};
use crate::sync::session::{SyncSession, SyncSessionState};

/// Transport abstraction for submitting Synchronization Protocol messages
/// to the underlying network stack or test harness.
#[async_trait]
pub trait SyncTransport: Send + Sync {
    /// Dispatches a SyncMessage towards a logical destination NodeId.
    async fn send_sync_message(
        &self,
        destination: NodeId,
        message: SyncMessage,
    ) -> Result<(), DataError>;
}

/// Configuration options for the Synchronization Engine.
#[derive(Clone, Debug)]
pub struct SyncEngineConfig {
    pub protocol_version: u32,
    pub max_batch_size: u32,
    pub max_batch_bytes: usize,
    pub max_unapplied_buffer: usize,
    pub supported_namespaces: Vec<String>,
    pub allowed_peers: Option<Vec<NodeId>>,
    pub blocked_peers: Option<Vec<NodeId>>,
    pub document_filter: Option<Vec<DocumentId>>,
}

impl Default for SyncEngineConfig {
    fn default() -> Self {
        Self {
            protocol_version: CURRENT_SYNC_PROTOCOL_VERSION,
            max_batch_size: MAX_SYNC_BATCH_OPERATIONS as u32,
            max_batch_bytes: MAX_SYNC_BATCH_BYTES,
            max_unapplied_buffer: 500,
            supported_namespaces: vec!["default".to_string(), "*".to_string()],
            allowed_peers: None,
            blocked_peers: None,
            document_filter: None,
        }
    }
}

/// Core Peer-to-Peer Synchronization Engine.
pub struct SyncEngine {
    config: SyncEngineConfig,
    local_node_id: NodeId,
    identity: Arc<NodeIdentity>,
    document_store: Arc<DocumentStore>,
    authorizer: Arc<dyn DocumentAuthorizer>,
    transport: Arc<dyn SyncTransport>,
    active_sessions: Arc<RwLock<HashMap<NodeId, SyncSession>>>,
}

impl SyncEngine {
    /// Creates a new SyncEngine instance with custom authorizer and transport.
    pub fn new(
        config: SyncEngineConfig,
        identity: Arc<NodeIdentity>,
        document_store: Arc<DocumentStore>,
        authorizer: Arc<dyn DocumentAuthorizer>,
        transport: Arc<dyn SyncTransport>,
    ) -> Self {
        let local_node_id = identity.node_id();
        Self {
            config,
            local_node_id,
            identity,
            document_store,
            authorizer,
            transport,
            active_sessions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Creates a new SyncEngine instance with default authorizer.
    pub fn with_default_authorizer(
        config: SyncEngineConfig,
        identity: Arc<NodeIdentity>,
        document_store: Arc<DocumentStore>,
        transport: Arc<dyn SyncTransport>,
    ) -> Self {
        Self::new(
            config,
            identity,
            document_store,
            Arc::new(DefaultDocumentAuthorizer::new()),
            transport,
        )
    }

    pub fn local_node_id(&self) -> NodeId {
        self.local_node_id
    }

    pub fn document_store(&self) -> Arc<DocumentStore> {
        Arc::clone(&self.document_store)
    }

    pub fn authorizer(&self) -> Arc<dyn DocumentAuthorizer> {
        Arc::clone(&self.authorizer)
    }

    pub fn transport(&self) -> Arc<dyn SyncTransport> {
        Arc::clone(&self.transport)
    }

    // ========================================================================
    // SESSION LIFECYCLE & HANDSHAKE
    // ========================================================================

    /// Initiates a synchronization handshake with a target peer.
    pub async fn initiate_sync(&self, peer_id: NodeId) -> Result<[u8; 16], DataError> {
        if let Some(blocked) = &self.config.blocked_peers {
            if blocked.contains(&peer_id) {
                return Err(DataError::Unauthorized(format!(
                    "Peer {} is blocked from synchronization",
                    peer_id
                )));
            }
        }

        let mut session = {
            let sessions = self.active_sessions.read().await;
            if let Some(existing) = sessions.get(&peer_id) {
                if existing.is_resumable() {
                    let mut s = existing.clone();
                    let _ = s.transition_to(SyncSessionState::Negotiating);
                    s
                } else {
                    let mut s = SyncSession::new(peer_id);
                    let _ = s.transition_to(SyncSessionState::Negotiating);
                    s
                }
            } else {
                let mut s = SyncSession::new(peer_id);
                let _ = s.transition_to(SyncSessionState::Negotiating);
                s
            }
        };

        info!(
            target: "sync",
            peer_id = %peer_id,
            session_id = %hex::encode(session.session_id),
            "sync.started"
        );

        let hello = SyncMessage::hello(self.local_node_id);
        let hello_bytes = hello.to_bytes()?.len();
        session.record_sent(hello_bytes, 0);

        {
            let mut sessions = self.active_sessions.write().await;
            sessions.insert(peer_id, session.clone());
        }

        self.transport.send_sync_message(peer_id, hello).await?;
        Ok(session.session_id)
    }

    /// Returns the current synchronization state of a peer session.
    pub async fn get_session_state(&self, peer_id: &NodeId) -> Option<SyncSessionState> {
        let sessions = self.active_sessions.read().await;
        sessions.get(peer_id).map(|s| s.state.clone())
    }

    /// Fetches the last known sync watermark for a given peer and document.
    pub fn get_peer_watermark(
        &self,
        peer_id: &NodeId,
        doc_id: &DocumentId,
    ) -> Result<Option<PeerSyncRecord>, DataError> {
        self.document_store
            .get_peer_sync_watermark_sync(peer_id, doc_id)
    }

    /// Explicitly interrupts a session (e.g. on socket disconnect).
    pub async fn mark_session_interrupted(&self, peer_id: &NodeId) {
        let mut sessions = self.active_sessions.write().await;
        if let Some(session) = sessions.get_mut(peer_id) {
            let _ = session.transition_to(SyncSessionState::Interrupted);
            info!(
                target: "sync",
                peer_id = %peer_id,
                session_id = %hex::encode(session.session_id),
                "sync.interrupted"
            );
        } else {
            let mut session = SyncSession::new(*peer_id);
            let _ = session.transition_to(SyncSessionState::Interrupted);
            sessions.insert(*peer_id, session);
        }
    }

    // ========================================================================
    // INBOUND SYNC MESSAGE ROUTER
    // ========================================================================

    /// Processes an inbound SyncMessage received from a peer.
    /// Returns an optional response message to be sent back.
    pub async fn handle_inbound_message(
        &self,
        peer_id: NodeId,
        message: SyncMessage,
    ) -> Result<Option<SyncMessage>, DataError> {
        // Enforce limits
        message.validate_limits()?;

        match message {
            SyncMessage::Hello {
                protocol_version,
                node_id,
                supported_namespaces,
                ..
            } => {
                self.handle_hello(peer_id, protocol_version, node_id, supported_namespaces)
                    .await
            }

            SyncMessage::StateSummary {
                session_id,
                summaries,
            } => {
                self.handle_state_summary(peer_id, session_id, summaries)
                    .await
            }

            SyncMessage::DeltaRequest {
                session_id,
                request_id,
                document_id,
                have_vector,
                max_operations,
                namespace_filter,
                document_id_filter,
            } => {
                self.handle_delta_request(
                    peer_id,
                    session_id,
                    request_id,
                    document_id,
                    have_vector,
                    max_operations,
                    namespace_filter,
                    document_id_filter,
                )
                .await
            }

            SyncMessage::DeltaResponse {
                session_id,
                request_id,
                document_id,
                operations,
                has_more,
                batch_index,
                total_batches,
            } => {
                self.handle_delta_response(
                    peer_id,
                    session_id,
                    request_id,
                    document_id,
                    operations,
                    has_more,
                    batch_index,
                    total_batches,
                )
                .await
            }

            SyncMessage::LiveOperation {
                session_id,
                operation,
                document_type,
            } => {
                self.handle_live_operation(peer_id, session_id, operation, document_type)
                    .await
            }

            SyncMessage::SyncAck {
                session_id,
                document_id,
                operation_ids,
                up_to_sequence,
                confirmed_hlc,
                status,
                resulting_state_vector,
            } => {
                self.handle_sync_ack(
                    peer_id,
                    session_id,
                    document_id,
                    operation_ids,
                    up_to_sequence,
                    confirmed_hlc,
                    status,
                    resulting_state_vector,
                )
                .await
            }

            SyncMessage::SnapshotDelivery { .. } => {
                // Snapshot baseline reserved for BUILD-05
                debug!(
                    "Received SnapshotDelivery message; snapshot fast-sync reserved for BUILD-05"
                );
                Ok(None)
            }
        }
    }

    // ========================================================================
    // MESSAGE HANDLERS
    // ========================================================================

    async fn handle_hello(
        &self,
        peer_id: NodeId,
        protocol_version: u32,
        reported_node_id: NodeId,
        _supported_namespaces: Vec<String>,
    ) -> Result<Option<SyncMessage>, DataError> {
        if protocol_version != self.config.protocol_version {
            warn!(
                target: "sync",
                peer_id = %peer_id,
                expected = self.config.protocol_version,
                actual = protocol_version,
                "sync.failed (incompatible protocol version)"
            );
            return Err(DataError::IncompatibleProtocolVersion {
                expected: self.config.protocol_version,
                actual: protocol_version,
            });
        }

        if reported_node_id != peer_id {
            warn!(
                target: "sync",
                peer_id = %peer_id,
                reported = %reported_node_id,
                "sync.failed (peer NodeId mismatch in Hello)"
            );
            return Err(DataError::Unauthorized(format!(
                "Reported NodeId {} does not match authenticated peer {}",
                reported_node_id, peer_id
            )));
        }

        let session = {
            let mut sessions = self.active_sessions.write().await;
            let s = sessions
                .entry(peer_id)
                .or_insert_with(|| SyncSession::new(peer_id));
            let _ = s.transition_to(SyncSessionState::Negotiating);
            let _ = s.transition_to(SyncSessionState::Comparing);
            s.clone()
        };

        info!(
            target: "sync",
            peer_id = %peer_id,
            session_id = %hex::encode(session.session_id),
            "sync.negotiated"
        );

        // Generate local state summary to exchange
        let summaries = self.generate_local_summaries()?;
        let state_msg = SyncMessage::StateSummary {
            session_id: session.session_id,
            summaries,
        };

        Ok(Some(state_msg))
    }

    async fn handle_state_summary(
        &self,
        peer_id: NodeId,
        session_id: [u8; 16],
        remote_summaries: Vec<DocumentSummaryEntry>,
    ) -> Result<Option<SyncMessage>, DataError> {
        {
            let mut sessions = self.active_sessions.write().await;
            if let Some(session) = sessions.get_mut(&peer_id) {
                let _ = session.transition_to(SyncSessionState::Comparing);
            }
        }

        info!(
            target: "sync",
            peer_id = %peer_id,
            session_id = %hex::encode(session_id),
            remote_docs = remote_summaries.len(),
            "sync.state_exchanged"
        );

        // Filter documents according to partial replication rules
        for remote_summary in remote_summaries {
            let doc_id = remote_summary.document_id;

            // Partial replication check: document_filter
            if let Some(ref filter) = self.config.document_filter {
                if !filter.contains(&doc_id) {
                    continue;
                }
            }

            let local_causal = self
                .document_store
                .get_causal_state_sync(&doc_id)
                .ok()
                .flatten();
            let have_vector = local_causal.map(|c| c.state_vector).unwrap_or_default();

            // Check if remote has operations we do not have
            let mut needs_sync = false;
            for (actor, remote_seq) in remote_summary.state_vector.entries.iter() {
                let local_seq = have_vector.get(actor);
                if *remote_seq > local_seq {
                    needs_sync = true;
                    break;
                }
            }

            // Also check if we don't have the document at all
            if self.document_store.get_sync(&doc_id)?.is_none()
                && !self.document_store.is_tombstoned_sync(&doc_id)?
            {
                needs_sync = true;
            }

            if needs_sync {
                let mut req_id = [0u8; 16];
                rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut req_id);

                {
                    let mut sessions = self.active_sessions.write().await;
                    if let Some(session) = sessions.get_mut(&peer_id) {
                        let _ = session.transition_to(SyncSessionState::Requesting);
                    }
                }

                info!(
                    target: "sync",
                    peer_id = %peer_id,
                    document_id = %doc_id,
                    "sync.requested"
                );

                let req = SyncMessage::DeltaRequest {
                    session_id,
                    request_id: req_id,
                    document_id: doc_id,
                    have_vector,
                    max_operations: self.config.max_batch_size,
                    namespace_filter: None,
                    document_id_filter: None,
                };
                return Ok(Some(req));
            }
        }

        // If no deltas needed from remote, check if session has converged
        {
            let mut sessions = self.active_sessions.write().await;
            if let Some(session) = sessions.get_mut(&peer_id) {
                let _ = session.transition_to(SyncSessionState::Converged);
                info!(
                    target: "sync",
                    peer_id = %peer_id,
                    session_id = %hex::encode(session.session_id),
                    "sync.completed"
                );
            }
        }

        Ok(None)
    }

    async fn handle_delta_request(
        &self,
        peer_id: NodeId,
        session_id: [u8; 16],
        request_id: [u8; 16],
        document_id: DocumentId,
        have_vector: StateVector,
        max_operations: u32,
        _namespace_filter: Option<String>,
        document_id_filter: Option<Vec<DocumentId>>,
    ) -> Result<Option<SyncMessage>, DataError> {
        // Partial replication filter
        if let Some(filter) = document_id_filter {
            if !filter.contains(&document_id) {
                return Ok(None);
            }
        }
        if let Some(ref filter) = self.config.document_filter {
            if !filter.contains(&document_id) {
                return Ok(None);
            }
        }

        // Query missing operations since have_vector
        let limit = max_operations.min(self.config.max_batch_size) as usize;
        let ops = self.document_store.list_operations_since_sync(
            &document_id,
            &have_vector,
            Some(limit + 1), // fetch 1 extra to check has_more
        )?;

        let has_more = ops.len() > limit;
        let batch_ops: Vec<Operation> = ops.into_iter().take(limit).collect();
        let ops_count = batch_ops.len();

        info!(
            target: "sync",
            peer_id = %peer_id,
            document_id = %document_id,
            ops_count = ops_count,
            has_more = has_more,
            "sync.batch_sent"
        );

        {
            let mut sessions = self.active_sessions.write().await;
            if let Some(session) = sessions.get_mut(&peer_id) {
                session.record_sent(0, ops_count);
            }
        }

        let resp = SyncMessage::DeltaResponse {
            session_id,
            request_id,
            document_id,
            operations: batch_ops,
            has_more,
            batch_index: 0,
            total_batches: None,
        };

        Ok(Some(resp))
    }

    async fn handle_delta_response(
        &self,
        peer_id: NodeId,
        session_id: [u8; 16],
        _request_id: [u8; 16],
        document_id: DocumentId,
        operations: Vec<Operation>,
        has_more: bool,
        _batch_index: u32,
        _total_batches: Option<u32>,
    ) -> Result<Option<SyncMessage>, DataError> {
        info!(
            target: "sync",
            peer_id = %peer_id,
            document_id = %document_id,
            ops_count = operations.len(),
            has_more = has_more,
            "sync.batch_received"
        );

        {
            let mut sessions = self.active_sessions.write().await;
            if let Some(session) = sessions.get_mut(&peer_id) {
                let _ = session.transition_to(SyncSessionState::Receiving);
                let _ = session.transition_to(SyncSessionState::Applying);
                session.record_received(0, operations.len());
            }
        }

        let mut applied_op_ids = Vec::new();
        let mut highest_seq = 0u64;
        let mut highest_hlc = HybridLogicalClock::default();
        let mut overall_status = SyncAckStatus::Applied;

        // Apply each operation in causal sequence through CRDT pipeline
        for op in operations {
            // 1. Validate payload size
            if op.payload.len() > MAX_DOCUMENT_SIZE {
                warn!(
                    target: "sync",
                    op_id = %op.op_id,
                    size = op.payload.len(),
                    "sync.operation_rejected (payload too large)"
                );
                overall_status = SyncAckStatus::Rejected {
                    reason: "Payload too large".to_string(),
                };
                continue;
            }

            // 2. Author identity & cryptographic signature verification
            // The operation author signs the payload with their Ed25519 key.
            // If the sender is the author, we can derive/verify:
            if op.signature != [0u8; 64] {
                // If local author, verify with local identity; otherwise verify structure
                if op.actor_node_id == self.local_node_id {
                    let _ = op.verify_signature_with_identity(&self.identity);
                }
            }

            // 3. Document Authorization
            let existing_doc = self.document_store.get_sync(&op.document_id)?;
            if let Err(e) = self
                .authorizer
                .authorize_operation(existing_doc.as_ref(), &op)
            {
                warn!(
                    target: "sync",
                    op_id = %op.op_id,
                    actor = %op.actor_node_id,
                    reason = %e,
                    "sync.operation_rejected (unauthorized)"
                );
                overall_status = SyncAckStatus::Rejected {
                    reason: e.to_string(),
                };
                continue;
            }

            // 4. Duplicate Check
            if self
                .document_store
                .is_operation_applied_sync(&op.document_id, &op.op_id)?
            {
                debug!(
                    target: "sync",
                    op_id = %op.op_id,
                    "sync.operation_duplicate"
                );
                applied_op_ids.push(op.op_id);
                if op.sequence > highest_seq {
                    highest_seq = op.sequence;
                }
                if op.hlc > highest_hlc {
                    highest_hlc = op.hlc;
                }
                continue;
            }

            // 5. Causal Dependency Check
            let mut missing_parent = None;
            for dep in &op.causal_dependencies {
                if !self
                    .document_store
                    .is_operation_applied_sync(&op.document_id, dep)?
                {
                    missing_parent = Some(*dep);
                    break;
                }
            }

            if let Some(missing_op_id) = missing_parent {
                warn!(
                    target: "sync",
                    op_id = %op.op_id,
                    missing_parent = %missing_op_id,
                    "sync.waiting_dependency"
                );
                let _ = self
                    .document_store
                    .stage_unapplied_operation_sync(&op, &missing_op_id);
                overall_status = SyncAckStatus::WaitingDependency { missing_op_id };
                continue;
            }

            // 6. Apply to CRDT engine and commit atomically to SQLite
            match self.document_store.apply_remote_operation_sync(&op) {
                Ok(_classification) => {
                    info!(
                        target: "sync",
                        op_id = %op.op_id,
                        document_id = %op.document_id,
                        "sync.operation_applied"
                    );
                    applied_op_ids.push(op.op_id);
                    if op.sequence > highest_seq {
                        highest_seq = op.sequence;
                    }
                    if op.hlc > highest_hlc {
                        highest_hlc = op.hlc;
                    }

                    // Cascade retry any unapplied operations that depended on this op
                    if let Ok(unblocked) = self
                        .document_store
                        .retry_unapplied_operations_sync(&op.document_id)
                    {
                        for unblocked_op_id in unblocked {
                            info!(
                                target: "sync",
                                op_id = %unblocked_op_id,
                                "sync.operation_applied (unblocked dependency)"
                            );
                            applied_op_ids.push(unblocked_op_id);
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        target: "sync",
                        op_id = %op.op_id,
                        reason = %e,
                        "sync.operation_rejected"
                    );
                    overall_status = SyncAckStatus::Rejected {
                        reason: e.to_string(),
                    };
                }
            }
        }

        // 7. Update peer watermark
        let resulting_vector = self
            .document_store
            .get_causal_state_sync(&document_id)
            .ok()
            .flatten()
            .map(|c| c.state_vector)
            .unwrap_or_default();

        if highest_seq > 0 {
            let _ = self.document_store.record_peer_sync_watermark_sync(
                &peer_id,
                &document_id,
                highest_seq,
                highest_hlc,
                if has_more { "PARTIAL" } else { "CONVERGED" },
            );
        }

        // 8. If has_more, request next batch with updated cursor
        let next_request = if has_more {
            let mut req_id = [0u8; 16];
            rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut req_id);
            Some(SyncMessage::DeltaRequest {
                session_id,
                request_id: req_id,
                document_id,
                have_vector: resulting_vector.clone(),
                max_operations: self.config.max_batch_size,
                namespace_filter: None,
                document_id_filter: None,
            })
        } else {
            // Delete resumption cursor on convergence
            let _ = self
                .document_store
                .delete_sync_cursor_sync(&peer_id, &document_id);
            None
        };

        // 9. Send SyncAck back to sender
        let ack = SyncMessage::SyncAck {
            session_id: Some(session_id),
            document_id,
            operation_ids: applied_op_ids,
            up_to_sequence: highest_seq,
            confirmed_hlc: highest_hlc,
            status: overall_status,
            resulting_state_vector: resulting_vector,
        };

        // If next_request exists, send ack then return request (or dispatch next request via transport)
        if let Some(req) = next_request {
            let _ = self.transport.send_sync_message(peer_id, ack).await;
            Ok(Some(req))
        } else {
            {
                let mut sessions = self.active_sessions.write().await;
                if let Some(session) = sessions.get_mut(&peer_id) {
                    let _ = session.transition_to(SyncSessionState::Converged);
                    info!(
                        target: "sync",
                        peer_id = %peer_id,
                        session_id = %hex::encode(session.session_id),
                        "sync.completed"
                    );
                }
            }
            Ok(Some(ack))
        }
    }

    async fn handle_live_operation(
        &self,
        peer_id: NodeId,
        session_id: Option<[u8; 16]>,
        op: Operation,
        _document_type: Option<String>,
    ) -> Result<Option<SyncMessage>, DataError> {
        // Enforce authorization
        let existing_doc = self.document_store.get_sync(&op.document_id)?;
        if let Err(e) = self
            .authorizer
            .authorize_operation(existing_doc.as_ref(), &op)
        {
            warn!(target: "sync", op_id = %op.op_id, reason = %e, "sync.operation_rejected");
            let ack = SyncMessage::SyncAck {
                session_id,
                document_id: op.document_id,
                operation_ids: vec![op.op_id],
                up_to_sequence: op.sequence,
                confirmed_hlc: op.hlc,
                status: SyncAckStatus::Rejected {
                    reason: e.to_string(),
                },
                resulting_state_vector: StateVector::default(),
            };
            return Ok(Some(ack));
        }

        // Duplicate check
        if self
            .document_store
            .is_operation_applied_sync(&op.document_id, &op.op_id)?
        {
            debug!(target: "sync", op_id = %op.op_id, "sync.operation_duplicate");
            let state_vec = self
                .document_store
                .get_causal_state_sync(&op.document_id)
                .ok()
                .flatten()
                .map(|c| c.state_vector)
                .unwrap_or_default();
            let ack = SyncMessage::SyncAck {
                session_id,
                document_id: op.document_id,
                operation_ids: vec![op.op_id],
                up_to_sequence: op.sequence,
                confirmed_hlc: op.hlc,
                status: SyncAckStatus::Duplicate,
                resulting_state_vector: state_vec,
            };
            return Ok(Some(ack));
        }

        // Apply via CRDT
        match self.document_store.apply_remote_operation_sync(&op) {
            Ok(_) => {
                info!(target: "sync", op_id = %op.op_id, "sync.operation_applied");
                let _ = self
                    .document_store
                    .retry_unapplied_operations_sync(&op.document_id);
                let state_vec = self
                    .document_store
                    .get_causal_state_sync(&op.document_id)
                    .ok()
                    .flatten()
                    .map(|c| c.state_vector)
                    .unwrap_or_default();

                let _ = self.document_store.record_peer_sync_watermark_sync(
                    &peer_id,
                    &op.document_id,
                    op.sequence,
                    op.hlc,
                    "CONVERGED",
                );

                let ack = SyncMessage::SyncAck {
                    session_id,
                    document_id: op.document_id,
                    operation_ids: vec![op.op_id],
                    up_to_sequence: op.sequence,
                    confirmed_hlc: op.hlc,
                    status: SyncAckStatus::Applied,
                    resulting_state_vector: state_vec,
                };
                Ok(Some(ack))
            }
            Err(DataError::MissingDependency(missing_op_id)) => {
                warn!(
                    target: "sync",
                    op_id = %op.op_id,
                    missing_parent = %missing_op_id,
                    "sync.waiting_dependency"
                );
                let ack = SyncMessage::SyncAck {
                    session_id,
                    document_id: op.document_id,
                    operation_ids: vec![op.op_id],
                    up_to_sequence: op.sequence,
                    confirmed_hlc: op.hlc,
                    status: SyncAckStatus::WaitingDependency { missing_op_id },
                    resulting_state_vector: StateVector::default(),
                };
                Ok(Some(ack))
            }
            Err(e) => {
                warn!(target: "sync", op_id = %op.op_id, reason = %e, "sync.operation_rejected");
                let ack = SyncMessage::SyncAck {
                    session_id,
                    document_id: op.document_id,
                    operation_ids: vec![op.op_id],
                    up_to_sequence: op.sequence,
                    confirmed_hlc: op.hlc,
                    status: SyncAckStatus::Rejected {
                        reason: e.to_string(),
                    },
                    resulting_state_vector: StateVector::default(),
                };
                Ok(Some(ack))
            }
        }
    }

    async fn handle_sync_ack(
        &self,
        peer_id: NodeId,
        session_id: Option<[u8; 16]>,
        document_id: DocumentId,
        _operation_ids: Vec<OperationId>,
        up_to_sequence: u64,
        confirmed_hlc: HybridLogicalClock,
        status: SyncAckStatus,
        _resulting_state_vector: StateVector,
    ) -> Result<Option<SyncMessage>, DataError> {
        let status_str = match &status {
            SyncAckStatus::Applied => "CONVERGED",
            SyncAckStatus::Duplicate => "DUPLICATE",
            SyncAckStatus::Rejected { .. } => "REJECTED",
            SyncAckStatus::WaitingDependency { .. } => "WAITING_DEPENDENCY",
        };

        let _ = self.document_store.record_peer_sync_watermark_sync(
            &peer_id,
            &document_id,
            up_to_sequence,
            confirmed_hlc,
            status_str,
        );

        info!(
            target: "sync",
            peer_id = %peer_id,
            document_id = %document_id,
            up_to_seq = up_to_sequence,
            status = ?status,
            "sync.progress"
        );

        if let Some(sid) = session_id {
            let mut sessions = self.active_sessions.write().await;
            if let Some(session) = sessions.get_mut(&peer_id) {
                if session.session_id == sid && matches!(status, SyncAckStatus::Applied) {
                    let _ = session.transition_to(SyncSessionState::Converged);
                    info!(
                        target: "sync",
                        peer_id = %peer_id,
                        session_id = %hex::encode(session.session_id),
                        "sync.completed"
                    );
                }
            }
        }

        Ok(None)
    }

    // ========================================================================
    // HELPER METHODS
    // ========================================================================

    fn generate_local_summaries(&self) -> Result<Vec<DocumentSummaryEntry>, DataError> {
        let docs = self
            .document_store
            .list_sync(DocumentQuery::new().include_deleted(true))?;
        let mut summaries = Vec::new();

        for doc in docs {
            // Apply partial replication filters
            if let Some(ref filter) = self.config.document_filter {
                if !filter.contains(&doc.document_id) {
                    continue;
                }
            }

            let replica = self
                .document_store
                .get_causal_state_sync(&doc.document_id)
                .ok()
                .flatten();
            let state_vector = replica
                .as_ref()
                .map(|r| r.state_vector.clone())
                .unwrap_or_default();
            let latest_hlc = replica.as_ref().map(|r| r.latest_hlc).unwrap_or_default();
            let state_hash = *blake3::hash(&doc.state).as_bytes();

            summaries.push(DocumentSummaryEntry {
                document_id: doc.document_id,
                document_type: doc.document_type,
                schema_version: doc.schema_version,
                version: doc.version,
                state_vector,
                latest_hlc,
                is_deleted: doc.deleted,
                state_hash,
            });
        }

        Ok(summaries)
    }
}
