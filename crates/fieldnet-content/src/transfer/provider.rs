use fieldnet_core::NodeId;
use std::sync::Arc;
use tracing::{debug, warn};

use super::protocol::{ContentChunk, ContentRequest, ContentResponse, TransferResponseStatus};
use crate::auth::{ContentAction, ContentAuthorizer};
use crate::chunks::store::ChunkStore;
use crate::error::ContentError;
use crate::manifest::store::ManifestStore;
use crate::store::ContentStore;

/// Output result produced by the provider service when handling a content request.
#[derive(Clone, Debug)]
pub enum ProviderResponse {
    /// Manifest negotiation response frame.
    Response(Box<ContentResponse>),
    /// Stream of chunk payloads to be transmitted.
    Chunks(Vec<ContentChunk>),
    /// Explicit error frame to be transmitted back.
    Error(TransferResponseStatus, String),
}

/// Service handling inbound content requests on a content provider node.
pub struct TransferProviderService {
    content_store: Arc<ContentStore>,
    chunk_store: Arc<ChunkStore>,
    manifest_store: Arc<ManifestStore>,
    max_serving_chunks_per_req: usize,
    authorized_requesters: Arc<tokio::sync::RwLock<Option<std::collections::HashSet<NodeId>>>>,
    authorizer: Arc<tokio::sync::RwLock<Option<Arc<ContentAuthorizer>>>>,
}

impl TransferProviderService {
    /// Creates a new `TransferProviderService`.
    pub fn new(
        content_store: Arc<ContentStore>,
        chunk_store: Arc<ChunkStore>,
        manifest_store: Arc<ManifestStore>,
        max_serving_chunks_per_req: usize,
    ) -> Self {
        Self {
            content_store,
            chunk_store,
            manifest_store,
            max_serving_chunks_per_req,
            authorized_requesters: Arc::new(tokio::sync::RwLock::new(None)),
            authorizer: Arc::new(tokio::sync::RwLock::new(None)),
        }
    }

    /// Sets or clears the explicit allowlist of authorized content requesters.
    pub async fn set_authorized_requesters(
        &self,
        requesters: Option<std::collections::HashSet<NodeId>>,
    ) {
        let mut guard = self.authorized_requesters.write().await;
        *guard = requesters;
    }

    /// Sets the authoritative ContentAuthorizer for fine-grained content access control.
    pub async fn set_authorizer(&self, authorizer: Arc<ContentAuthorizer>) {
        let mut guard = self.authorizer.write().await;
        *guard = Some(authorizer);
    }

    /// Checks if a requester NodeId is authorized according to local provider policy.
    pub async fn is_requester_authorized(&self, requester: &NodeId) -> bool {
        let guard = self.authorized_requesters.read().await;
        if let Some(set) = &*guard {
            set.contains(requester)
        } else {
            true
        }
    }

    /// Processes an incoming `ContentRequest` and generates the corresponding response frames.
    ///
    /// Never serves content if authorization fails.
    pub async fn handle_request(
        &self,
        req: &ContentRequest,
        is_authorized: bool,
    ) -> Result<ProviderResponse, ContentError> {
        // 1. Authorize access via node authentication and peer allowlist
        let mut authorized =
            is_authorized && self.is_requester_authorized(&req.requester_node_id).await;

        // 2. Fine-grained Content Authorization check (BEFORE serving any bytes)
        if authorized {
            let auth_guard = self.authorizer.read().await;
            if let Some(ref auth) = *auth_guard {
                let token_decoded: Option<crate::auth::CapabilityToken> = req
                    .auth_token
                    .as_ref()
                    .and_then(|bytes| serde_json::from_slice(bytes).ok());
                if let Err(e) = auth.authorize(
                    &req.requester_node_id,
                    ContentAction::Read,
                    &req.content_id,
                    token_decoded.as_ref(),
                ) {
                    warn!(
                        requester = %req.requester_node_id,
                        content_id = %req.content_id,
                        error = %e,
                        "ContentAuthorizer rejected content access"
                    );
                    authorized = false;
                }
            }
        }

        if !authorized {
            warn!(
                requester = %req.requester_node_id,
                content_id = %req.content_id,
                "Rejecting unauthorized content request"
            );
            return Ok(ProviderResponse::Response(Box::new(ContentResponse {
                transfer_id: req.transfer_id,
                content_id: req.content_id,
                manifest: None,
                status: TransferResponseStatus::Unauthorized,
                accepted_chunks: Vec::new(),
                rejected_chunks: req.requested_chunks.clone(),
                total_chunks: 0,
                total_bytes: 0,
                nominal_chunk_size: 0,
                error_message: Some("Requester unauthorized to access content".to_string()),
            })));
        }

        // 3. Validate local content availability and integrity state
        let has_content = self.content_store.has(&req.content_id).unwrap_or(false);
        let manifest = match self.manifest_store.get_by_content_id(&req.content_id) {
            Ok(m) => m,
            Err(e) => {
                if !has_content {
                    debug!(
                        content_id = %req.content_id,
                        "Requested content object not found in local store or manifest store"
                    );
                    return Ok(ProviderResponse::Response(Box::new(ContentResponse {
                        transfer_id: req.transfer_id,
                        content_id: req.content_id,
                        manifest: None,
                        status: TransferResponseStatus::NotFound,
                        accepted_chunks: Vec::new(),
                        rejected_chunks: req.requested_chunks.clone(),
                        total_chunks: 0,
                        total_bytes: 0,
                        nominal_chunk_size: 0,
                        error_message: Some("ContentId not found in provider store".to_string()),
                    })));
                } else {
                    warn!(
                        content_id = %req.content_id,
                        error = %e,
                        "Failed to retrieve manifest for possessed content"
                    );
                    return Ok(ProviderResponse::Response(Box::new(ContentResponse {
                        transfer_id: req.transfer_id,
                        content_id: req.content_id,
                        manifest: None,
                        status: TransferResponseStatus::NotFound,
                        accepted_chunks: Vec::new(),
                        rejected_chunks: req.requested_chunks.clone(),
                        total_chunks: 0,
                        total_bytes: 0,
                        nominal_chunk_size: 0,
                        error_message: Some("Manifest unavailable for content".to_string()),
                    })));
                }
            }
        };

        // 5. Initial request: Manifest negotiation (empty requested_chunks)
        if req.requested_chunks.is_empty() {
            debug!(
                transfer_id = %req.transfer_id,
                content_id = %req.content_id,
                "Responding to manifest negotiation request"
            );
            return Ok(ProviderResponse::Response(Box::new(ContentResponse {
                transfer_id: req.transfer_id,
                content_id: req.content_id,
                manifest: Some(manifest.clone()),
                status: TransferResponseStatus::Accepted,
                accepted_chunks: Vec::new(),
                rejected_chunks: Vec::new(),
                total_chunks: manifest.chunk_count,
                total_bytes: manifest.total_size,
                nominal_chunk_size: manifest.chunk_size,
                error_message: None,
            })));
        }

        // 6. Chunk streaming request: Fetch and stream requested chunks without loading full object
        let mut chunks_to_send = Vec::new();
        let limit = req
            .requested_chunks
            .len()
            .min(self.max_serving_chunks_per_req);

        for &chunk_idx in &req.requested_chunks[..limit] {
            if let Some(chunk_desc) = manifest.chunks.iter().find(|c| c.index == chunk_idx) {
                match self.chunk_store.get(&chunk_desc.chunk_id).await {
                    Ok(data) => {
                        chunks_to_send.push(ContentChunk {
                            transfer_id: req.transfer_id,
                            content_id: req.content_id,
                            chunk_index: chunk_idx,
                            chunk_id: chunk_desc.chunk_id,
                            data,
                        });
                    }
                    Err(e) => {
                        warn!(
                            chunk_index = chunk_idx,
                            chunk_id = %chunk_desc.chunk_id,
                            error = %e,
                            "Missing chunk in provider chunk store"
                        );
                    }
                }
            } else {
                warn!(
                    chunk_index = chunk_idx,
                    "Requested chunk index out of bounds in manifest"
                );
            }
        }

        Ok(ProviderResponse::Chunks(chunks_to_send))
    }
}
