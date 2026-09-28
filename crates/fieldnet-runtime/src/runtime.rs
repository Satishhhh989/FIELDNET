use async_trait::async_trait;
use fieldnet_core::{MessageId, NodeId, Priority, Timestamp};
use fieldnet_data::sync::{SyncEngine, SyncEngineConfig, SyncMessage, SyncTransport};
use fieldnet_identity::NodeIdentity;
use fieldnet_network::{
    ConnectionEvent, ConnectionManager, DiscoveredPeer, DiscoveryConfig, DiscoveryService,
};
use fieldnet_protocol::{
    AckPayload, AckStatus, Deduplicator, Message, MessageType, RouteAdvertisement,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, Notify, RwLock};
use tracing::{debug, error, info, warn};

use crate::dtn::DtnQueue;
use crate::routing::RoutingTable;

struct RuntimeSyncTransport {
    tx: tokio::sync::mpsc::UnboundedSender<(NodeId, SyncMessage)>,
}

#[async_trait]
impl SyncTransport for RuntimeSyncTransport {
    async fn send_sync_message(
        &self,
        destination: NodeId,
        message: SyncMessage,
    ) -> Result<(), fieldnet_data::DataError> {
        self.tx
            .send((destination, message))
            .map_err(|e| fieldnet_data::DataError::SyncError(e.to_string()))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MessageDeliveryStatus {
    Sent {
        sent_at: Timestamp,
    },
    Forwarded {
        forwarded_at: Timestamp,
        next_hop: NodeId,
    },
    AckReceived {
        ack_at: Timestamp,
        status: AckStatus,
    },
}

#[derive(Clone, Debug)]
pub struct ReceivedMessage {
    pub message: Message,
    pub received_at: Timestamp,
    pub is_duplicate: bool,
}

#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub data_dir: PathBuf,
    pub tcp_port: u16,
    pub discovery_port: u16,
    pub auto_connect: bool,
    pub beacon_interval: Duration,
    pub allowed_peers: Option<Vec<NodeId>>,
    pub blocked_peers: Option<Vec<NodeId>>,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            tcp_port: 0, // dynamic ephemeral by default
            discovery_port: fieldnet_network::DEFAULT_DISCOVERY_PORT,
            auto_connect: true,
            beacon_interval: Duration::from_millis(500),
            allowed_peers: None,
            blocked_peers: None,
        }
    }
}

pub struct FieldnetRuntime {
    config: RuntimeConfig,
    identity: Arc<NodeIdentity>,
    actual_tcp_port: u16,
    discovery: Arc<DiscoveryService>,
    connection_manager: Arc<ConnectionManager>,
    routing_table: Arc<RoutingTable>,
    dtn_queue: Arc<DtnQueue>,
    document_store: Arc<fieldnet_data::DocumentStore>,
    content_store: Arc<fieldnet_content::ContentStore>,
    transfer_manager: Arc<fieldnet_content::TransferManager>,
    sync_engine: Arc<SyncEngine>,
    deduplicator: Arc<Deduplicator>,
    message_delivery: Arc<RwLock<HashMap<MessageId, MessageDeliveryStatus>>>,
    received_messages: Arc<RwLock<Vec<ReceivedMessage>>>,
    known_peers: Arc<RwLock<HashMap<NodeId, DiscoveredPeer>>>,
    sequence_counter: Arc<RwLock<u64>>,
    dtn_flush_notify: Arc<Notify>,
    delivery_notify: broadcast::Sender<(MessageId, MessageDeliveryStatus)>,
    shutdown_tx: broadcast::Sender<()>,
}

impl FieldnetRuntime {
    pub async fn start(
        config: RuntimeConfig,
    ) -> Result<Arc<Self>, Box<dyn std::error::Error + Send + Sync>> {
        info!(
            "Starting FIELDNET Runtime (data_dir: {})",
            config.data_dir.display()
        );

        // 1. Load or generate persistent Node Identity
        tokio::fs::create_dir_all(&config.data_dir).await?;
        let identity_path = config.data_dir.join("identity.json");
        let identity = if identity_path.exists() {
            info!(
                "Loading existing Node Identity from {}",
                identity_path.display()
            );
            NodeIdentity::load_from_file(&identity_path)?
        } else {
            info!(
                "Generating new Node Identity and saving to {}",
                identity_path.display()
            );
            let new_id = NodeIdentity::generate();
            new_id.save_to_file(&identity_path)?;
            new_id
        };

        let node_id = identity.node_id();
        info!("Local Node ID: {}", node_id);

        let identity = Arc::new(identity);

        // 2. Open persistent SQLite DTN queue
        let dtn_db_path = config.data_dir.join("dtn_queue.db");
        let dtn_queue = Arc::new(DtnQueue::open(&dtn_db_path)?);

        // 3. Open persistent SQLite Local-First Document Store
        let doc_db_path = config.data_dir.join("documents.db");
        let document_store = Arc::new(fieldnet_data::DocumentStore::with_identity(
            &doc_db_path,
            Arc::clone(&identity),
        )?);

        // 3b. Open persistent Content Store (Phase 07)
        let content_dir = config.data_dir.join("content");
        let content_store = fieldnet_content::ContentStore::open(&content_dir)?;

        // 3c. Initialize Content Transfer Engine (Phase 07 Build 04)
        let transfer_manager = fieldnet_content::TransferManager::open(
            node_id,
            Arc::clone(&content_store),
            Arc::clone(content_store.conn()),
            content_store.event_tx().clone(),
            fieldnet_content::TransferConfig::default(),
        )?;

        // Connect ContentAuthorizer to TransferProviderService
        transfer_manager
            .provider_service()
            .set_authorizer(Arc::clone(content_store.authorizer()))
            .await;

        // 4. Initialize Routing Table
        let routing_table = Arc::new(RoutingTable::new(node_id));

        // 5. Initialize ConnectionManager and start TCP listener
        let connection_manager = Arc::new(ConnectionManager::new(
            Arc::clone(&identity),
            config.tcp_port,
        ));
        let actual_tcp_port = connection_manager.start_listener().await?;

        // 6. Initialize DiscoveryService with real UDP sockets
        let discovery_config = DiscoveryConfig {
            discovery_port: config.discovery_port,
            beacon_interval: config.beacon_interval,
            beacon_ttl_secs: 15,
        };
        let discovery = Arc::new(DiscoveryService::new(
            node_id,
            actual_tcp_port,
            discovery_config,
        )?);
        discovery.start().await?;

        let (delivery_notify, _) = broadcast::channel(128);
        let (shutdown_tx, _) = broadcast::channel(1);
        let dtn_flush_notify = Arc::new(Notify::new());

        let (sync_tx, sync_rx) = tokio::sync::mpsc::unbounded_channel();
        let sync_transport = Arc::new(RuntimeSyncTransport { tx: sync_tx });

        let sync_config = SyncEngineConfig {
            allowed_peers: config.allowed_peers.clone(),
            blocked_peers: config.blocked_peers.clone(),
            ..Default::default()
        };

        let sync_engine = Arc::new(SyncEngine::with_default_authorizer(
            sync_config,
            Arc::clone(&identity),
            Arc::clone(&document_store),
            sync_transport,
        ));

        let runtime = Arc::new(Self {
            config,
            identity,
            actual_tcp_port,
            discovery,
            connection_manager,
            routing_table,
            dtn_queue,
            document_store,
            content_store,
            transfer_manager,
            sync_engine,
            deduplicator: Arc::new(Deduplicator::new(1000)),
            message_delivery: Arc::new(RwLock::new(HashMap::new())),
            received_messages: Arc::new(RwLock::new(Vec::new())),
            known_peers: Arc::new(RwLock::new(HashMap::new())),
            sequence_counter: Arc::new(RwLock::new(1)),
            dtn_flush_notify,
            delivery_notify,
            shutdown_tx,
        });

        // 6. Start internal runtime coordination loops
        runtime.spawn_discovery_loop();
        runtime.spawn_connection_event_loop();
        runtime.spawn_routing_advertisement_loop();
        runtime.spawn_dtn_worker_loop();
        runtime.spawn_sync_dispatch_loop(sync_rx);
        runtime.spawn_provider_maintenance_loop();
        runtime.spawn_message_processing_loop().await;

        info!("FIELDNET Runtime successfully started and operational");
        Ok(runtime)
    }

    pub fn node_id(&self) -> NodeId {
        self.identity.node_id()
    }

    pub fn tcp_port(&self) -> u16 {
        self.actual_tcp_port
    }

    pub fn identity(&self) -> Arc<NodeIdentity> {
        Arc::clone(&self.identity)
    }

    pub fn connection_manager(&self) -> Arc<ConnectionManager> {
        Arc::clone(&self.connection_manager)
    }

    pub fn routing_table(&self) -> Arc<RoutingTable> {
        Arc::clone(&self.routing_table)
    }

    pub fn dtn_queue(&self) -> Arc<DtnQueue> {
        Arc::clone(&self.dtn_queue)
    }

    pub fn document_store(&self) -> Arc<fieldnet_data::DocumentStore> {
        Arc::clone(&self.document_store)
    }

    pub fn content_store(&self) -> Arc<fieldnet_content::ContentStore> {
        Arc::clone(&self.content_store)
    }

    pub async fn content_put(
        &self,
        data: &[u8],
        mime_type: Option<String>,
    ) -> Result<
        (fieldnet_content::ContentId, fieldnet_content::PutResult),
        fieldnet_content::ContentError,
    > {
        let options = fieldnet_content::PutOptions {
            mime_type,
            creator_node_id: Some(self.node_id()),
            is_pinned: false,
            custom_metadata: std::collections::HashMap::new(),
        };
        let mut cursor = std::io::Cursor::new(data);
        self.content_store
            .put_stream(&mut cursor, Some(options))
            .await
    }

    pub async fn content_get(
        &self,
        content_id: &fieldnet_content::ContentId,
    ) -> Result<Vec<u8>, fieldnet_content::ContentError> {
        self.content_store.get(content_id).await
    }

    pub fn content_has(
        &self,
        content_id: &fieldnet_content::ContentId,
    ) -> Result<bool, fieldnet_content::ContentError> {
        self.content_store.has(content_id)
    }

    pub fn content_stat(
        &self,
        content_id: &fieldnet_content::ContentId,
    ) -> Result<fieldnet_content::ContentMetadata, fieldnet_content::ContentError> {
        self.content_store.stat(content_id)
    }

    pub async fn content_delete(
        &self,
        content_id: &fieldnet_content::ContentId,
    ) -> Result<bool, fieldnet_content::ContentError> {
        self.content_store.delete(content_id).await
    }

    pub fn content_list(
        &self,
    ) -> Result<Vec<fieldnet_content::ContentMetadata>, fieldnet_content::ContentError> {
        self.content_store.list()
    }

    pub fn content_chunk_store(&self) -> &Arc<fieldnet_content::ChunkStore> {
        self.content_store.chunk_store()
    }

    pub fn content_manifest_store(&self) -> &Arc<fieldnet_content::ManifestStore> {
        self.content_store.manifest_store()
    }

    pub async fn content_put_chunked_stream<R: tokio::io::AsyncRead + Unpin>(
        &self,
        reader: &mut R,
        mime_type: Option<String>,
    ) -> Result<
        (
            fieldnet_content::ContentId,
            fieldnet_content::PutResult,
            fieldnet_content::ContentManifest,
        ),
        fieldnet_content::ContentError,
    > {
        let options = fieldnet_content::PutOptions {
            mime_type,
            creator_node_id: Some(self.node_id()),
            is_pinned: false,
            custom_metadata: std::collections::HashMap::new(),
        };
        self.content_store
            .put_chunked_stream(reader, Some(options))
            .await
    }

    pub fn content_has_chunk(
        &self,
        chunk_id: &fieldnet_content::ChunkId,
    ) -> Result<bool, fieldnet_content::ContentError> {
        self.content_store.has_chunk(chunk_id)
    }

    pub async fn content_get_chunk(
        &self,
        chunk_id: &fieldnet_content::ChunkId,
    ) -> Result<Vec<u8>, fieldnet_content::ContentError> {
        self.content_store.get_chunk(chunk_id).await
    }

    pub fn content_get_manifest(
        &self,
        content_id: &fieldnet_content::ContentId,
    ) -> Result<fieldnet_content::ContentManifest, fieldnet_content::ContentError> {
        self.content_store.get_manifest(content_id)
    }

    pub async fn content_reconstruct_to_stream<W: tokio::io::AsyncWrite + Unpin>(
        &self,
        content_id: &fieldnet_content::ContentId,
        writer: &mut W,
    ) -> Result<u64, fieldnet_content::ContentError> {
        self.content_store
            .reconstruct_to_stream(content_id, writer)
            .await
    }

    pub async fn content_reconstruct_to_bytes(
        &self,
        content_id: &fieldnet_content::ContentId,
    ) -> Result<Vec<u8>, fieldnet_content::ContentError> {
        self.content_store.reconstruct_to_bytes(content_id).await
    }

    pub async fn content_open_stream(
        &self,
        content_id: &fieldnet_content::ContentId,
    ) -> Result<tokio::fs::File, fieldnet_content::ContentError> {
        self.content_store.get_stream(content_id).await
    }

    pub fn content_provider_registry(&self) -> &Arc<fieldnet_content::ProviderRegistry> {
        self.content_store.provider_registry()
    }

    pub fn content_find_providers(
        &self,
        content_id: &fieldnet_content::ContentId,
    ) -> Vec<fieldnet_content::ProviderRecord> {
        self.content_store
            .provider_registry()
            .find_providers(content_id)
    }

    pub fn content_find_providers_for_chunks(
        &self,
        content_id: &fieldnet_content::ContentId,
        missing_chunks: &[u32],
    ) -> Vec<fieldnet_content::ProviderRecord> {
        self.content_store
            .provider_registry()
            .find_providers_for_chunks(content_id, missing_chunks)
    }

    pub fn content_resolve(
        &self,
        content_id: &fieldnet_content::ContentId,
    ) -> Result<fieldnet_content::ContentResolution, fieldnet_content::ContentError> {
        let resolver = fieldnet_content::ContentResolver::new(
            Arc::clone(&self.content_store),
            Arc::clone(self.content_store.provider_registry()),
        );
        resolver.resolve(content_id)
    }

    pub fn content_provider_status(
        &self,
        content_id: &fieldnet_content::ContentId,
        provider_id: &NodeId,
    ) -> Option<fieldnet_content::ProviderState> {
        self.content_store
            .provider_registry()
            .get(content_id, provider_id)
            .map(|r| r.availability_state)
    }

    pub async fn content_advertise_local(
        &self,
        content_id: &fieldnet_content::ContentId,
        destination: Option<NodeId>,
    ) -> Result<fieldnet_content::ContentAdvertise, fieldnet_content::ContentError> {
        if !self.content_store.has(content_id)? {
            return Err(fieldnet_content::ContentError::ContentNotFound(*content_id));
        }

        let metadata = self.content_store.stat(content_id).ok();
        let manifest = self.content_store.get_manifest(content_id).ok();

        let mut ad = fieldnet_content::ContentAdvertise::new_full(
            *content_id,
            manifest.as_ref().map(|m| m.manifest_id),
            self.node_id(),
            self.identity.public_key_bytes(),
            metadata.as_ref().map(|m| m.size_bytes),
            300,
            1,
        );
        ad.sign(&self.identity);

        // Register locally as an Authorized provider
        self.content_store
            .provider_registry()
            .handle_advertisement(&ad, fieldnet_content::TrustTier::Authorized)?;

        let content_msg = fieldnet_content::ContentMessage::Advertise(ad.clone());

        if let Some(dest) = destination {
            let _ = self.send_content_message(dest, content_msg).await;
        } else {
            let peers = self.connection_manager.connected_peers().await;
            for peer in peers {
                let _ = self.send_content_message(peer, content_msg.clone()).await;
            }
        }

        Ok(ad)
    }

    pub async fn content_query_network(
        &self,
        content_id: &fieldnet_content::ContentId,
        desired_chunk_range: Option<(u32, u32)>,
        destination: Option<NodeId>,
    ) -> Result<fieldnet_content::ContentQuery, fieldnet_content::ContentError> {
        let mut query = fieldnet_content::ContentQuery::create(
            *content_id,
            None,
            desired_chunk_range,
            self.node_id(),
            self.identity.public_key_bytes(),
            60,
        );
        query.sign(&self.identity);

        let content_msg = fieldnet_content::ContentMessage::Query(query.clone());

        if let Some(dest) = destination {
            let _ = self.send_content_message(dest, content_msg).await;
        } else {
            let peers = self.connection_manager.connected_peers().await;
            for peer in peers {
                let _ = self.send_content_message(peer, content_msg.clone()).await;
            }
        }

        Ok(query)
    }

    /// Accessor for the Content Transfer Engine manager.
    pub fn transfer_manager(&self) -> Arc<fieldnet_content::TransferManager> {
        Arc::clone(&self.transfer_manager)
    }

    /// Requests a content object from the network or a specific provider.
    pub async fn content_request(
        &self,
        content_id: fieldnet_content::ContentId,
        provider: Option<NodeId>,
    ) -> Result<fieldnet_content::TransferId, fieldnet_content::ContentError> {
        let (transfer_id, target_provider, req) = self
            .transfer_manager
            .create_transfer(content_id, provider, fieldnet_core::Priority::Normal)
            .await?;

        let content_msg = fieldnet_content::ContentMessage::Request(req);
        if let Err(e) = self
            .send_content_message(target_provider, content_msg)
            .await
        {
            warn!(
                "Failed to dispatch content request to {}: {}",
                target_provider, e
            );
        }

        Ok(transfer_id)
    }

    /// Retrieves status of an active, paused, or completed transfer.
    pub async fn content_transfer_status(
        &self,
        transfer_id: &fieldnet_content::TransferId,
    ) -> Result<fieldnet_content::TransferSession, fieldnet_content::ContentError> {
        self.transfer_manager.get_session(transfer_id).await.ok_or(
            fieldnet_content::ContentError::TransferNotFound(*transfer_id),
        )
    }

    /// Pauses an active transfer.
    pub async fn content_pause_transfer(
        &self,
        transfer_id: &fieldnet_content::TransferId,
    ) -> Result<(), fieldnet_content::ContentError> {
        self.transfer_manager.pause_transfer(transfer_id).await
    }

    /// Resumes a paused or interrupted transfer, requesting only missing chunks.
    pub async fn content_resume_transfer(
        &self,
        transfer_id: &fieldnet_content::TransferId,
    ) -> Result<(), fieldnet_content::ContentError> {
        if let Some(req) = self.transfer_manager.resume_transfer(transfer_id).await? {
            let session = self.transfer_manager.get_session(transfer_id).await.ok_or(
                fieldnet_content::ContentError::TransferNotFound(*transfer_id),
            )?;
            let target_provider = session.provider_node_id;
            let content_msg = fieldnet_content::ContentMessage::Request(req);
            if let Err(e) = self
                .send_content_message(target_provider, content_msg)
                .await
            {
                warn!(
                    "Failed to dispatch resumed content request to {}: {}",
                    target_provider, e
                );
            }
        }
        Ok(())
    }

    /// Cancels a transfer session and notifies the provider.
    pub async fn content_cancel_transfer(
        &self,
        transfer_id: &fieldnet_content::TransferId,
    ) -> Result<(), fieldnet_content::ContentError> {
        self.transfer_manager
            .cancel_transfer(transfer_id, "User cancelled")
            .await?;
        if let Some(session) = self.transfer_manager.get_session(transfer_id).await {
            let cancel_msg =
                fieldnet_content::ContentMessage::Cancel(fieldnet_content::TransferCancel {
                    transfer_id: *transfer_id,
                    reason: "User cancelled".to_string(),
                });
            let target_provider = session.provider_node_id;
            let _ = self.send_content_message(target_provider, cancel_msg).await;
        }
        Ok(())
    }

    /// Sets or clears explicit authorized peer allowlist for serving content objects.
    pub async fn set_content_authorized_peers(&self, peers: Option<Vec<NodeId>>) {
        let set = peers.map(|p| p.into_iter().collect());
        self.transfer_manager
            .provider_service()
            .set_authorized_requesters(set)
            .await;
    }

    /// Marks content as pinned, protecting it from deletion and GC eviction.
    pub fn content_pin(
        &self,
        content_id: &fieldnet_content::ContentId,
    ) -> Result<(), fieldnet_content::ContentError> {
        self.content_store.pin(content_id)
    }

    /// Unpins content, returning it to evictable cache tier.
    pub fn content_unpin(
        &self,
        content_id: &fieldnet_content::ContentId,
    ) -> Result<(), fieldnet_content::ContentError> {
        self.content_store.unpin(content_id)
    }

    /// Checks if content is pinned.
    pub fn content_is_pinned(
        &self,
        content_id: &fieldnet_content::ContentId,
    ) -> Result<bool, fieldnet_content::ContentError> {
        self.content_store.is_pinned(content_id)
    }

    /// Audits integrity of a content object, its manifest, constituent chunks, and physical file.
    pub async fn content_audit(
        &self,
        content_id: &fieldnet_content::ContentId,
    ) -> Result<fieldnet_content::ContentAuditReport, fieldnet_content::ContentError> {
        self.content_store.audit_content(content_id).await
    }

    /// Audits integrity of all stored content objects.
    pub async fn content_audit_all(
        &self,
    ) -> Result<Vec<fieldnet_content::ContentAuditReport>, fieldnet_content::ContentError> {
        self.content_store.audit_all().await
    }

    /// Runs a garbage collection cycle to safely reclaim storage.
    pub async fn content_run_gc(
        &self,
        target_bytes: Option<u64>,
    ) -> Result<fieldnet_content::GcReport, fieldnet_content::ContentError> {
        self.content_store.run_gc(target_bytes).await
    }

    /// Queries current storage quota telemetry.
    pub async fn content_quota_status(&self) -> fieldnet_content::QuotaStatus {
        self.content_store.quota_manager().status().await
    }

    /// Grants content action permissions to a peer in the local ContentAuthorizer ACL.
    pub fn content_grant_permission(
        &self,
        content_id: &fieldnet_content::ContentId,
        peer: &NodeId,
        actions: &[fieldnet_content::ContentAction],
    ) -> Result<(), fieldnet_content::ContentError> {
        self.content_store
            .authorizer()
            .grant_permission(content_id, peer, actions)
    }

    /// Revokes content permissions for a peer in the local ContentAuthorizer ACL.
    pub fn content_revoke_permission(
        &self,
        content_id: &fieldnet_content::ContentId,
        peer: &NodeId,
    ) -> Result<(), fieldnet_content::ContentError> {
        self.content_store
            .authorizer()
            .revoke_permission(content_id, peer)
    }

    /// Marks a content object as public or private.
    pub fn content_set_public(
        &self,
        content_id: &fieldnet_content::ContentId,
        is_public: bool,
    ) -> Result<(), fieldnet_content::ContentError> {
        self.content_store
            .authorizer()
            .set_public(content_id, is_public)
    }

    /// Issues a signed capability token for content access delegation.
    pub fn content_issue_capability_token(
        &self,
        subject: Option<NodeId>,
        content_id: Option<fieldnet_content::ContentId>,
        actions: Vec<fieldnet_content::ContentAction>,
        ttl_secs: u64,
    ) -> fieldnet_content::CapabilityToken {
        fieldnet_content::CapabilityToken::new(
            &self.identity,
            subject,
            content_id,
            actions,
            ttl_secs,
        )
    }

    pub fn sync_engine(&self) -> Arc<SyncEngine> {
        Arc::clone(&self.sync_engine)
    }

    pub async fn synchronize_with(
        &self,
        peer_id: NodeId,
    ) -> Result<[u8; 16], fieldnet_data::DataError> {
        self.sync_engine.initiate_sync(peer_id).await
    }

    pub fn subscribe_delivery(&self) -> broadcast::Receiver<(MessageId, MessageDeliveryStatus)> {
        self.delivery_notify.subscribe()
    }

    pub async fn get_delivery_status(
        &self,
        message_id: &MessageId,
    ) -> Option<MessageDeliveryStatus> {
        let delivery = self.message_delivery.read().await;
        delivery.get(message_id).cloned()
    }

    pub async fn get_received_messages(&self) -> Vec<ReceivedMessage> {
        let msgs = self.received_messages.read().await;
        msgs.clone()
    }

    pub async fn get_connected_peers(&self) -> Vec<NodeId> {
        self.connection_manager.connected_peers().await
    }

    pub async fn get_discovered_peers(&self) -> Vec<DiscoveredPeer> {
        let peers = self.known_peers.read().await;
        peers.values().cloned().collect()
    }

    /// Checks if a peer is allowed based on topology configuration.
    pub fn is_peer_allowed(&self, peer_id: &NodeId) -> bool {
        if let Some(blocked) = &self.config.blocked_peers {
            if blocked.contains(peer_id) {
                return false;
            }
        }
        if let Some(allowed) = &self.config.allowed_peers {
            return allowed.contains(peer_id);
        }
        true
    }

    /// Sends a signed DATA message to a logical destination NodeId.
    /// The routing engine resolves the next hop automatically.
    /// The message is durably written to the persistent DTN queue.
    pub async fn send_data_message(
        &self,
        destination: NodeId,
        payload_text: &str,
    ) -> Result<MessageId, Box<dyn std::error::Error + Send + Sync>> {
        self.send_data_message_with_priority(
            destination,
            payload_text,
            Priority::Normal,
            16,
            120_000,
        )
        .await
    }

    pub async fn send_data_message_with_priority(
        &self,
        destination: NodeId,
        payload_text: &str,
        priority: Priority,
        ttl: u8,
        expires_in_millis: u64,
    ) -> Result<MessageId, Box<dyn std::error::Error + Send + Sync>> {
        let sequence = {
            let mut seq = self.sequence_counter.write().await;
            let current = *seq;
            *seq += 1;
            current
        };

        let message = Message::sign_and_build(
            &self.identity,
            destination,
            MessageType::Data,
            payload_text.as_bytes().to_vec(),
            ttl,
            expires_in_millis,
            sequence,
            priority,
        )?;

        let message_id = message.message_id;

        // Record initial SENT delivery state
        let sent_status = MessageDeliveryStatus::Sent {
            sent_at: Timestamp::now(),
        };
        {
            let mut deliv = self.message_delivery.write().await;
            deliv.insert(message_id, sent_status.clone());
        }
        let _ = self.delivery_notify.send((message_id, sent_status));

        // Always save to persistent DTN queue to survive sudden reboots and route drops
        self.dtn_queue.enqueue(&message)?;

        // Check if an immediate next hop route is available
        if let Some(next_hop) = self.routing_table.get_next_hop(&destination).await {
            if self.connection_manager.is_connected(&next_hop).await {
                info!(
                    "Sending DATA message {} towards destination {} via next hop {} (len: {} bytes, seq: {})",
                    message_id, destination, next_hop, message.payload.len(), sequence
                );
                if let Err(e) = self
                    .connection_manager
                    .send_message(&next_hop, message)
                    .await
                {
                    debug!(
                        "Direct transmission to next hop {} failed: {}; kept in DTN queue",
                        next_hop, e
                    );
                }
            } else {
                debug!(
                    "Next hop {} currently disconnected; message {} staged in DTN queue",
                    next_hop, message_id
                );
            }
        } else {
            info!(
                "No immediate route known for destination {}; message {} staged in persistent DTN queue",
                destination, message_id
            );
        }

        Ok(message_id)
    }

    /// Sends a signed SYNC protocol message to a logical destination NodeId.
    /// Stages in persistent DTN queue and attempts immediate transmission if next hop is connected.
    pub async fn send_sync_message(
        &self,
        destination: NodeId,
        sync_msg: SyncMessage,
    ) -> Result<MessageId, Box<dyn std::error::Error + Send + Sync>> {
        let payload = sync_msg.to_bytes()?;
        let sequence = {
            let mut seq = self.sequence_counter.write().await;
            let current = *seq;
            *seq += 1;
            current
        };

        let message = Message::sign_and_build(
            &self.identity,
            destination,
            MessageType::Sync,
            payload,
            16,      // multi-hop TTL
            120_000, // 2-minute expiration
            sequence,
            Priority::Normal,
        )?;

        let message_id = message.message_id;

        // If an immediate next hop route is available and connected, send directly.
        // Otherwise, stage in persistent DTN queue for store-and-forward delivery.
        if let Some(next_hop) = self.routing_table.get_next_hop(&destination).await {
            if self.connection_manager.is_connected(&next_hop).await {
                info!(
                    "Dispatching SYNC message {} ({}) towards destination {} via next hop {}",
                    message_id,
                    sync_msg.message_name(),
                    destination,
                    next_hop
                );
                if let Err(e) = self
                    .connection_manager
                    .send_message(&next_hop, message.clone())
                    .await
                {
                    debug!(
                        "Direct sync transmission to next hop {} failed: {}; staging in DTN queue",
                        next_hop, e
                    );
                    self.dtn_queue.enqueue(&message)?;
                }
            } else {
                debug!(
                    "Next hop {} disconnected; staging SYNC message {} in DTN queue",
                    next_hop, message_id
                );
                self.dtn_queue.enqueue(&message)?;
            }
        } else {
            info!(
                "No route known for destination {}; staging SYNC message {} in DTN queue",
                destination, message_id
            );
            self.dtn_queue.enqueue(&message)?;
        }

        Ok(message_id)
    }

    /// Sends a signed FILE/CONTENT discovery protocol message (framed as `MessageType::File = 7`)
    /// to a logical destination NodeId.
    /// Stages in persistent DTN queue and attempts immediate transmission if next hop is connected.
    pub async fn send_content_message(
        &self,
        destination: NodeId,
        content_msg: fieldnet_content::ContentMessage,
    ) -> Result<MessageId, Box<dyn std::error::Error + Send + Sync>> {
        let payload = content_msg.to_bytes()?;
        let sequence = {
            let mut seq = self.sequence_counter.write().await;
            let current = *seq;
            *seq += 1;
            current
        };

        let message = Message::sign_and_build(
            &self.identity,
            destination,
            MessageType::File,
            payload,
            16,      // multi-hop TTL
            120_000, // 2-minute expiration
            sequence,
            Priority::Normal,
        )?;

        let message_id = message.message_id;

        // If an immediate next hop route is available and connected, send directly.
        // Otherwise, stage in persistent DTN queue for store-and-forward delivery.
        if let Some(next_hop) = self.routing_table.get_next_hop(&destination).await {
            if self.connection_manager.is_connected(&next_hop).await {
                info!(
                    "Dispatching FILE/CONTENT message {} towards destination {} via next hop {}",
                    message_id, destination, next_hop
                );
                if let Err(e) = self
                    .connection_manager
                    .send_message(&next_hop, message.clone())
                    .await
                {
                    debug!(
                        "Direct content transmission to next hop {} failed: {}; staging in DTN queue",
                        next_hop, e
                    );
                    self.dtn_queue.enqueue(&message)?;
                }
                return Ok(message_id);
            }
        }

        if self.connection_manager.is_connected(&destination).await {
            info!(
                "Dispatching direct FILE/CONTENT message {} to destination {}",
                message_id, destination
            );
            if let Err(e) = self
                .connection_manager
                .send_message(&destination, message.clone())
                .await
            {
                debug!(
                    "Direct content transmission to destination {} failed: {}; staging in DTN queue",
                    destination, e
                );
                self.dtn_queue.enqueue(&message)?;
            }
        } else {
            info!(
                "No connected route for FILE/CONTENT message {} to destination {}; staging in DTN queue",
                message_id,
                destination
            );
            self.dtn_queue.enqueue(&message)?;
        }

        Ok(message_id)
    }

    fn spawn_sync_dispatch_loop(
        self: &Arc<Self>,
        mut sync_rx: tokio::sync::mpsc::UnboundedReceiver<(NodeId, SyncMessage)>,
    ) {
        let runtime = Arc::clone(self);
        let mut shutdown_rx = runtime.shutdown_tx.subscribe();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    item = sync_rx.recv() => {
                        match item {
                            Some((dest, msg)) => {
                                if let Err(e) = runtime.send_sync_message(dest, msg).await {
                                    debug!("Failed to dispatch sync message to {}: {}", dest, e);
                                }
                            }
                            None => break,
                        }
                    }
                    _ = shutdown_rx.recv() => break,
                }
            }
        });
    }

    /// Waits until an ACK is received for the given message ID or timeout occurs.
    pub async fn wait_for_ack(
        &self,
        message_id: MessageId,
        timeout: Duration,
    ) -> Result<MessageDeliveryStatus, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(status) = self.get_delivery_status(&message_id).await {
            if matches!(status, MessageDeliveryStatus::AckReceived { .. }) {
                return Ok(status);
            }
        }

        let mut rx = self.subscribe_delivery();
        let timeout_fut = tokio::time::sleep(timeout);
        tokio::pin!(timeout_fut);

        loop {
            tokio::select! {
                res = rx.recv() => {
                    match res {
                        Ok((id, status)) => {
                            if id == message_id && matches!(status, MessageDeliveryStatus::AckReceived { .. }) {
                                return Ok(status);
                            }
                        }
                        Err(e) => {
                            return Err(format!("Delivery notification stream error: {}", e).into());
                        }
                    }
                }
                _ = &mut timeout_fut => {
                    return Err(format!("Timed out waiting for ACK for message {}", message_id).into());
                }
            }
        }
    }

    fn spawn_discovery_loop(self: &Arc<Self>) {
        let runtime = Arc::clone(self);
        let mut peer_rx = runtime.discovery.subscribe();
        let mut shutdown_rx = runtime.shutdown_tx.subscribe();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    peer_res = peer_rx.recv() => {
                        match peer_res {
                            Ok(peer) => {
                                let peer_id = peer.node_id;
                                let peer_addr = peer.tcp_addr;

                                // Filter out peers not permitted by topology constraints
                                if !runtime.is_peer_allowed(&peer_id) {
                                    continue;
                                }

                                runtime.content_store.provider_registry().handle_peer_discovered(&peer_id);

                                {
                                    let mut peers = runtime.known_peers.write().await;
                                    peers.insert(peer_id, peer.clone());
                                }

                                if runtime.config.auto_connect {
                                    // Deterministic P2P tie-breaker:
                                    // Lower NodeId acts as connection initiator.
                                    if runtime.node_id() < peer_id {
                                        if !runtime.connection_manager.is_connected(&peer_id).await {
                                            info!(
                                                "Discovery triggered auto-connect (initiator: local < remote) to peer {} at {}",
                                                peer_id, peer_addr
                                            );
                                            let cm = Arc::clone(&runtime.connection_manager);
                                            tokio::spawn(async move {
                                                if let Err(e) = cm.connect_peer(peer_id, peer_addr).await {
                                                    debug!("Failed to connect to discovered peer {}: {}", peer_id, e);
                                                }
                                            });
                                        }
                                    } else {
                                        debug!("Discovery recorded peer {} (responder: local > remote), awaiting incoming TCP", peer_id);
                                    }
                                }
                            }
                            Err(e) => {
                                debug!("Discovery channel lagged: {}", e);
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("Discovery coordination loop shutting down");
                        break;
                    }
                }
            }
        });
    }

    fn spawn_connection_event_loop(self: &Arc<Self>) {
        let runtime = Arc::clone(self);
        let mut event_rx = runtime.connection_manager.subscribe_events();
        let mut shutdown_rx = runtime.shutdown_tx.subscribe();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    ev_res = event_rx.recv() => {
                        match ev_res {
                            Ok(ConnectionEvent::PeerConnected { peer_id, session_id }) => {
                                info!("PeerConnected event for {} (session: {})", peer_id, session_id);
                                // Add as direct 1-hop route
                                runtime.routing_table.add_direct_neighbor(peer_id).await;
                                runtime.content_store.provider_registry().handle_peer_discovered(&peer_id);

                                // Immediately send our route advertisement to the newly connected neighbor
                                let advert = runtime.routing_table.generate_advertisement().await;
                                if let Ok(bytes) = advert.to_bytes() {
                                    let seq = {
                                        let mut s = runtime.sequence_counter.write().await;
                                        let cur = *s;
                                        *s += 1;
                                        cur
                                    };
                                    if let Ok(msg) = Message::sign_and_build(
                                        &runtime.identity,
                                        peer_id,
                                        MessageType::Route,
                                        bytes,
                                        1,
                                        60_000,
                                        seq,
                                        Priority::High,
                                    ) {
                                        let _ = runtime.connection_manager.send_message(&peer_id, msg).await;
                                    }
                                }

                                // Trigger DTN queue flush upon connection establishment
                                runtime.dtn_flush_notify.notify_waiters();

                                // Automatically trigger Sync handshake upon peer connection
                                let se = Arc::clone(&runtime.sync_engine);
                                tokio::spawn(async move {
                                    let _ = se.initiate_sync(peer_id).await;
                                });
                            }
                            Ok(ConnectionEvent::PeerDisconnected { peer_id }) => {
                                info!("PeerDisconnected event for {}", peer_id);
                                // Invalidate routes that used this peer as next_hop
                                runtime.routing_table.remove_routes_via(&peer_id).await;
                                runtime.content_store.provider_registry().handle_peer_expired(&peer_id);
                            }
                            Ok(ConnectionEvent::StateChanged { .. }) => {}
                            Err(e) => {
                                debug!("Connection event channel lagged: {}", e);
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("Connection event loop shutting down");
                        break;
                    }
                }
            }
        });
    }

    fn spawn_routing_advertisement_loop(self: &Arc<Self>) {
        let runtime = Arc::clone(self);
        let mut shutdown_rx = runtime.shutdown_tx.subscribe();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(500));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        // Purge expired routes
                        let _ = runtime.routing_table.purge_expired().await;

                        let connected = runtime.connection_manager.connected_peers().await;
                        if connected.is_empty() {
                            continue;
                        }

                        let advert = runtime.routing_table.generate_advertisement().await;
                        if let Ok(bytes) = advert.to_bytes() {
                            for peer in connected {
                                let seq = {
                                    let mut s = runtime.sequence_counter.write().await;
                                    let cur = *s;
                                    *s += 1;
                                    cur
                                };
                                if let Ok(msg) = Message::sign_and_build(
                                    &runtime.identity,
                                    peer,
                                    MessageType::Route,
                                    bytes.clone(),
                                    1, // 1-hop control message
                                    60_000,
                                    seq,
                                    Priority::Normal,
                                ) {
                                    let _ = runtime.connection_manager.send_message(&peer, msg).await;
                                }
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("Routing advertisement loop shutting down");
                        break;
                    }
                }
            }
        });
    }

    fn spawn_provider_maintenance_loop(self: &Arc<Self>) {
        let runtime = Arc::clone(self);
        let mut shutdown_rx = runtime.shutdown_tx.subscribe();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(5));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let now = fieldnet_core::Timestamp::now();
                        let expired = runtime.content_store.provider_registry().expire(now);
                        if expired > 0 {
                            debug!("Provider maintenance loop expired {} records", expired);
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("Provider maintenance loop shutting down");
                        break;
                    }
                }
            }
        });
    }

    fn spawn_dtn_worker_loop(self: &Arc<Self>) {
        let runtime = Arc::clone(self);
        let mut shutdown_rx = runtime.shutdown_tx.subscribe();
        let notify = Arc::clone(&runtime.dtn_flush_notify);

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(250));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        runtime.flush_dtn_queue().await;
                    }
                    _ = notify.notified() => {
                        runtime.flush_dtn_queue().await;
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("DTN worker loop shutting down");
                        break;
                    }
                }
            }
        });
    }

    async fn flush_dtn_queue(&self) {
        let now = Timestamp::now().as_millis();
        let _ = self.dtn_queue.purge_expired(now);

        if let Ok(runnable) = self.dtn_queue.get_runnable(now, 20) {
            for msg in runnable {
                if let Some(next_hop) = self.routing_table.get_next_hop(&msg.destination).await {
                    if self.connection_manager.is_connected(&next_hop).await {
                        debug!(
                            "DTN Worker: dispatching bundle {} to next hop {}",
                            msg.message_id, next_hop
                        );
                        if let Err(e) = self
                            .connection_manager
                            .send_message(&next_hop, msg.clone())
                            .await
                        {
                            debug!("DTN Worker: dispatch failed to {}: {}", next_hop, e);
                            let _ = self.dtn_queue.record_retry_failure(&msg.message_id);
                        }
                    } else {
                        let _ = self.dtn_queue.record_retry_failure(&msg.message_id);
                    }
                } else {
                    let _ = self.dtn_queue.record_retry_failure(&msg.message_id);
                }
            }
        }
    }

    async fn spawn_message_processing_loop(self: &Arc<Self>) {
        let runtime = Arc::clone(self);
        let mut inbound_rx = match runtime.connection_manager.take_inbound_receiver().await {
            Some(rx) => rx,
            None => {
                error!("Inbound message receiver already taken");
                return;
            }
        };
        let mut shutdown_rx = runtime.shutdown_tx.subscribe();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    msg_res = inbound_rx.recv() => {
                        match msg_res {
                            Some((peer_id, message)) => {
                                runtime.handle_incoming_message(peer_id, message).await;
                            }
                            None => {
                                debug!("Inbound message channel closed");
                                break;
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("Message processing loop shutting down");
                        break;
                    }
                }
            }
        });
    }

    async fn handle_incoming_message(self: &Arc<Self>, peer_id: NodeId, message: Message) {
        // 1. Verify message expiration
        if message.is_expired() {
            warn!(
                "Discarding expired message {} from peer {} (expired at {})",
                message.message_id, peer_id, message.expires_at
            );
            return;
        }

        // 2. Verify hop TTL
        if message.ttl == 0 {
            warn!(
                "Discarding message {} with zero TTL from peer {}",
                message.message_id, peer_id
            );
            return;
        }

        // 3. Handle Route advertisements (control traffic)
        if message.message_type == MessageType::Route {
            match RouteAdvertisement::from_bytes(&message.payload) {
                Ok(advert) => {
                    let changed = self
                        .routing_table
                        .update_from_advertisement(peer_id, advert)
                        .await;
                    if changed {
                        self.dtn_flush_notify.notify_waiters();
                    }
                }
                Err(e) => {
                    warn!(
                        "Malformed RouteAdvertisement payload from peer {}: {}",
                        peer_id, e
                    );
                }
            }
            return;
        }

        // 4. Destination Branch: Is this message destined for us, or a multi-hop forward?
        if message.destination == self.node_id() {
            // Local Delivery to this node
            self.handle_local_delivery(peer_id, message).await;
        } else {
            // Multi-Hop Relay Forwarding
            self.handle_multi_hop_forward(peer_id, message).await;
        }
    }

    async fn handle_local_delivery(self: &Arc<Self>, peer_id: NodeId, message: Message) {
        // Duplicate check
        let is_new = self.deduplicator.check_and_record(&message.message_id);
        if !is_new {
            warn!(
                "Duplicate message {} detected from peer {}",
                message.message_id, peer_id
            );
            // Re-acknowledge duplicate DATA message so sender can clear its queue
            if message.message_type == MessageType::Data {
                let _ = self.send_ack_for(message.source, message.message_id).await;
            }
            return;
        }

        match message.message_type {
            MessageType::Data => {
                info!(
                    "DATA message {} verified and received at destination from originator {} (payload len: {} bytes, hops: {})",
                    message.message_id, message.source, message.payload.len(), 16 - message.ttl
                );

                if let Ok(text) = std::str::from_utf8(&message.payload) {
                    info!("DATA content from {}: \"{}\"", message.source, text);
                }

                // Record received message
                {
                    let mut msgs = self.received_messages.write().await;
                    msgs.push(ReceivedMessage {
                        message: message.clone(),
                        received_at: Timestamp::now(),
                        is_duplicate: false,
                    });
                }

                // Send ACK (RECEIVED) back to originator
                if let Err(e) = self.send_ack_for(message.source, message.message_id).await {
                    error!("Failed to send ACK for {}: {}", message.message_id, e);
                }
            }
            MessageType::Ack => {
                match AckPayload::from_bytes(&message.payload) {
                    Ok(ack) => {
                        info!(
                            "ACK received from peer {} for message {} (status: {:?})",
                            peer_id, ack.message_id, ack.status
                        );

                        // Remove from DTN queue as delivery is confirmed
                        let _ = self.dtn_queue.remove(&ack.message_id);

                        let ack_status = MessageDeliveryStatus::AckReceived {
                            ack_at: Timestamp::now(),
                            status: ack.status,
                        };

                        {
                            let mut deliv = self.message_delivery.write().await;
                            deliv.insert(ack.message_id, ack_status.clone());
                        }
                        let _ = self.delivery_notify.send((ack.message_id, ack_status));
                    }
                    Err(e) => {
                        warn!("Malformed ACK payload from peer {}: {}", peer_id, e);
                    }
                }
            }
            MessageType::Auth => {
                debug!("AUTH message received from peer {}", peer_id);
            }
            MessageType::Error => {
                warn!(
                    "ERROR message received from peer {}: {:?}",
                    peer_id, message.payload
                );
            }
            MessageType::Route => {}
            MessageType::Sync => {
                info!(
                    "SYNC message {} verified and received at destination from originator {} (payload len: {} bytes, hops: {})",
                    message.message_id, message.source, message.payload.len(), 16 - message.ttl
                );

                match SyncMessage::from_bytes(&message.payload) {
                    Ok(sync_msg) => {
                        match self
                            .sync_engine
                            .handle_inbound_message(message.source, sync_msg)
                            .await
                        {
                            Ok(Some(response)) => {
                                if let Err(e) =
                                    self.send_sync_message(message.source, response).await
                                {
                                    warn!(
                                        "Failed to dispatch sync response to {}: {}",
                                        message.source, e
                                    );
                                }
                            }
                            Ok(None) => {}
                            Err(e) => {
                                warn!(
                                    "SyncEngine error handling message from {}: {}",
                                    message.source, e
                                );
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Malformed SyncMessage from {}: {}", message.source, e);
                    }
                }
            }
            MessageType::File => {
                info!(
                    "FILE/CONTENT message {} received at destination from {} (payload len: {} bytes)",
                    message.message_id, message.source, message.payload.len()
                );

                match fieldnet_content::ContentMessage::from_bytes(&message.payload) {
                    Ok(fieldnet_content::ContentMessage::Query(query)) => {
                        let now = Timestamp::now();
                        if let Err(e) = query.validate(now) {
                            warn!(
                                "Received invalid ContentQuery from {}: {}",
                                message.source, e
                            );
                            return;
                        }
                        info!(
                            "Received ContentQuery for {} from {}",
                            query.content_id, query.requester
                        );

                        // Check if we have this content or manifest locally
                        if let Ok(true) = self.content_store.has(&query.content_id) {
                            let metadata = self.content_store.stat(&query.content_id).ok();
                            let manifest = self.content_store.get_manifest(&query.content_id).ok();

                            let mut ad = fieldnet_content::ContentAdvertise::new_full(
                                query.content_id,
                                manifest.as_ref().map(|m| m.manifest_id),
                                self.node_id(),
                                self.identity.public_key_bytes(),
                                metadata.as_ref().map(|m| m.size_bytes),
                                300,
                                1,
                            );
                            ad.query_id = Some(query.query_id);
                            ad.sign(&self.identity);

                            let resp_msg = fieldnet_content::ContentMessage::Advertise(ad);
                            let self_clone = Arc::clone(self);
                            tokio::spawn(async move {
                                if let Err(e) = self_clone
                                    .send_content_message(query.requester, resp_msg)
                                    .await
                                {
                                    warn!(
                                        "Failed to dispatch ContentAdvertise response to {}: {}",
                                        query.requester, e
                                    );
                                }
                            });
                        }
                    }
                    Ok(fieldnet_content::ContentMessage::Advertise(ad)) => {
                        let now = Timestamp::now();
                        if let Err(e) = ad.validate(now) {
                            warn!(
                                "Received invalid ContentAdvertise from {}: {}",
                                message.source, e
                            );
                            return;
                        }

                        // Determine trust tier based on topology / security state
                        let trust_tier = if ad.provider_node_id == self.node_id()
                            || self.is_peer_allowed(&ad.provider_node_id)
                        {
                            fieldnet_content::TrustTier::Authorized
                        } else if self
                            .connection_manager
                            .is_connected(&ad.provider_node_id)
                            .await
                        {
                            fieldnet_content::TrustTier::Authenticated
                        } else {
                            fieldnet_content::TrustTier::Discovered
                        };

                        info!(
                            "Received valid ContentAdvertise from {} for {} (trust: {:?})",
                            ad.provider_node_id, ad.content_id, trust_tier
                        );

                        if let Err(e) = self
                            .content_store
                            .provider_registry()
                            .handle_advertisement(&ad, trust_tier)
                        {
                            warn!("Failed to register provider advertisement: {}", e);
                        }
                    }
                    Ok(fieldnet_content::ContentMessage::Request(req)) => {
                        info!(
                            "Received ContentRequest from {} for transfer {} (chunks: {:?})",
                            message.source, req.transfer_id, req.requested_chunks
                        );

                        // Authenticate: Ensure message sender matches claimed requester
                        if message.source != req.requester_node_id {
                            warn!(
                                "ContentRequest sender {} mismatch with claimed requester {}",
                                message.source, req.requester_node_id
                            );
                            return;
                        }

                        let is_authorized = self.is_peer_allowed(&req.requester_node_id);
                        let provider_svc = self.transfer_manager.provider_service();

                        match provider_svc.handle_request(&req, is_authorized).await {
                            Ok(fieldnet_content::ProviderResponse::Response(resp)) => {
                                let resp_msg = fieldnet_content::ContentMessage::Response(*resp);
                                let self_clone = Arc::clone(self);
                                tokio::spawn(async move {
                                    if let Err(e) = self_clone
                                        .send_content_message(req.requester_node_id, resp_msg)
                                        .await
                                    {
                                        warn!(
                                            "Failed to send ContentResponse to {}: {}",
                                            req.requester_node_id, e
                                        );
                                    }
                                });
                            }
                            Ok(fieldnet_content::ProviderResponse::Chunks(chunks)) => {
                                let self_clone = Arc::clone(self);
                                tokio::spawn(async move {
                                    for chunk in chunks {
                                        let chunk_msg =
                                            fieldnet_content::ContentMessage::Chunk(chunk);
                                        if let Err(e) = self_clone
                                            .send_content_message(req.requester_node_id, chunk_msg)
                                            .await
                                        {
                                            warn!(
                                                "Failed to stream chunk to {}: {}",
                                                req.requester_node_id, e
                                            );
                                            break;
                                        }
                                    }
                                });
                            }
                            Ok(fieldnet_content::ProviderResponse::Error(_status, err_msg)) => {
                                let err_frame = fieldnet_content::ContentMessage::Error(
                                    fieldnet_content::TransferError {
                                        transfer_id: req.transfer_id,
                                        code: fieldnet_content::TransferErrorCode::Unauthorized,
                                        message: err_msg,
                                    },
                                );
                                let self_clone = Arc::clone(self);
                                tokio::spawn(async move {
                                    let _ = self_clone
                                        .send_content_message(req.requester_node_id, err_frame)
                                        .await;
                                });
                            }
                            Err(e) => {
                                warn!("Provider service error handling request: {}", e);
                            }
                        }
                    }
                    Ok(fieldnet_content::ContentMessage::Response(resp)) => {
                        info!(
                            "Received ContentResponse from {} for transfer {} (status: {:?})",
                            message.source, resp.transfer_id, resp.status
                        );
                        let self_clone = Arc::clone(self);
                        tokio::spawn(async move {
                            match self_clone.transfer_manager.handle_response(resp).await {
                                Ok(Some(next_req)) => {
                                    let req_msg =
                                        fieldnet_content::ContentMessage::Request(next_req);
                                    if let Err(e) = self_clone
                                        .send_content_message(message.source, req_msg)
                                        .await
                                    {
                                        warn!(
                                            "Failed to dispatch chunk request to {}: {}",
                                            message.source, e
                                        );
                                    }
                                }
                                Ok(None) => {}
                                Err(e) => {
                                    warn!("Error processing ContentResponse: {}", e);
                                }
                            }
                        });
                    }
                    Ok(fieldnet_content::ContentMessage::Chunk(chunk)) => {
                        info!(
                            "Received ContentChunk from {} for transfer {} index {} (bytes: {})",
                            message.source,
                            chunk.transfer_id,
                            chunk.chunk_index,
                            chunk.data.len()
                        );
                        let self_clone = Arc::clone(self);
                        tokio::spawn(async move {
                            match self_clone.transfer_manager.handle_chunk(chunk).await {
                                Ok((ack, maybe_next_req, is_complete)) => {
                                    // Send ACK back to provider
                                    let ack_msg =
                                        fieldnet_content::ContentMessage::Ack(ack.clone());
                                    let _ = self_clone
                                        .send_content_message(message.source, ack_msg)
                                        .await;

                                    // If more chunks need to be scheduled within backpressure window
                                    if let Some(next_req) = maybe_next_req {
                                        let req_msg =
                                            fieldnet_content::ContentMessage::Request(next_req);
                                        let _ = self_clone
                                            .send_content_message(message.source, req_msg)
                                            .await;
                                    }

                                    // If complete, send TransferComplete notification
                                    if is_complete {
                                        if let Some(session) = self_clone
                                            .transfer_manager
                                            .get_session(&ack.transfer_id)
                                            .await
                                        {
                                            let comp_msg =
                                                fieldnet_content::ContentMessage::Complete(
                                                    fieldnet_content::TransferComplete {
                                                        transfer_id: ack.transfer_id,
                                                        content_id: session.content_id,
                                                        total_bytes: session
                                                            .total_bytes
                                                            .unwrap_or(0),
                                                    },
                                                );
                                            let _ = self_clone
                                                .send_content_message(message.source, comp_msg)
                                                .await;
                                        }
                                        info!(transfer_id = %ack.transfer_id, "Content transfer successfully verified and complete!");
                                    }
                                }
                                Err(e) => {
                                    warn!("Error processing ContentChunk: {}", e);
                                }
                            }
                        });
                    }
                    Ok(fieldnet_content::ContentMessage::Ack(ack)) => {
                        debug!(
                            "Received ContentAck from {} for transfer {} chunk {} (status: {:?})",
                            message.source, ack.transfer_id, ack.chunk_index, ack.status
                        );
                    }
                    Ok(fieldnet_content::ContentMessage::Cancel(cancel)) => {
                        info!(
                            "Received TransferCancel for transfer {}: {}",
                            cancel.transfer_id, cancel.reason
                        );
                        let _ = self
                            .transfer_manager
                            .cancel_transfer(&cancel.transfer_id, &cancel.reason)
                            .await;
                    }
                    Ok(fieldnet_content::ContentMessage::Complete(comp)) => {
                        info!(
                            "Received TransferComplete for transfer {}: total_bytes {}",
                            comp.transfer_id, comp.total_bytes
                        );
                    }
                    Ok(fieldnet_content::ContentMessage::Error(err)) => {
                        warn!(
                            "Received TransferError from {} for transfer {}: {:?}",
                            message.source, err.transfer_id, err.message
                        );
                        let _ = self
                            .transfer_manager
                            .fail_transfer(&err.transfer_id, &err.message)
                            .await;
                    }
                    Err(e) => {
                        warn!(
                            "Malformed ContentMessage payload from {}: {}",
                            message.source, e
                        );
                    }
                }
            }
        }
    }

    async fn handle_multi_hop_forward(self: &Arc<Self>, peer_id: NodeId, mut message: Message) {
        if !message.message_type.is_forwardable() {
            warn!(
                "Message type {:?} is not forwardable; dropping",
                message.message_type
            );
            return;
        }

        // Loop prevention: do not relay messages originated from ourselves
        if message.source == self.node_id() {
            warn!(
                "Relay node {}: message {} originated from self; dropping loop",
                self.node_id(),
                message.message_id
            );
            return;
        }

        // Decrement TTL during relay forwarding
        if let Err(e) = message.decrement_ttl() {
            warn!(
                "TTL exhausted on message {} during forwarding: {}",
                message.message_id, e
            );
            return;
        }

        let destination = message.destination;
        let message_id = message.message_id;

        info!(
            "Relay node {}: forwarding multi-hop message {} received from peer {} destined for {} (remaining ttl: {})",
            self.node_id(), message_id, peer_id, destination, message.ttl
        );

        if let Some(next_hop) = self.routing_table.get_next_hop(&destination).await {
            // Loop prevention: do not bounce packet back to the immediate sender
            if next_hop == peer_id {
                warn!(
                    "Relay node {}: routing loop detected (next hop is incoming peer {}); dropping",
                    self.node_id(),
                    peer_id
                );
                return;
            }

            if self.connection_manager.is_connected(&next_hop).await {
                info!(
                    "Forwarding message {} towards {} via next hop {}",
                    message_id, destination, next_hop
                );
                if let Err(e) = self
                    .connection_manager
                    .send_message(&next_hop, message.clone())
                    .await
                {
                    debug!(
                        "Forwarding failed to {}: {}; staging in DTN queue",
                        next_hop, e
                    );
                    let _ = self.dtn_queue.enqueue(&message);
                }
            } else {
                info!(
                    "Next hop {} disconnected; staging message {} in DTN queue",
                    next_hop, message_id
                );
                let _ = self.dtn_queue.enqueue(&message);
            }
        } else {
            info!(
                "No route currently known for destination {}; staging message {} in DTN queue",
                destination, message_id
            );
            let _ = self.dtn_queue.enqueue(&message);
        }
    }

    pub async fn send_ack_for(
        &self,
        destination: NodeId,
        target_message_id: MessageId,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ack_payload = AckPayload::new_received(target_message_id);
        let payload_bytes = ack_payload.to_bytes()?;

        let sequence = {
            let mut seq = self.sequence_counter.write().await;
            let current = *seq;
            *seq += 1;
            current
        };

        let ack_msg = Message::sign_and_build(
            &self.identity,
            destination,
            MessageType::Ack,
            payload_bytes,
            16,      // multi-hop ttl
            120_000, // 2-minute expiration
            sequence,
            Priority::High,
        )?;

        info!(
            "Sending ACK for message {} to destination {}",
            target_message_id, destination
        );

        // Route ACK via routing table
        if let Some(next_hop) = self.routing_table.get_next_hop(&destination).await {
            if self.connection_manager.is_connected(&next_hop).await {
                self.connection_manager
                    .send_message(&next_hop, ack_msg)
                    .await?;
                return Ok(());
            }
        }

        // If next_hop not connected, stage in DTN
        self.dtn_queue.enqueue(&ack_msg)?;
        Ok(())
    }

    pub fn shutdown(&self) {
        info!("Shutting down FIELDNET Runtime");
        let _ = self.shutdown_tx.send(());
        self.discovery.shutdown();
        self.connection_manager.shutdown();
    }
}
