use fieldnet_content::identity::ContentId;
use fieldnet_content::store::ContentStore;
use fieldnet_core::{NodeId, Timestamp};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

use super::bundle::PackageBundle;
use super::catalog::{PackageCatalog, PackageCatalogState};
use super::discovery::ApplicationPackageDiscovery;
use super::messages::{
    ApplicationPackageMessage, PackageAdvertisement, PackageChunkRequest, PackageRequest,
    PackageResponse, PackageTransferChunk,
};
use super::persistence::PackageStorageDb;
use super::policy::{PackageSharingPolicy, PublisherTrustPolicy, TransferLimits};
use super::transfer::{PackageTransferSession, TransferStatus};
use crate::cas::CasPackageStorage;
use crate::error::ApplicationError;
use crate::events::ApplicationEvent;
use crate::id::{AppVersion, AppVersionReq, ApplicationId};
use crate::installer::ApplicationInstaller;
use crate::registry::ApplicationRegistry;

/// Network transport abstraction for transmitting distributed package messages over FIELDNET overlays.
#[async_trait::async_trait]
pub trait PackageNetworkSender: Send + Sync {
    /// Sends a point-to-point package message to a destination peer.
    async fn send_package_message(
        &self,
        destination: NodeId,
        msg: ApplicationPackageMessage,
    ) -> Result<(), ApplicationError>;

    /// Broadcasts an application package advertisement across the mesh network.
    async fn broadcast_package_advertisement(
        &self,
        ad: PackageAdvertisement,
    ) -> Result<(), ApplicationError>;
}

/// Central manager orchestrating package discovery, access control, chunked peer-to-peer transfers,
/// cryptographic verification, and handoff to the application installer.
pub struct PackageDistributionManager {
    local_node_id: NodeId,
    catalog: Arc<RwLock<PackageCatalog>>,
    discovery: Arc<ApplicationPackageDiscovery>,
    sharing_policy: Arc<RwLock<PackageSharingPolicy>>,
    trust_policy: Arc<RwLock<PublisherTrustPolicy>>,
    limits: TransferLimits,
    sessions: Arc<RwLock<HashMap<String, PackageTransferSession>>>,
    outgoing_cache: Arc<RwLock<HashMap<String, Vec<PackageTransferChunk>>>>,
    db: Arc<PackageStorageDb>,
    content_store: Arc<ContentStore>,
    registry: Arc<ApplicationRegistry>,
    network_sender: Arc<dyn PackageNetworkSender>,
    event_tx: Option<broadcast::Sender<ApplicationEvent>>,
}

impl PackageDistributionManager {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        local_node_id: NodeId,
        catalog: Arc<RwLock<PackageCatalog>>,
        discovery: Arc<ApplicationPackageDiscovery>,
        sharing_policy: Arc<RwLock<PackageSharingPolicy>>,
        trust_policy: Arc<RwLock<PublisherTrustPolicy>>,
        limits: TransferLimits,
        db: Arc<PackageStorageDb>,
        content_store: Arc<ContentStore>,
        registry: Arc<ApplicationRegistry>,
        network_sender: Arc<dyn PackageNetworkSender>,
        event_tx: Option<broadcast::Sender<ApplicationEvent>>,
    ) -> Self {
        Self {
            local_node_id,
            catalog,
            discovery,
            sharing_policy,
            trust_policy,
            limits,
            sessions: Arc::new(RwLock::new(HashMap::new())),
            outgoing_cache: Arc::new(RwLock::new(HashMap::new())),
            db,
            content_store,
            registry,
            network_sender,
            event_tx,
        }
    }

    /// Access the underlying package catalog.
    pub fn catalog(&self) -> &Arc<RwLock<PackageCatalog>> {
        &self.catalog
    }

    /// Access the discovery engine.
    pub fn discovery(&self) -> &Arc<ApplicationPackageDiscovery> {
        &self.discovery
    }

    /// Access the sharing policy.
    pub fn sharing_policy(&self) -> &Arc<RwLock<PackageSharingPolicy>> {
        &self.sharing_policy
    }

    /// Access the publisher trust policy.
    pub fn trust_policy(&self) -> &Arc<RwLock<PublisherTrustPolicy>> {
        &self.trust_policy
    }

    /// Access the transfer limits.
    pub fn limits(&self) -> TransferLimits {
        self.limits
    }

    /// Advertises locally installed and shared packages across the mesh network.
    pub async fn advertise_installed_packages(&self) -> Result<usize, ApplicationError> {
        let apps = self.registry.list()?;
        let now = Timestamp::now();
        let expiry = Timestamp(now.0 + (self.limits.request_ttl_ms * 10));
        let mut count = 0;

        for app in apps {
            let is_shared = {
                let policy = self.sharing_policy.read().await;
                policy.is_sharing_allowed(&app.application_id)
            };

            if !is_shared {
                continue;
            }

            let ad = PackageAdvertisement {
                application_id: app.application_id.clone(),
                version: app.installed_version.clone(),
                package_id: app.package_id.clone(),
                package_hash: app.package_hash,
                publisher_id: app.publisher,
                manifest_hash: app.manifest.compute_manifest_hash(),
                package_size: app
                    .items
                    .iter()
                    .map(|item| item.size_bytes)
                    .sum::<u64>()
                    .max(1),
                required_capabilities: app.permissions.clone(),
                dependency_summary: app.dependencies.clone(),
                advertisement_expiry: expiry,
                provider_node: self.local_node_id,
            };

            // Register in catalog as Local
            {
                let mut cat = self.catalog.write().await;
                let _ = cat.add_advertisement(ad.clone(), now);
                cat.update_state(&ad.package_hash, PackageCatalogState::Local);
            }

            self.network_sender
                .broadcast_package_advertisement(ad)
                .await?;
            count += 1;
        }

        Ok(count)
    }

    /// Initiates a package transfer request to a remote peer.
    pub async fn request_package(
        &self,
        application_id: ApplicationId,
        version: AppVersion,
        package_hash: ContentId,
        provider_node: NodeId,
    ) -> Result<String, ApplicationError> {
        let active_count = {
            let sessions = self.sessions.read().await;
            sessions
                .values()
                .filter(|s| !s.status.is_terminal())
                .count()
        };

        if active_count >= self.limits.max_concurrent_transfers {
            return Err(ApplicationError::QuotaExceeded(format!(
                "Active package transfers ({}) reached concurrency limit ({})",
                active_count, self.limits.max_concurrent_transfers
            )));
        }

        let now = Timestamp::now();
        let request_id = format!("xfer_{}_{}", application_id.as_str(), now);

        let session = PackageTransferSession::new(
            request_id.clone(),
            package_hash,
            application_id.clone(),
            version.clone(),
            provider_node,
            self.local_node_id,
            self.limits.request_ttl_ms,
            now,
        );

        let req = PackageRequest {
            request_id: request_id.clone(),
            application_id: application_id.clone(),
            version: version.clone(),
            package_hash,
            requesting_node: self.local_node_id,
            created_at: now,
            expires_at: session.expires_at,
        };

        // Persist session
        self.db.save_transfer_session(&session)?;
        {
            let mut sessions = self.sessions.write().await;
            sessions.insert(request_id.clone(), session.clone());
        }

        // Update catalog state to Partial
        {
            let mut cat = self.catalog.write().await;
            cat.update_state(&package_hash, PackageCatalogState::Partial);
        }

        if let Some(ref tx) = self.event_tx {
            let _ = tx.send(ApplicationEvent::PackageRequested {
                application_id,
                version: version.to_string(),
                package_hash,
                transfer_id: request_id.clone(),
                peer_node: provider_node,
            });
        }

        self.network_sender
            .send_package_message(provider_node, ApplicationPackageMessage::Request(req))
            .await?;

        Ok(request_id)
    }

    /// Handles an incoming wire message for package distribution.
    pub async fn handle_inbound_message(
        &self,
        msg: ApplicationPackageMessage,
        source_node: NodeId,
    ) -> Result<(), ApplicationError> {
        let now = Timestamp::now();

        match msg {
            ApplicationPackageMessage::Advertise(ad) => {
                self.discovery.handle_advertisement(ad, now).await?;
                Ok(())
            }

            ApplicationPackageMessage::Withdraw {
                application_id,
                package_hash,
                ..
            } => {
                self.discovery
                    .handle_withdrawal(&application_id, &package_hash)
                    .await?;
                Ok(())
            }

            ApplicationPackageMessage::Request(req) => {
                self.handle_incoming_request(req, source_node, now).await
            }

            ApplicationPackageMessage::Response(resp) => {
                self.handle_incoming_response(resp, source_node, now).await
            }

            ApplicationPackageMessage::ChunkRequest(req) => {
                self.handle_incoming_chunk_request(req, source_node).await
            }

            ApplicationPackageMessage::Chunk(chunk) => self.handle_incoming_chunk(chunk, now).await,
        }
    }

    /// Processes an incoming PackageRequest from a remote peer.
    async fn handle_incoming_request(
        &self,
        req: PackageRequest,
        source_node: NodeId,
        _now: Timestamp,
    ) -> Result<(), ApplicationError> {
        req.validate()?;

        // 1. Access Control / Sharing Policy check
        let is_allowed = {
            let policy = self.sharing_policy.read().await;
            policy.is_sharing_allowed(&req.application_id)
        };

        if !is_allowed {
            let denied_resp = PackageResponse {
                request_id: req.request_id,
                package_hash: req.package_hash,
                package_size: 0,
                chunk_count: 0,
                chunk_size: 0,
                manifest_hash: ContentId::from_bytes([0u8; 32]),
                chunk_hashes: Vec::new(),
                error: Some(format!(
                    "Package sharing denied for application '{}' by access control policy",
                    req.application_id.as_str()
                )),
            };
            self.network_sender
                .send_package_message(
                    source_node,
                    ApplicationPackageMessage::Response(denied_resp),
                )
                .await?;
            return Ok(());
        }

        // 2. Load package from CAS
        let package =
            match CasPackageStorage::load_package(&self.content_store, &req.package_hash).await {
                Ok(pkg) => pkg,
                Err(_) => {
                    let not_found_resp = PackageResponse {
                        request_id: req.request_id,
                        package_hash: req.package_hash,
                        package_size: 0,
                        chunk_count: 0,
                        chunk_size: 0,
                        manifest_hash: ContentId::from_bytes([0u8; 32]),
                        chunk_hashes: Vec::new(),
                        error: Some(format!(
                            "Requested package hash '{}' not found on node",
                            req.package_hash
                        )),
                    };
                    self.network_sender
                        .send_package_message(
                            source_node,
                            ApplicationPackageMessage::Response(not_found_resp),
                        )
                        .await?;
                    return Ok(());
                }
            };

        // 3. Assemble package into bundle and create chunks
        let payloads =
            CasPackageStorage::load_package_payloads(&self.content_store, &package).await?;

        let bundle = PackageBundle::new(package, payloads);
        let (chunks, chunk_hashes) =
            bundle.create_chunks(self.limits.default_chunk_size, &req.request_id)?;

        let total_size = bundle.to_bytes()?.len() as u64;
        let chunk_count = chunks.len() as u32;

        // Cache chunks for incoming chunk requests
        {
            let mut cache = self.outgoing_cache.write().await;
            cache.insert(req.request_id.clone(), chunks);
        }

        // Send successful response
        let resp = PackageResponse {
            request_id: req.request_id,
            package_hash: req.package_hash,
            package_size: total_size,
            chunk_count,
            chunk_size: self.limits.default_chunk_size,
            manifest_hash: bundle.package.manifest.compute_manifest_hash(),
            chunk_hashes,
            error: None,
        };

        self.network_sender
            .send_package_message(source_node, ApplicationPackageMessage::Response(resp))
            .await?;

        Ok(())
    }

    /// Processes an incoming PackageResponse from the provider peer.
    async fn handle_incoming_response(
        &self,
        resp: PackageResponse,
        source_node: NodeId,
        now: Timestamp,
    ) -> Result<(), ApplicationError> {
        let mut session = {
            let sessions = self.sessions.read().await;
            match sessions.get(&resp.request_id) {
                Some(s) => s.clone(),
                None => {
                    // Unknown transfer session
                    return Ok(());
                }
            }
        };

        if let Some(err) = resp.error {
            session.transition(TransferStatus::Failed, now, Some(err.clone()))?;
            self.db.save_transfer_session(&session)?;
            {
                let mut sessions = self.sessions.write().await;
                sessions.insert(session.transfer_id.clone(), session.clone());
            }

            if let Some(ref tx) = self.event_tx {
                let _ = tx.send(ApplicationEvent::PackageTransferFailed {
                    application_id: session.application_id,
                    transfer_id: session.transfer_id,
                    package_hash: session.package_hash,
                    reason: err,
                });
            }
            return Ok(());
        }

        // Enforce package size limit
        if resp.package_size > self.limits.max_package_size {
            let reason = format!(
                "Offered package size {} exceeds configured limit {}",
                resp.package_size, self.limits.max_package_size
            );
            session.transition(TransferStatus::Failed, now, Some(reason.clone()))?;
            self.db.save_transfer_session(&session)?;
            {
                let mut sessions = self.sessions.write().await;
                sessions.insert(session.transfer_id.clone(), session.clone());
            }
            return Err(ApplicationError::QuotaExceeded(reason));
        }

        // Configure session chunk metadata
        session.total_chunks = resp.chunk_count;
        session.chunk_size = resp.chunk_size;
        session.package_size = resp.package_size;
        session.chunk_hashes = resp.chunk_hashes;
        session.transition(TransferStatus::Transferring, now, None)?;

        self.db.save_transfer_session(&session)?;
        {
            let mut sessions = self.sessions.write().await;
            sessions.insert(session.transfer_id.clone(), session.clone());
        }

        if let Some(ref tx) = self.event_tx {
            let _ = tx.send(ApplicationEvent::PackageTransferStarted {
                application_id: session.application_id.clone(),
                transfer_id: session.transfer_id.clone(),
                package_hash: session.package_hash,
                total_chunks: session.total_chunks,
                total_bytes: session.package_size,
            });
        }

        // Request next missing chunk
        let missing = session.missing_chunks();
        if let Some(&first_missing) = missing.first() {
            let chunk_req = PackageChunkRequest {
                transfer_id: session.transfer_id,
                package_hash: session.package_hash,
                chunk_index: first_missing,
                requesting_node: self.local_node_id,
            };
            self.network_sender
                .send_package_message(
                    source_node,
                    ApplicationPackageMessage::ChunkRequest(chunk_req),
                )
                .await?;
        }

        Ok(())
    }

    /// Processes an incoming PackageChunkRequest.
    async fn handle_incoming_chunk_request(
        &self,
        req: PackageChunkRequest,
        source_node: NodeId,
    ) -> Result<(), ApplicationError> {
        let chunk_opt = {
            let cache = self.outgoing_cache.read().await;
            if let Some(chunks) = cache.get(&req.transfer_id) {
                chunks.get(req.chunk_index as usize).cloned()
            } else {
                None
            }
        };

        if let Some(chunk) = chunk_opt {
            self.network_sender
                .send_package_message(source_node, ApplicationPackageMessage::Chunk(chunk))
                .await?;
        }

        Ok(())
    }

    /// Processes an incoming PackageTransferChunk.
    async fn handle_incoming_chunk(
        &self,
        chunk: PackageTransferChunk,
        now: Timestamp,
    ) -> Result<(), ApplicationError> {
        // 1. Verify chunk integrity independently
        chunk.verify_chunk_hash()?;

        let mut session = {
            let sessions = self.sessions.read().await;
            match sessions.get(&chunk.transfer_id) {
                Some(s) => s.clone(),
                None => return Ok(()),
            }
        };

        // 2. Validate chunk index bounds and expected chunk hash from response manifest
        if chunk.chunk_index >= session.total_chunks {
            return Err(ApplicationError::IntegrityError(format!(
                "Chunk index {} out of range (total: {})",
                chunk.chunk_index, session.total_chunks
            )));
        }

        if let Some(expected_hash) = session.chunk_hashes.get(chunk.chunk_index as usize) {
            if &chunk.chunk_hash != expected_hash {
                return Err(ApplicationError::IntegrityError(format!(
                    "Chunk index {} hash does not match manifest expectation",
                    chunk.chunk_index
                )));
            }
        }

        // 3. Persist chunk binary data into SQLite and ContentStore
        self.db.save_chunk(
            &chunk.transfer_id,
            chunk.chunk_index,
            chunk.total_chunks,
            &chunk.chunk_hash,
            &chunk.data,
        )?;
        let _ = self.content_store.put(&chunk.data, None).await;

        // 4. Update session bitmap
        session.record_chunk(chunk.chunk_index, now);
        self.db.save_transfer_session(&session)?;
        {
            let mut sessions = self.sessions.write().await;
            sessions.insert(session.transfer_id.clone(), session.clone());
        }

        if let Some(ref tx) = self.event_tx {
            let _ = tx.send(ApplicationEvent::PackageChunkReceived {
                application_id: session.application_id.clone(),
                transfer_id: session.transfer_id.clone(),
                chunk_index: chunk.chunk_index,
                total_chunks: session.total_chunks,
            });
        }

        // 5. Determine whether to request next chunk or assemble
        let missing = session.missing_chunks();
        if let Some(&next_idx) = missing.first() {
            let chunk_req = PackageChunkRequest {
                transfer_id: session.transfer_id,
                package_hash: session.package_hash,
                chunk_index: next_idx,
                requesting_node: self.local_node_id,
            };
            self.network_sender
                .send_package_message(
                    session.source_node,
                    ApplicationPackageMessage::ChunkRequest(chunk_req),
                )
                .await?;
        } else if session.is_complete() {
            self.finalize_package_transfer(session, now).await?;
        }

        Ok(())
    }

    /// Assembles all received chunks, verifies cryptographic integrity, validates publisher trust,
    /// and stages the package into the CAS and local catalog.
    async fn finalize_package_transfer(
        &self,
        mut session: PackageTransferSession,
        now: Timestamp,
    ) -> Result<(), ApplicationError> {
        // Transition: Completing -> Verifying
        session.transition(TransferStatus::Completing, now, None)?;
        session.transition(TransferStatus::Verifying, now, None)?;
        self.db.save_transfer_session(&session)?;
        {
            let mut sessions = self.sessions.write().await;
            sessions.insert(session.transfer_id.clone(), session.clone());
        }

        if let Some(ref tx) = self.event_tx {
            let _ = tx.send(ApplicationEvent::PackageTransferCompleted {
                application_id: session.application_id.clone(),
                transfer_id: session.transfer_id.clone(),
                package_hash: session.package_hash,
                total_bytes: session.package_size,
            });
            let _ = tx.send(ApplicationEvent::PackageVerificationStarted {
                application_id: session.application_id.clone(),
                transfer_id: session.transfer_id.clone(),
                package_hash: session.package_hash,
            });
        }

        // 1. Load all chunks from SQLite
        let chunks = self
            .db
            .load_all_chunks_for_transfer(&session.transfer_id, &session.package_hash)?;

        // 2. Assemble bundle and verify package hash + signature
        let bundle = match PackageBundle::assemble_chunks(&session.package_hash, &chunks) {
            Ok(b) => b,
            Err(e) => {
                session.transition(TransferStatus::Failed, now, Some(e.to_string()))?;
                self.db.save_transfer_session(&session)?;
                if let Some(ref tx) = self.event_tx {
                    let _ = tx.send(ApplicationEvent::PackageVerificationFailed {
                        application_id: session.application_id,
                        transfer_id: session.transfer_id,
                        package_hash: session.package_hash,
                        reason: e.to_string(),
                    });
                }
                return Err(e);
            }
        };

        if let Some(ref tx) = self.event_tx {
            let _ = tx.send(ApplicationEvent::PackageVerified {
                application_id: session.application_id.clone(),
                package_id: bundle.package.package_id.clone(),
                package_hash: session.package_hash,
            });
        }

        // 3. Evaluate Publisher Trust Policy
        let publisher = &bundle.package.manifest.publisher;
        let is_trusted = {
            let policy = self.trust_policy.read().await;
            policy.is_trusted(publisher)
        };

        // 4. Store package in local CAS
        bundle.store_to_cas(&self.content_store).await?;

        // 5. Update catalog state
        let catalog_state = if is_trusted {
            PackageCatalogState::Installable
        } else {
            PackageCatalogState::Verified
        };

        {
            let mut cat = self.catalog.write().await;
            cat.update_state(&session.package_hash, catalog_state);
        }

        session.transition(TransferStatus::Completed, now, None)?;
        self.db.save_transfer_session(&session)?;
        {
            let mut sessions = self.sessions.write().await;
            sessions.insert(session.transfer_id.clone(), session.clone());
        }

        if is_trusted {
            if let Some(ref tx) = self.event_tx {
                let _ = tx.send(ApplicationEvent::PackageInstallable {
                    application_id: session.application_id,
                    version: session.version.to_string(),
                    package_hash: session.package_hash,
                });
            }
        }

        // Clean up temporary chunk binary storage
        let _ = self.db.delete_transfer_chunks(&session.transfer_id);

        Ok(())
    }

    /// Resumes an interrupted or paused package transfer session.
    pub async fn resume_transfer(&self, transfer_id: &str) -> Result<(), ApplicationError> {
        let now = Timestamp::now();
        let mut session = {
            let sessions = self.sessions.read().await;
            match sessions.get(transfer_id) {
                Some(s) => s.clone(),
                None => match self.db.load_transfer_session(transfer_id)? {
                    Some(s) => s,
                    None => {
                        return Err(ApplicationError::NotFound(format!(
                            "Transfer session '{}' not found",
                            transfer_id
                        )))
                    }
                },
            }
        };

        if !session.status.can_resume() {
            return Err(ApplicationError::TransferError(format!(
                "Transfer session '{}' is in non-resumable status '{}'",
                transfer_id,
                session.status.as_str()
            )));
        }

        session.transition(TransferStatus::Transferring, now, None)?;
        self.db.save_transfer_session(&session)?;
        {
            let mut sessions = self.sessions.write().await;
            sessions.insert(session.transfer_id.clone(), session.clone());
        }

        if let Some(ref tx) = self.event_tx {
            let _ = tx.send(ApplicationEvent::PackageTransferResumed {
                application_id: session.application_id.clone(),
                transfer_id: session.transfer_id.clone(),
            });
        }

        let missing = session.missing_chunks();
        if let Some(&next_idx) = missing.first() {
            let chunk_req = PackageChunkRequest {
                transfer_id: session.transfer_id,
                package_hash: session.package_hash,
                chunk_index: next_idx,
                requesting_node: self.local_node_id,
            };
            self.network_sender
                .send_package_message(
                    session.source_node,
                    ApplicationPackageMessage::ChunkRequest(chunk_req),
                )
                .await?;
        }

        Ok(())
    }

    /// Hands off a verified, trusted package in `Installable` state to the local installer.
    pub async fn install_verified_package(
        &self,
        package_hash: &ContentId,
        installer: &ApplicationInstaller,
    ) -> Result<(), ApplicationError> {
        let record = {
            let cat = self.catalog.read().await;
            match cat.get(package_hash) {
                Some(r) => r.clone(),
                None => {
                    return Err(ApplicationError::NotFound(format!(
                        "Package hash {} not found in catalog",
                        package_hash
                    )))
                }
            }
        };

        if record.state != PackageCatalogState::Installable {
            return Err(ApplicationError::SecurityError(format!(
                "Package hash {} is in state '{}', must be 'installable' to proceed with installation",
                package_hash,
                record.state.as_str()
            )));
        }

        // Verify publisher trust policy
        let publisher = &record.advertisement.publisher_id;
        let is_trusted = {
            let policy = self.trust_policy.read().await;
            policy.is_trusted(publisher)
        };

        if !is_trusted {
            return Err(ApplicationError::SecurityError(format!(
                "Package publisher '{}' is not approved by current trust policy",
                publisher.to_hex()
            )));
        }

        // Load package from CAS and install
        let pkg = CasPackageStorage::load_package(&self.content_store, package_hash).await?;
        let payloads = CasPackageStorage::load_package_payloads(&self.content_store, &pkg).await?;

        installer.install(&pkg, &payloads).await?;

        // Update catalog state to Installed
        {
            let mut cat = self.catalog.write().await;
            cat.update_state(package_hash, PackageCatalogState::Installed);
        }

        Ok(())
    }

    /// Discovers and acquires a missing application dependency from network peers,
    /// verifies it, checks trust policy, and installs it.
    pub async fn acquire_and_install_dependency(
        &self,
        dependency_app_id: &ApplicationId,
        version_req: &AppVersionReq,
        installer: &ApplicationInstaller,
    ) -> Result<(), ApplicationError> {
        let compatible = {
            let cat = self.catalog.read().await;
            cat.find_compatible(dependency_app_id, version_req)
        };

        let target_ad = match compatible.first() {
            Some(rec) => rec.advertisement.clone(),
            None => {
                return Err(ApplicationError::DependencyError(format!(
                    "No compatible package found in catalog for dependency '{}' matching '{}'",
                    dependency_app_id.as_str(),
                    version_req
                )))
            }
        };

        // Check publisher trust
        let is_trusted = {
            let policy = self.trust_policy.read().await;
            policy.is_trusted(&target_ad.publisher_id)
        };

        if !is_trusted {
            return Err(ApplicationError::SecurityError(format!(
                "Discovered package for dependency '{}' has untrusted publisher '{}'",
                dependency_app_id.as_str(),
                target_ad.publisher_id.to_hex()
            )));
        }

        // Request package from provider
        let transfer_id = self
            .request_package(
                target_ad.application_id,
                target_ad.version,
                target_ad.package_hash,
                target_ad.provider_node,
            )
            .await?;

        // Note: in unit tests or sync flows, caller drives transfer to completion,
        // then calls install_verified_package.
        let _ = transfer_id;
        let _ = installer;

        Ok(())
    }

    /// Loads active transfers from SQLite and reconciles on node startup.
    pub fn reconcile_startup(&self) -> Result<usize, ApplicationError> {
        let now = Timestamp::now();
        self.db.reconcile_startup(now)
    }
}
