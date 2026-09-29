use fieldnet_core::{NodeId, Timestamp};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, info, warn};

use crate::error::ServiceError;
use crate::events::ServiceEvent;
use crate::model::{ServiceId, ServiceInstanceId};
use crate::registry::ServiceRegistry;
use crate::wire::{ServiceMessage, ServiceQueryResponseWire};

/// Receiving and ingestion handler for inbound service discovery and advertisement messages.
///
/// Enforces the strict Phase 08 trust chain:
/// authenticated peer -> structural validation -> provider identity verification ->
/// signature verification -> expiration validation -> registry ingestion.
pub struct ServiceAdvertisementHandler {
    local_node_id: NodeId,
    registry: Arc<ServiceRegistry>,
    event_tx: broadcast::Sender<ServiceEvent>,
    /// Bounded deduplication tracking `(ServiceInstanceId, sequence)`.
    seen_advertisements: Arc<RwLock<HashSet<(ServiceInstanceId, u64)>>>,
    /// Bounded deduplication tracking query `MessageId`.
    seen_queries: Arc<RwLock<HashSet<fieldnet_core::MessageId>>>,
    /// Provider public key cache `ProviderNodeId -> [u8; 32]` for withdrawal verification.
    known_provider_keys: Arc<RwLock<HashMap<NodeId, [u8; 32]>>>,
}

impl ServiceAdvertisementHandler {
    /// Creates a new `ServiceAdvertisementHandler`.
    pub fn new(
        local_node_id: NodeId,
        registry: Arc<ServiceRegistry>,
        event_tx: broadcast::Sender<ServiceEvent>,
    ) -> Self {
        Self {
            local_node_id,
            registry,
            event_tx,
            seen_advertisements: Arc::new(RwLock::new(HashSet::new())),
            seen_queries: Arc::new(RwLock::new(HashSet::new())),
            known_provider_keys: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Returns the local `NodeId`.
    pub fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    /// Processes an inbound `ServiceMessage` received across the overlay network from `sender_peer_id`.
    ///
    /// If the message is a `Query`, may return an optional response `ServiceMessage` to reply with.
    pub async fn handle_inbound_message(
        &self,
        sender_peer_id: &NodeId,
        message: ServiceMessage,
        now: Timestamp,
    ) -> Result<Option<ServiceMessage>, ServiceError> {
        match message {
            ServiceMessage::Advertise(manifest) => {
                // 1. Provider Identity Check: sender peer must be the claimed provider
                // (or in multi-hop origin preservation, origin NodeId matches provider_node_id)
                if sender_peer_id != &manifest.provider_node_id {
                    warn!(
                        "Rejected advertisement: sender peer {} does not match manifest provider {}",
                        sender_peer_id, manifest.provider_node_id
                    );
                    return Err(ServiceError::MismatchedProviderKey {
                        expected: *sender_peer_id,
                        got: manifest.provider_node_id,
                    });
                }

                // 2. Structural and timestamp validation
                manifest.validate_basics(now)?;

                // 3. Cryptographic signature verification
                manifest.verify_signature()?;

                // 4. Duplicate / Replay suppression
                let dedupe_key = (manifest.instance_id.clone(), manifest.sequence);
                let already_seen = {
                    let seen = self.seen_advertisements.read().await;
                    seen.contains(&dedupe_key)
                };

                if already_seen {
                    // Check if instance is currently tracked as Unavailable (e.g. peer previously disconnected)
                    if let Some(record) = self.registry.get_instance(&manifest.instance_id).await {
                        if record.state() == crate::model::ServiceState::Unavailable {
                            // Re-mark available since provider has re-advertised
                            let _ = self
                                .registry
                                .mark_provider_available(&manifest.provider_node_id)
                                .await;
                        }
                    }
                    debug!(
                        "Suppressed duplicate service advertisement for '{}' (seq: {})",
                        manifest.instance_id, manifest.sequence
                    );
                    return Ok(None);
                }

                // Cache provider public key for future withdrawal verification
                {
                    let mut keys = self.known_provider_keys.write().await;
                    keys.insert(manifest.provider_node_id, manifest.provider_public_key);
                }

                // 5. Ingest into central ServiceRegistry
                let instance_id = manifest.instance_id.clone();
                let service_id = manifest.service_id.clone();
                let provider_node_id = manifest.provider_node_id;

                self.registry.register_remote(manifest, now).await?;

                // Record in dedupe cache
                {
                    let mut seen = self.seen_advertisements.write().await;
                    if seen.len() > 10_000 {
                        seen.clear(); // Bounded flush if capacity reached
                    }
                    seen.insert(dedupe_key);
                }

                // 6. Emit Discovered event
                let _ = self.event_tx.send(ServiceEvent::Discovered {
                    service_id,
                    instance_id,
                    provider_node_id,
                });

                info!(
                    "Ingested remote service advertisement from peer {}",
                    sender_peer_id
                );
                Ok(None)
            }

            ServiceMessage::Withdraw(withdrawal) => {
                // 1. Verify provider identity matches sender peer
                if sender_peer_id != &withdrawal.provider_node_id {
                    warn!(
                        "Rejected withdrawal: sender peer {} does not match withdrawal provider {}",
                        sender_peer_id, withdrawal.provider_node_id
                    );
                    return Err(ServiceError::MismatchedProviderKey {
                        expected: *sender_peer_id,
                        got: withdrawal.provider_node_id,
                    });
                }

                // 2. Lookup cached public key for this provider
                let public_key = {
                    let keys = self.known_provider_keys.read().await;
                    keys.get(&withdrawal.provider_node_id).copied()
                };

                // If not cached, look up in registry
                let public_key = match public_key {
                    Some(k) => k,
                    None => {
                        if let Some(record) =
                            self.registry.get_instance(&withdrawal.instance_id).await
                        {
                            record.manifest.provider_public_key
                        } else {
                            // Instance not even registered, ignore
                            return Ok(None);
                        }
                    }
                };

                // 3. Cryptographically verify withdrawal signature
                withdrawal.verify_signature(&public_key)?;

                // 4. Remove or mark unavailable in registry
                let instance_id = withdrawal.instance_id.clone();
                let service_id = withdrawal.service_id.clone();
                let provider_node_id = withdrawal.provider_node_id;

                if let Err(e) = self.registry.unregister(&instance_id).await {
                    debug!("Withdrawal instance not found in registry: {}", e);
                }

                // 5. Emit Withdrawn event
                let _ = self.event_tx.send(ServiceEvent::Withdrawn {
                    service_id,
                    instance_id: instance_id.clone(),
                    provider_node_id,
                });

                info!(
                    "Processed service withdrawal for instance '{}' from peer {}",
                    instance_id, sender_peer_id
                );
                Ok(None)
            }

            ServiceMessage::Query(query_wire) => {
                debug!(
                    "Received ServiceQuery for '{}' from peer {}",
                    query_wire.service_id, sender_peer_id
                );

                // 1. Authenticate query requester: sender must match claimed requester
                if &query_wire.requester_node_id != sender_peer_id {
                    warn!(
                        "Rejected ServiceQuery: sender peer {} does not match claimed requester {}",
                        sender_peer_id, query_wire.requester_node_id
                    );
                    return Err(ServiceError::MismatchedProviderKey {
                        expected: *sender_peer_id,
                        got: query_wire.requester_node_id,
                    });
                }

                // 2. Validate query timestamp (reject stale/replay queries older than 60s)
                if now
                    .as_millis()
                    .saturating_sub(query_wire.timestamp.as_millis())
                    > 60_000
                {
                    warn!("Rejected expired ServiceQuery from peer {}", sender_peer_id);
                    return Err(ServiceError::InvalidQuery(
                        "Expired ServiceQuery timestamp".to_string(),
                    ));
                }

                // 3. Duplicate query suppression
                let is_dup = {
                    let seen = self.seen_queries.read().await;
                    seen.contains(&query_wire.query_id)
                };
                if is_dup {
                    debug!("Suppressed duplicate ServiceQuery {}", query_wire.query_id);
                    return Ok(None);
                }
                {
                    let mut seen = self.seen_queries.write().await;
                    if seen.len() > 10_000 {
                        seen.clear();
                    }
                    seen.insert(query_wire.query_id);
                }

                // 4. Emit QueryReceived event
                let _ = self.event_tx.send(ServiceEvent::QueryReceived {
                    service_id: query_wire.service_id.clone(),
                    query_id: query_wire.query_id,
                    requester: *sender_peer_id,
                });

                // 5. Query local registry for matching callable instances
                let candidates = self
                    .registry
                    .list_callable_instances(&query_wire.service_id)
                    .await;

                let mut matching = Vec::new();
                for cand in candidates {
                    // Check version if requested
                    if let Some(ref min_v) = query_wire.min_version {
                        if !cand.manifest.version.is_compatible_with(min_v)
                            || cand.manifest.version < *min_v
                        {
                            continue;
                        }
                    }
                    // Check capabilities
                    let mut has_all = true;
                    for req_cap in &query_wire.required_capabilities {
                        if !cand.manifest.capabilities.contains(req_cap) {
                            has_all = false;
                            break;
                        }
                    }
                    if !has_all {
                        continue;
                    }

                    matching.push(cand.manifest);
                }

                if !matching.is_empty() {
                    let _ = self.event_tx.send(ServiceEvent::QueryResponseSent {
                        service_id: query_wire.service_id.clone(),
                        query_id: query_wire.query_id,
                        responder: self.local_node_id,
                        destination: *sender_peer_id,
                        count: matching.len(),
                    });

                    let resp = ServiceMessage::QueryResponse(ServiceQueryResponseWire {
                        query_id: query_wire.query_id,
                        manifests: matching,
                        responder_node_id: self.local_node_id,
                        timestamp: now,
                    });
                    return Ok(Some(resp));
                }

                Ok(None)
            }

            ServiceMessage::QueryResponse(response_wire) => {
                debug!(
                    "Received ServiceQueryResponse with {} manifest(s) from {}",
                    response_wire.manifests.len(),
                    sender_peer_id
                );

                // 1. Authenticate responder identity: sender must match claimed responder
                if &response_wire.responder_node_id != sender_peer_id {
                    warn!(
                        "Rejected QueryResponse: sender peer {} does not match claimed responder {}",
                        sender_peer_id, response_wire.responder_node_id
                    );
                    return Err(ServiceError::MismatchedProviderKey {
                        expected: *sender_peer_id,
                        got: response_wire.responder_node_id,
                    });
                }

                // 2. Validate response timestamp
                if now
                    .as_millis()
                    .saturating_sub(response_wire.timestamp.as_millis())
                    > 60_000
                {
                    warn!(
                        "Rejected expired QueryResponse from peer {}",
                        sender_peer_id
                    );
                    return Err(ServiceError::InvalidQuery(
                        "Expired QueryResponse timestamp".to_string(),
                    ));
                }

                let service_id = response_wire
                    .manifests
                    .first()
                    .map(|m| m.service_id.clone())
                    .unwrap_or_else(|| ServiceId::new("generic.service").unwrap());

                let _ = self.event_tx.send(ServiceEvent::QueryResponseReceived {
                    service_id,
                    query_id: response_wire.query_id,
                    responder: *sender_peer_id,
                    count: response_wire.manifests.len(),
                });

                for manifest in response_wire.manifests {
                    if let Err(e) = manifest.validate_basics(now) {
                        debug!("Skipping invalid response manifest: {}", e);
                        continue;
                    }
                    if let Err(e) = manifest.verify_signature() {
                        debug!("Skipping unverified response manifest: {}", e);
                        continue;
                    }

                    let instance_id = manifest.instance_id.clone();
                    let service_id = manifest.service_id.clone();
                    let provider_node_id = manifest.provider_node_id;

                    if let Ok(()) = self.registry.register_remote(manifest, now).await {
                        let _ = self.event_tx.send(ServiceEvent::Discovered {
                            service_id,
                            instance_id,
                            provider_node_id,
                        });
                    }
                }

                Ok(None)
            }
            ServiceMessage::RpcRequest(_) | ServiceMessage::RpcResponse(_) => {
                // RPC invocation and responses are handled by the runtime execution engine
                Ok(None)
            }
        }
    }
}
