use async_trait::async_trait;
use fieldnet_core::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tracing::info;

use crate::error::ApplicationError;
use crate::host::abi::{NetworkSendRequest, NetworkSendResponse, MAX_REQUEST_PAYLOAD_BYTES};
use crate::host::errors::HostError;
use crate::host::permissions::CapabilityContext;
use crate::id::{ApplicationId, ExecutionId};
use crate::permissions::AppPermission;

/// Application-level message routed via the FIELDNET multi-hop networking layer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApplicationNetworkMessage {
    pub source_application: ApplicationId,
    pub source_execution: ExecutionId,
    pub source_node: Option<NodeId>,
    pub destination: NodeId,
    pub message_type: String,
    pub payload: Vec<u8>,
    pub metadata: HashMap<String, String>,
    pub ttl: Option<u8>,
    pub expires_in_ms: Option<u64>,
}

/// Abstract contract allowing the host environment to route application messages
/// through the existing FIELDNET routing and transport layer without creating a second networking stack.
#[async_trait]
pub trait ApplicationNetworkSender: Send + Sync {
    async fn send_message(
        &self,
        msg: ApplicationNetworkMessage,
    ) -> Result<String, ApplicationError>;
}

/// Network interface bridging WASM host calls to the FIELDNET routing and transport subsystem.
#[derive(Clone)]
pub struct HostNetwork {
    application_id: ApplicationId,
    execution_id: ExecutionId,
    local_node_id: Option<NodeId>,
    sender: Arc<dyn ApplicationNetworkSender>,
    tokio_handle: tokio::runtime::Handle,
}

impl HostNetwork {
    pub fn new(
        application_id: ApplicationId,
        execution_id: ExecutionId,
        local_node_id: Option<NodeId>,
        sender: Arc<dyn ApplicationNetworkSender>,
        tokio_handle: tokio::runtime::Handle,
    ) -> Self {
        Self {
            application_id,
            execution_id,
            local_node_id,
            sender,
            tokio_handle,
        }
    }

    /// Sends a message to a destination node using FIELDNET routing and DTN.
    ///
    /// Requires capability: `network.send`
    /// Derives source identity directly from execution context to prevent spoofing.
    pub fn send(
        &self,
        caps: &CapabilityContext,
        req: &NetworkSendRequest,
    ) -> Result<NetworkSendResponse, HostError> {
        caps.check_permission(AppPermission::NetworkSend)?;

        let dest = NodeId::from_str(req.destination.trim())
            .map_err(|e| HostError::invalid_id(format!("Invalid destination NodeId: {}", e)))?;

        if req.payload.len() > MAX_REQUEST_PAYLOAD_BYTES {
            return Err(HostError::invalid_request(
                "Payload exceeds maximum allowed network message size",
            ));
        }

        // Construct message strictly bound to verified execution identities
        let msg = ApplicationNetworkMessage {
            source_application: self.application_id.clone(),
            source_execution: self.execution_id.clone(),
            source_node: self.local_node_id,
            destination: dest,
            message_type: req.message_type.clone(),
            payload: req.payload.clone(),
            metadata: req.metadata.clone().unwrap_or_default(),
            ttl: req.ttl,
            expires_in_ms: req.expires_in_ms,
        };

        let msg_id = self
            .tokio_handle
            .block_on(async { self.sender.send_message(msg).await })
            .map_err(|e| {
                HostError::network_unavailable(format!("Network routing failed: {}", e))
            })?;

        info!(
            application_id = %self.application_id,
            execution_id = %self.execution_id,
            destination = %dest,
            message_id = %msg_id,
            payload_size = req.payload.len(),
            "Network message handed over to FIELDNET routing layer"
        );

        Ok(NetworkSendResponse {
            message_id: msg_id,
            queued: true,
        })
    }
}
