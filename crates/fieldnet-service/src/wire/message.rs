use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use fieldnet_core::{MessageId, NodeId, Timestamp};
use fieldnet_identity::NodeIdentity;
use serde::{Deserialize, Serialize};

use crate::error::ServiceError;
use crate::model::{
    ServiceCapability, ServiceId, ServiceInstanceId, ServiceManifest, ServiceVersion,
};

/// Maximum combined service discovery message wire size (512 KiB).
pub const MAX_SERVICE_MESSAGE_SIZE: usize = 512 * 1024;

/// Cryptographically signed withdrawal of a previously advertised service instance.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ServiceWithdrawal {
    /// Service identifier of the instance being withdrawn.
    pub service_id: ServiceId,
    /// Specific instance identifier being withdrawn.
    pub instance_id: ServiceInstanceId,
    /// Sovereign `NodeId` of the hosting provider.
    pub provider_node_id: NodeId,
    /// Monotonically increasing sequence number (must be >= registered manifest sequence).
    pub sequence: u64,
    /// Timestamp at which the withdrawal was issued.
    pub timestamp: Timestamp,
    /// Ed25519 digital signature over canonical withdrawal bytes.
    #[serde(with = "hex")]
    pub signature: [u8; 64],
}

impl ServiceWithdrawal {
    /// Constructs canonical bytes for signing and verifying the withdrawal.
    pub fn canonical_signing_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(128);
        buf.extend_from_slice(b"FIELDNET_SERVICE_WITHDRAW_V1");
        let s_bytes = self.service_id.as_str().as_bytes();
        buf.extend_from_slice(&(s_bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(s_bytes);
        let i_bytes = self.instance_id.as_str().as_bytes();
        buf.extend_from_slice(&(i_bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(i_bytes);
        buf.extend_from_slice(self.provider_node_id.as_bytes());
        buf.extend_from_slice(&self.sequence.to_be_bytes());
        buf.extend_from_slice(&self.timestamp.as_millis().to_be_bytes());
        buf
    }

    /// Signs the withdrawal notice with the provider's sovereign `NodeIdentity`.
    pub fn sign(&mut self, identity: &NodeIdentity) {
        self.provider_node_id = identity.node_id();
        let signing_bytes = self.canonical_signing_bytes();
        self.signature = identity.sign(&signing_bytes);
    }

    /// Verifies the cryptographic authenticity and integrity of the withdrawal notice.
    pub fn verify_signature(&self, public_key: &[u8; 32]) -> Result<(), ServiceError> {
        let derived_node_id = NodeId::from_ed25519_public_key(public_key);
        if derived_node_id != self.provider_node_id {
            return Err(ServiceError::MismatchedProviderKey {
                expected: derived_node_id,
                got: self.provider_node_id,
            });
        }

        let signing_bytes = self.canonical_signing_bytes();
        let verifying_key = VerifyingKey::from_bytes(public_key).map_err(|e| {
            ServiceError::InvalidSignature(format!("Invalid public key bytes: {}", e))
        })?;
        let signature = Signature::from_bytes(&self.signature);

        verifying_key
            .verify(&signing_bytes, &signature)
            .map_err(|e| {
                ServiceError::InvalidSignature(format!("Withdrawal signature invalid: {}", e))
            })
    }
}

/// Targeted or reactive service discovery query transmitted across the mesh.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ServiceQueryWire {
    /// Unique correlation identifier for this discovery query.
    pub query_id: MessageId,
    /// Target service identifier to discover (e.g. "files.service").
    pub service_id: ServiceId,
    /// Optional minimum compatible SemVer version.
    pub min_version: Option<ServiceVersion>,
    /// Capabilities required from the candidate.
    pub required_capabilities: Vec<ServiceCapability>,
    /// Sovereign NodeId of the requesting node.
    pub requester_node_id: NodeId,
    /// Timestamp at which the query was dispatched.
    pub timestamp: Timestamp,
}

/// Unicast discovery query response containing matching advertised service manifests.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ServiceQueryResponseWire {
    /// Correlation ID matching the original `ServiceQueryWire`.
    pub query_id: MessageId,
    /// Set of matching, cryptographically signed service manifests.
    pub manifests: Vec<ServiceManifest>,
    /// Sovereign NodeId of the responding node.
    pub responder_node_id: NodeId,
    /// Timestamp of the response.
    pub timestamp: Timestamp,
}

/// Invocation request for a resolved service instance routed across the FIELDNET mesh.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ServiceRpcRequestWire {
    /// Unique correlation ID for the request.
    pub request_id: MessageId,
    /// Target logical service ID.
    pub service_id: ServiceId,
    /// Target resolved instance ID.
    pub instance_id: ServiceInstanceId,
    /// Relative resource path (e.g. "/list", "/health").
    pub path: String,
    /// Method semantics (e.g. "GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS").
    pub method: String,
    /// Optional URL query string.
    pub query: Option<String>,
    /// Safe request headers.
    pub headers: std::collections::HashMap<String, String>,
    /// Request payload bytes.
    pub body: Vec<u8>,
    /// Requester NodeId.
    pub requester_node_id: NodeId,
    /// Target provider NodeId.
    pub provider_node_id: NodeId,
    /// Execution deadline.
    pub deadline: Timestamp,
    /// Optional idempotency key for safe retries of state-changing operations.
    pub idempotency_key: Option<String>,
}

/// Invocation response returned by a service provider across the FIELDNET mesh.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ServiceRpcResponseWire {
    /// Correlates to request_id in ServiceRpcRequestWire.
    pub request_id: MessageId,
    /// HTTP status code equivalent (e.g. 200, 400, 404, 500).
    pub status_code: u16,
    /// Response headers (e.g. Content-Type).
    pub headers: std::collections::HashMap<String, String>,
    /// Response body payload.
    pub body: Vec<u8>,
    /// Sovereign NodeId of the responding provider.
    pub responder_node_id: NodeId,
    /// Timestamp of response generation.
    pub timestamp: Timestamp,
}

/// Authoritative protocol envelope for all Phase 08 Service messages
/// (framed within overlay `MessageType::Service = 8`).
#[allow(clippy::large_enum_variant)]
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum ServiceMessage {
    /// Provider announcements, updates, or keepalive refreshes.
    Advertise(ServiceManifest),
    /// Explicit provider withdrawal of a service instance.
    Withdraw(ServiceWithdrawal),
    /// Reactive network discovery query.
    Query(ServiceQueryWire),
    /// Response to a reactive network discovery query.
    QueryResponse(ServiceQueryResponseWire),
    /// Service invocation request.
    RpcRequest(ServiceRpcRequestWire),
    /// Service invocation response.
    RpcResponse(ServiceRpcResponseWire),
}

impl ServiceMessage {
    /// Serializes the `ServiceMessage` into framed bytes conforming to envelope limits.
    pub fn to_bytes(&self) -> Result<Vec<u8>, ServiceError> {
        let bytes = serde_json::to_vec(self)?;
        if bytes.len() > MAX_SERVICE_MESSAGE_SIZE {
            return Err(ServiceError::SerializationError(format!(
                "ServiceMessage payload exceeds maximum wire limit: {} > {} bytes",
                bytes.len(),
                MAX_SERVICE_MESSAGE_SIZE
            )));
        }
        Ok(bytes)
    }

    /// Deserializes a framed byte slice into a `ServiceMessage`.
    pub fn from_bytes(slice: &[u8]) -> Result<Self, ServiceError> {
        if slice.len() > MAX_SERVICE_MESSAGE_SIZE {
            return Err(ServiceError::SerializationError(format!(
                "ServiceMessage payload exceeds maximum wire limit: {} > {} bytes",
                slice.len(),
                MAX_SERVICE_MESSAGE_SIZE
            )));
        }
        serde_json::from_slice(slice).map_err(|e| ServiceError::SerializationError(e.to_string()))
    }
}
