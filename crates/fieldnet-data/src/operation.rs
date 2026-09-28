use fieldnet_core::{NodeId, Timestamp};
use fieldnet_identity::NodeIdentity;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

use crate::causality::HybridLogicalClock;
use crate::document::DocumentId;
use crate::error::DataError;
use crate::schema::CURRENT_SCHEMA_VERSION;

/// A globally unique 32-byte identifier for an operation.
/// Computed as the cryptographic BLAKE3 digest over all canonical operation fields.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct OperationId(pub [u8; 32]);

impl OperationId {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    pub fn from_hex(s: &str) -> Result<Self, DataError> {
        let bytes = hex::decode(s)
            .map_err(|e| DataError::InvalidDocumentId(format!("Hex decode error: {}", e)))?;
        if bytes.len() != 32 {
            return Err(DataError::InvalidDocumentId(format!(
                "Invalid OperationId length: expected 32, got {}",
                bytes.len()
            )));
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(Self(arr))
    }
}

impl fmt::Display for OperationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl FromStr for OperationId {
    type Err = DataError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_hex(s)
    }
}

/// Generic operation category.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum OperationType {
    Create = 1,
    Update = 2,
    Delete = 3,
    CrdtDelta = 4,
}

impl OperationType {
    pub fn from_u8(val: u8) -> Result<Self, DataError> {
        match val {
            1 => Ok(Self::Create),
            2 => Ok(Self::Update),
            3 => Ok(Self::Delete),
            4 => Ok(Self::CrdtDelta),
            other => Err(DataError::CorruptedData(format!(
                "Unknown OperationType: {}",
                other
            ))),
        }
    }

    pub fn as_u8(&self) -> u8 {
        *self as u8
    }
}

/// Lifecycle status of a persisted operation record.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum OpStatus {
    Created = 1,
    Durable = 2,
    PendingSync = 3,
    Sent = 4,
    Acknowledged = 5,
    Applied = 6,
    Rejected = 7,
    Tombstone = 8,
}

impl OpStatus {
    pub fn from_u8(val: u8) -> Result<Self, DataError> {
        match val {
            1 => Ok(Self::Created),
            2 => Ok(Self::Durable),
            3 => Ok(Self::PendingSync),
            4 => Ok(Self::Sent),
            5 => Ok(Self::Acknowledged),
            6 => Ok(Self::Applied),
            7 => Ok(Self::Rejected),
            8 => Ok(Self::Tombstone),
            other => Err(DataError::CorruptedData(format!(
                "Unknown OpStatus: {}",
                other
            ))),
        }
    }

    pub fn as_u8(&self) -> u8 {
        *self as u8
    }
}

/// Canonical, immutable, cryptographically authenticatable operation record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Operation {
    /// Deterministic 32-byte BLAKE3 identifier.
    pub op_id: OperationId,

    /// Target document identifier.
    pub document_id: DocumentId,

    /// Sovereign NodeId of the authoring actor.
    pub actor_node_id: NodeId,

    /// Monotonic sequence counter from this actor for this document.
    pub sequence: u64,

    /// Operation category (Create, Update, Delete, CrdtDelta).
    pub op_type: OperationType,

    /// State or delta payload bytes.
    pub payload: Vec<u8>,

    /// Hybrid Logical Clock timestamp at creation.
    pub hlc: HybridLogicalClock,

    /// Causal parent dependencies (OpIds that this operation directly succeeds).
    pub causal_dependencies: Vec<OperationId>,

    /// 64-byte Ed25519 signature by actor_node_id.
    #[serde(with = "hex")]
    pub signature: [u8; 64],

    /// Local physical ingestion timestamp.
    pub created_at: Timestamp,

    /// Schema version for application data.
    pub schema_version: u32,

    /// Current lifecycle status.
    pub status: OpStatus,
}

impl Operation {
    /// Computes the deterministic signing payload and hash for an operation.
    pub fn compute_signing_payload(
        document_id: &DocumentId,
        actor_node_id: &NodeId,
        sequence: u64,
        op_type: OperationType,
        payload: &[u8],
        hlc: &HybridLogicalClock,
        causal_dependencies: &[OperationId],
        schema_version: u32,
    ) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(128 + payload.len() + causal_dependencies.len() * 32);
        bytes.extend_from_slice(b"FIELDNET_OP_v1:");
        bytes.extend_from_slice(document_id.as_bytes());
        bytes.extend_from_slice(actor_node_id.as_bytes());
        bytes.extend_from_slice(&sequence.to_be_bytes());
        bytes.push(op_type.as_u8());
        bytes.extend_from_slice(&schema_version.to_be_bytes());
        bytes.extend_from_slice(&hlc.to_bytes());

        bytes.extend_from_slice(&(causal_dependencies.len() as u32).to_be_bytes());
        for dep in causal_dependencies {
            bytes.extend_from_slice(dep.as_bytes());
        }

        bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        bytes.extend_from_slice(payload);
        bytes
    }

    /// Computes the deterministic OperationId from canonical operation fields.
    pub fn compute_id(
        document_id: &DocumentId,
        actor_node_id: &NodeId,
        sequence: u64,
        op_type: OperationType,
        payload: &[u8],
        hlc: &HybridLogicalClock,
        causal_dependencies: &[OperationId],
        schema_version: u32,
    ) -> OperationId {
        let signing_payload = Self::compute_signing_payload(
            document_id,
            actor_node_id,
            sequence,
            op_type,
            payload,
            hlc,
            causal_dependencies,
            schema_version,
        );
        let hash = blake3::hash(&signing_payload);
        let mut id_bytes = [0u8; 32];
        id_bytes.copy_from_slice(hash.as_bytes());
        OperationId::from_bytes(id_bytes)
    }

    /// Constructs and signs a new local operation.
    pub fn new_local(
        document_id: DocumentId,
        actor_identity: &NodeIdentity,
        sequence: u64,
        op_type: OperationType,
        payload: Vec<u8>,
        hlc: HybridLogicalClock,
        causal_dependencies: Vec<OperationId>,
    ) -> Self {
        let actor_node_id = actor_identity.node_id();
        let schema_version = CURRENT_SCHEMA_VERSION;
        let signing_payload = Self::compute_signing_payload(
            &document_id,
            &actor_node_id,
            sequence,
            op_type,
            &payload,
            &hlc,
            &causal_dependencies,
            schema_version,
        );

        let hash = blake3::hash(&signing_payload);
        let mut id_bytes = [0u8; 32];
        id_bytes.copy_from_slice(hash.as_bytes());
        let op_id = OperationId::from_bytes(id_bytes);

        let signature = actor_identity.sign(&signing_payload);

        Self {
            op_id,
            document_id,
            actor_node_id,
            sequence,
            op_type,
            payload,
            hlc,
            causal_dependencies,
            signature,
            created_at: Timestamp::now(),
            schema_version,
            status: OpStatus::Durable,
        }
    }

    /// Verifies the cryptographic signature against an Ed25519 public key,
    /// ensuring both that the public key derives to actor_node_id and that the signature is valid.
    pub fn verify_signature_with_pubkey(
        &self,
        public_key_bytes: &[u8; 32],
    ) -> Result<(), DataError> {
        let derived_node_id = NodeId::from_ed25519_public_key(public_key_bytes);
        if derived_node_id != self.actor_node_id {
            return Err(DataError::StorageFailure(format!(
                "Public key derives to {}, which does not match actor {}",
                derived_node_id, self.actor_node_id
            )));
        }

        let signing_payload = Self::compute_signing_payload(
            &self.document_id,
            &self.actor_node_id,
            self.sequence,
            self.op_type,
            &self.payload,
            &self.hlc,
            &self.causal_dependencies,
            self.schema_version,
        );

        NodeIdentity::verify(public_key_bytes, &signing_payload, &self.signature)
            .map_err(|e| DataError::StorageFailure(format!("Invalid operation signature: {}", e)))
    }

    /// Verifies the cryptographic signature using a NodeIdentity reference.
    pub fn verify_signature_with_identity(&self, identity: &NodeIdentity) -> Result<(), DataError> {
        self.verify_signature_with_pubkey(&identity.public_key_bytes())
    }
}

/// Document-level causal and sequence state summary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaState {
    pub document_id: DocumentId,
    pub state_vector: crate::causality::StateVector,
    pub latest_hlc: HybridLogicalClock,
    pub local_sequence: u64,
    pub updated_at: Timestamp,
}
