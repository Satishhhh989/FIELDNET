//! Synchronization Protocol Wire Messages and Types for FIELDNET Phase 06.
//!
//! Conforms strictly to PHASE-06-03-SYNC-PROTOCOL-IMPLEMENTATION-CONTRACTS.md.
//! All sync messages are framed inside Phase 05 Message envelopes with MessageType::Sync = 6.

use fieldnet_core::NodeId;
use serde::{Deserialize, Serialize};

use crate::causality::{HybridLogicalClock, StateVector};
use crate::crdt::CrdtType;
use crate::document::DocumentId;
use crate::error::DataError;
use crate::operation::{Operation, OperationId};
use crate::schema::{
    CURRENT_SCHEMA_VERSION, CURRENT_SYNC_PROTOCOL_VERSION, MAX_SYNC_BATCH_BYTES,
    MAX_SYNC_BATCH_OPERATIONS,
};

/// Compact summary of a single document's causal state for synchronization comparisons.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentSummaryEntry {
    pub document_id: DocumentId,
    pub document_type: String,
    pub schema_version: u32,
    pub version: u64,
    pub state_vector: StateVector,
    pub latest_hlc: HybridLogicalClock,
    pub is_deleted: bool,
    pub state_hash: [u8; 32],
}

/// Acknowledgment status for an operation or operation batch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncAckStatus {
    /// Operation successfully validated and committed to SQLite.
    Applied,
    /// Operation has already been applied previously; no double-mutation performed.
    Duplicate,
    /// Operation was rejected during the validation pipeline.
    Rejected { reason: String },
    /// Operation is missing causal dependencies; held in unapplied buffer.
    WaitingDependency { missing_op_id: OperationId },
}

/// Synchronization Protocol Messages for FIELDNET peer-to-peer data replication.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncMessage {
    /// 1. Initial Handshake Greeting
    Hello {
        protocol_version: u32,
        node_id: NodeId,
        supported_crdt_types: Vec<CrdtType>,
        capabilities: Vec<String>,
        supported_namespaces: Vec<String>,
        max_batch_size: u32,
        snapshot_support: bool,
        causal_model: String,
        schema_version: u32,
    },

    /// 2. Compact State Summary for a collection of documents
    StateSummary {
        session_id: [u8; 16],
        summaries: Vec<DocumentSummaryEntry>,
    },

    /// 3. Targeted Delta Request for missing operations
    DeltaRequest {
        session_id: [u8; 16],
        request_id: [u8; 16],
        document_id: DocumentId,
        have_vector: StateVector,
        max_operations: u32,
        namespace_filter: Option<String>,
        document_id_filter: Option<Vec<DocumentId>>,
    },

    /// 4. Paged Delta Response containing batched operations
    DeltaResponse {
        session_id: [u8; 16],
        request_id: [u8; 16],
        document_id: DocumentId,
        operations: Vec<Operation>,
        has_more: bool,
        batch_index: u32,
        total_batches: Option<u32>,
    },

    /// 5. Immediate Live Operation Push (event-driven real-time sync)
    LiveOperation {
        session_id: Option<[u8; 16]>,
        operation: Operation,
        document_type: Option<String>,
    },

    /// 6. Synchronized Watermark Acknowledgment
    SyncAck {
        session_id: Option<[u8; 16]>,
        document_id: DocumentId,
        operation_ids: Vec<OperationId>,
        up_to_sequence: u64,
        confirmed_hlc: HybridLogicalClock,
        status: SyncAckStatus,
        resulting_state_vector: StateVector,
    },

    /// 7. Snapshot Baseline for Fast-Sync / Compaction Recovery
    SnapshotDelivery {
        session_id: Option<[u8; 16]>,
        document_id: DocumentId,
        snapshot_version: u64,
        state_vector: StateVector,
        snapshot_bytes: Vec<u8>,
        state_hash: [u8; 32],
    },
}

impl SyncMessage {
    /// Creates a standard Hello message with node capabilities.
    pub fn hello(node_id: NodeId) -> Self {
        Self::Hello {
            protocol_version: CURRENT_SYNC_PROTOCOL_VERSION,
            node_id,
            supported_crdt_types: vec![
                CrdtType::PnCounter,
                CrdtType::OrSet,
                CrdtType::LwwRegister,
                CrdtType::MvRegister,
                CrdtType::CrdtMap,
            ],
            capabilities: vec![
                "incremental".to_string(),
                "bounded-batches".to_string(),
                "tombstones".to_string(),
                "crdt-merge".to_string(),
                "resumable".to_string(),
            ],
            supported_namespaces: vec!["default".to_string(), "*".to_string()],
            max_batch_size: MAX_SYNC_BATCH_OPERATIONS as u32,
            snapshot_support: true,
            causal_model: "HLC_v1".to_string(),
            schema_version: CURRENT_SCHEMA_VERSION,
        }
    }

    /// Serializes the sync message canonically to JSON bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, DataError> {
        let bytes = serde_json::to_vec(self).map_err(|e| {
            DataError::SerializationFailure(format!("SyncMessage serialize: {}", e))
        })?;
        Ok(bytes)
    }

    /// Deserializes a sync message from wire bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DataError> {
        let msg = serde_json::from_slice(bytes).map_err(|e| {
            DataError::SerializationFailure(format!("SyncMessage deserialize: {}", e))
        })?;
        Ok(msg)
    }

    /// Returns human-readable message type name for structured observability.
    pub fn message_name(&self) -> &'static str {
        match self {
            Self::Hello { .. } => "SYNC_HELLO",
            Self::StateSummary { .. } => "SYNC_STATE",
            Self::DeltaRequest { .. } => "SYNC_REQUEST",
            Self::DeltaResponse { .. } => "SYNC_RESPONSE",
            Self::LiveOperation { .. } => "SYNC_OPERATION",
            Self::SyncAck { .. } => "SYNC_ACK",
            Self::SnapshotDelivery { .. } => "SYNC_SNAPSHOT",
        }
    }

    /// Validates payload size and batch counts.
    pub fn validate_limits(&self) -> Result<(), DataError> {
        if let Self::DeltaResponse { operations, .. } = self {
            if operations.len() > MAX_SYNC_BATCH_OPERATIONS {
                return Err(DataError::BatchSizeExceeded {
                    size: operations.len(),
                    max: MAX_SYNC_BATCH_OPERATIONS,
                });
            }
            let total_payload_bytes: usize = operations.iter().map(|op| op.payload.len()).sum();
            if total_payload_bytes > MAX_SYNC_BATCH_BYTES {
                return Err(DataError::BatchSizeExceeded {
                    size: total_payload_bytes,
                    max: MAX_SYNC_BATCH_BYTES,
                });
            }
        }
        Ok(())
    }
}
