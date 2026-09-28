use rand::RngCore;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

use fieldnet_core::NodeId;

use crate::chunks::id::ChunkId;
use crate::error::ContentError;
use crate::identity::ContentId;
use crate::manifest::id::ManifestId;
use crate::manifest::model::ContentManifest;

/// 16-byte unique identifier for a content transfer session.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TransferId([u8; 16]);

impl TransferId {
    /// Creates a new random 16-byte `TransferId`.
    pub fn random() -> Self {
        let mut bytes = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut bytes);
        Self(bytes)
    }

    /// Creates a `TransferId` from a raw 16-byte array.
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the underlying 16-byte slice.
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Formats as 32-character lowercase hex string.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Parses from a 32-character hex string.
    pub fn from_hex(s: &str) -> Result<Self, ContentError> {
        let s = s.trim();
        if s.len() != 32 {
            return Err(ContentError::InvalidContentId(format!(
                "Invalid TransferId hex length: expected 32, got {}",
                s.len()
            )));
        }
        let mut bytes = [0u8; 16];
        hex::decode_to_slice(s, &mut bytes).map_err(|e| {
            ContentError::InvalidContentId(format!("Failed to parse TransferId hex: {}", e))
        })?;
        Ok(Self(bytes))
    }
}

impl fmt::Debug for TransferId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TransferId({})", self.to_hex())
    }
}

impl fmt::Display for TransferId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl Serialize for TransferId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for TransferId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::from_hex(&s).map_err(serde::de::Error::custom)
    }
}

/// Explicit acknowledgment status for transferred chunks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AckStatus {
    /// Chunk request was dispatched to outbound queue.
    Sent,
    /// Chunk was forwarded across an intermediate mesh hop.
    Forwarded,
    /// Chunk payload bytes received at target node.
    Received,
    /// Cryptographic hash verified against expected ChunkId and committed to storage.
    Verified,
    /// Chunk incorporated into overall reconstruction.
    Processed,
    /// Chunk verification failed or chunk data corrupted.
    Failed,
}

/// Negotiation response status from provider.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferResponseStatus {
    Accepted,
    Rejected,
    NotFound,
    Unauthorized,
    CapacityExceeded,
}

/// Structured transfer error classification code.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferErrorCode {
    NotFound,
    Unauthorized,
    InvalidManifest,
    StorageError,
    Timeout,
    CapacityExceeded,
    Corrupted,
    InternalError,
}

/// Wire request message asking a provider for manifest or specific chunks.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentRequest {
    pub transfer_id: TransferId,
    pub content_id: ContentId,
    pub manifest_id: Option<ManifestId>,
    /// Empty requested_chunks denotes initial manifest request & negotiation.
    pub requested_chunks: Vec<u32>,
    pub requester_node_id: NodeId,
    pub max_window_bytes: u32,
    pub priority: u8,
    pub created_at: u64,
    pub expires_at: u64,
    pub auth_token: Option<Vec<u8>>,
}

/// Negotiation response from provider acknowledging content request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentResponse {
    pub transfer_id: TransferId,
    pub content_id: ContentId,
    pub manifest: Option<ContentManifest>,
    pub status: TransferResponseStatus,
    pub accepted_chunks: Vec<u32>,
    pub rejected_chunks: Vec<u32>,
    pub total_chunks: u32,
    pub total_bytes: u64,
    pub nominal_chunk_size: u32,
    pub error_message: Option<String>,
}

/// Hex serialization helper for chunk binary data.
mod hex_data {
    use super::*;

    pub fn serialize<S>(data: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&hex::encode(data))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        hex::decode(&s).map_err(serde::de::Error::custom)
    }
}

/// Bulk chunk wire transmission frame.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentChunk {
    pub transfer_id: TransferId,
    pub content_id: ContentId,
    pub chunk_index: u32,
    pub chunk_id: ChunkId,
    #[serde(with = "hex_data")]
    pub data: Vec<u8>,
}

/// Acknowledgement frame confirming chunk receipt and verification state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentAck {
    pub transfer_id: TransferId,
    pub chunk_index: u32,
    pub chunk_id: ChunkId,
    pub status: AckStatus,
    pub bytes_received: u32,
}

/// Cancellation message terminating an active transfer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferCancel {
    pub transfer_id: TransferId,
    pub reason: String,
}

/// Notification sent to provider when receiver completes full verification.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferComplete {
    pub transfer_id: TransferId,
    pub content_id: ContentId,
    pub total_bytes: u64,
}

/// Explicit error frame transmitted between transfer peers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferError {
    pub transfer_id: TransferId,
    pub code: TransferErrorCode,
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transfer_id_roundtrip() {
        let tid = TransferId::random();
        let hex = tid.to_hex();
        assert_eq!(hex.len(), 32);

        let parsed = TransferId::from_hex(&hex).unwrap();
        assert_eq!(tid, parsed);

        let json = serde_json::to_string(&tid).unwrap();
        let from_json: TransferId = serde_json::from_str(&json).unwrap();
        assert_eq!(tid, from_json);
    }

    #[test]
    fn test_content_chunk_serialization() {
        let chunk = ContentChunk {
            transfer_id: TransferId::random(),
            content_id: ContentId::from_bytes([1u8; 32]),
            chunk_index: 0,
            chunk_id: ChunkId::from_bytes([2u8; 32]),
            data: vec![0xDE, 0xAD, 0xBE, 0xEF],
        };

        let json = serde_json::to_string(&chunk).unwrap();
        assert!(json.contains("deadbeef"));

        let decoded: ContentChunk = serde_json::from_str(&json).unwrap();
        assert_eq!(chunk, decoded);
    }
}
