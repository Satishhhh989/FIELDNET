use serde::{Deserialize, Serialize};

use super::advertisement::ContentAdvertise;
use super::query::ContentQuery;
use crate::error::ContentError;
use crate::transfer::{
    ContentAck, ContentChunk, ContentRequest, ContentResponse, TransferCancel, TransferComplete,
    TransferError,
};

/// Maximum combined content wire message envelope size (2 MiB to accommodate 1 MiB chunk payloads + hex encoding).
pub const MAX_CONTENT_MESSAGE_SIZE: usize = 2 * 1024 * 1024 + 64 * 1024;

/// Authoritative content protocol envelope (Framed in `MessageType::File = 7`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContentMessage {
    /// Broadcast or routed query discovering providers for a content object.
    Query(ContentQuery),
    /// Provider advertisement announcing content or chunk possession.
    Advertise(ContentAdvertise),
    /// Targeted request for manifest or specific chunks.
    Request(ContentRequest),
    /// Negotiation response accepting or rejecting request.
    Response(ContentResponse),
    /// Bulk payload containing a single verified chunk.
    Chunk(ContentChunk),
    /// Acknowledgement of verified chunk receipt.
    Ack(ContentAck),
    /// Cancellation of transfer session.
    Cancel(TransferCancel),
    /// Notification of successful complete content verification.
    Complete(TransferComplete),
    /// Explicit error frame.
    Error(TransferError),
}

impl ContentMessage {
    /// Serializes the content message into framed bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, ContentError> {
        let bytes = serde_json::to_vec(self).map_err(|e| {
            ContentError::StorageFailure(format!("Failed to serialize ContentMessage: {}", e))
        })?;
        if bytes.len() > MAX_CONTENT_MESSAGE_SIZE {
            return Err(ContentError::StorageFailure(format!(
                "ContentMessage exceeds maximum allowable envelope size: {} > {} bytes",
                bytes.len(),
                MAX_CONTENT_MESSAGE_SIZE
            )));
        }
        Ok(bytes)
    }

    /// Deserializes a framed byte slice into a `ContentMessage`.
    pub fn from_bytes(slice: &[u8]) -> Result<Self, ContentError> {
        if slice.len() > MAX_CONTENT_MESSAGE_SIZE {
            return Err(ContentError::StorageFailure(format!(
                "ContentMessage payload exceeds maximum allowable size: {} > {} bytes",
                slice.len(),
                MAX_CONTENT_MESSAGE_SIZE
            )));
        }
        serde_json::from_slice(slice).map_err(|e| {
            ContentError::InvalidAdvertisement(format!("Failed to parse ContentMessage: {}", e))
        })
    }
}
