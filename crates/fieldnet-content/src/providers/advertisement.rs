use serde::{Deserialize, Serialize};

use fieldnet_core::{MessageId, NodeId, Timestamp};
use fieldnet_identity::NodeIdentity;

use crate::error::ContentError;
use crate::identity::ContentId;
use crate::manifest::id::ManifestId;
use crate::manifest::model::ChunkAvailabilityBitmap;

/// Maximum allowable serialized size for a content advertisement frame (64 KiB).
pub const MAX_ADVERTISEMENT_BYTES: usize = 64 * 1024;

/// Authoritative content advertisement wire model.
///
/// Announces possession of a complete content object or specific chunk subset.
/// Signed by the provider's sovereign Ed25519 identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentAdvertise {
    /// Correlation query ID if responding directly to a `ContentQuery`.
    pub query_id: Option<MessageId>,
    /// Target content identifier.
    pub content_id: ContentId,
    /// Associated manifest ID, if known.
    pub manifest_id: Option<ManifestId>,
    /// Provider's sovereign node ID.
    pub provider_node_id: NodeId,
    /// Provider's Ed25519 public verifying key.
    #[serde(with = "hex")]
    pub provider_public_key: [u8; 32],
    /// True if the provider possesses the complete content object.
    pub has_full_content: bool,
    /// Specific chunk availability bitset if advertising partial content.
    pub available_chunks: Option<ChunkAvailabilityBitmap>,
    /// Total number of constituent chunks in the content.
    pub total_chunks: Option<u32>,
    /// Total content byte size, if known.
    pub content_size: Option<u64>,
    /// Advertisement time-to-live in seconds.
    pub ttl_seconds: u32,
    /// Monotonically increasing sequence number from the provider node.
    pub sequence: u64,
    /// Creation timestamp.
    pub created_at: Timestamp,
    /// Expiration timestamp.
    pub expires_at: Timestamp,
    /// Ed25519 digital signature over canonical signing bytes.
    #[serde(with = "hex")]
    pub signature: [u8; 64],
}

impl ContentAdvertise {
    /// Constructs an unsigned content advertisement.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        query_id: Option<MessageId>,
        content_id: ContentId,
        manifest_id: Option<ManifestId>,
        provider_node_id: NodeId,
        provider_public_key: [u8; 32],
        has_full_content: bool,
        available_chunks: Option<ChunkAvailabilityBitmap>,
        total_chunks: Option<u32>,
        content_size: Option<u64>,
        ttl_seconds: u32,
        sequence: u64,
    ) -> Self {
        let now = Timestamp::now();
        let expires_at = now.add_seconds(ttl_seconds as u64);

        Self {
            query_id,
            content_id,
            manifest_id,
            provider_node_id,
            provider_public_key,
            has_full_content,
            available_chunks,
            total_chunks,
            content_size,
            ttl_seconds,
            sequence,
            created_at: now,
            expires_at,
            signature: [0u8; 64],
        }
    }

    /// Constructs a full content advertisement without chunk bitmap.
    pub fn new_full(
        content_id: ContentId,
        manifest_id: Option<ManifestId>,
        provider_node_id: NodeId,
        provider_public_key: [u8; 32],
        content_size: Option<u64>,
        ttl_seconds: u32,
        sequence: u64,
    ) -> Self {
        Self::new(
            None,
            content_id,
            manifest_id,
            provider_node_id,
            provider_public_key,
            true,
            None,
            None,
            content_size,
            ttl_seconds,
            sequence,
        )
    }

    /// Serializes canonical bytes over which the provider's signature is computed.
    pub fn canonical_signing_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(256);
        bytes.extend_from_slice(b"FIELDNET_CONTENT_ADVERTISE_V1:");
        if let Some(qid) = &self.query_id {
            bytes.extend_from_slice(qid.as_bytes());
        }
        bytes.extend_from_slice(self.content_id.as_bytes());
        if let Some(mid) = &self.manifest_id {
            bytes.extend_from_slice(mid.as_bytes());
        }
        bytes.extend_from_slice(self.provider_node_id.as_bytes());
        bytes.extend_from_slice(&self.provider_public_key);
        bytes.push(if self.has_full_content { 1 } else { 0 });
        if let Some(bitmap) = &self.available_chunks {
            bytes.extend_from_slice(bitmap.as_bytes());
        }
        if let Some(total) = self.total_chunks {
            bytes.extend_from_slice(&total.to_be_bytes());
        }
        if let Some(size) = self.content_size {
            bytes.extend_from_slice(&size.to_be_bytes());
        }
        bytes.extend_from_slice(&self.ttl_seconds.to_be_bytes());
        bytes.extend_from_slice(&self.sequence.to_be_bytes());
        bytes.extend_from_slice(&self.created_at.as_millis().to_be_bytes());
        bytes.extend_from_slice(&self.expires_at.as_millis().to_be_bytes());
        bytes
    }

    /// Signs the advertisement with the provider's sovereign NodeIdentity.
    pub fn sign(&mut self, identity: &NodeIdentity) {
        let signing_bytes = self.canonical_signing_bytes();
        self.provider_public_key = identity.public_key_bytes();
        self.provider_node_id = identity.node_id();
        self.signature = identity.sign(&signing_bytes);
    }

    /// Verifies the digital signature and public key alignment with `provider_node_id`.
    pub fn verify(&self) -> Result<(), ContentError> {
        let derived_id = NodeId::from_ed25519_public_key(&self.provider_public_key);
        if derived_id != self.provider_node_id {
            return Err(ContentError::InvalidAdvertisement(format!(
                "Public key does not match provider NodeId: expected {}, derived {}",
                self.provider_node_id, derived_id
            )));
        }

        let signing_bytes = self.canonical_signing_bytes();
        NodeIdentity::verify(&self.provider_public_key, &signing_bytes, &self.signature).map_err(
            |e| ContentError::InvalidAdvertisement(format!("Signature verification failed: {}", e)),
        )
    }

    /// Checks if the advertisement has expired past `now`.
    pub fn is_expired(&self, now: Timestamp) -> bool {
        now >= self.expires_at
    }

    /// Full fail-closed validation of advertisement fields and cryptographic integrity.
    pub fn validate(&self, now: Timestamp) -> Result<(), ContentError> {
        if self.is_expired(now) {
            return Err(ContentError::AdvertisementExpired {
                created_at: self.created_at.as_millis(),
                expires_at: self.expires_at.as_millis(),
                now: now.as_millis(),
            });
        }

        if self.expires_at < self.created_at {
            return Err(ContentError::InvalidAdvertisement(
                "Advertisement expires_at is before created_at".into(),
            ));
        }

        self.verify()?;
        Ok(())
    }

    /// Serializes the advertisement to JSON bytes, verifying payload bounds.
    pub fn to_bytes(&self) -> Result<Vec<u8>, ContentError> {
        let bytes = serde_json::to_vec(self).map_err(|e| {
            ContentError::StorageFailure(format!("Failed to serialize advertisement: {}", e))
        })?;
        if bytes.len() > MAX_ADVERTISEMENT_BYTES {
            return Err(ContentError::StorageFailure(format!(
                "Advertisement exceeds max payload size: {} > {} bytes",
                bytes.len(),
                MAX_ADVERTISEMENT_BYTES
            )));
        }
        Ok(bytes)
    }

    /// Deserializes and validates maximum byte length.
    pub fn from_bytes(slice: &[u8]) -> Result<Self, ContentError> {
        if slice.len() > MAX_ADVERTISEMENT_BYTES {
            return Err(ContentError::StorageFailure(format!(
                "Advertisement slice exceeds max payload size: {} > {} bytes",
                slice.len(),
                MAX_ADVERTISEMENT_BYTES
            )));
        }
        serde_json::from_slice(slice).map_err(|e| {
            ContentError::InvalidAdvertisement(format!("Deserialization error: {}", e))
        })
    }
}
