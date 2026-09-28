use serde::{Deserialize, Serialize};

use fieldnet_core::{MessageId, NodeId, Timestamp};
use fieldnet_identity::NodeIdentity;

use crate::error::ContentError;
use crate::identity::ContentId;
use crate::manifest::id::ManifestId;

/// Maximum allowable serialized size for a content query frame (16 KiB).
pub const MAX_QUERY_BYTES: usize = 16 * 1024;

/// Authoritative content query wire model.
///
/// Dispatched across the mesh or DTN to discover remote providers for a given `ContentId`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentQuery {
    /// Unique correlation message identifier for this query.
    pub query_id: MessageId,
    /// Target content identifier requested.
    pub content_id: ContentId,
    /// Optional manifest ID if querying a specific manifest version.
    pub manifest_id: Option<ManifestId>,
    /// Optional inclusive range of chunk indices needed `(start_index, end_index)`.
    pub desired_chunk_range: Option<(u32, u32)>,
    /// Sovereign node ID of the requester.
    pub requester: NodeId,
    /// Requester's Ed25519 public key.
    #[serde(with = "hex")]
    pub requester_public_key: [u8; 32],
    /// Query creation timestamp.
    pub created_at: Timestamp,
    /// Query expiration deadline.
    pub expires_at: Timestamp,
    /// Maximum number of provider replies requested.
    pub max_response_providers: u8,
    /// Digital signature over canonical query bytes.
    #[serde(with = "hex")]
    pub signature: [u8; 64],
}

impl ContentQuery {
    /// Constructs a new unsigned content query with a specified TTL in milliseconds.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        query_id: MessageId,
        content_id: ContentId,
        manifest_id: Option<ManifestId>,
        desired_chunk_range: Option<(u32, u32)>,
        requester: NodeId,
        requester_public_key: [u8; 32],
        ttl_millis: u64,
        max_response_providers: u8,
    ) -> Self {
        let now = Timestamp::now();
        let expires_at = Timestamp::from_millis(now.as_millis().saturating_add(ttl_millis));

        Self {
            query_id,
            content_id,
            manifest_id,
            desired_chunk_range,
            requester,
            requester_public_key,
            created_at: now,
            expires_at,
            max_response_providers,
            signature: [0u8; 64],
        }
    }

    /// Constructs a new content query with simplified parameters.
    pub fn create(
        content_id: ContentId,
        manifest_id: Option<ManifestId>,
        desired_chunk_range: Option<(u32, u32)>,
        requester: NodeId,
        requester_public_key: [u8; 32],
        ttl_seconds: u32,
    ) -> Self {
        let query_id = MessageId::compute(
            &requester,
            0,
            Timestamp::now().as_millis(),
            content_id.as_bytes(),
        );
        Self::new(
            query_id,
            content_id,
            manifest_id,
            desired_chunk_range,
            requester,
            requester_public_key,
            (ttl_seconds as u64) * 1000,
            10,
        )
    }

    /// Serializes canonical bytes over which the requester's signature is computed.
    pub fn canonical_signing_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(160);
        bytes.extend_from_slice(b"FIELDNET_CONTENT_QUERY_V1:");
        bytes.extend_from_slice(self.query_id.as_bytes());
        bytes.extend_from_slice(self.content_id.as_bytes());
        if let Some(mid) = &self.manifest_id {
            bytes.extend_from_slice(mid.as_bytes());
        }
        if let Some((start, end)) = self.desired_chunk_range {
            bytes.extend_from_slice(&start.to_be_bytes());
            bytes.extend_from_slice(&end.to_be_bytes());
        }
        bytes.extend_from_slice(self.requester.as_bytes());
        bytes.extend_from_slice(&self.requester_public_key);
        bytes.extend_from_slice(&self.created_at.as_millis().to_be_bytes());
        bytes.extend_from_slice(&self.expires_at.as_millis().to_be_bytes());
        bytes.push(self.max_response_providers);
        bytes
    }

    /// Signs the query using the requester's sovereign NodeIdentity.
    pub fn sign(&mut self, identity: &NodeIdentity) {
        let signing_bytes = self.canonical_signing_bytes();
        self.requester_public_key = identity.public_key_bytes();
        self.requester = identity.node_id();
        self.signature = identity.sign(&signing_bytes);
    }

    /// Cryptographically verifies the signature against the requester's public key.
    pub fn verify(&self) -> Result<(), ContentError> {
        let derived_id = NodeId::from_ed25519_public_key(&self.requester_public_key);
        if derived_id != self.requester {
            return Err(ContentError::InvalidAdvertisement(format!(
                "Requester public key does not match NodeId: expected {}, derived {}",
                self.requester, derived_id
            )));
        }

        let signing_bytes = self.canonical_signing_bytes();
        NodeIdentity::verify(&self.requester_public_key, &signing_bytes, &self.signature).map_err(
            |e| {
                ContentError::InvalidAdvertisement(format!(
                    "Query signature verification failed: {}",
                    e
                ))
            },
        )
    }

    /// Checks if the query has expired past `now`.
    pub fn is_expired(&self, now: Timestamp) -> bool {
        now >= self.expires_at
    }

    /// Validates the query fields, expiration, and cryptographic signature.
    pub fn validate(&self, now: Timestamp) -> Result<(), ContentError> {
        if self.is_expired(now) {
            return Err(ContentError::QueryExpired {
                created_at: self.created_at.as_millis(),
                expires_at: self.expires_at.as_millis(),
                now: now.as_millis(),
            });
        }

        if self.expires_at < self.created_at {
            return Err(ContentError::InvalidAdvertisement(
                "Query expires_at is before created_at".into(),
            ));
        }

        self.verify()?;
        Ok(())
    }

    /// Serializes query to JSON bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, ContentError> {
        let bytes = serde_json::to_vec(self).map_err(|e| {
            ContentError::StorageFailure(format!("Failed to serialize query: {}", e))
        })?;
        if bytes.len() > MAX_QUERY_BYTES {
            return Err(ContentError::StorageFailure(format!(
                "Query exceeds max payload size: {} > {} bytes",
                bytes.len(),
                MAX_QUERY_BYTES
            )));
        }
        Ok(bytes)
    }

    /// Deserializes and validates maximum byte length.
    pub fn from_bytes(slice: &[u8]) -> Result<Self, ContentError> {
        if slice.len() > MAX_QUERY_BYTES {
            return Err(ContentError::StorageFailure(format!(
                "Query slice exceeds max payload size: {} > {} bytes",
                slice.len(),
                MAX_QUERY_BYTES
            )));
        }
        serde_json::from_slice(slice).map_err(|e| {
            ContentError::InvalidAdvertisement(format!("Query deserialization error: {}", e))
        })
    }
}
