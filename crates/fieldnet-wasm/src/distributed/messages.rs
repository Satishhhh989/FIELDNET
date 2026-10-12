use fieldnet_content::hashing::hash_bytes;
use fieldnet_content::identity::ContentId;
use fieldnet_core::{NodeId, Timestamp};
use serde::{Deserialize, Serialize};

use crate::dependencies::AppDependency;
use crate::error::ApplicationError;
use crate::id::{AppVersion, ApplicationId, PackageId, PublisherIdentity};
use crate::permissions::AppPermission;

/// Maximum allowable package size (100 MB).
pub const MAX_PACKAGE_SIZE: u64 = 100 * 1024 * 1024;

/// Maximum allowable single chunk size (1 MB).
pub const MAX_CHUNK_SIZE: u32 = 1024 * 1024;

/// Safe metadata advertised over the network announcing application package availability.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct PackageAdvertisement {
    /// Canonical application identifier.
    pub application_id: ApplicationId,

    /// Concrete semantic version of this package.
    pub version: AppVersion,

    /// Stable package identity.
    pub package_id: PackageId,

    /// Authoritative content hash of the package.
    pub package_hash: ContentId,

    /// Cryptographic publisher identity.
    pub publisher_id: PublisherIdentity,

    /// Hash of the canonical application manifest.
    pub manifest_hash: ContentId,

    /// Total size in bytes of the package items.
    pub package_size: u64,

    /// Permissions declared by the package manifest.
    pub required_capabilities: Vec<AppPermission>,

    /// Summary of dependencies required by this package.
    pub dependency_summary: Vec<AppDependency>,

    /// Expiration timestamp after which this advertisement must be purged.
    pub advertisement_expiry: Timestamp,

    /// Network NodeId providing/announcing this package.
    pub provider_node: NodeId,
}

impl PackageAdvertisement {
    /// Validates internal consistency of the package advertisement.
    pub fn validate(&self) -> Result<(), ApplicationError> {
        if self.package_size == 0 {
            return Err(ApplicationError::ValidationError(
                "Package size in advertisement must be greater than zero".to_string(),
            ));
        }

        if self.package_size > MAX_PACKAGE_SIZE {
            return Err(ApplicationError::ValidationError(format!(
                "Package size {} exceeds maximum allowable limit of {} bytes",
                self.package_size, MAX_PACKAGE_SIZE
            )));
        }

        for dep in &self.dependency_summary {
            dep.validate()?;
        }

        Ok(())
    }

    /// Checks whether this advertisement has expired.
    pub fn is_expired(&self, now: Timestamp) -> bool {
        now >= self.advertisement_expiry
    }
}

/// Request sent by a node to fetch an application package by exact content hash.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct PackageRequest {
    /// Unique identifier for this request / transfer session.
    pub request_id: String,

    /// Application identity.
    pub application_id: ApplicationId,

    /// Expected application version.
    pub version: AppVersion,

    /// Authoritative package content hash expected by the requester.
    pub package_hash: ContentId,

    /// NodeId of the requester.
    pub requesting_node: NodeId,

    /// Timestamp when this request was created.
    pub created_at: Timestamp,

    /// Timestamp after which this request expires.
    pub expires_at: Timestamp,
}

impl PackageRequest {
    pub fn validate(&self) -> Result<(), ApplicationError> {
        if self.request_id.trim().is_empty() {
            return Err(ApplicationError::ValidationError(
                "Request ID cannot be empty".to_string(),
            ));
        }
        Ok(())
    }

    pub fn is_expired(&self, now: Timestamp) -> bool {
        now.0 >= self.expires_at.0
    }
}

/// Initial response from a package provider describing package chunking and transfer metadata.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct PackageResponse {
    /// Corresponds to the request_id of the initiating PackageRequest.
    pub request_id: String,

    /// Content hash of the offered package. Must match requested package_hash.
    pub package_hash: ContentId,

    /// Total size in bytes of the serialized package bundle.
    pub package_size: u64,

    /// Total number of chunks the package is divided into.
    pub chunk_count: u32,

    /// Size of each chunk in bytes (except potentially the final chunk).
    pub chunk_size: u32,

    /// Canonical manifest hash.
    pub manifest_hash: ContentId,

    /// Ordered cryptographic hashes of each chunk for independent verification.
    pub chunk_hashes: Vec<ContentId>,

    /// Provider error message if the package cannot be served (e.g. sharing denied or not found).
    #[serde(default)]
    pub error: Option<String>,
}

impl PackageResponse {
    pub fn is_success(&self) -> bool {
        self.error.is_none()
            && self.chunk_count > 0
            && self.chunk_hashes.len() == self.chunk_count as usize
    }
}

/// Request for a specific chunk index within an active transfer.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct PackageChunkRequest {
    /// Active transfer session ID.
    pub transfer_id: String,

    /// Package content hash.
    pub package_hash: ContentId,

    /// Zero-based chunk index being requested.
    pub chunk_index: u32,

    /// Requesting node identity.
    pub requesting_node: NodeId,
}

/// Data payload of a single package chunk.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct PackageTransferChunk {
    /// Active transfer session ID.
    pub transfer_id: String,

    /// Package content hash.
    pub package_hash: ContentId,

    /// Zero-based chunk index.
    pub chunk_index: u32,

    /// Total number of chunks in this transfer.
    pub total_chunks: u32,

    /// Expected SHA-256 hash of this chunk's data.
    pub chunk_hash: ContentId,

    /// Size of this chunk in bytes.
    pub chunk_size: u32,

    /// Raw chunk bytes.
    pub data: Vec<u8>,
}

impl PackageTransferChunk {
    /// Cryptographically validates that the chunk payload matches `chunk_hash` and `chunk_size`.
    pub fn verify_chunk_hash(&self) -> Result<(), ApplicationError> {
        if self.data.len() as u32 != self.chunk_size {
            return Err(ApplicationError::IntegrityError(format!(
                "Chunk {} byte length {} does not match declared chunk_size {}",
                self.chunk_index,
                self.data.len(),
                self.chunk_size
            )));
        }

        let computed = hash_bytes(&self.data);
        if computed != self.chunk_hash {
            return Err(ApplicationError::IntegrityError(format!(
                "Chunk {} hash mismatch: expected {}, got {}",
                self.chunk_index, self.chunk_hash, computed
            )));
        }

        Ok(())
    }
}

/// Top-level wire message enum for the distributed application package system.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum ApplicationPackageMessage {
    /// Broadcast announcement of an available application package.
    Advertise(PackageAdvertisement),

    /// Announcement that an application package has been withdrawn or is no longer shared.
    Withdraw {
        application_id: ApplicationId,
        package_hash: ContentId,
        provider_node: NodeId,
    },

    /// Request to fetch an application package by exact content hash.
    Request(PackageRequest),

    /// Initial provider response with chunk manifest and transfer parameters.
    Response(PackageResponse),

    /// Request for an individual chunk.
    ChunkRequest(PackageChunkRequest),

    /// Response containing an individual chunk payload.
    Chunk(PackageTransferChunk),
}

impl ApplicationPackageMessage {
    /// Serializes this message to UTF-8 JSON bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, ApplicationError> {
        serde_json::to_vec(self).map_err(|e| ApplicationError::SerializationError(e.to_string()))
    }

    /// Deserializes a message from UTF-8 JSON bytes.
    pub fn from_bytes(slice: &[u8]) -> Result<Self, ApplicationError> {
        serde_json::from_slice(slice)
            .map_err(|e| ApplicationError::SerializationError(e.to_string()))
    }
}
