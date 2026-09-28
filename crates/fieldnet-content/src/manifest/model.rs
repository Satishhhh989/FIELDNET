use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use fieldnet_core::{NodeId, Timestamp};

use crate::chunks::id::{ChunkDescriptor, ChunkId};
use crate::error::ContentError;
use crate::hashing::{hash_bytes, ContentHasher};
use crate::identity::ContentId;
use crate::manifest::id::ManifestId;

/// Current schema version for FIELDNET Content Manifests.
pub const CURRENT_MANIFEST_SCHEMA_VERSION: u32 = 1;

/// Computes the balanced binary Merkle tree root hash over an array of chunk identifiers.
pub fn compute_merkle_root(chunks: &[ChunkDescriptor]) -> [u8; 32] {
    if chunks.is_empty() {
        return [0u8; 32];
    }

    let mut current_layer: Vec<[u8; 32]> = chunks.iter().map(|c| c.chunk_id.to_bytes()).collect();

    while current_layer.len() > 1 {
        let mut next_layer = Vec::with_capacity(current_layer.len().div_ceil(2));
        for chunk in current_layer.chunks(2) {
            if chunk.len() == 2 {
                let mut combined = [0u8; 64];
                combined[..32].copy_from_slice(&chunk[0]);
                combined[32..].copy_from_slice(&chunk[1]);
                next_layer.push(hash_bytes(&combined).to_bytes());
            } else {
                // Odd element: hash with itself to balance tree
                let mut combined = [0u8; 64];
                combined[..32].copy_from_slice(&chunk[0]);
                combined[32..].copy_from_slice(&chunk[0]);
                next_layer.push(hash_bytes(&combined).to_bytes());
            }
        }
        current_layer = next_layer;
    }

    current_layer[0]
}

/// Compact representation of chunk availability using an indexable bitset.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkAvailabilityBitmap {
    total_chunks: u32,
    bits: Vec<u8>,
}

impl ChunkAvailabilityBitmap {
    /// Creates a new bitmap for `total_chunks`, initialized with all chunks missing.
    pub fn new(total_chunks: u32) -> Self {
        let num_bytes = (total_chunks as usize).div_ceil(8);
        Self {
            total_chunks,
            bits: vec![0u8; num_bytes],
        }
    }

    /// Creates a bitmap with all chunks marked present and verified.
    pub fn full(total_chunks: u32) -> Self {
        let mut bitmap = Self::new(total_chunks);
        for i in 0..total_chunks {
            bitmap.mark_available(i);
        }
        bitmap
    }

    /// Marks chunk at `chunk_index` as available.
    pub fn mark_available(&mut self, chunk_index: u32) {
        if chunk_index < self.total_chunks {
            let byte_idx = (chunk_index / 8) as usize;
            let bit_idx = chunk_index % 8;
            self.bits[byte_idx] |= 1 << bit_idx;
        }
    }

    /// Marks chunk at `chunk_index` as missing.
    pub fn mark_missing(&mut self, chunk_index: u32) {
        if chunk_index < self.total_chunks {
            let byte_idx = (chunk_index / 8) as usize;
            let bit_idx = chunk_index % 8;
            self.bits[byte_idx] &= !(1 << bit_idx);
        }
    }

    /// Checks if chunk at `chunk_index` is available.
    pub fn is_available(&self, chunk_index: u32) -> bool {
        if chunk_index >= self.total_chunks {
            return false;
        }
        let byte_idx = (chunk_index / 8) as usize;
        let bit_idx = chunk_index % 8;
        (self.bits[byte_idx] & (1 << bit_idx)) != 0
    }

    /// Total count of available chunks.
    pub fn available_count(&self) -> u32 {
        let mut count = 0;
        for i in 0..self.total_chunks {
            if self.is_available(i) {
                count += 1;
            }
        }
        count
    }

    /// Total number of constituent chunks in the manifest.
    pub fn total_chunks(&self) -> u32 {
        self.total_chunks
    }

    /// Returns true if all chunks are available.
    pub fn is_complete(&self) -> bool {
        self.available_count() == self.total_chunks
    }

    /// Returns list of missing chunk indices.
    pub fn missing_indices(&self) -> Vec<u32> {
        (0..self.total_chunks)
            .filter(|&i| !self.is_available(i))
            .collect()
    }

    /// Returns list of present chunk indices.
    pub fn present_indices(&self) -> Vec<u32> {
        (0..self.total_chunks)
            .filter(|&i| self.is_available(i))
            .collect()
    }

    /// Converts available chunks into contiguous `(start_idx, end_idx_inclusive)` index ranges.
    pub fn to_ranges(&self) -> Vec<(u32, u32)> {
        let mut ranges = Vec::new();
        let mut in_range = false;
        let mut range_start = 0;

        for i in 0..self.total_chunks {
            if self.is_available(i) {
                if !in_range {
                    in_range = true;
                    range_start = i;
                }
            } else if in_range {
                ranges.push((range_start, i - 1));
                in_range = false;
            }
        }

        if in_range {
            ranges.push((range_start, self.total_chunks - 1));
        }

        ranges
    }

    /// Returns raw byte representation of the bitmap.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bits
    }

    /// Reconstructs bitmap from raw bytes.
    pub fn from_bytes(total_chunks: u32, bytes: &[u8]) -> Result<Self, ContentError> {
        let expected_bytes = (total_chunks as usize).div_ceil(8);
        if bytes.len() != expected_bytes {
            return Err(ContentError::StorageFailure(format!(
                "Invalid bitmap byte length: expected {}, got {}",
                expected_bytes,
                bytes.len()
            )));
        }
        Ok(Self {
            total_chunks,
            bits: bytes.to_vec(),
        })
    }
}

/// Strongly typed content manifest describing the chunk decomposition of a content object.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentManifest {
    /// Schema format version.
    pub schema_version: u32,
    /// Root ContentId representing the SHA-256 digest of unchunked content.
    pub content_id: ContentId,
    /// Authoritative ManifestId derived deterministically from the canonical manifest structure.
    pub manifest_id: ManifestId,
    /// Total logical byte size of the content.
    pub total_size: u64,
    /// Nominal chunk size in bytes (e.g. 262,144 for 256 KiB).
    pub chunk_size: u32,
    /// Total number of constituent chunks.
    pub chunk_count: u32,
    /// Constituent chunk descriptors in strict reconstruction order.
    pub chunks: Vec<ChunkDescriptor>,
    /// Chunking algorithm used (e.g. "FIXED_256KB").
    pub chunking_algorithm: String,
    /// Merkle tree root hash across all chunk identifiers.
    pub merkle_root: [u8; 32],
    /// Optional MIME type.
    pub mime_type: Option<String>,
    /// User or application-defined custom metadata.
    pub metadata: HashMap<String, String>,
    /// Creation timestamp.
    pub created_at: Timestamp,
    /// Creator node identifier, if known.
    pub creator_node_id: Option<NodeId>,
}

impl ContentManifest {
    /// Creates and finalizes a new `ContentManifest`, calculating the Merkle root and `ManifestId`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        content_id: ContentId,
        total_size: u64,
        chunk_size: u32,
        chunks: Vec<ChunkDescriptor>,
        chunking_algorithm: impl Into<String>,
        mime_type: Option<String>,
        metadata: HashMap<String, String>,
        creator_node_id: Option<NodeId>,
    ) -> Self {
        let chunking_algorithm = chunking_algorithm.into();
        let chunk_count = chunks.len() as u32;
        let merkle_root = compute_merkle_root(&chunks);
        let created_at = Timestamp::now();

        let manifest_id = Self::compute_manifest_id(
            &content_id,
            total_size,
            chunk_size,
            chunk_count,
            &merkle_root,
            &chunking_algorithm,
        );

        Self {
            schema_version: CURRENT_MANIFEST_SCHEMA_VERSION,
            content_id,
            manifest_id,
            total_size,
            chunk_size,
            chunk_count,
            chunks,
            chunking_algorithm,
            merkle_root,
            mime_type,
            metadata,
            created_at,
            creator_node_id,
        }
    }

    /// Deterministically computes the `ManifestId` from authoritative invariant fields.
    pub fn compute_manifest_id(
        content_id: &ContentId,
        total_size: u64,
        chunk_size: u32,
        chunk_count: u32,
        merkle_root: &[u8; 32],
        chunking_algorithm: &str,
    ) -> ManifestId {
        let mut hasher = ContentHasher::new();
        hasher.update(b"FIELDNET_MANIFEST_V1:");
        hasher.update(content_id.as_bytes());
        hasher.update(&total_size.to_be_bytes());
        hasher.update(&chunk_size.to_be_bytes());
        hasher.update(&chunk_count.to_be_bytes());
        hasher.update(merkle_root);
        hasher.update(chunking_algorithm.as_bytes());
        let digest = hasher.finalize();
        ManifestId::from_bytes(digest.to_bytes())
    }

    /// Returns list of all constituent `ChunkId`s in manifest order.
    pub fn chunk_ids(&self) -> Vec<ChunkId> {
        self.chunks.iter().map(|c| c.chunk_id).collect()
    }
}
