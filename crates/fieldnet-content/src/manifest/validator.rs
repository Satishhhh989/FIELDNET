use crate::chunks::chunker::{CHUNKING_ALGORITHM_FIXED_256K, MAX_CHUNK_SIZE, MIN_CHUNK_SIZE};
use crate::error::ContentError;
use crate::manifest::model::{
    compute_merkle_root, ContentManifest, CURRENT_MANIFEST_SCHEMA_VERSION,
};

/// Authoritative manifest validation engine.
pub struct ManifestValidator;

impl ManifestValidator {
    /// Validates a `ContentManifest` against structural, cryptographic, and ordering invariants.
    pub fn validate(manifest: &ContentManifest) -> Result<(), ContentError> {
        // 1. Schema version
        if manifest.schema_version != CURRENT_MANIFEST_SCHEMA_VERSION {
            return Err(ContentError::ManifestInvalid(format!(
                "Unsupported schema version: expected {}, got {}",
                CURRENT_MANIFEST_SCHEMA_VERSION, manifest.schema_version
            )));
        }

        // 2. Chunking algorithm
        if manifest.chunking_algorithm != CHUNKING_ALGORITHM_FIXED_256K {
            return Err(ContentError::UnsupportedChunking(
                manifest.chunking_algorithm.clone(),
            ));
        }

        // 3. Chunk size bounds
        if (manifest.chunk_size as usize) < MIN_CHUNK_SIZE
            || (manifest.chunk_size as usize) > MAX_CHUNK_SIZE
        {
            return Err(ContentError::ManifestInvalid(format!(
                "Chunk size {} out of bounds [{}, {}]",
                manifest.chunk_size, MIN_CHUNK_SIZE, MAX_CHUNK_SIZE
            )));
        }

        // 4. Chunk count consistency
        if manifest.chunk_count != manifest.chunks.len() as u32 {
            return Err(ContentError::ManifestInvalid(format!(
                "Chunk count mismatch: header declares {}, chunks array contains {}",
                manifest.chunk_count,
                manifest.chunks.len()
            )));
        }

        // 5. Empty content handling
        if manifest.total_size == 0 {
            if manifest.chunk_count > 1 {
                return Err(ContentError::ManifestInvalid(
                    "Zero-size content cannot contain more than 1 chunk".into(),
                ));
            }
            if let Some(first) = manifest.chunks.first() {
                if first.size != 0 {
                    return Err(ContentError::ManifestInvalid(
                        "Zero-size content must have zero-size chunk".into(),
                    ));
                }
            }
        }

        // 6. Sequential chunk verification
        let mut expected_offset: u64 = 0;
        let mut sum_sizes: u64 = 0;

        for (expected_idx, chunk) in manifest.chunks.iter().enumerate() {
            // Index must match position
            if chunk.index != expected_idx as u32 {
                return Err(ContentError::ManifestInvalid(format!(
                    "Out of order chunk: expected index {}, got {}",
                    expected_idx, chunk.index
                )));
            }

            // Offset must match expected sequential offset
            if chunk.offset != expected_offset {
                return Err(ContentError::ManifestInvalid(format!(
                    "Offset mismatch for chunk {}: expected {}, got {}",
                    chunk.index, expected_offset, chunk.offset
                )));
            }

            // Non-final chunk must equal nominal chunk_size
            let is_last = expected_idx == manifest.chunks.len() - 1;
            if !is_last && chunk.size != manifest.chunk_size {
                return Err(ContentError::ManifestInvalid(format!(
                    "Intermediate chunk {} has size {}, expected nominal {}",
                    chunk.index, chunk.size, manifest.chunk_size
                )));
            }

            // Final chunk must not exceed nominal chunk_size
            if is_last && chunk.size > manifest.chunk_size {
                return Err(ContentError::ManifestInvalid(format!(
                    "Final chunk {} exceeds nominal chunk size: {} > {}",
                    chunk.index, chunk.size, manifest.chunk_size
                )));
            }

            expected_offset += chunk.size as u64;
            sum_sizes += chunk.size as u64;
        }

        // 7. Total size matches sum of chunk sizes
        if sum_sizes != manifest.total_size {
            return Err(ContentError::ManifestInvalid(format!(
                "Sum of chunk sizes ({}) does not match manifest total_size ({})",
                sum_sizes, manifest.total_size
            )));
        }

        // 8. Merkle root integrity
        let computed_merkle = compute_merkle_root(&manifest.chunks);
        if computed_merkle != manifest.merkle_root {
            return Err(ContentError::ManifestIntegrityFailure(
                "Merkle root mismatch over chunk IDs".into(),
            ));
        }

        // 9. Deterministic ManifestId verification
        let computed_manifest_id = ContentManifest::compute_manifest_id(
            &manifest.content_id,
            manifest.total_size,
            manifest.chunk_size,
            manifest.chunk_count,
            &manifest.merkle_root,
            &manifest.chunking_algorithm,
        );

        if computed_manifest_id != manifest.manifest_id {
            return Err(ContentError::ManifestIntegrityFailure(format!(
                "ManifestId mismatch: header declared {}, recomputed {}",
                manifest.manifest_id, computed_manifest_id
            )));
        }

        Ok(())
    }
}
