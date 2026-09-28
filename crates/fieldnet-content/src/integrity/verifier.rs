use std::sync::Arc;

use crate::chunks::id::ChunkId;
use crate::chunks::store::ChunkStore;
use crate::error::ContentError;
use crate::hashing::{hash_bytes, hash_stream};
use crate::identity::ContentId;
use crate::manifest::model::ContentManifest;
use crate::manifest::validator::ManifestValidator;
use crate::store::ContentStore;

/// Core cryptographic verification functions for chunks, manifests, and content objects.
pub struct ContentVerifier;

impl ContentVerifier {
    /// Verifies that the provided raw chunk bytes match the expected ChunkId.
    pub fn verify_chunk_bytes(expected_id: &ChunkId, bytes: &[u8]) -> Result<(), ContentError> {
        let actual_hash = hash_bytes(bytes);
        let actual_id = ChunkId::from_bytes(actual_hash.to_bytes());

        if &actual_id != expected_id {
            return Err(ContentError::ChunkIntegrityFailure {
                expected: *expected_id,
                actual: actual_id,
            });
        }
        Ok(())
    }

    /// Reads a stored chunk from ChunkStore and verifies its cryptographic integrity.
    pub async fn verify_stored_chunk(
        chunk_id: &ChunkId,
        chunk_store: &ChunkStore,
    ) -> Result<(), ContentError> {
        let buffer = chunk_store.get(chunk_id).await?;
        Self::verify_chunk_bytes(chunk_id, &buffer)
    }

    /// Verifies the structural and cryptographic integrity of a ContentManifest.
    pub fn verify_manifest(manifest: &ContentManifest) -> Result<(), ContentError> {
        ManifestValidator::validate(manifest)
    }

    /// Performs full-tier verification of a stored content object, checking the manifest,
    /// verifying all constituent chunks, and validating the final ContentId.
    pub async fn verify_stored_content(
        content_id: &ContentId,
        content_store: &Arc<ContentStore>,
    ) -> Result<(), ContentError> {
        // 1. Retrieve and verify manifest
        let manifest = content_store
            .manifest_store()
            .get_by_content_id(content_id)?;
        Self::verify_manifest(&manifest)?;

        // 2. Verify all constituent chunks in ChunkStore
        let chunk_store = content_store.chunk_store();
        for chunk_desc in &manifest.chunks {
            Self::verify_stored_chunk(&chunk_desc.chunk_id, chunk_store).await?;
        }

        // 3. Verify physical reconstructed file if it exists
        let rel_path = content_id.relative_storage_path();
        let obj_path = content_store.objects_dir().join(rel_path);

        if obj_path.exists() {
            let mut file = tokio::fs::File::open(&obj_path).await.map_err(|e| {
                ContentError::IOError(format!("Failed to open physical content object: {}", e))
            })?;
            let computed_content_id = hash_stream(&mut file).await?;

            if &computed_content_id != content_id {
                return Err(ContentError::IntegrityFailure {
                    expected: *content_id,
                    actual: computed_content_id,
                });
            }
        }

        Ok(())
    }
}
