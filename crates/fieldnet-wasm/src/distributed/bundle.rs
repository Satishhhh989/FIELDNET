use fieldnet_content::hashing::hash_bytes;
use fieldnet_content::identity::ContentId;
use fieldnet_content::store::ContentStore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use super::messages::PackageTransferChunk;
use crate::cas::CasPackageStorage;
use crate::error::ApplicationError;
use crate::package::ApplicationPackage;

/// Self-contained canonical bundle representing an application package and all its item payloads.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct PackageBundle {
    /// Complete application package metadata, manifest, and signature.
    pub package: ApplicationPackage,

    /// Item file payloads mapped by relative path.
    pub payloads: HashMap<String, Vec<u8>>,
}

impl PackageBundle {
    /// Creates a new package bundle.
    pub fn new(package: ApplicationPackage, payloads: HashMap<String, Vec<u8>>) -> Self {
        Self { package, payloads }
    }

    /// Loads a package bundle from the local `ContentStore` given an `ApplicationPackage`.
    pub async fn from_cas(
        content_store: &Arc<ContentStore>,
        package: &ApplicationPackage,
    ) -> Result<Self, ApplicationError> {
        let payloads = CasPackageStorage::load_package_payloads(content_store, package).await?;
        Ok(Self {
            package: package.clone(),
            payloads,
        })
    }

    /// Stores all package items and the package descriptor into the `ContentStore`.
    pub async fn store_to_cas(
        &self,
        content_store: &Arc<ContentStore>,
    ) -> Result<ContentId, ApplicationError> {
        CasPackageStorage::store_package(content_store, &self.package, &self.payloads).await
    }

    /// Serializes the package bundle to canonical JSON bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, ApplicationError> {
        serde_json::to_vec(self).map_err(|e| ApplicationError::SerializationError(e.to_string()))
    }

    /// Deserializes a package bundle from bytes and verifies its cryptographic integrity and signature.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ApplicationError> {
        let bundle: Self = serde_json::from_slice(bytes)
            .map_err(|e| ApplicationError::SerializationError(e.to_string()))?;
        bundle.verify()?;
        Ok(bundle)
    }

    /// Cryptographically validates the bundle:
    /// 1. Manifest structure and entrypoint presence.
    /// 2. Payload existence and SHA-256 hash match for each declared item.
    /// 3. Publisher signature verification against computed package hash.
    pub fn verify(&self) -> Result<ContentId, ApplicationError> {
        let computed_hash = self.package.verify_integrity(Some(&self.payloads))?;
        self.package.verify_signature()?;
        Ok(computed_hash)
    }

    /// Partitions the serialized bundle into a sequence of chunk payloads.
    pub fn create_chunks(
        &self,
        chunk_size: u32,
        transfer_id: &str,
    ) -> Result<(Vec<PackageTransferChunk>, Vec<ContentId>), ApplicationError> {
        if chunk_size == 0 {
            return Err(ApplicationError::ValidationError(
                "Chunk size must be greater than zero".to_string(),
            ));
        }

        let bundle_bytes = self.to_bytes()?;
        let total_bytes = bundle_bytes.len() as u64;
        let package_hash = self.package.compute_package_hash();

        let chunk_count = if total_bytes == 0 {
            1
        } else {
            total_bytes.div_ceil(chunk_size as u64) as u32
        };

        let mut chunks = Vec::with_capacity(chunk_count as usize);
        let mut chunk_hashes = Vec::with_capacity(chunk_count as usize);

        for index in 0..chunk_count {
            let start = (index as usize) * (chunk_size as usize);
            let end = (start + (chunk_size as usize)).min(bundle_bytes.len());
            let slice = &bundle_bytes[start..end];
            let hash = hash_bytes(slice);

            chunk_hashes.push(hash);
            chunks.push(PackageTransferChunk {
                transfer_id: transfer_id.to_string(),
                package_hash,
                chunk_index: index,
                total_chunks: chunk_count,
                chunk_hash: hash,
                chunk_size: slice.len() as u32,
                data: slice.to_vec(),
            });
        }

        Ok((chunks, chunk_hashes))
    }

    /// Reassembles a complete package bundle from an ordered or unordered set of chunks,
    /// validating that all chunks are present, their hashes match, and the resulting package
    /// matches `expected_package_hash`.
    pub fn assemble_chunks(
        expected_package_hash: &ContentId,
        chunks: &[PackageTransferChunk],
    ) -> Result<Self, ApplicationError> {
        if chunks.is_empty() {
            return Err(ApplicationError::IntegrityError(
                "Cannot assemble package bundle from empty chunk list".to_string(),
            ));
        }

        let total_chunks = chunks[0].total_chunks;
        if chunks.len() != total_chunks as usize {
            return Err(ApplicationError::IntegrityError(format!(
                "Incomplete chunks for package assembly: got {}, expected {}",
                chunks.len(),
                total_chunks
            )));
        }

        let mut sorted = chunks.to_vec();
        sorted.sort_by_key(|c| c.chunk_index);

        for (i, chunk) in sorted.iter().enumerate() {
            if chunk.chunk_index != i as u32 {
                return Err(ApplicationError::IntegrityError(format!(
                    "Missing chunk index {} during package assembly",
                    i
                )));
            }
            chunk.verify_chunk_hash()?;
        }

        let mut bundle_bytes = Vec::new();
        for chunk in &sorted {
            bundle_bytes.extend_from_slice(&chunk.data);
        }

        let bundle = Self::from_bytes(&bundle_bytes)?;
        let computed = bundle.package.compute_package_hash();
        if &computed != expected_package_hash {
            return Err(ApplicationError::IntegrityError(format!(
                "Assembled package hash mismatch: expected {}, got {}",
                expected_package_hash, computed
            )));
        }

        Ok(bundle)
    }
}
