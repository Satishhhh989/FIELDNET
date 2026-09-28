use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::broadcast;
use tracing::{debug, error, info};

use crate::chunks::store::ChunkStore;
use crate::error::ContentError;
use crate::events::ContentEvent;
use crate::hashing::ContentHasher;
use crate::manifest::model::ContentManifest;
use crate::manifest::validator::ManifestValidator;

/// Engine for streaming reconstruction of complete content from constituent chunks and manifest.
pub struct ContentReconstructor;

impl ContentReconstructor {
    /// Reconstructs content from chunks defined in `manifest` directly into an `AsyncWrite` destination,
    /// streaming without holding the entire payload in memory.
    ///
    /// Verifies each chunk's cryptographic integrity and performs the authoritative final `ContentId`
    /// verification before completing.
    pub async fn reconstruct_to_stream<W: AsyncWrite + Unpin>(
        manifest: &ContentManifest,
        chunk_store: &ChunkStore,
        writer: &mut W,
        event_tx: Option<&broadcast::Sender<ContentEvent>>,
    ) -> Result<u64, ContentError> {
        ManifestValidator::validate(manifest)?;

        if let Some(tx) = event_tx {
            let _ = tx.send(ContentEvent::ReconstructionStarted {
                content_id: manifest.content_id,
                manifest_id: manifest.manifest_id,
            });
        }

        info!(
            content_id = %manifest.content_id,
            manifest_id = %manifest.manifest_id,
            chunk_count = manifest.chunk_count,
            "Beginning streaming content reconstruction"
        );

        let mut overall_hasher = ContentHasher::new();
        let mut total_bytes_written: u64 = 0;

        for chunk_desc in &manifest.chunks {
            // Check if chunk exists in ChunkStore
            if !chunk_store.has(&chunk_desc.chunk_id)? {
                let err = ContentError::MissingChunk {
                    index: chunk_desc.index,
                    chunk_id: chunk_desc.chunk_id,
                };
                if let Some(tx) = event_tx {
                    let _ = tx.send(ContentEvent::ReconstructionFailed {
                        content_id: manifest.content_id,
                        reason: format!(
                            "Missing chunk index {}: {}",
                            chunk_desc.index, chunk_desc.chunk_id
                        ),
                    });
                }
                return Err(err);
            }

            // Retrieve and cryptographically verify chunk bytes
            let chunk_bytes = chunk_store.get_verified(&chunk_desc.chunk_id).await?;

            if chunk_bytes.len() != chunk_desc.size as usize {
                let err = ContentError::ChunkCorrupted {
                    id: chunk_desc.chunk_id,
                    reason: format!(
                        "Chunk size mismatch: expected {} bytes, read {}",
                        chunk_desc.size,
                        chunk_bytes.len()
                    ),
                };
                if let Some(tx) = event_tx {
                    let _ = tx.send(ContentEvent::ReconstructionFailed {
                        content_id: manifest.content_id,
                        reason: err.to_string(),
                    });
                }
                return Err(err);
            }

            overall_hasher.update(&chunk_bytes);
            writer.write_all(&chunk_bytes).await.map_err(|e| {
                ContentError::IOError(format!(
                    "Write failure during content reconstruction: {}",
                    e
                ))
            })?;

            total_bytes_written += chunk_bytes.len() as u64;
            debug!(
                chunk_index = chunk_desc.index,
                chunk_id = %chunk_desc.chunk_id,
                bytes = chunk_bytes.len(),
                "Streamed chunk into reconstruction pipeline"
            );
        }

        writer.flush().await.map_err(|e| {
            ContentError::IOError(format!(
                "Flush failure during content reconstruction: {}",
                e
            ))
        })?;

        // Authoritative final ContentId cryptographic verification
        let computed_content_id = overall_hasher.finalize();
        if computed_content_id != manifest.content_id {
            error!(
                expected = %manifest.content_id,
                computed = %computed_content_id,
                "Cryptographic integrity verification failed on reconstructed content"
            );
            if let Some(tx) = event_tx {
                let _ = tx.send(ContentEvent::ReconstructionFailed {
                    content_id: manifest.content_id,
                    reason: format!(
                        "Final hash mismatch: expected {}, got {}",
                        manifest.content_id, computed_content_id
                    ),
                });
            }
            return Err(ContentError::IntegrityFailure {
                expected: manifest.content_id,
                actual: computed_content_id,
            });
        }

        if total_bytes_written != manifest.total_size {
            let err = ContentError::ReconstructionFailed(format!(
                "Total bytes reconstructed ({}) does not match manifest total_size ({})",
                total_bytes_written, manifest.total_size
            ));
            if let Some(tx) = event_tx {
                let _ = tx.send(ContentEvent::ReconstructionFailed {
                    content_id: manifest.content_id,
                    reason: err.to_string(),
                });
            }
            return Err(err);
        }

        if let Some(tx) = event_tx {
            let _ = tx.send(ContentEvent::ReconstructionCompleted {
                content_id: manifest.content_id,
                manifest_id: manifest.manifest_id,
                total_bytes: total_bytes_written,
            });
        }

        info!(
            content_id = %manifest.content_id,
            total_bytes = total_bytes_written,
            "Content reconstruction successfully verified and completed"
        );

        Ok(total_bytes_written)
    }

    /// Reconstructs content from chunks defined in `manifest` into an in-memory `Vec<u8>`.
    pub async fn reconstruct_to_bytes(
        manifest: &ContentManifest,
        chunk_store: &ChunkStore,
        event_tx: Option<&broadcast::Sender<ContentEvent>>,
    ) -> Result<Vec<u8>, ContentError> {
        let mut buffer = Vec::with_capacity(manifest.total_size as usize);
        Self::reconstruct_to_stream(manifest, chunk_store, &mut buffer, event_tx).await?;
        Ok(buffer)
    }
}
