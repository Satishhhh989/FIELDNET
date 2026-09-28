use rusqlite::{params, Connection};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::fs;
use tokio::sync::broadcast;
use tracing::{info, warn};

use super::verifier::ContentVerifier;
use crate::chunks::id::ChunkId;
use crate::chunks::store::{ChunkStorageState, ChunkStore};
use crate::error::ContentError;
use crate::events::ContentEvent;
use crate::identity::ContentId;
use crate::manifest::store::ManifestStore;
use crate::metadata::ContentStorageState;
use fieldnet_core::{NodeId, Timestamp};

/// Engine that handles content corruption detection, quarantining, and restoration.
pub struct CorruptionRecoveryEngine {
    quarantine_dir: PathBuf,
    objects_dir: PathBuf,
    conn: Arc<Mutex<Connection>>,
    chunk_store: Arc<ChunkStore>,
    manifest_store: Arc<ManifestStore>,
    event_tx: broadcast::Sender<ContentEvent>,
}

impl CorruptionRecoveryEngine {
    pub fn new(
        base_dir: impl AsRef<Path>,
        conn: Arc<Mutex<Connection>>,
        chunk_store: Arc<ChunkStore>,
        manifest_store: Arc<ManifestStore>,
        event_tx: broadcast::Sender<ContentEvent>,
    ) -> Result<Self, ContentError> {
        let base_dir = base_dir.as_ref();
        let quarantine_dir = base_dir.join("quarantine");
        let objects_dir = base_dir.join("objects");

        std::fs::create_dir_all(&quarantine_dir).map_err(|e| {
            ContentError::StorageFailure(format!("Failed to create quarantine dir: {}", e))
        })?;

        Ok(Self {
            quarantine_dir,
            objects_dir,
            conn,
            chunk_store,
            manifest_store,
            event_tx,
        })
    }

    pub fn quarantine_dir(&self) -> &Path {
        &self.quarantine_dir
    }

    /// Quarantines a corrupted chunk file, moving it to quarantine/ and marking it corrupted in index.
    pub async fn quarantine_chunk(
        &self,
        chunk_id: &ChunkId,
        content_id: Option<ContentId>,
        reason: &str,
    ) -> Result<(), ContentError> {
        let rel_path = chunk_id.relative_storage_path();
        let src_path = self.chunk_store.objects_dir().join(&rel_path);

        if src_path.exists() {
            let timestamp = Timestamp::now().as_millis();
            let dest_name = format!("{}.{}.bad", chunk_id, timestamp);
            let dest_path = self.quarantine_dir.join(dest_name);
            let _ = fs::rename(&src_path, &dest_path).await;
            warn!(
                chunk_id = %chunk_id,
                quarantined_to = %dest_path.display(),
                reason = %reason,
                "Quarantined corrupted chunk file"
            );
        }

        // Mark corrupted in chunk index
        {
            let conn = self
                .conn
                .lock()
                .map_err(|e| ContentError::LockError(e.to_string()))?;
            let _ = conn.execute(
                "UPDATE chunk_index SET storage_state = ?2 WHERE chunk_id = ?1",
                params![chunk_id.to_hex(), ChunkStorageState::Corrupted.as_str()],
            );
        }

        if let Some(cid) = content_id {
            let _ = self.event_tx.send(ContentEvent::CorruptionDetected {
                content_id: cid,
                chunk_id: Some(*chunk_id),
                reason: reason.to_string(),
            });
        }

        Ok(())
    }

    /// Restores a corrupted chunk by writing and verifying replacement bytes.
    pub async fn restore_chunk(
        &self,
        chunk_id: &ChunkId,
        bytes: &[u8],
        content_id: Option<ContentId>,
        provider: Option<NodeId>,
    ) -> Result<(), ContentError> {
        // 1. Verify replacement bytes match expected ChunkId
        ContentVerifier::verify_chunk_bytes(chunk_id, bytes)?;

        // 2. Commit replacement to ChunkStore
        self.chunk_store.put(bytes).await?;

        info!(chunk_id = %chunk_id, "Corrupted chunk successfully restored and committed");

        if let Some(cid) = content_id {
            let _ = self.event_tx.send(ContentEvent::CorruptionRecovered {
                content_id: cid,
                chunk_id: Some(*chunk_id),
                provider,
            });
        }

        Ok(())
    }

    /// Re-evaluates content after chunks are restored; reconstructs physical object and marks COMPLETE.
    pub async fn restore_content(&self, content_id: &ContentId) -> Result<(), ContentError> {
        let manifest = self.manifest_store.get_by_content_id(content_id)?;

        // Ensure all constituent chunks are present and verified
        for desc in &manifest.chunks {
            ContentVerifier::verify_stored_chunk(&desc.chunk_id, &self.chunk_store).await?;
        }

        // Reconstruct physical file
        let rel_path = content_id.relative_storage_path();
        let dest_path = self.objects_dir.join(rel_path);
        if let Some(parent) = dest_path.parent() {
            fs::create_dir_all(parent).await.map_err(|e| {
                ContentError::StorageFailure(format!("Failed to create object parent dir: {}", e))
            })?;
        }

        let mut file = fs::File::create(&dest_path).await.map_err(|e| {
            ContentError::StorageFailure(format!("Failed to create destination file: {}", e))
        })?;

        crate::reconstruction::ContentReconstructor::reconstruct_to_stream(
            &manifest,
            &self.chunk_store,
            &mut file,
            Some(&self.event_tx),
        )
        .await?;

        use tokio::io::AsyncWriteExt;
        file.flush().await.map_err(|e| {
            ContentError::StorageFailure(format!("Failed to flush reconstructed file: {}", e))
        })?;

        // Update database state to complete
        {
            let conn = self
                .conn
                .lock()
                .map_err(|e| ContentError::LockError(e.to_string()))?;
            conn.execute(
                "UPDATE content_index SET storage_state = ?2 WHERE content_id = ?1",
                params![content_id.to_hex(), ContentStorageState::Complete.as_str()],
            )?;
        }

        let _ = self.event_tx.send(ContentEvent::CorruptionRecovered {
            content_id: *content_id,
            chunk_id: None,
            provider: None,
        });

        let _ = self.event_tx.send(ContentEvent::Available {
            id: *content_id,
            size_bytes: manifest.total_size,
        });

        info!(content_id = %content_id, "Corrupted content fully restored and marked AVAILABLE");

        Ok(())
    }
}
