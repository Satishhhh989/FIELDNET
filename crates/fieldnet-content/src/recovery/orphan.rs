use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::fs;
use tracing::info;

use crate::chunks::id::ChunkId;
use crate::chunks::store::ChunkStore;
use crate::error::ContentError;

/// Classification of detected orphaned or anomalous storage items.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrphanClassification {
    /// Object or chunk can be automatically reconstructed or linked.
    Recoverable,
    /// Unreferenced chunk or temporary file with no owner manifest.
    Orphaned,
    /// Missing or structurally invalid data.
    Corrupted,
    /// Ambiguous classification; quarantined for safety.
    Unknown,
}

/// An identified anomalous or orphaned storage record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrphanItem {
    pub id: String,
    pub item_type: String,
    pub classification: OrphanClassification,
    pub path: Option<String>,
    pub reason: String,
}

/// Service that detects orphaned chunks, broken manifests, and unreferenced staging buffers.
pub struct OrphanDetector {
    conn: Arc<Mutex<Connection>>,
    chunk_store: Arc<ChunkStore>,
    objects_dir: PathBuf,
    staging_dir: PathBuf,
    quarantine_dir: PathBuf,
}

impl OrphanDetector {
    pub fn new(
        base_dir: impl AsRef<Path>,
        conn: Arc<Mutex<Connection>>,
        chunk_store: Arc<ChunkStore>,
    ) -> Self {
        let base_dir = base_dir.as_ref();
        Self {
            conn,
            chunk_store,
            objects_dir: base_dir.join("objects"),
            staging_dir: base_dir.join("staging"),
            quarantine_dir: base_dir.join("quarantine"),
        }
    }

    /// Scans the database and filesystem to detect orphaned or inconsistent items.
    pub async fn detect_orphans(&self) -> Result<Vec<OrphanItem>, ContentError> {
        let mut orphans = Vec::new();

        // 1. Gather all chunk IDs referenced across all manifests
        let referenced_chunk_hexes: HashSet<String> = {
            let conn = self
                .conn
                .lock()
                .map_err(|e| ContentError::LockError(e.to_string()))?;

            let mut stmt = conn.prepare("SELECT manifest_json FROM manifest_index")?;
            let mut rows = stmt.query([])?;
            let mut set = HashSet::new();

            while let Ok(Some(row)) = rows.next() {
                let json: String = row.get(0)?;
                if let Ok(manifest) =
                    serde_json::from_str::<crate::manifest::model::ContentManifest>(&json)
                {
                    for desc in manifest.chunks {
                        set.insert(desc.chunk_id.to_hex());
                    }
                }
            }
            set
        };

        // 2. Scan chunk_index for chunks not referenced by any manifest
        {
            let conn = self
                .conn
                .lock()
                .map_err(|e| ContentError::LockError(e.to_string()))?;

            let mut stmt = conn.prepare("SELECT chunk_id, storage_path FROM chunk_index")?;
            let rows = stmt.query_map([], |row| {
                let id: String = row.get(0)?;
                let path: String = row.get(1)?;
                Ok((id, path))
            })?;

            for (chunk_hex, path) in rows.flatten() {
                if !referenced_chunk_hexes.contains(&chunk_hex) {
                    orphans.push(OrphanItem {
                        id: chunk_hex,
                        item_type: "chunk".to_string(),
                        classification: OrphanClassification::Orphaned,
                        path: Some(path),
                        reason: "Chunk has no referencing manifest in local store".to_string(),
                    });
                }
            }
        }

        // 3. Scan content_index for objects whose physical file is missing
        {
            let conn = self
                .conn
                .lock()
                .map_err(|e| ContentError::LockError(e.to_string()))?;

            let mut stmt = conn.prepare(
                "SELECT content_id, storage_path, storage_state FROM content_index WHERE storage_state = 'COMPLETE'",
            )?;
            let rows = stmt.query_map([], |row| {
                let id: String = row.get(0)?;
                let path: String = row.get(1)?;
                let state: String = row.get(2)?;
                Ok((id, path, state))
            })?;

            for (cid_hex, rel_path, _state) in rows.flatten() {
                let full_path = self.objects_dir.join(&rel_path);
                if !full_path.exists() {
                    orphans.push(OrphanItem {
                        id: cid_hex,
                        item_type: "content".to_string(),
                        classification: OrphanClassification::Corrupted,
                        path: Some(rel_path),
                        reason: "Physical object file missing from CAS objects directory"
                            .to_string(),
                    });
                }
            }
        }

        // 4. Scan staging directory for unlinked .part files
        if self.staging_dir.exists() {
            if let Ok(mut entries) = fs::read_dir(&self.staging_dir).await {
                while let Ok(Some(entry)) = entries.next_entry().await {
                    let path = entry.path();
                    if path.extension().and_then(|e| e.to_str()) == Some("part") {
                        orphans.push(OrphanItem {
                            id: path
                                .file_name()
                                .unwrap_or_default()
                                .to_string_lossy()
                                .to_string(),
                            item_type: "staging".to_string(),
                            classification: OrphanClassification::Orphaned,
                            path: Some(path.to_string_lossy().to_string()),
                            reason: "Orphaned ingestion staging buffer on disk".to_string(),
                        });
                    }
                }
            }
        }

        Ok(orphans)
    }

    /// Safely quarantines detected orphaned chunks into quarantine/ directory.
    pub async fn quarantine_orphaned_chunks(
        &self,
        orphans: &[OrphanItem],
    ) -> Result<usize, ContentError> {
        let mut quarantined = 0;
        let _ = fs::create_dir_all(&self.quarantine_dir).await;

        for item in orphans {
            if item.item_type == "chunk" && item.classification == OrphanClassification::Orphaned {
                if let Ok(cid) = ChunkId::from_hex(&item.id) {
                    let rel_path = cid.relative_storage_path();
                    let src_path = self.chunk_store.objects_dir().join(rel_path);
                    if src_path.exists() {
                        let dest_path = self.quarantine_dir.join(format!("{}.orphaned", item.id));
                        if fs::rename(&src_path, &dest_path).await.is_ok() {
                            quarantined += 1;
                        }
                    }
                    // Remove from chunk_index
                    let conn = self
                        .conn
                        .lock()
                        .map_err(|e| ContentError::LockError(e.to_string()))?;
                    let _ = conn.execute(
                        "DELETE FROM chunk_index WHERE chunk_id = ?1",
                        [item.id.as_str()],
                    );
                }
            }
        }

        if quarantined > 0 {
            info!(count = quarantined, "Quarantined orphaned chunks safely");
        }

        Ok(quarantined)
    }
}
