use rusqlite::{params, Connection, OptionalExtension};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::fs;
use tokio::io::AsyncRead;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use fieldnet_core::Timestamp;

use crate::chunks::id::ChunkId;
use crate::error::ContentError;
use crate::events::ContentEvent;
use crate::hashing::{hash_and_copy_stream, hash_bytes};
use crate::store::PutResult;

/// Storage verification state for a content chunk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChunkStorageState {
    Staging,
    Verified,
    Corrupted,
}

impl ChunkStorageState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Staging => "STAGING",
            Self::Verified => "VERIFIED",
            Self::Corrupted => "CORRUPTED",
        }
    }
}

/// Metadata record for a stored content chunk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkMetadata {
    pub chunk_id: ChunkId,
    pub size_bytes: u32,
    pub storage_state: ChunkStorageState,
    pub created_at: Timestamp,
    pub last_accessed_at: Timestamp,
    pub ref_count: u32,
}

/// Persistent, crash-safe, content-addressed store for individual content chunks.
pub struct ChunkStore {
    base_dir: PathBuf,
    objects_dir: PathBuf,
    staging_dir: PathBuf,
    conn: Arc<Mutex<Connection>>,
    commit_lock: tokio::sync::Mutex<()>,
    event_tx: broadcast::Sender<ContentEvent>,
}

impl ChunkStore {
    /// Opens or initializes a persistent `ChunkStore` rooted at `base_dir`.
    pub fn open(
        base_dir: impl AsRef<Path>,
        conn: Arc<Mutex<Connection>>,
        event_tx: broadcast::Sender<ContentEvent>,
    ) -> Result<Arc<Self>, ContentError> {
        let base_dir = base_dir.as_ref().to_path_buf();
        let objects_dir = base_dir.join("objects");
        let staging_dir = base_dir.join("staging");

        std::fs::create_dir_all(&objects_dir).map_err(|e| {
            ContentError::StorageFailure(format!("Failed to create chunk objects directory: {}", e))
        })?;
        std::fs::create_dir_all(&staging_dir).map_err(|e| {
            ContentError::StorageFailure(format!("Failed to create chunk staging directory: {}", e))
        })?;

        {
            let conn_guard = conn
                .lock()
                .map_err(|e| ContentError::LockError(e.to_string()))?;
            conn_guard.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS chunk_index (
                    chunk_id TEXT PRIMARY KEY,
                    size_bytes INTEGER NOT NULL,
                    storage_path TEXT NOT NULL,
                    storage_state TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    last_accessed_at INTEGER NOT NULL,
                    ref_count INTEGER NOT NULL DEFAULT 1
                );
                CREATE INDEX IF NOT EXISTS idx_chunk_state ON chunk_index(storage_state);
                "#,
            )?;
        }

        let store = Arc::new(Self {
            base_dir,
            objects_dir,
            staging_dir,
            conn,
            commit_lock: tokio::sync::Mutex::new(()),
            event_tx,
        });

        let cleaned = store.cleanup_orphaned_staging()?;
        if cleaned > 0 {
            info!(
                "Cleaned up {} orphaned chunk staging files on startup",
                cleaned
            );
        }

        Ok(store)
    }

    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    pub fn objects_dir(&self) -> &Path {
        &self.objects_dir
    }

    pub fn staging_dir(&self) -> &Path {
        &self.staging_dir
    }

    /// Stores in-memory chunk bytes into the chunk store, deduplicating identical chunks.
    pub async fn put(&self, data: &[u8]) -> Result<(ChunkId, PutResult), ContentError> {
        let mut cursor = std::io::Cursor::new(data);
        self.put_stream(&mut cursor, None).await
    }

    /// Streams chunk bytes into staging, computes SHA-256, verifies hash, and commits atomically.
    pub async fn put_stream<R: AsyncRead + Unpin>(
        &self,
        reader: &mut R,
        expected_id: Option<ChunkId>,
    ) -> Result<(ChunkId, PutResult), ContentError> {
        let part_filename = format!(
            "{}_{}.part",
            Timestamp::now().as_millis(),
            rand::random::<u64>()
        );
        let staging_path = self.staging_dir.join(&part_filename);

        let mut part_file = fs::File::create(&staging_path).await.map_err(|e| {
            ContentError::StorageFailure(format!("Failed to create chunk staging file: {}", e))
        })?;

        let (computed_content_id, total_bytes) =
            hash_and_copy_stream(reader, &mut part_file).await?;
        let computed_chunk_id = ChunkId::from_bytes(computed_content_id.to_bytes());

        // Verify against expected ChunkId if provided
        if let Some(expected) = expected_id {
            if expected != computed_chunk_id {
                let _ = std::fs::remove_file(&staging_path);
                let _ = self.event_tx.send(ContentEvent::ChunkRejected {
                    chunk_id: expected,
                    reason: "Hash mismatch against expected chunk identity".into(),
                });
                return Err(ContentError::ChunkIntegrityFailure {
                    expected,
                    actual: computed_chunk_id,
                });
            }
        }

        part_file.sync_all().await.map_err(|e| {
            let _ = std::fs::remove_file(&staging_path);
            ContentError::StorageFailure(format!("Failed to sync chunk staging file: {}", e))
        })?;
        drop(part_file);

        let relative_obj_path = computed_chunk_id.relative_storage_path();
        let dest_path = self.objects_dir.join(&relative_obj_path);

        let _commit_guard = self.commit_lock.lock().await;

        // Check if chunk already exists
        let already_exists = {
            let conn = self
                .conn
                .lock()
                .map_err(|e| ContentError::LockError(e.to_string()))?;
            let existing: Option<(String, String, i64)> = conn
                .query_row(
                    "SELECT storage_path, storage_state, size_bytes FROM chunk_index WHERE chunk_id = ?1",
                    params![computed_chunk_id.to_hex()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()?;

            if let Some((stored_path, state_str, size_bytes)) = existing {
                let existing_file_path = self.objects_dir.join(&stored_path);
                if state_str == ChunkStorageState::Verified.as_str() && existing_file_path.exists()
                {
                    // Increment reference count for deduplicated chunk
                    let _ = conn.execute(
                        "UPDATE chunk_index SET ref_count = ref_count + 1, last_accessed_at = ?2 WHERE chunk_id = ?1",
                        params![computed_chunk_id.to_hex(), Timestamp::now().as_millis() as i64],
                    );
                    Some(size_bytes as u32)
                } else {
                    None
                }
            } else {
                None
            }
        };

        if let Some(existing_size) = already_exists {
            let _ = std::fs::remove_file(&staging_path);
            let meta = crate::metadata::ContentMetadata::new_complete(
                computed_content_id,
                existing_size as u64,
                None,
                None,
            );
            return Ok((computed_chunk_id, PutResult::AlreadyExists(meta)));
        }

        // Commit new chunk
        if let Some(parent) = dest_path.parent() {
            fs::create_dir_all(parent).await.map_err(|e| {
                let _ = std::fs::remove_file(&staging_path);
                ContentError::StorageFailure(format!("Failed to create chunk shard dir: {}", e))
            })?;
        }

        fs::rename(&staging_path, &dest_path).await.map_err(|e| {
            let _ = std::fs::remove_file(&staging_path);
            ContentError::StorageFailure(format!("Failed to commit staging chunk to CAS: {}", e))
        })?;

        let now = Timestamp::now();
        let relative_str = relative_obj_path.to_string_lossy().to_string();

        {
            let conn = self
                .conn
                .lock()
                .map_err(|e| ContentError::LockError(e.to_string()))?;
            conn.execute(
                r#"
                INSERT INTO chunk_index (
                    chunk_id, size_bytes, storage_path, storage_state,
                    created_at, last_accessed_at, ref_count
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1)
                ON CONFLICT(chunk_id) DO UPDATE SET
                    storage_state = excluded.storage_state,
                    last_accessed_at = excluded.last_accessed_at
                "#,
                params![
                    computed_chunk_id.to_hex(),
                    total_bytes as i64,
                    relative_str,
                    ChunkStorageState::Verified.as_str(),
                    now.as_millis() as i64,
                    now.as_millis() as i64,
                ],
            )?;
        }

        let _ = self.event_tx.send(ContentEvent::ChunkCreated {
            chunk_id: computed_chunk_id,
            size_bytes: total_bytes as u32,
        });
        let _ = self.event_tx.send(ContentEvent::ChunkVerified {
            chunk_id: computed_chunk_id,
            size_bytes: total_bytes as u32,
        });

        let meta = crate::metadata::ContentMetadata::new_complete(
            computed_content_id,
            total_bytes,
            None,
            None,
        );
        Ok((computed_chunk_id, PutResult::Created(meta)))
    }

    /// Checks if a verified chunk exists in the store.
    pub fn has(&self, chunk_id: &ChunkId) -> Result<bool, ContentError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;
        let res: Option<(String, String)> = conn
            .query_row(
                "SELECT storage_path, storage_state FROM chunk_index WHERE chunk_id = ?1",
                params![chunk_id.to_hex()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;

        match res {
            Some((path_str, state_str)) => {
                if state_str == ChunkStorageState::Verified.as_str() {
                    let obj_path = self.objects_dir.join(path_str);
                    Ok(obj_path.exists())
                } else {
                    Ok(false)
                }
            }
            None => Ok(false),
        }
    }

    /// Retrieves raw bytes of a stored chunk.
    pub async fn get(&self, chunk_id: &ChunkId) -> Result<Vec<u8>, ContentError> {
        let obj_path = {
            let conn = self
                .conn
                .lock()
                .map_err(|e| ContentError::LockError(e.to_string()))?;
            let (rel_path, state): (String, String) = conn
                .query_row(
                    "SELECT storage_path, storage_state FROM chunk_index WHERE chunk_id = ?1",
                    params![chunk_id.to_hex()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?
                .ok_or(ContentError::ChunkNotFound(*chunk_id))?;

            if state == ChunkStorageState::Corrupted.as_str() {
                return Err(ContentError::ChunkCorrupted {
                    id: *chunk_id,
                    reason: "Chunk marked corrupted in index".to_string(),
                });
            }

            if state != ChunkStorageState::Verified.as_str() {
                return Err(ContentError::ChunkNotFound(*chunk_id));
            }

            self.objects_dir.join(rel_path)
        };

        if !obj_path.exists() {
            self.mark_corrupted(chunk_id, "Physical chunk file missing from disk")?;
            return Err(ContentError::ChunkCorrupted {
                id: *chunk_id,
                reason: "Physical chunk file missing from disk".to_string(),
            });
        }

        let bytes = fs::read(&obj_path)
            .await
            .map_err(|e| ContentError::IOError(format!("Failed to read chunk file: {}", e)))?;

        self.touch_access(chunk_id)?;
        Ok(bytes)
    }

    /// Retrieves chunk bytes and validates its SHA-256 integrity against the `ChunkId`.
    pub async fn get_verified(&self, chunk_id: &ChunkId) -> Result<Vec<u8>, ContentError> {
        let bytes = self.get(chunk_id).await?;
        let computed = ChunkId::from_bytes(hash_bytes(&bytes).to_bytes());

        if computed != *chunk_id {
            self.mark_corrupted(chunk_id, "Cryptographic hash mismatch during verification")?;
            return Err(ContentError::ChunkIntegrityFailure {
                expected: *chunk_id,
                actual: computed,
            });
        }

        Ok(bytes)
    }

    /// Queries chunk metadata.
    pub fn stat(&self, chunk_id: &ChunkId) -> Result<ChunkMetadata, ContentError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;
        let row = conn
            .query_row(
                "SELECT size_bytes, storage_state, created_at, last_accessed_at, ref_count FROM chunk_index WHERE chunk_id = ?1",
                params![chunk_id.to_hex()],
                |row| {
                    let size_bytes: i64 = row.get(0)?;
                    let state_str: String = row.get(1)?;
                    let created_at: i64 = row.get(2)?;
                    let last_accessed_at: i64 = row.get(3)?;
                    let ref_count: i64 = row.get(4)?;
                    Ok((size_bytes, state_str, created_at, last_accessed_at, ref_count))
                },
            )
            .optional()?
            .ok_or(ContentError::ChunkNotFound(*chunk_id))?;

        let (size_bytes, state_str, created_at, last_accessed_at, ref_count) = row;
        let storage_state = match state_str.as_str() {
            "VERIFIED" => ChunkStorageState::Verified,
            "CORRUPTED" => ChunkStorageState::Corrupted,
            _ => ChunkStorageState::Staging,
        };

        Ok(ChunkMetadata {
            chunk_id: *chunk_id,
            size_bytes: size_bytes as u32,
            storage_state,
            created_at: Timestamp::from_millis(created_at as u64),
            last_accessed_at: Timestamp::from_millis(last_accessed_at as u64),
            ref_count: ref_count as u32,
        })
    }

    /// Deletes a chunk from local storage.
    pub async fn delete(&self, chunk_id: &ChunkId) -> Result<bool, ContentError> {
        let obj_path = {
            let conn = self
                .conn
                .lock()
                .map_err(|e| ContentError::LockError(e.to_string()))?;
            let rel_path: Option<String> = conn
                .query_row(
                    "SELECT storage_path FROM chunk_index WHERE chunk_id = ?1",
                    params![chunk_id.to_hex()],
                    |row| row.get(0),
                )
                .optional()?;

            match rel_path {
                Some(path) => {
                    conn.execute(
                        "DELETE FROM chunk_index WHERE chunk_id = ?1",
                        params![chunk_id.to_hex()],
                    )?;
                    self.objects_dir.join(path)
                }
                None => return Err(ContentError::ChunkNotFound(*chunk_id)),
            }
        };

        if obj_path.exists() {
            let _ = fs::remove_file(&obj_path).await;
        }

        Ok(true)
    }

    /// Lists all verified chunk IDs currently stored.
    pub fn list(&self) -> Result<Vec<ChunkId>, ContentError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;
        let mut stmt =
            conn.prepare("SELECT chunk_id FROM chunk_index WHERE storage_state = 'VERIFIED'")?;
        let rows = stmt.query_map([], |row| {
            let hex_str: String = row.get(0)?;
            Ok(hex_str)
        })?;

        let mut list = Vec::new();
        for r in rows {
            let hex_str = r?;
            if let Ok(cid) = ChunkId::from_hex(&hex_str) {
                list.push(cid);
            }
        }
        Ok(list)
    }

    /// Verifies the physical integrity of a stored chunk against its `ChunkId`.
    pub fn verify_chunk(&self, chunk_id: &ChunkId) -> Result<bool, ContentError> {
        let (rel_path, state_str) = {
            let conn = self
                .conn
                .lock()
                .map_err(|e| ContentError::LockError(e.to_string()))?;
            conn.query_row(
                "SELECT storage_path, storage_state FROM chunk_index WHERE chunk_id = ?1",
                params![chunk_id.to_hex()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
            .ok_or(ContentError::ChunkNotFound(*chunk_id))?
        };

        if state_str == "CORRUPTED" {
            return Ok(false);
        }

        let obj_path = self.objects_dir.join(rel_path);
        if !obj_path.exists() {
            self.mark_corrupted(chunk_id, "Physical chunk file missing")?;
            return Ok(false);
        }

        let mut file = std::fs::File::open(&obj_path)?;
        let computed = ChunkId::from_bytes(crate::hashing::hash_reader(&mut file)?.to_bytes());

        if computed != *chunk_id {
            self.mark_corrupted(chunk_id, "Cryptographic hash mismatch")?;
            return Ok(false);
        }

        Ok(true)
    }

    pub fn count(&self) -> Result<usize, ContentError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;
        let count: i64 = conn.query_row(
            "SELECT count(*) FROM chunk_index WHERE storage_state = 'VERIFIED'",
            [],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    pub fn total_size_bytes(&self) -> Result<u64, ContentError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;
        let total: Option<i64> = conn.query_row(
            "SELECT sum(size_bytes) FROM chunk_index WHERE storage_state = 'VERIFIED'",
            [],
            |row| row.get(0),
        )?;
        Ok(total.unwrap_or(0) as u64)
    }

    pub fn cleanup_orphaned_staging(&self) -> Result<usize, ContentError> {
        let mut count = 0;
        if self.staging_dir.exists() {
            for entry in std::fs::read_dir(&self.staging_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("part") {
                    debug!("Removing orphaned chunk staging file: {}", path.display());
                    let _ = std::fs::remove_file(path);
                    count += 1;
                }
            }
        }
        Ok(count)
    }

    fn touch_access(&self, chunk_id: &ChunkId) -> Result<(), ContentError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;
        let _ = conn.execute(
            "UPDATE chunk_index SET last_accessed_at = ?2 WHERE chunk_id = ?1",
            params![chunk_id.to_hex(), Timestamp::now().as_millis() as i64],
        );
        Ok(())
    }

    fn mark_corrupted(&self, chunk_id: &ChunkId, reason: &str) -> Result<(), ContentError> {
        warn!(chunk_id = %chunk_id, reason = reason, "Marking chunk as CORRUPTED");
        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;
        let _ = conn.execute(
            "UPDATE chunk_index SET storage_state = 'CORRUPTED' WHERE chunk_id = ?1",
            params![chunk_id.to_hex()],
        );
        let _ = self.event_tx.send(ContentEvent::ChunkRejected {
            chunk_id: *chunk_id,
            reason: reason.to_string(),
        });
        Ok(())
    }
}
