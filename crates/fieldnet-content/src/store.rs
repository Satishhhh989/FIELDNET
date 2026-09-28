use rusqlite::{params, Connection, OptionalExtension};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::fs;
use tokio::io::AsyncRead;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, info, warn};

use fieldnet_core::{NodeId, Timestamp};

use crate::error::ContentError;
use crate::events::ContentEvent;
use crate::hashing::{hash_and_copy_stream, hash_bytes};
use crate::identity::ContentId;
use crate::metadata::{ContentMetadata, ContentStorageState};

/// Configuration options for inserting content into the store.
#[derive(Clone, Debug, Default)]
pub struct PutOptions {
    /// Optional MIME type descriptor.
    pub mime_type: Option<String>,
    /// Optional Node ID of the creator node.
    pub creator_node_id: Option<NodeId>,
    /// Whether the inserted content should be immediately pinned against eviction/deletion.
    pub is_pinned: bool,
    /// Custom metadata key-value pairs.
    pub custom_metadata: HashMap<String, String>,
}

/// The result of an insertion operation in the content store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PutResult {
    /// Fresh content was written and committed.
    Created(ContentMetadata),
    /// Content with identical ContentId was already present (deduplication applied).
    AlreadyExists(ContentMetadata),
}

impl PutResult {
    /// Returns the associated content metadata.
    pub fn metadata(&self) -> &ContentMetadata {
        match self {
            Self::Created(m) | Self::AlreadyExists(m) => m,
        }
    }

    /// Returns the ContentId of the inserted or deduplicated content.
    pub fn content_id(&self) -> ContentId {
        self.metadata().content_id
    }

    /// Returns true if the content was freshly written.
    pub fn is_new(&self) -> bool {
        matches!(self, Self::Created(_))
    }
}

/// RAII lock guard protecting content from deletion while an active reader or transfer is using it.
pub struct ContentLock {
    content_id: ContentId,
    active_refs: Arc<RwLock<HashMap<ContentId, usize>>>,
}

impl Drop for ContentLock {
    fn drop(&mut self) {
        let content_id = self.content_id;
        let active_refs = Arc::clone(&self.active_refs);
        tokio::spawn(async move {
            let mut refs = active_refs.write().await;
            if let Some(count) = refs.get_mut(&content_id) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    refs.remove(&content_id);
                }
            }
        });
    }
}

/// Persistent, crash-safe, content-addressed storage engine for FIELDNET (Phase 07).
pub struct ContentStore {
    base_dir: PathBuf,
    objects_dir: PathBuf,
    staging_dir: PathBuf,
    conn: Arc<Mutex<Connection>>,
    active_refs: Arc<RwLock<HashMap<ContentId, usize>>>,
    commit_lock: tokio::sync::Mutex<()>,
    event_tx: broadcast::Sender<ContentEvent>,
    chunk_store: Arc<crate::chunks::store::ChunkStore>,
    manifest_store: Arc<crate::manifest::store::ManifestStore>,
    provider_registry: Arc<crate::providers::ProviderRegistry>,
    authorizer: Arc<crate::auth::ContentAuthorizer>,
    quota_manager: Arc<crate::quota::StorageQuotaManager>,
    gc: Arc<crate::gc::ContentGarbageCollector>,
    auditor: Arc<crate::integrity::IntegrityAuditor>,
    recovery_engine: Arc<crate::integrity::CorruptionRecoveryEngine>,
    crash_recovery: Arc<crate::recovery::CrashRecoveryEngine>,
}

impl ContentStore {
    /// Opens or initializes a persistent `ContentStore` rooted at `base_dir`.
    pub fn open(base_dir: impl AsRef<Path>) -> Result<Arc<Self>, ContentError> {
        let base_dir = base_dir.as_ref().to_path_buf();
        let objects_dir = base_dir.join("objects");
        let staging_dir = base_dir.join("staging");
        let db_path = base_dir.join("content.db");

        std::fs::create_dir_all(&objects_dir).map_err(|e| {
            ContentError::StorageFailure(format!("Failed to create objects directory: {}", e))
        })?;
        std::fs::create_dir_all(&staging_dir).map_err(|e| {
            ContentError::StorageFailure(format!("Failed to create staging directory: {}", e))
        })?;

        info!("Opening ContentStore database at {}", db_path.display());
        let conn = Connection::open(&db_path).map_err(|e| {
            ContentError::DatabaseError(format!("Failed to open content.db: {}", e))
        })?;

        // Configure robust SQLite PRAGMAs
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.pragma_update(None, "busy_timeout", 5000)?;

        // Initialize schema
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS content_index (
                content_id TEXT PRIMARY KEY,
                size_bytes INTEGER NOT NULL,
                storage_path TEXT NOT NULL,
                storage_state TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                last_accessed_at INTEGER NOT NULL,
                mime_type TEXT,
                version INTEGER NOT NULL DEFAULT 1,
                creator_node_id TEXT,
                is_pinned INTEGER NOT NULL DEFAULT 0,
                metadata_json TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_content_state ON content_index(storage_state);
            CREATE INDEX IF NOT EXISTS idx_content_pinned ON content_index(is_pinned);
            "#,
        )?;

        let (event_tx, _) = broadcast::channel(256);
        let shared_conn = Arc::new(Mutex::new(conn));

        let chunk_store = crate::chunks::store::ChunkStore::open(
            base_dir.join("chunks"),
            Arc::clone(&shared_conn),
            event_tx.clone(),
        )?;
        let manifest_store = crate::manifest::store::ManifestStore::open(
            Arc::clone(&shared_conn),
            event_tx.clone(),
        )?;
        let provider_registry = Arc::new(crate::providers::ProviderRegistry::with_persistence(
            crate::providers::ProviderRegistryConfig::default(),
            Arc::clone(&shared_conn),
            event_tx.clone(),
        )?);

        let authorizer = crate::auth::ContentAuthorizer::new(
            Arc::clone(&shared_conn),
            NodeId::default(),
            event_tx.clone(),
        )?;
        let quota_manager = Arc::new(crate::quota::StorageQuotaManager::new(
            crate::quota::QuotaConfig::default(),
            crate::quota::ResourceLimits::default(),
            event_tx.clone(),
        ));
        let gc = Arc::new(crate::gc::ContentGarbageCollector::new(
            &base_dir,
            Arc::clone(&shared_conn),
            Arc::clone(&chunk_store),
            Arc::clone(&manifest_store),
            None,
            event_tx.clone(),
        ));
        let auditor = Arc::new(crate::integrity::IntegrityAuditor::new(
            Arc::clone(&shared_conn),
            Arc::clone(&chunk_store),
            Arc::clone(&manifest_store),
            objects_dir.clone(),
            event_tx.clone(),
        ));
        let recovery_engine = Arc::new(crate::integrity::CorruptionRecoveryEngine::new(
            &base_dir,
            Arc::clone(&shared_conn),
            Arc::clone(&chunk_store),
            Arc::clone(&manifest_store),
            event_tx.clone(),
        )?);
        let crash_recovery = Arc::new(crate::recovery::CrashRecoveryEngine::new(
            &base_dir,
            Arc::clone(&shared_conn),
            Arc::clone(&chunk_store),
            Arc::clone(&manifest_store),
            event_tx.clone(),
        ));

        // Connect GC to quota manager
        let quota_clone = Arc::clone(&quota_manager);
        let gc_clone = Arc::clone(&gc);
        tokio::spawn(async move {
            quota_clone.set_gc(gc_clone).await;
        });

        // Sync initial storage usage from DB
        if let Ok(guard) = shared_conn.lock() {
            let _ = quota_manager.sync_usage_from_db(&guard);
        }

        let store = Arc::new(Self {
            base_dir,
            objects_dir,
            staging_dir,
            conn: shared_conn,
            active_refs: Arc::new(RwLock::new(HashMap::new())),
            commit_lock: tokio::sync::Mutex::new(()),
            event_tx,
            chunk_store,
            manifest_store,
            provider_registry,
            authorizer,
            quota_manager,
            gc,
            auditor,
            recovery_engine,
            crash_recovery,
        });

        // Clean up any stale staging artifacts from prior process interruption
        let cleaned = store.cleanup_orphaned_staging()?;
        if cleaned > 0 {
            info!("Cleaned up {} orphaned staging files on startup", cleaned);
        }

        Ok(store)
    }

    /// Returns the root base directory of the content store.
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    /// Returns the directory path where content objects are sharded and stored.
    pub fn objects_dir(&self) -> &Path {
        &self.objects_dir
    }

    /// Returns the staging directory path where incoming temporary writes are staged.
    pub fn staging_dir(&self) -> &Path {
        &self.staging_dir
    }

    /// Subscribes to the content lifecycle event bus.
    pub fn subscribe_events(&self) -> broadcast::Receiver<ContentEvent> {
        self.event_tx.subscribe()
    }

    /// Returns the ContentAuthorizer.
    pub fn authorizer(&self) -> &Arc<crate::auth::ContentAuthorizer> {
        &self.authorizer
    }

    /// Returns the StorageQuotaManager.
    pub fn quota_manager(&self) -> &Arc<crate::quota::StorageQuotaManager> {
        &self.quota_manager
    }

    /// Returns the ContentGarbageCollector.
    pub fn gc(&self) -> &Arc<crate::gc::ContentGarbageCollector> {
        &self.gc
    }

    /// Returns the IntegrityAuditor.
    pub fn auditor(&self) -> &Arc<crate::integrity::IntegrityAuditor> {
        &self.auditor
    }

    /// Returns the CorruptionRecoveryEngine.
    pub fn recovery_engine(&self) -> &Arc<crate::integrity::CorruptionRecoveryEngine> {
        &self.recovery_engine
    }

    /// Returns the CrashRecoveryEngine.
    pub fn crash_recovery(&self) -> &Arc<crate::recovery::CrashRecoveryEngine> {
        &self.crash_recovery
    }

    /// Performs a comprehensive integrity audit on a single content object.
    pub async fn audit_content(
        &self,
        content_id: &ContentId,
    ) -> Result<crate::integrity::ContentAuditReport, ContentError> {
        self.auditor.audit_content(content_id).await
    }

    /// Performs an integrity audit across all stored content objects.
    pub async fn audit_all(
        &self,
    ) -> Result<Vec<crate::integrity::ContentAuditReport>, ContentError> {
        self.auditor.audit_all().await
    }

    /// Executes a garbage collection cycle to reclaim up to `target_bytes` (or all evictable).
    pub async fn run_gc(
        &self,
        target_bytes: Option<u64>,
    ) -> Result<crate::gc::GcReport, ContentError> {
        let report = self.gc.run_gc(target_bytes).await?;
        self.quota_manager.record_freed(report.bytes_reclaimed);
        Ok(report)
    }

    /// Checks whether a content object is marked as pinned.
    pub fn is_pinned(&self, content_id: &ContentId) -> Result<bool, ContentError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;
        let res: Option<i64> = conn
            .query_row(
                "SELECT is_pinned FROM content_index WHERE content_id = ?1",
                params![content_id.to_hex()],
                |row| row.get(0),
            )
            .optional()?;

        match res {
            Some(p) => Ok(p != 0),
            None => Err(ContentError::ContentNotFound(*content_id)),
        }
    }

    /// Emits a content event to listeners.
    fn emit_event(&self, event: ContentEvent) {
        let _ = self.event_tx.send(event);
    }

    /// Acquires an active reference lock on a content object, preventing its deletion during operations.
    pub async fn acquire_reference(
        &self,
        content_id: &ContentId,
    ) -> Result<ContentLock, ContentError> {
        if !self.has(content_id)? {
            return Err(ContentError::ContentNotFound(*content_id));
        }
        let mut refs = self.active_refs.write().await;
        *refs.entry(*content_id).or_insert(0) += 1;

        Ok(ContentLock {
            content_id: *content_id,
            active_refs: Arc::clone(&self.active_refs),
        })
    }

    /// Inserts raw byte content into the store, returning `PutResult::Created` or `PutResult::AlreadyExists`.
    pub async fn put(
        &self,
        data: &[u8],
        options: Option<PutOptions>,
    ) -> Result<PutResult, ContentError> {
        let mut cursor = std::io::Cursor::new(data);
        let (_id, result) = self.put_stream(&mut cursor, options).await?;
        Ok(result)
    }

    /// Streams arbitrary data from an async reader into the store using a crash-safe staging protocol.
    ///
    /// The data is staged, hashed on the fly, synced to disk, verified, and atomically committed.
    pub async fn put_stream<R: AsyncRead + Unpin>(
        &self,
        reader: &mut R,
        options: Option<PutOptions>,
    ) -> Result<(ContentId, PutResult), ContentError> {
        let start_time = Instant::now();
        let options = options.unwrap_or_default();

        // 1. Stage incoming content to a temporary part file
        let part_filename = format!(
            "{}_{}.part",
            Timestamp::now().as_millis(),
            rand::random::<u64>()
        );
        let staging_path = self.staging_dir.join(&part_filename);

        let mut part_file = fs::File::create(&staging_path).await.map_err(|e| {
            ContentError::StorageFailure(format!(
                "Failed to create staging file {}: {}",
                staging_path.display(),
                e
            ))
        })?;

        // 2. Stream data and compute SHA-256 simultaneously
        let (content_id, total_bytes) = hash_and_copy_stream(reader, &mut part_file).await?;

        // 3. Flush and synchronize to underlying storage
        part_file.sync_all().await.map_err(|e| {
            let _ = std::fs::remove_file(&staging_path);
            ContentError::StorageFailure(format!("Failed to sync staging file to disk: {}", e))
        })?;
        drop(part_file);

        // 4. Atomically commit or deduplicate under commit serialization lock
        let relative_obj_path = content_id.relative_storage_path();
        let dest_path = self.objects_dir.join(&relative_obj_path);

        let _commit_guard = self.commit_lock.lock().await;

        // Check if content already exists as COMPLETE
        let already_exists = {
            let conn = self
                .conn
                .lock()
                .map_err(|e| ContentError::LockError(e.to_string()))?;
            let existing: Option<(String, String)> = conn
                .query_row(
                    "SELECT storage_path, storage_state FROM content_index WHERE content_id = ?1",
                    params![content_id.to_hex()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;

            if let Some((stored_path, state_str)) = existing {
                let existing_file_path = self.objects_dir.join(&stored_path);
                if state_str == ContentStorageState::Complete.as_str()
                    && existing_file_path.exists()
                {
                    Some(self.load_metadata_locked(&conn, &content_id)?)
                } else {
                    None
                }
            } else {
                None
            }
        };

        if let Some(existing_meta) = already_exists {
            let _ = fs::remove_file(&staging_path).await;
            info!(
                content_id = %content_id,
                size = total_bytes,
                duration_ms = start_time.elapsed().as_millis(),
                "Duplicate content detected (deduplication applied)"
            );
            return Ok((content_id, PutResult::AlreadyExists(existing_meta)));
        }

        // Move from staging to content-addressed sharded layout
        if let Some(parent) = dest_path.parent() {
            fs::create_dir_all(parent).await.map_err(|e| {
                let _ = std::fs::remove_file(&staging_path);
                ContentError::StorageFailure(format!("Failed to create object shard dir: {}", e))
            })?;
        }

        fs::rename(&staging_path, &dest_path).await.map_err(|e| {
            let _ = std::fs::remove_file(&staging_path);
            ContentError::StorageFailure(format!(
                "Failed to commit staging object to destination: {}",
                e
            ))
        })?;

        // Insert or update SQLite index
        let now = Timestamp::now();
        let metadata_json = serde_json::to_string(&options.custom_metadata)?;
        let relative_str = relative_obj_path.to_string_lossy().to_string();
        let creator_str = options.creator_node_id.map(|nid| nid.to_string());

        {
            let conn = self
                .conn
                .lock()
                .map_err(|e| ContentError::LockError(e.to_string()))?;
            conn.execute(
                r#"
                INSERT INTO content_index (
                    content_id, size_bytes, storage_path, storage_state,
                    created_at, updated_at, last_accessed_at,
                    mime_type, version, creator_node_id, is_pinned, metadata_json
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                ON CONFLICT(content_id) DO UPDATE SET
                    size_bytes = excluded.size_bytes,
                    storage_path = excluded.storage_path,
                    storage_state = excluded.storage_state,
                    updated_at = excluded.updated_at,
                    last_accessed_at = excluded.last_accessed_at,
                    mime_type = coalesce(excluded.mime_type, content_index.mime_type),
                    version = content_index.version + 1,
                    creator_node_id = coalesce(excluded.creator_node_id, content_index.creator_node_id),
                    is_pinned = max(content_index.is_pinned, excluded.is_pinned),
                    metadata_json = excluded.metadata_json
                "#,
                params![
                    content_id.to_hex(),
                    total_bytes as i64,
                    relative_str,
                    ContentStorageState::Complete.as_str(),
                    now.as_millis() as i64,
                    now.as_millis() as i64,
                    now.as_millis() as i64,
                    options.mime_type,
                    1,
                    creator_str,
                    if options.is_pinned { 1 } else { 0 },
                    metadata_json
                ],
            )?;
        }

        let meta = ContentMetadata {
            content_id,
            size_bytes: total_bytes,
            storage_state: ContentStorageState::Complete,
            created_at: now,
            updated_at: now,
            last_accessed_at: now,
            mime_type: options.mime_type,
            version: 1,
            creator_node_id: options.creator_node_id,
            is_pinned: options.is_pinned,
            custom_metadata: options.custom_metadata,
        };

        let put_result = PutResult::Created(meta);
        self.quota_manager.record_allocated(total_bytes);

        self.emit_event(ContentEvent::Created {
            id: content_id,
            size_bytes: total_bytes,
        });
        self.emit_event(ContentEvent::Available {
            id: content_id,
            size_bytes: total_bytes,
        });

        info!(
            content_id = %content_id,
            size = total_bytes,
            duration_ms = start_time.elapsed().as_millis(),
            "Content committed successfully to CAS"
        );

        Ok((content_id, put_result))
    }

    /// Checks if complete content exists in the store.
    pub fn has(&self, content_id: &ContentId) -> Result<bool, ContentError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;
        let res: Option<(String, String)> = conn
            .query_row(
                "SELECT storage_path, storage_state FROM content_index WHERE content_id = ?1",
                params![content_id.to_hex()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;

        match res {
            Some((path_str, state_str)) => {
                if state_str == ContentStorageState::Complete.as_str() {
                    let obj_path = self.objects_dir.join(path_str);
                    if obj_path.exists() {
                        return Ok(true);
                    }
                    let manifest_exists: bool = conn
                        .query_row(
                            "SELECT 1 FROM manifest_index WHERE content_id = ?1 LIMIT 1",
                            params![content_id.to_hex()],
                            |_| Ok(true),
                        )
                        .optional()?
                        .unwrap_or(false);
                    Ok(manifest_exists)
                } else {
                    Ok(false)
                }
            }
            None => Ok(false),
        }
    }

    /// Retrieves metadata for a stored content object.
    pub fn stat(&self, content_id: &ContentId) -> Result<ContentMetadata, ContentError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;
        self.load_metadata_locked(&conn, content_id)
    }

    /// Retrieves raw content bytes for `content_id`.
    pub async fn get(&self, content_id: &ContentId) -> Result<Vec<u8>, ContentError> {
        let obj_path = {
            let conn = self
                .conn
                .lock()
                .map_err(|e| ContentError::LockError(e.to_string()))?;
            let (rel_path, state): (String, String) = conn
                .query_row(
                    "SELECT storage_path, storage_state FROM content_index WHERE content_id = ?1",
                    params![content_id.to_hex()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?
                .ok_or(ContentError::ContentNotFound(*content_id))?;

            if state == ContentStorageState::Corrupted.as_str() {
                return Err(ContentError::ContentCorrupted {
                    id: *content_id,
                    reason: "Content object marked corrupted in index".to_string(),
                });
            }
            if state != ContentStorageState::Complete.as_str() {
                return Err(ContentError::ContentNotFound(*content_id));
            }

            self.objects_dir.join(rel_path)
        };

        if !obj_path.exists() {
            if let Ok(manifest) = self.manifest_store.get_by_content_id(content_id) {
                if let Ok(bytes) =
                    crate::reconstruction::ContentReconstructor::reconstruct_to_bytes(
                        &manifest,
                        &self.chunk_store,
                        None,
                    )
                    .await
                {
                    if let Some(parent) = obj_path.parent() {
                        let _ = fs::create_dir_all(parent).await;
                    }
                    let _ = fs::write(&obj_path, &bytes).await;
                    self.touch_access(content_id)?;
                    return Ok(bytes);
                }
            }
            // Physical file missing - mark corrupted
            self.mark_corrupted(content_id, "Physical object file missing from storage")?;
            return Err(ContentError::ContentCorrupted {
                id: *content_id,
                reason: "Physical object file missing from storage".to_string(),
            });
        }

        let bytes = fs::read(&obj_path)
            .await
            .map_err(|e| ContentError::IOError(format!("Failed to read content file: {}", e)))?;

        // Update last accessed timestamp
        self.touch_access(content_id)?;

        Ok(bytes)
    }

    /// Retrieves raw content bytes and performs strict cryptographic integrity verification.
    pub async fn get_verified(&self, content_id: &ContentId) -> Result<Vec<u8>, ContentError> {
        let bytes = self.get(content_id).await?;
        let computed_id = hash_bytes(&bytes);

        if computed_id != *content_id {
            self.mark_corrupted(
                content_id,
                "Cryptographic hash mismatch during verification",
            )?;
            self.emit_event(ContentEvent::IntegrityFailed {
                id: *content_id,
                expected: *content_id,
                actual: computed_id,
            });
            return Err(ContentError::IntegrityFailure {
                expected: *content_id,
                actual: computed_id,
            });
        }

        Ok(bytes)
    }

    /// Opens an async read stream for the stored content object.
    pub async fn get_stream(&self, content_id: &ContentId) -> Result<fs::File, ContentError> {
        let obj_path = {
            let conn = self
                .conn
                .lock()
                .map_err(|e| ContentError::LockError(e.to_string()))?;
            let (rel_path, state): (String, String) = conn
                .query_row(
                    "SELECT storage_path, storage_state FROM content_index WHERE content_id = ?1",
                    params![content_id.to_hex()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?
                .ok_or(ContentError::ContentNotFound(*content_id))?;

            if state == ContentStorageState::Corrupted.as_str() {
                return Err(ContentError::ContentCorrupted {
                    id: *content_id,
                    reason: "Content object marked corrupted in index".to_string(),
                });
            }
            if state != ContentStorageState::Complete.as_str() {
                return Err(ContentError::ContentNotFound(*content_id));
            }

            self.objects_dir.join(rel_path)
        };

        if !obj_path.exists() {
            self.mark_corrupted(content_id, "Physical object file missing from storage")?;
            return Err(ContentError::ContentCorrupted {
                id: *content_id,
                reason: "Physical object file missing from storage".to_string(),
            });
        }

        self.touch_access(content_id)?;

        fs::File::open(&obj_path)
            .await
            .map_err(|e| ContentError::IOError(format!("Failed to open content stream: {}", e)))
    }

    /// Safely deletes a content object from local storage, respecting pinning and active locks.
    pub async fn delete(&self, content_id: &ContentId) -> Result<bool, ContentError> {
        // 1. Check active in-flight references
        {
            let refs = self.active_refs.read().await;
            if refs.get(content_id).copied().unwrap_or(0) > 0 {
                return Err(ContentError::ActiveReference(*content_id));
            }
        }

        // 2. Check metadata and pinned status in DB
        let obj_path = {
            let conn = self
                .conn
                .lock()
                .map_err(|e| ContentError::LockError(e.to_string()))?;
            let row: Option<(String, i64)> = conn
                .query_row(
                    "SELECT storage_path, is_pinned FROM content_index WHERE content_id = ?1",
                    params![content_id.to_hex()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;

            match row {
                Some((rel_path, is_pinned)) => {
                    if is_pinned != 0 {
                        return Err(ContentError::PinnedContent(*content_id));
                    }
                    // Delete from database
                    conn.execute(
                        "DELETE FROM content_index WHERE content_id = ?1",
                        params![content_id.to_hex()],
                    )?;
                    self.objects_dir.join(rel_path)
                }
                None => return Err(ContentError::ContentNotFound(*content_id)),
            }
        };

        // 3. Delete physical object file
        if obj_path.exists() {
            if let Ok(meta) = fs::metadata(&obj_path).await {
                self.quota_manager.record_freed(meta.len());
            }
            let _ = fs::remove_file(&obj_path).await;
        }

        self.emit_event(ContentEvent::Deleted { id: *content_id });
        info!(content_id = %content_id, "Content deleted safely from CAS");

        Ok(true)
    }

    /// Marks content as pinned, protecting it from deletion and cache eviction.
    pub fn pin(&self, content_id: &ContentId) -> Result<(), ContentError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;
        let rows = conn.execute(
            "UPDATE content_index SET is_pinned = 1, updated_at = ?2 WHERE content_id = ?1",
            params![content_id.to_hex(), Timestamp::now().as_millis() as i64],
        )?;

        if rows == 0 {
            return Err(ContentError::ContentNotFound(*content_id));
        }

        self.emit_event(ContentEvent::Pinned { id: *content_id });
        Ok(())
    }

    /// Removes the pinned status from a content object.
    pub fn unpin(&self, content_id: &ContentId) -> Result<(), ContentError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;
        let rows = conn.execute(
            "UPDATE content_index SET is_pinned = 0, updated_at = ?2 WHERE content_id = ?1",
            params![content_id.to_hex(), Timestamp::now().as_millis() as i64],
        )?;

        if rows == 0 {
            return Err(ContentError::ContentNotFound(*content_id));
        }

        self.emit_event(ContentEvent::Unpinned { id: *content_id });
        Ok(())
    }

    /// Lists all stored content metadata.
    pub fn list(&self) -> Result<Vec<ContentMetadata>, ContentError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;
        let mut stmt = conn.prepare(
            r#"
            SELECT content_id, size_bytes, storage_state, created_at, updated_at,
                   last_accessed_at, mime_type, version, creator_node_id, is_pinned, metadata_json
            FROM content_index
            WHERE storage_state = 'COMPLETE'
            ORDER BY created_at DESC
            "#,
        )?;

        let rows = stmt.query_map([], |row| {
            let cid_hex: String = row.get(0)?;
            let size_bytes: i64 = row.get(1)?;
            let state_str: String = row.get(2)?;
            let created_at: i64 = row.get(3)?;
            let updated_at: i64 = row.get(4)?;
            let last_accessed_at: i64 = row.get(5)?;
            let mime_type: Option<String> = row.get(6)?;
            let version: u32 = row.get(7)?;
            let creator_str: Option<String> = row.get(8)?;
            let is_pinned: i64 = row.get(9)?;
            let meta_json: Option<String> = row.get(10)?;

            Ok((
                cid_hex,
                size_bytes,
                state_str,
                created_at,
                updated_at,
                last_accessed_at,
                mime_type,
                version,
                creator_str,
                is_pinned,
                meta_json,
            ))
        })?;

        let mut list = Vec::new();
        for r in rows {
            let (
                cid_hex,
                size_bytes,
                state_str,
                created_at,
                updated_at,
                last_accessed_at,
                mime_type,
                version,
                creator_str,
                is_pinned,
                meta_json,
            ) = r?;

            let content_id = ContentId::from_hex(&cid_hex)?;
            let storage_state = state_str.parse().unwrap_or(ContentStorageState::Complete);
            let creator_node_id = creator_str.and_then(|s| s.parse().ok());
            let custom_metadata = meta_json
                .and_then(|j| serde_json::from_str(&j).ok())
                .unwrap_or_default();

            list.push(ContentMetadata {
                content_id,
                size_bytes: size_bytes as u64,
                storage_state,
                created_at: Timestamp::from_millis(created_at as u64),
                updated_at: Timestamp::from_millis(updated_at as u64),
                last_accessed_at: Timestamp::from_millis(last_accessed_at as u64),
                mime_type,
                version,
                creator_node_id,
                is_pinned: is_pinned != 0,
                custom_metadata,
            });
        }

        Ok(list)
    }

    /// Verifies the cryptographic integrity of a stored content object on disk against its ContentId.
    pub fn verify_integrity(&self, content_id: &ContentId) -> Result<bool, ContentError> {
        let (rel_path, state) = {
            let conn = self
                .conn
                .lock()
                .map_err(|e| ContentError::LockError(e.to_string()))?;
            conn.query_row(
                "SELECT storage_path, storage_state FROM content_index WHERE content_id = ?1",
                params![content_id.to_hex()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
            .ok_or(ContentError::ContentNotFound(*content_id))?
        };

        if state != ContentStorageState::Complete.as_str() {
            return Ok(false);
        }

        let obj_path = self.objects_dir.join(rel_path);
        if !obj_path.exists() {
            self.mark_corrupted(content_id, "Physical object file missing from storage")?;
            return Ok(false);
        }

        let mut file = std::fs::File::open(&obj_path)?;
        let computed = crate::hashing::hash_reader(&mut file)?;

        if computed != *content_id {
            self.mark_corrupted(content_id, "Cryptographic hash mismatch detected on disk")?;
            self.emit_event(ContentEvent::IntegrityFailed {
                id: *content_id,
                expected: *content_id,
                actual: computed,
            });
            return Ok(false);
        }

        Ok(true)
    }

    /// Returns the total number of complete content objects in the store.
    pub fn count(&self) -> Result<usize, ContentError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;
        let count: i64 = conn.query_row(
            "SELECT count(*) FROM content_index WHERE storage_state = 'COMPLETE'",
            [],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    /// Returns the total bytes occupied by complete content objects in the store.
    pub fn total_size_bytes(&self) -> Result<u64, ContentError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;
        let total: Option<i64> = conn.query_row(
            "SELECT sum(size_bytes) FROM content_index WHERE storage_state = 'COMPLETE'",
            [],
            |row| row.get(0),
        )?;
        Ok(total.unwrap_or(0) as u64)
    }

    /// Scans the staging directory and removes any orphaned `.part` files left behind by past interruptions.
    pub fn cleanup_orphaned_staging(&self) -> Result<usize, ContentError> {
        let mut count = 0;
        if self.staging_dir.exists() {
            for entry in std::fs::read_dir(&self.staging_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("part") {
                    debug!("Removing orphaned staging file: {}", path.display());
                    let _ = std::fs::remove_file(path);
                    count += 1;
                }
            }
        }
        Ok(count)
    }

    fn touch_access(&self, content_id: &ContentId) -> Result<(), ContentError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;
        let _ = conn.execute(
            "UPDATE content_index SET last_accessed_at = ?2 WHERE content_id = ?1",
            params![content_id.to_hex(), Timestamp::now().as_millis() as i64],
        );
        Ok(())
    }

    fn mark_corrupted(&self, content_id: &ContentId, reason: &str) -> Result<(), ContentError> {
        warn!(content_id = %content_id, reason = reason, "Marking content object as CORRUPTED");
        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;
        let _ = conn.execute(
            "UPDATE content_index SET storage_state = ?2, updated_at = ?3 WHERE content_id = ?1",
            params![
                content_id.to_hex(),
                ContentStorageState::Corrupted.as_str(),
                Timestamp::now().as_millis() as i64,
            ],
        );
        self.emit_event(ContentEvent::Corrupted {
            id: *content_id,
            reason: reason.to_string(),
        });
        Ok(())
    }

    fn load_metadata_locked(
        &self,
        conn: &Connection,
        content_id: &ContentId,
    ) -> Result<ContentMetadata, ContentError> {
        let row = conn
            .query_row(
                r#"
                SELECT size_bytes, storage_state, created_at, updated_at,
                       last_accessed_at, mime_type, version, creator_node_id, is_pinned, metadata_json
                FROM content_index
                WHERE content_id = ?1
                "#,
                params![content_id.to_hex()],
                |row| {
                    let size_bytes: i64 = row.get(0)?;
                    let state_str: String = row.get(1)?;
                    let created_at: i64 = row.get(2)?;
                    let updated_at: i64 = row.get(3)?;
                    let last_accessed_at: i64 = row.get(4)?;
                    let mime_type: Option<String> = row.get(5)?;
                    let version: u32 = row.get(6)?;
                    let creator_str: Option<String> = row.get(7)?;
                    let is_pinned: i64 = row.get(8)?;
                    let meta_json: Option<String> = row.get(9)?;

                    Ok((
                        size_bytes,
                        state_str,
                        created_at,
                        updated_at,
                        last_accessed_at,
                        mime_type,
                        version,
                        creator_str,
                        is_pinned,
                        meta_json,
                    ))
                },
            )
            .optional()?
            .ok_or(ContentError::ContentNotFound(*content_id))?;

        let (
            size_bytes,
            state_str,
            created_at,
            updated_at,
            last_accessed_at,
            mime_type,
            version,
            creator_str,
            is_pinned,
            meta_json,
        ) = row;

        let storage_state = state_str.parse().unwrap_or(ContentStorageState::Complete);
        let creator_node_id = creator_str.and_then(|s| s.parse().ok());
        let custom_metadata = meta_json
            .and_then(|j| serde_json::from_str(&j).ok())
            .unwrap_or_default();

        Ok(ContentMetadata {
            content_id: *content_id,
            size_bytes: size_bytes as u64,
            storage_state,
            created_at: Timestamp::from_millis(created_at as u64),
            updated_at: Timestamp::from_millis(updated_at as u64),
            last_accessed_at: Timestamp::from_millis(last_accessed_at as u64),
            mime_type,
            version,
            creator_node_id,
            is_pinned: is_pinned != 0,
            custom_metadata,
        })
    }

    /// Accessor for the underlying `ChunkStore`.
    pub fn chunk_store(&self) -> &Arc<crate::chunks::store::ChunkStore> {
        &self.chunk_store
    }

    /// Accessor for the underlying `ManifestStore`.
    pub fn manifest_store(&self) -> &Arc<crate::manifest::store::ManifestStore> {
        &self.manifest_store
    }

    /// Accessor for the underlying `ProviderRegistry`.
    pub fn provider_registry(&self) -> &Arc<crate::providers::ProviderRegistry> {
        &self.provider_registry
    }

    /// Accessor for the shared SQLite database connection.
    pub fn conn(&self) -> &Arc<Mutex<Connection>> {
        &self.conn
    }

    /// Accessor for the content event broadcast sender.
    pub fn event_tx(&self) -> &broadcast::Sender<ContentEvent> {
        &self.event_tx
    }

    /// Checks if a verified chunk exists in the store.
    pub fn has_chunk(&self, chunk_id: &crate::chunks::id::ChunkId) -> Result<bool, ContentError> {
        self.chunk_store.has(chunk_id)
    }

    /// Retrieves raw bytes of a stored chunk.
    pub async fn get_chunk(
        &self,
        chunk_id: &crate::chunks::id::ChunkId,
    ) -> Result<Vec<u8>, ContentError> {
        self.chunk_store.get(chunk_id).await
    }

    /// Retrieves the manifest for a given `ContentId`.
    pub fn get_manifest(
        &self,
        content_id: &ContentId,
    ) -> Result<crate::manifest::model::ContentManifest, ContentError> {
        self.manifest_store.get_by_content_id(content_id)
    }

    /// Retrieves the manifest by its `ManifestId`.
    pub fn get_manifest_by_id(
        &self,
        manifest_id: &crate::manifest::id::ManifestId,
    ) -> Result<crate::manifest::model::ContentManifest, ContentError> {
        self.manifest_store.get(manifest_id)
    }

    /// Returns the chunk availability bitmap for a manifest.
    pub fn chunk_availability(
        &self,
        manifest_id: &crate::manifest::id::ManifestId,
    ) -> Result<crate::manifest::model::ChunkAvailabilityBitmap, ContentError> {
        self.manifest_store
            .get_chunk_availability(manifest_id, &self.chunk_store)
    }

    /// Checks if all chunks for a manifest are available and verified.
    pub fn is_manifest_available(
        &self,
        manifest_id: &crate::manifest::id::ManifestId,
    ) -> Result<bool, ContentError> {
        self.manifest_store
            .is_available(manifest_id, &self.chunk_store)
    }

    /// Streams incoming content, divides it into fixed-size chunks, stores each chunk in `ChunkStore`,
    /// creates and stores the `ContentManifest`, and registers the content metadata.
    pub async fn put_chunked_stream<R: AsyncRead + Unpin>(
        &self,
        reader: &mut R,
        options: Option<PutOptions>,
    ) -> Result<
        (
            ContentId,
            PutResult,
            crate::manifest::model::ContentManifest,
        ),
        ContentError,
    > {
        let chunker = crate::chunks::chunker::FixedSizeChunker::default();
        let chunk_size = chunker.chunk_size() as u32;
        let chunk_descriptors = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        let chunk_store = Arc::clone(&self.chunk_store);
        let descriptors_clone = Arc::clone(&chunk_descriptors);

        let (content_id, total_bytes, _num_chunks) = chunker
            .chunk_stream(reader, |chunk_index, offset, chunk_bytes| {
                let chunk_store = Arc::clone(&chunk_store);
                let descriptors = Arc::clone(&descriptors_clone);
                async move {
                    let chunk_len = chunk_bytes.len() as u32;
                    let (chunk_id, _put_res) = chunk_store.put(&chunk_bytes).await?;
                    let mut desc_guard = descriptors.lock().await;
                    desc_guard.push(crate::chunks::id::ChunkDescriptor {
                        index: chunk_index,
                        offset,
                        size: chunk_len,
                        chunk_id,
                    });
                    Ok(())
                }
            })
            .await?;

        let options = options.unwrap_or_default();
        let chunks = {
            let mut guard = chunk_descriptors.lock().await;
            guard.sort_by_key(|c| c.index);
            guard.clone()
        };

        let manifest = crate::manifest::model::ContentManifest::new(
            content_id,
            total_bytes,
            chunk_size,
            chunks,
            crate::chunks::chunker::CHUNKING_ALGORITHM_FIXED_256K,
            options.mime_type.clone(),
            options.custom_metadata.clone(),
            options.creator_node_id,
        );

        // Store manifest
        self.manifest_store.put(&manifest)?;

        let relative_obj_path = content_id.relative_storage_path();
        let dest_path = self.objects_dir.join(&relative_obj_path);
        if let Some(parent) = dest_path.parent() {
            fs::create_dir_all(parent).await.map_err(|e| {
                ContentError::StorageFailure(format!("Failed to create object shard dir: {}", e))
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
            ContentError::StorageFailure(format!("Failed to flush destination file: {}", e))
        })?;

        self.quota_manager.record_allocated(total_bytes);

        // Register content metadata in SQLite
        let now = Timestamp::now();
        let metadata_json = serde_json::to_string(&options.custom_metadata)?;
        let creator_str = options.creator_node_id.map(|nid| nid.to_string());
        let relative_str = relative_obj_path.to_string_lossy().to_string();

        {
            let conn = self
                .conn
                .lock()
                .map_err(|e| ContentError::LockError(e.to_string()))?;
            conn.execute(
                r#"
                INSERT INTO content_index (
                    content_id, size_bytes, storage_path, storage_state,
                    created_at, updated_at, last_accessed_at,
                    mime_type, version, creator_node_id, is_pinned, metadata_json
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                ON CONFLICT(content_id) DO UPDATE SET
                    size_bytes = excluded.size_bytes,
                    storage_state = excluded.storage_state,
                    updated_at = excluded.updated_at,
                    last_accessed_at = excluded.last_accessed_at,
                    mime_type = coalesce(excluded.mime_type, content_index.mime_type),
                    version = content_index.version + 1,
                    creator_node_id = coalesce(excluded.creator_node_id, content_index.creator_node_id),
                    is_pinned = max(content_index.is_pinned, excluded.is_pinned),
                    metadata_json = excluded.metadata_json
                "#,
                params![
                    content_id.to_hex(),
                    total_bytes as i64,
                    relative_str,
                    ContentStorageState::Complete.as_str(),
                    now.as_millis() as i64,
                    now.as_millis() as i64,
                    now.as_millis() as i64,
                    options.mime_type,
                    1,
                    creator_str,
                    if options.is_pinned { 1 } else { 0 },
                    metadata_json
                ],
            )?;
        }

        let meta = ContentMetadata {
            content_id,
            size_bytes: total_bytes,
            storage_state: ContentStorageState::Complete,
            created_at: now,
            updated_at: now,
            last_accessed_at: now,
            mime_type: options.mime_type,
            version: 1,
            creator_node_id: options.creator_node_id,
            is_pinned: options.is_pinned,
            custom_metadata: options.custom_metadata,
        };

        Ok((content_id, PutResult::Created(meta), manifest))
    }

    /// Reconstructs content from chunks defined by its manifest into an `AsyncWrite` destination.
    pub async fn reconstruct_to_stream<W: tokio::io::AsyncWrite + Unpin>(
        &self,
        content_id: &ContentId,
        writer: &mut W,
    ) -> Result<u64, ContentError> {
        let manifest = self.manifest_store.get_by_content_id(content_id)?;
        crate::reconstruction::ContentReconstructor::reconstruct_to_stream(
            &manifest,
            &self.chunk_store,
            writer,
            Some(&self.event_tx),
        )
        .await
    }

    /// Reconstructs content from chunks into a byte vector.
    pub async fn reconstruct_to_bytes(
        &self,
        content_id: &ContentId,
    ) -> Result<Vec<u8>, ContentError> {
        let manifest = self.manifest_store.get_by_content_id(content_id)?;
        crate::reconstruction::ContentReconstructor::reconstruct_to_bytes(
            &manifest,
            &self.chunk_store,
            Some(&self.event_tx),
        )
        .await
    }
}
