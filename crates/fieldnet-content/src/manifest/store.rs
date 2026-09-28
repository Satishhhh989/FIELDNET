use rusqlite::{params, Connection, OptionalExtension};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tracing::info;

use crate::chunks::store::ChunkStore;
use crate::error::ContentError;
use crate::events::ContentEvent;
use crate::identity::ContentId;
use crate::manifest::id::ManifestId;
use crate::manifest::model::{ChunkAvailabilityBitmap, ContentManifest};
use crate::manifest::validator::ManifestValidator;

/// Persistent local storage engine for Content Manifests.
pub struct ManifestStore {
    conn: Arc<Mutex<Connection>>,
    event_tx: broadcast::Sender<ContentEvent>,
}

impl ManifestStore {
    /// Opens or connects the `ManifestStore` using the shared SQLite connection.
    pub fn open(
        conn: Arc<Mutex<Connection>>,
        event_tx: broadcast::Sender<ContentEvent>,
    ) -> Result<Arc<Self>, ContentError> {
        {
            let conn_guard = conn
                .lock()
                .map_err(|e| ContentError::LockError(e.to_string()))?;
            conn_guard.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS manifest_index (
                    manifest_id TEXT PRIMARY KEY,
                    content_id TEXT NOT NULL,
                    total_size INTEGER NOT NULL,
                    chunk_count INTEGER NOT NULL,
                    chunk_size INTEGER NOT NULL,
                    chunking_algorithm TEXT NOT NULL,
                    merkle_root TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    manifest_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_manifest_content_id ON manifest_index(content_id);
                "#,
            )?;
        }

        Ok(Arc::new(Self { conn, event_tx }))
    }

    /// Stores a manifest, validating its structure and cryptographic integrity first.
    pub fn put(&self, manifest: &ContentManifest) -> Result<(), ContentError> {
        ManifestValidator::validate(manifest)?;

        let manifest_json = serde_json::to_string(manifest)?;
        let merkle_hex = hex::encode(manifest.merkle_root);

        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;
        conn.execute(
            r#"
            INSERT INTO manifest_index (
                manifest_id, content_id, total_size, chunk_count, chunk_size,
                chunking_algorithm, merkle_root, created_at, manifest_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            ON CONFLICT(manifest_id) DO UPDATE SET
                manifest_json = excluded.manifest_json
            "#,
            params![
                manifest.manifest_id.to_hex(),
                manifest.content_id.to_hex(),
                manifest.total_size as i64,
                manifest.chunk_count as i64,
                manifest.chunk_size as i64,
                manifest.chunking_algorithm,
                merkle_hex,
                manifest.created_at.as_millis() as i64,
                manifest_json,
            ],
        )?;

        let _ = self.event_tx.send(ContentEvent::ManifestCreated {
            manifest_id: manifest.manifest_id,
            content_id: manifest.content_id,
            chunk_count: manifest.chunk_count,
        });

        info!(
            manifest_id = %manifest.manifest_id,
            content_id = %manifest.content_id,
            chunks = manifest.chunk_count,
            "Manifest persisted to ManifestStore"
        );

        Ok(())
    }

    /// Retrieves a manifest by its `ManifestId`.
    pub fn get(&self, manifest_id: &ManifestId) -> Result<ContentManifest, ContentError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;
        let json_str: String = conn
            .query_row(
                "SELECT manifest_json FROM manifest_index WHERE manifest_id = ?1",
                params![manifest_id.to_hex()],
                |row| row.get(0),
            )
            .optional()?
            .ok_or(ContentError::ManifestNotFound(*manifest_id))?;

        let manifest: ContentManifest = serde_json::from_str(&json_str)?;
        Ok(manifest)
    }

    /// Retrieves the manifest for a given `ContentId`.
    pub fn get_by_content_id(
        &self,
        content_id: &ContentId,
    ) -> Result<ContentManifest, ContentError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;
        let json_str: String = conn
            .query_row(
                "SELECT manifest_json FROM manifest_index WHERE content_id = ?1 ORDER BY created_at DESC LIMIT 1",
                params![content_id.to_hex()],
                |row| row.get(0),
            )
            .optional()?
            .ok_or(ContentError::ContentNotFound(*content_id))?;

        let manifest: ContentManifest = serde_json::from_str(&json_str)?;
        Ok(manifest)
    }

    /// Checks if a manifest exists by its `ManifestId`.
    pub fn has(&self, manifest_id: &ManifestId) -> Result<bool, ContentError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;
        let count: i64 = conn.query_row(
            "SELECT count(*) FROM manifest_index WHERE manifest_id = ?1",
            params![manifest_id.to_hex()],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Checks if a manifest exists for a given `ContentId`.
    pub fn has_for_content(&self, content_id: &ContentId) -> Result<bool, ContentError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;
        let count: i64 = conn.query_row(
            "SELECT count(*) FROM manifest_index WHERE content_id = ?1",
            params![content_id.to_hex()],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Deletes a manifest by `ManifestId`.
    pub fn delete(&self, manifest_id: &ManifestId) -> Result<bool, ContentError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;
        let rows = conn.execute(
            "DELETE FROM manifest_index WHERE manifest_id = ?1",
            params![manifest_id.to_hex()],
        )?;
        if rows == 0 {
            return Err(ContentError::ManifestNotFound(*manifest_id));
        }
        Ok(true)
    }

    /// Lists all stored manifests.
    pub fn list(&self) -> Result<Vec<ContentManifest>, ContentError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;
        let mut stmt =
            conn.prepare("SELECT manifest_json FROM manifest_index ORDER BY created_at DESC")?;
        let rows = stmt.query_map([], |row| {
            let json: String = row.get(0)?;
            Ok(json)
        })?;

        let mut list = Vec::new();
        for r in rows {
            let json = r?;
            if let Ok(manifest) = serde_json::from_str(&json) {
                list.push(manifest);
            }
        }
        Ok(list)
    }

    /// Computes the exact chunk availability bitmap for a manifest against the given `ChunkStore`.
    pub fn get_chunk_availability(
        &self,
        manifest_id: &ManifestId,
        chunk_store: &ChunkStore,
    ) -> Result<ChunkAvailabilityBitmap, ContentError> {
        let manifest = self.get(manifest_id)?;
        let mut bitmap = ChunkAvailabilityBitmap::new(manifest.chunk_count);

        for chunk in &manifest.chunks {
            if chunk_store.has(&chunk.chunk_id)? {
                bitmap.mark_available(chunk.index);
            }
        }

        Ok(bitmap)
    }

    /// Determines if a manifest is completely AVAILABLE (all constituent chunks present and verified).
    pub fn is_available(
        &self,
        manifest_id: &ManifestId,
        chunk_store: &ChunkStore,
    ) -> Result<bool, ContentError> {
        let bitmap = self.get_chunk_availability(manifest_id, chunk_store)?;
        let complete = bitmap.is_complete();
        if complete {
            if let Ok(manifest) = self.get(manifest_id) {
                let _ = self.event_tx.send(ContentEvent::ManifestVerified {
                    manifest_id: *manifest_id,
                    content_id: manifest.content_id,
                });
            }
        }
        Ok(complete)
    }
}
