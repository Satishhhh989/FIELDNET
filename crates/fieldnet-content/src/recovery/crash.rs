use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::fs;
use tokio::sync::broadcast;
use tracing::info;

use super::orphan::OrphanDetector;
use crate::chunks::store::ChunkStore;
use crate::error::ContentError;
use crate::events::ContentEvent;
use crate::manifest::store::ManifestStore;
use crate::metadata::ContentStorageState;
use fieldnet_core::Timestamp;

/// Summary telemetry report generated after startup crash recovery.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CrashRecoveryReport {
    pub staging_files_cleaned: usize,
    pub corrupted_objects_flagged: usize,
    pub orphaned_chunks_quarantined: usize,
    pub transfer_sessions_recovered: usize,
    pub provider_records_swept: usize,
    pub storage_bytes_used: u64,
}

/// Startup crash recovery engine (Phase 07 BUILD 06).
/// Executes a deterministic 8-step startup sequence to reconcile persistent storage,
/// quarantine anomalous artifacts, recover in-flight transfers, and establish runtime readiness.
pub struct CrashRecoveryEngine {
    base_dir: PathBuf,
    objects_dir: PathBuf,
    staging_dir: PathBuf,
    quarantine_dir: PathBuf,
    conn: Arc<Mutex<Connection>>,
    orphan_detector: OrphanDetector,
    event_tx: broadcast::Sender<ContentEvent>,
}

impl CrashRecoveryEngine {
    pub fn new(
        base_dir: impl AsRef<Path>,
        conn: Arc<Mutex<Connection>>,
        chunk_store: Arc<ChunkStore>,
        _manifest_store: Arc<ManifestStore>,
        event_tx: broadcast::Sender<ContentEvent>,
    ) -> Self {
        let base_dir = base_dir.as_ref().to_path_buf();
        let orphan_detector = OrphanDetector::new(&base_dir, Arc::clone(&conn), chunk_store);
        Self {
            objects_dir: base_dir.join("objects"),
            staging_dir: base_dir.join("staging"),
            quarantine_dir: base_dir.join("quarantine"),
            base_dir,
            conn,
            orphan_detector,
            event_tx,
        }
    }

    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    /// Executes the full 8-step startup crash recovery sequence.
    pub async fn execute_startup_recovery(&self) -> Result<CrashRecoveryReport, ContentError> {
        info!("Executing startup crash recovery sequence...");
        let mut report = CrashRecoveryReport::default();

        // Step 1: Storage directory sanity
        std::fs::create_dir_all(&self.objects_dir).map_err(|e| {
            ContentError::StorageFailure(format!("Failed to create objects dir: {}", e))
        })?;
        std::fs::create_dir_all(&self.staging_dir).map_err(|e| {
            ContentError::StorageFailure(format!("Failed to create staging dir: {}", e))
        })?;
        std::fs::create_dir_all(&self.quarantine_dir).map_err(|e| {
            ContentError::StorageFailure(format!("Failed to create quarantine dir: {}", e))
        })?;

        // Step 2: Orphan staging sweep
        if self.staging_dir.exists() {
            if let Ok(mut entries) = fs::read_dir(&self.staging_dir).await {
                while let Ok(Some(entry)) = entries.next_entry().await {
                    let path = entry.path();
                    if path.is_file() && fs::remove_file(&path).await.is_ok() {
                        report.staging_files_cleaned += 1;
                    }
                }
            }
        }

        // Step 3: Content metadata & physical file verification
        {
            let conn = self
                .conn
                .lock()
                .map_err(|e| ContentError::LockError(e.to_string()))?;

            let mut stmt = conn.prepare(
                "SELECT content_id, storage_path FROM content_index WHERE storage_state = 'COMPLETE'",
            )?;
            let rows = stmt.query_map([], |row| {
                let cid: String = row.get(0)?;
                let path: String = row.get(1)?;
                Ok((cid, path))
            })?;

            let mut missing_cids = Vec::new();
            for (cid_hex, rel_path) in rows.flatten() {
                let full_path = self.objects_dir.join(&rel_path);
                if !full_path.exists() {
                    missing_cids.push(cid_hex);
                }
            }

            for cid_hex in missing_cids {
                let _ = conn.execute(
                    "UPDATE content_index SET storage_state = ?2 WHERE content_id = ?1",
                    params![cid_hex, ContentStorageState::Corrupted.as_str()],
                );
                report.corrupted_objects_flagged += 1;
            }
        }

        // Step 4: Chunk Store orphan detection & safe quarantine
        let orphans = self.orphan_detector.detect_orphans().await?;
        let quarantined = self
            .orphan_detector
            .quarantine_orphaned_chunks(&orphans)
            .await?;
        report.orphaned_chunks_quarantined = quarantined;

        // Step 5: Transfer Session recovery across reboot
        // In-flight sessions (TRANSFERRING, REQUESTED, NEGOTIATING) transition to PAUSED
        {
            let conn = self
                .conn
                .lock()
                .map_err(|e| ContentError::LockError(e.to_string()))?;

            // Check if content_transfers table exists
            let table_exists: bool = conn
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='content_transfers'",
                    [],
                    |row| Ok(row.get::<_, i64>(0)? > 0),
                )
                .unwrap_or(false);

            if table_exists {
                let updated = conn.execute(
                    r#"
                    UPDATE content_transfers
                    SET status = 'PAUSED',
                        error_message = 'Paused due to node restart recovery',
                        updated_at = ?1
                    WHERE status IN ('TRANSFERRING', 'REQUESTED', 'NEGOTIATING', 'RESUMING')
                    "#,
                    params![Timestamp::now().as_millis() as i64],
                )?;
                report.transfer_sessions_recovered = updated;
            }
        }

        // Step 6: Provider Registry recovery & sweep of expired transient hints
        {
            let conn = self
                .conn
                .lock()
                .map_err(|e| ContentError::LockError(e.to_string()))?;

            let table_exists: bool = conn
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='provider_cache'",
                    [],
                    |row| Ok(row.get::<_, i64>(0)? > 0),
                )
                .unwrap_or(false);

            if table_exists {
                let swept = conn.execute(
                    "DELETE FROM provider_cache WHERE expires_at < ?1",
                    params![Timestamp::now().as_millis() as i64],
                )?;
                report.provider_records_swept = swept;
            }
        }

        // Step 7: Storage usage recalculation
        {
            let conn = self
                .conn
                .lock()
                .map_err(|e| ContentError::LockError(e.to_string()))?;

            let chunk_bytes: i64 = conn
                .query_row(
                    "SELECT COALESCE(SUM(size_bytes), 0) FROM chunk_index",
                    [],
                    |row| row.get(0),
                )
                .unwrap_or(0);

            let object_bytes: i64 = conn
                .query_row(
                    "SELECT COALESCE(SUM(size_bytes), 0) FROM content_index",
                    [],
                    |row| row.get(0),
                )
                .unwrap_or(0);

            report.storage_bytes_used = (chunk_bytes.max(0) + object_bytes.max(0)) as u64;
        }

        // Step 8: Emit recovery completed event
        let _ = self.event_tx.send(ContentEvent::RecoveryCompleted {
            recovered_objects: report.corrupted_objects_flagged,
            recovered_chunks: report.orphaned_chunks_quarantined,
            recovered_sessions: report.transfer_sessions_recovered,
        });

        info!(
            staging_cleaned = report.staging_files_cleaned,
            corrupted_flagged = report.corrupted_objects_flagged,
            orphans_quarantined = report.orphaned_chunks_quarantined,
            sessions_paused = report.transfer_sessions_recovered,
            bytes_used = report.storage_bytes_used,
            "Startup crash recovery completed successfully"
        );

        Ok(report)
    }
}
