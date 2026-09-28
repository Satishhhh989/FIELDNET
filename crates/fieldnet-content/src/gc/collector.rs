use rusqlite::{params, Connection};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::fs;
use tokio::sync::broadcast;
use tracing::info;

use super::classifier::StorageTier;
use super::policy::{EvictionCandidate, GcReport};
use crate::chunks::id::ChunkId;
use crate::chunks::store::ChunkStore;
use crate::error::ContentError;
use crate::events::ContentEvent;
use crate::identity::ContentId;
use crate::manifest::store::ManifestStore;

/// Trait implemented by subsystems (e.g. TransferManager) to shield active in-flight chunks from GC.
pub trait ActiveTransferProtector: Send + Sync {
    fn active_chunk_ids(&self) -> HashSet<ChunkId>;
}

/// Dummy default protector when no active transfers subsystem is registered.
pub struct EmptyTransferProtector;
impl ActiveTransferProtector for EmptyTransferProtector {
    fn active_chunk_ids(&self) -> HashSet<ChunkId> {
        HashSet::new()
    }
}

/// The Content Garbage Collector (Phase 07 BUILD 06).
/// Deterministically reclaims storage while strictly enforcing the 5 Safe Eviction Invariants:
/// 1. Pinned content is NEVER removed.
/// 2. Chunks required by pinned content are NEVER removed.
/// 3. Chunks required by active in-flight transfers are NEVER removed.
/// 4. Shared chunks referenced by other retained manifests are NEVER removed.
/// 5. Corrupted and orphaned temporary data is removed first.
pub struct ContentGarbageCollector {
    conn: Arc<Mutex<Connection>>,
    chunk_store: Arc<ChunkStore>,
    manifest_store: Arc<ManifestStore>,
    objects_dir: PathBuf,
    staging_dir: PathBuf,
    quarantine_dir: PathBuf,
    transfer_protector: Arc<dyn ActiveTransferProtector>,
    event_tx: broadcast::Sender<ContentEvent>,
}

impl ContentGarbageCollector {
    pub fn new(
        base_dir: impl AsRef<Path>,
        conn: Arc<Mutex<Connection>>,
        chunk_store: Arc<ChunkStore>,
        manifest_store: Arc<ManifestStore>,
        transfer_protector: Option<Arc<dyn ActiveTransferProtector>>,
        event_tx: broadcast::Sender<ContentEvent>,
    ) -> Self {
        let base_dir = base_dir.as_ref();
        Self {
            conn,
            chunk_store,
            manifest_store,
            objects_dir: base_dir.join("objects"),
            staging_dir: base_dir.join("staging"),
            quarantine_dir: base_dir.join("quarantine"),
            transfer_protector: transfer_protector
                .unwrap_or_else(|| Arc::new(EmptyTransferProtector)),
            event_tx,
        }
    }

    /// Executes a garbage collection cycle to reclaim up to `target_bytes` (or all evictable if None).
    pub async fn run_gc(&self, target_bytes: Option<u64>) -> Result<GcReport, ContentError> {
        let mut report = GcReport::default();

        let _ = self.event_tx.send(ContentEvent::GcStarted {
            total_candidates: 0,
        });

        // Step 1: Evict quarantined/corrupted files first (Priority 0)
        if self.quarantine_dir.exists() {
            if let Ok(mut entries) = fs::read_dir(&self.quarantine_dir).await {
                while let Ok(Some(entry)) = entries.next_entry().await {
                    if let Ok(meta) = entry.metadata().await {
                        let size = meta.len();
                        if fs::remove_file(entry.path()).await.is_ok() {
                            report.bytes_reclaimed += size;
                            let _ = self.event_tx.send(ContentEvent::GcEvicted {
                                content_id: None,
                                chunk_id: None,
                                bytes_reclaimed: size,
                            });
                        }
                    }
                }
            }
        }

        // Step 2: Evict stale staging files (Priority 1)
        if self.staging_dir.exists() {
            if let Ok(mut entries) = fs::read_dir(&self.staging_dir).await {
                while let Ok(Some(entry)) = entries.next_entry().await {
                    if let Ok(meta) = entry.metadata().await {
                        let size = meta.len();
                        if fs::remove_file(entry.path()).await.is_ok() {
                            report.bytes_reclaimed += size;
                        }
                    }
                }
            }
        }

        // Step 3: Query all stored objects from content_index
        let (pinned_ids, unpinned_candidates): (Vec<ContentId>, Vec<EvictionCandidate>) = {
            let conn = self
                .conn
                .lock()
                .map_err(|e| ContentError::LockError(e.to_string()))?;

            let mut stmt = conn.prepare(
                "SELECT content_id, size_bytes, storage_state, is_pinned, last_accessed_at FROM content_index",
            )?;

            let rows = stmt.query_map([], |row| {
                let cid_str: String = row.get(0)?;
                let size_bytes: i64 = row.get(1)?;
                let state_str: String = row.get(2)?;
                let is_pinned: i64 = row.get(3)?;
                let last_accessed_at: i64 = row.get(4)?;
                Ok((cid_str, size_bytes, state_str, is_pinned, last_accessed_at))
            })?;

            let mut pinned = Vec::new();
            let mut candidates = Vec::new();

            for (cid_str, size, state, is_pinned_val, last_acc) in rows.flatten() {
                if let Ok(cid) = ContentId::from_hex(&cid_str) {
                    if is_pinned_val != 0 {
                        pinned.push(cid);
                    } else {
                        let tier = if state == "CORRUPTED" {
                            StorageTier::Corrupted
                        } else {
                            StorageTier::Cached
                        };
                        candidates.push(EvictionCandidate {
                            content_id: cid,
                            tier,
                            size_bytes: size as u64,
                            last_accessed_at: last_acc as u64,
                            is_pinned: false,
                        });
                    }
                }
            }

            (pinned, candidates)
        };

        report.protected_objects = pinned_ids.len();
        report.candidates = unpinned_candidates.len();

        for pid in &pinned_ids {
            let _ = self.event_tx.send(ContentEvent::GcSkipped {
                content_id: Some(*pid),
                chunk_id: None,
                reason: "Content is pinned".to_string(),
            });
        }

        // Sort unpinned candidates by priority then LRU (oldest accessed first)
        let mut sorted_candidates = unpinned_candidates;
        sorted_candidates.sort_by(|a, b| {
            a.tier
                .eviction_priority()
                .cmp(&b.tier.eviction_priority())
                .then_with(|| a.last_accessed_at.cmp(&b.last_accessed_at))
        });

        // Step 4: Build protected chunks set
        // Active transfers chunks
        let mut protected_chunks: HashSet<ChunkId> = self.transfer_protector.active_chunk_ids();

        // Chunks referenced by pinned content manifests
        for pid in &pinned_ids {
            if let Ok(manifest) = self.manifest_store.get_by_content_id(pid) {
                for desc in manifest.chunks {
                    protected_chunks.insert(desc.chunk_id);
                }
            }
        }

        // We will evict candidate objects one by one until target_bytes met
        let mut evicted_content_ids = HashSet::new();

        for candidate in sorted_candidates {
            if let Some(target) = target_bytes {
                if report.bytes_reclaimed >= target {
                    break;
                }
            }

            let cid = candidate.content_id;
            let manifest_opt = self.manifest_store.get_by_content_id(&cid).ok();

            // Check what chunks are shared with other non-evicted manifests!
            let candidate_chunks = if let Some(ref m) = manifest_opt {
                m.chunks.clone()
            } else {
                Vec::new()
            };

            // Remove physical file from objects_dir
            let rel_path = cid.relative_storage_path();
            let obj_path = self.objects_dir.join(rel_path);
            if obj_path.exists() {
                if let Ok(meta) = fs::metadata(&obj_path).await {
                    report.bytes_reclaimed += meta.len();
                }
                let _ = fs::remove_file(&obj_path).await;
            }

            // Remove from SQLite content_index & manifest_index
            {
                let conn = self
                    .conn
                    .lock()
                    .map_err(|e| ContentError::LockError(e.to_string()))?;
                let _ = conn.execute(
                    "DELETE FROM content_index WHERE content_id = ?1",
                    params![cid.to_hex()],
                );
                let _ = conn.execute(
                    "DELETE FROM manifest_index WHERE content_id = ?1",
                    params![cid.to_hex()],
                );
            }

            evicted_content_ids.insert(cid);
            report.evicted_objects += 1;

            let _ = self.event_tx.send(ContentEvent::GcEvicted {
                content_id: Some(cid),
                chunk_id: None,
                bytes_reclaimed: candidate.size_bytes,
            });

            // Now evaluate constituent chunks: Safe Eviction (shared chunk protection)
            for desc in candidate_chunks {
                let chunk_id = desc.chunk_id;
                // If it's in protected_chunks (pinned or active transfer), DO NOT delete
                if protected_chunks.contains(&chunk_id) {
                    continue;
                }

                // Check if any remaining manifest in manifest_index references this chunk
                let is_referenced_elsewhere = {
                    let conn = self
                        .conn
                        .lock()
                        .map_err(|e| ContentError::LockError(e.to_string()))?;
                    let mut stmt = conn.prepare("SELECT manifest_json FROM manifest_index")?;
                    let mut rows = stmt.query([])?;
                    let mut referenced = false;
                    let chunk_hex = chunk_id.to_hex();
                    while let Ok(Some(row)) = rows.next() {
                        let json: String = row.get(0)?;
                        if json.contains(&chunk_hex) {
                            referenced = true;
                            break;
                        }
                    }
                    referenced
                };

                if is_referenced_elsewhere {
                    // Shared chunk protected! Add to protected set so we don't query again
                    protected_chunks.insert(chunk_id);
                    continue;
                }

                // No other manifest references this chunk -> Safe to remove!
                let chunk_rel_path = chunk_id.relative_storage_path();
                let chunk_path = self.chunk_store.objects_dir().join(chunk_rel_path);
                if chunk_path.exists() {
                    if let Ok(meta) = fs::metadata(&chunk_path).await {
                        report.bytes_reclaimed += meta.len();
                    }
                    let _ = fs::remove_file(&chunk_path).await;
                }

                // Remove from chunk_index in DB
                {
                    let conn = self
                        .conn
                        .lock()
                        .map_err(|e| ContentError::LockError(e.to_string()))?;
                    let _ = conn.execute(
                        "DELETE FROM chunk_index WHERE chunk_id = ?1",
                        params![chunk_id.to_hex()],
                    );
                }

                report.evicted_chunks += 1;
                let _ = self.event_tx.send(ContentEvent::GcEvicted {
                    content_id: None,
                    chunk_id: Some(chunk_id),
                    bytes_reclaimed: desc.size as u64,
                });
            }
        }

        if report.bytes_reclaimed > 0 {
            let _ = self.event_tx.send(ContentEvent::StorageRecovered {
                bytes_reclaimed: report.bytes_reclaimed,
            });
            info!(
                evicted_objects = report.evicted_objects,
                evicted_chunks = report.evicted_chunks,
                reclaimed = report.bytes_reclaimed,
                "Garbage collection cycle completed"
            );
        }

        Ok(report)
    }
}
