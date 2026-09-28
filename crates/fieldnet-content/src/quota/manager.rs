use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tracing::{info, warn};

use super::limits::{QuotaConfig, ResourceLimits};
use crate::error::ContentError;
use crate::events::ContentEvent;
use crate::gc::ContentGarbageCollector;
use fieldnet_core::Timestamp;

/// Represents an active storage space reservation for an incoming write or transfer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageReservation {
    pub reservation_id: u64,
    pub bytes: u64,
    pub reserved_at: Timestamp,
}

/// Telemetry status of current storage quota utilization.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuotaStatus {
    pub used_bytes: u64,
    pub reserved_bytes: u64,
    pub available_bytes: u64,
    pub quota_bytes: u64,
    pub usage_percent: f64,
}

/// Storage Quota and Resource Exhaustion Manager (Phase 07 BUILD 06).
pub struct StorageQuotaManager {
    used_bytes: AtomicU64,
    reserved_bytes: AtomicU64,
    next_reservation_id: AtomicU64,
    config: Arc<RwLock<QuotaConfig>>,
    limits: Arc<RwLock<ResourceLimits>>,
    gc: Arc<RwLock<Option<Arc<ContentGarbageCollector>>>>,
    event_tx: broadcast::Sender<ContentEvent>,
}

impl StorageQuotaManager {
    pub fn new(
        config: QuotaConfig,
        limits: ResourceLimits,
        event_tx: broadcast::Sender<ContentEvent>,
    ) -> Self {
        Self {
            used_bytes: AtomicU64::new(0),
            reserved_bytes: AtomicU64::new(0),
            next_reservation_id: AtomicU64::new(1),
            config: Arc::new(RwLock::new(config)),
            limits: Arc::new(RwLock::new(limits)),
            gc: Arc::new(RwLock::new(None)),
            event_tx,
        }
    }

    /// Sets the ContentGarbageCollector reference for automatic pressure eviction.
    pub async fn set_gc(&self, gc: Arc<ContentGarbageCollector>) {
        let mut guard = self.gc.write().await;
        *guard = Some(gc);
    }

    /// Updates quota configuration at runtime.
    pub async fn update_config(&self, new_config: QuotaConfig) {
        let mut guard = self.config.write().await;
        *guard = new_config;
    }

    /// Validates object and chunk sizes against configured resource limits.
    pub async fn check_limits(
        &self,
        object_size: u64,
        chunk_size: Option<u32>,
    ) -> Result<(), ContentError> {
        let limits = self.limits.read().await;

        if object_size > limits.max_object_size {
            return Err(ContentError::ResourceLimitExceeded(format!(
                "Object size {} exceeds maximum permitted size {}",
                object_size, limits.max_object_size
            )));
        }

        if let Some(cs) = chunk_size {
            if cs > limits.max_chunk_size {
                return Err(ContentError::ResourceLimitExceeded(format!(
                    "Chunk size {} exceeds maximum permitted chunk size {}",
                    cs, limits.max_chunk_size
                )));
            }
        }

        Ok(())
    }

    /// Reserves storage space before starting ingestion or transfer.
    /// If storage quota is exhausted, automatically triggers safe GC eviction.
    pub async fn reserve(&self, bytes: u64) -> Result<StorageReservation, ContentError> {
        self.check_limits(bytes, None).await?;

        let total_quota = self.config.read().await.total_storage_quota;
        let mut used = self.used_bytes.load(Ordering::SeqCst);
        let mut reserved = self.reserved_bytes.load(Ordering::SeqCst);

        // Check if allocation exceeds quota
        if used.saturating_add(reserved).saturating_add(bytes) > total_quota {
            warn!(
                used,
                reserved,
                requested = bytes,
                quota = total_quota,
                "Storage pressure detected: attempting safe garbage collection"
            );

            // Attempt safe GC eviction
            let gc_opt = self.gc.read().await.clone();
            if let Some(gc) = gc_opt {
                let needed = used
                    .saturating_add(reserved)
                    .saturating_add(bytes)
                    .saturating_sub(total_quota);
                if let Ok(report) = gc.run_gc(Some(needed)).await {
                    self.record_freed(report.bytes_reclaimed);
                }
            }

            used = self.used_bytes.load(Ordering::SeqCst);
            reserved = self.reserved_bytes.load(Ordering::SeqCst);

            // If still exceeding after GC eviction, fail-closed
            if used.saturating_add(reserved).saturating_add(bytes) > total_quota {
                let available = total_quota.saturating_sub(used.saturating_add(reserved));
                let _ = self.event_tx.send(ContentEvent::QuotaExceeded {
                    requested_bytes: bytes,
                    available_bytes: available,
                    quota_bytes: total_quota,
                });
                return Err(ContentError::QuotaExceeded {
                    requested_bytes: bytes,
                    available_bytes: available,
                    quota_bytes: total_quota,
                });
            }
        }

        // Reservation succeeded
        self.reserved_bytes.fetch_add(bytes, Ordering::SeqCst);
        let res_id = self.next_reservation_id.fetch_add(1, Ordering::SeqCst);

        // Check warning threshold
        let warning_pct = self.config.read().await.warning_threshold_percent;
        let total_allocated = used.saturating_add(reserved).saturating_add(bytes);
        let usage_pct = (total_allocated as f64 / total_quota as f64) * 100.0;

        if usage_pct >= warning_pct {
            let usage_permille = ((total_allocated as f64 / total_quota as f64) * 1000.0) as u32;
            let _ = self.event_tx.send(ContentEvent::QuotaWarning {
                used_bytes: total_allocated,
                quota_bytes: total_quota,
                usage_permille,
            });
        }

        Ok(StorageReservation {
            reservation_id: res_id,
            bytes,
            reserved_at: Timestamp::now(),
        })
    }

    /// Commits a reservation upon successful content write (moves reserved -> used).
    pub fn commit_reservation(&self, reservation: &StorageReservation) {
        self.reserved_bytes
            .fetch_sub(reservation.bytes, Ordering::SeqCst);
        self.used_bytes
            .fetch_add(reservation.bytes, Ordering::SeqCst);
    }

    /// Releases a reservation if the transfer or ingestion is aborted/failed.
    pub fn release_reservation(&self, reservation: &StorageReservation) {
        self.reserved_bytes
            .fetch_sub(reservation.bytes, Ordering::SeqCst);
    }

    /// Records directly allocated bytes (e.g. without prior reservation).
    pub fn record_allocated(&self, bytes: u64) {
        self.used_bytes.fetch_add(bytes, Ordering::SeqCst);
    }

    /// Records freed bytes after deletion or GC.
    pub fn record_freed(&self, bytes: u64) {
        let current = self.used_bytes.load(Ordering::SeqCst);
        let new_val = current.saturating_sub(bytes);
        self.used_bytes.store(new_val, Ordering::SeqCst);
    }

    /// Synchronizes current used storage by querying real bytes from SQLite indexes.
    pub fn sync_usage_from_db(&self, conn: &rusqlite::Connection) -> Result<u64, ContentError> {
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

        let total = (chunk_bytes.max(0) + object_bytes.max(0)) as u64;
        self.used_bytes.store(total, Ordering::SeqCst);

        info!(
            used_bytes = total,
            "Storage quota usage synchronized from database"
        );
        Ok(total)
    }

    /// Returns current quota utilization telemetry.
    pub async fn status(&self) -> QuotaStatus {
        let total_quota = self.config.read().await.total_storage_quota;
        let used = self.used_bytes.load(Ordering::SeqCst);
        let reserved = self.reserved_bytes.load(Ordering::SeqCst);
        let available = total_quota.saturating_sub(used.saturating_add(reserved));
        let usage_percent = if total_quota > 0 {
            ((used + reserved) as f64 / total_quota as f64) * 100.0
        } else {
            0.0
        };

        QuotaStatus {
            used_bytes: used,
            reserved_bytes: reserved,
            available_bytes: available,
            quota_bytes: total_quota,
            usage_percent,
        }
    }
}
