use rusqlite::{params, Connection};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use tracing::{info, warn};

use fieldnet_core::{NodeId, Timestamp};

use super::advertisement::ContentAdvertise;
use super::record::{ProviderRecord, ProviderState, TrustTier};
use crate::error::ContentError;
use crate::events::ContentEvent;
use crate::identity::ContentId;

/// Configurable resource and capacity bounds for the `ProviderRegistry`.
#[derive(Clone, Debug)]
pub struct ProviderRegistryConfig {
    /// Maximum number of distinct providers tracked per content object (default 32).
    pub max_providers_per_content: usize,
    /// Maximum number of distinct content objects tracked in the registry (default 10,000).
    pub max_tracked_contents: usize,
    /// Maximum number of provider records allowed from any single peer (default 1,000).
    pub max_advertisements_per_peer: usize,
    /// Maximum total provider records across all contents (default 50,000).
    pub max_total_providers: usize,
    /// Default TTL for advertisements in seconds when not specified (default 600s = 10 mins).
    pub default_ttl_seconds: u32,
}

impl Default for ProviderRegistryConfig {
    fn default() -> Self {
        Self {
            max_providers_per_content: 32,
            max_tracked_contents: 10_000,
            max_advertisements_per_peer: 1_000,
            max_total_providers: 50_000,
            default_ttl_seconds: 600,
        }
    }
}

struct RegistryInner {
    /// Index: ContentId -> (Provider NodeId -> ProviderRecord)
    providers: HashMap<ContentId, HashMap<NodeId, ProviderRecord>>,
    /// Quota tracking: Provider NodeId -> Count of active records
    peer_counts: HashMap<NodeId, usize>,
    /// Total count of records currently in memory
    total_records: usize,
}

impl RegistryInner {
    fn new() -> Self {
        Self {
            providers: HashMap::new(),
            peer_counts: HashMap::new(),
            total_records: 0,
        }
    }
}

/// Thread-safe registry tracking discovered remote providers and their chunk availability.
pub struct ProviderRegistry {
    config: ProviderRegistryConfig,
    inner: RwLock<RegistryInner>,
    conn: Option<Arc<Mutex<Connection>>>,
    event_tx: tokio::sync::broadcast::Sender<ContentEvent>,
}

impl ProviderRegistry {
    /// Constructs an in-memory `ProviderRegistry`.
    pub fn new(
        config: ProviderRegistryConfig,
        event_tx: tokio::sync::broadcast::Sender<ContentEvent>,
    ) -> Self {
        Self {
            config,
            inner: RwLock::new(RegistryInner::new()),
            conn: None,
            event_tx,
        }
    }

    /// Constructs a `ProviderRegistry` with optional SQLite cache persistence.
    pub fn with_persistence(
        config: ProviderRegistryConfig,
        conn: Arc<Mutex<Connection>>,
        event_tx: tokio::sync::broadcast::Sender<ContentEvent>,
    ) -> Result<Self, ContentError> {
        {
            let c = conn
                .lock()
                .map_err(|e| ContentError::LockError(e.to_string()))?;
            c.execute_batch(
                "CREATE TABLE IF NOT EXISTS provider_cache (
                    content_id TEXT NOT NULL,
                    provider_node_id TEXT NOT NULL,
                    record_json TEXT NOT NULL,
                    expires_at INTEGER NOT NULL,
                    PRIMARY KEY (content_id, provider_node_id)
                );
                CREATE INDEX IF NOT EXISTS idx_provider_cache_cid ON provider_cache(content_id);
                CREATE INDEX IF NOT EXISTS idx_provider_cache_expires ON provider_cache(expires_at);",
            )?;
        }

        let registry = Self {
            config,
            inner: RwLock::new(RegistryInner::new()),
            conn: Some(conn),
            event_tx,
        };

        let loaded = registry.load_from_db()?;
        if loaded > 0 {
            info!(
                loaded,
                "Loaded cached provider records from persistent database"
            );
        }

        Ok(registry)
    }

    /// Ingests or updates a `ProviderRecord` in the registry.
    pub fn register(&self, mut record: ProviderRecord) -> Result<(), ContentError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| ContentError::LockError(e.to_string()))?;

        // 1. Check peer quota
        let current_peer_count = inner
            .peer_counts
            .get(&record.provider_node_id)
            .copied()
            .unwrap_or(0);
        let exists = inner
            .providers
            .get(&record.content_id)
            .map(|p| p.contains_key(&record.provider_node_id))
            .unwrap_or(false);

        if !exists {
            if current_peer_count >= self.config.max_advertisements_per_peer {
                return Err(ContentError::RegistryCapacityExceeded(format!(
                    "Peer {} reached max advertisement quota ({})",
                    record.provider_node_id, self.config.max_advertisements_per_peer
                )));
            }

            if inner.total_records >= self.config.max_total_providers {
                // Evict an expired or oldest record
                drop(inner);
                self.expire(Timestamp::now());
                inner = self
                    .inner
                    .write()
                    .map_err(|e| ContentError::LockError(e.to_string()))?;

                if inner.total_records >= self.config.max_total_providers {
                    return Err(ContentError::RegistryCapacityExceeded(format!(
                        "Provider registry reached maximum capacity ({})",
                        self.config.max_total_providers
                    )));
                }
            }

            if !inner.providers.contains_key(&record.content_id)
                && inner.providers.len() >= self.config.max_tracked_contents
            {
                return Err(ContentError::RegistryCapacityExceeded(format!(
                    "Maximum tracked content objects reached ({})",
                    self.config.max_tracked_contents
                )));
            }
        }

        if !exists {
            let current_count = inner
                .providers
                .get(&record.content_id)
                .map(|m| m.len())
                .unwrap_or(0);
            if current_count >= self.config.max_providers_per_content {
                return Err(ContentError::RegistryCapacityExceeded(format!(
                    "Max providers ({}) reached for content {}",
                    self.config.max_providers_per_content, record.content_id
                )));
            }
        }

        let is_new = match inner.providers.get(&record.content_id) {
            Some(map) => !map.contains_key(&record.provider_node_id),
            None => true,
        };

        if !is_new {
            // Preserve historical telemetry
            if let Some(existing) = inner
                .providers
                .get(&record.content_id)
                .and_then(|m| m.get(&record.provider_node_id))
            {
                record.observed_rtt_ms = existing.observed_rtt_ms;
                record.reliability_permille = existing.reliability_permille;
                if existing.availability_state == ProviderState::Banned {
                    record.availability_state = ProviderState::Banned;
                }
            }
        } else {
            *inner
                .peer_counts
                .entry(record.provider_node_id)
                .or_insert(0) += 1;
            inner.total_records += 1;
        }

        let cid = record.content_id;
        let pid = record.provider_node_id;
        let has_full = record.has_full_content;
        let state = record.availability_state;

        inner
            .providers
            .entry(cid)
            .or_default()
            .insert(pid, record.clone());

        // Persist to SQLite cache if available
        if let Some(conn) = &self.conn {
            if let Ok(c) = conn.lock() {
                if let Ok(json) = serde_json::to_string(&record) {
                    let _ = c.execute(
                        "INSERT INTO provider_cache (content_id, provider_node_id, record_json, expires_at)
                         VALUES (?1, ?2, ?3, ?4)
                         ON CONFLICT(content_id, provider_node_id) DO UPDATE SET
                         record_json = excluded.record_json,
                         expires_at = excluded.expires_at",
                        params![cid.to_hex(), pid.to_hex(), json, record.expires_at.as_millis() as i64],
                    );
                }
            }
        }

        // Emit events
        if is_new {
            let _ = self.event_tx.send(ContentEvent::ProviderDiscovered {
                content_id: cid,
                provider_node_id: pid,
                has_full_content: has_full,
            });
        } else {
            let _ = self.event_tx.send(ContentEvent::ProviderUpdated {
                content_id: cid,
                provider_node_id: pid,
                state,
            });
        }

        Ok(())
    }

    /// Ingests and cryptographically validates a `ContentAdvertise` message.
    pub fn handle_advertisement(
        &self,
        adv: &ContentAdvertise,
        trust_tier: TrustTier,
    ) -> Result<(), ContentError> {
        let now = Timestamp::now();
        adv.validate(now)?;

        let mut record = ProviderRecord::new(
            adv.content_id,
            adv.provider_node_id,
            adv.has_full_content,
            adv.available_chunks.clone(),
            adv.ttl_seconds,
        );
        record.total_chunks = adv.total_chunks;
        record.content_size = adv.content_size;
        record.advertised_at = adv.created_at;
        record.expires_at = adv.expires_at;
        record.sequence = adv.sequence;
        record.trust_tier = trust_tier;

        self.register(record)
    }

    /// Queries a single provider record by `ContentId` and `provider_node_id`.
    pub fn get(&self, content_id: &ContentId, provider_node_id: &NodeId) -> Option<ProviderRecord> {
        let inner = self.inner.read().ok()?;
        inner
            .providers
            .get(content_id)?
            .get(provider_node_id)
            .cloned()
    }

    /// Finds candidate providers for a given `ContentId`, sorted by utility.
    ///
    /// Filters out `Expired` and `Banned` providers.
    /// Sorts candidates: `Active` > `Degraded` > `Stale`, higher reliability first, lowest RTT first.
    pub fn find_providers(&self, content_id: &ContentId) -> Vec<ProviderRecord> {
        let now = Timestamp::now();
        let inner = match self.inner.read() {
            Ok(guard) => guard,
            Err(_) => return Vec::new(),
        };

        let providers_map = match inner.providers.get(content_id) {
            Some(map) => map,
            None => return Vec::new(),
        };

        let mut list: Vec<ProviderRecord> = providers_map
            .values()
            .filter(|p| !p.is_expired(now) && p.availability_state.is_usable())
            .cloned()
            .collect();

        // Sort by utility
        list.sort_by(|a, b| {
            let state_rank = |s: ProviderState| match s {
                ProviderState::Active => 3,
                ProviderState::Degraded => 2,
                ProviderState::Stale => 1,
                _ => 0,
            };
            state_rank(b.availability_state)
                .cmp(&state_rank(a.availability_state))
                .then_with(|| b.reliability_permille.cmp(&a.reliability_permille))
                .then_with(|| a.observed_rtt_ms.cmp(&b.observed_rtt_ms))
        });

        list
    }

    /// Finds candidate providers that advertise possession of at least one chunk in `missing_chunks`.
    pub fn find_providers_for_chunks(
        &self,
        content_id: &ContentId,
        missing_chunks: &[u32],
    ) -> Vec<ProviderRecord> {
        let candidates = self.find_providers(content_id);
        if missing_chunks.is_empty() {
            return candidates;
        }

        candidates
            .into_iter()
            .filter(|p| missing_chunks.iter().any(|&c| p.has_chunk(c)))
            .collect()
    }

    /// Updates a provider record using a mutable callback.
    pub fn update(
        &self,
        content_id: &ContentId,
        provider_node_id: &NodeId,
        f: impl FnOnce(&mut ProviderRecord),
    ) -> Result<(), ContentError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| ContentError::LockError(e.to_string()))?;
        let entry = inner
            .providers
            .get_mut(content_id)
            .and_then(|p| p.get_mut(provider_node_id))
            .ok_or(ContentError::ProviderNotFound {
                content_id: *content_id,
                provider: *provider_node_id,
            })?;

        f(entry);
        let updated = entry.clone();

        // Update persistent cache
        if let Some(conn) = &self.conn {
            if let Ok(c) = conn.lock() {
                if let Ok(json) = serde_json::to_string(&updated) {
                    let _ = c.execute(
                        "INSERT INTO provider_cache (content_id, provider_node_id, record_json, expires_at)
                         VALUES (?1, ?2, ?3, ?4)
                         ON CONFLICT(content_id, provider_node_id) DO UPDATE SET
                         record_json = excluded.record_json,
                         expires_at = excluded.expires_at",
                        params![
                            content_id.to_hex(),
                            provider_node_id.to_hex(),
                            json,
                            updated.expires_at.as_millis() as i64
                        ],
                    );
                }
            }
        }

        let _ = self.event_tx.send(ContentEvent::ProviderUpdated {
            content_id: *content_id,
            provider_node_id: *provider_node_id,
            state: updated.availability_state,
        });

        Ok(())
    }

    /// Removes a provider record manually from the registry.
    pub fn remove(&self, content_id: &ContentId, provider_node_id: &NodeId) -> bool {
        let mut inner = match self.inner.write() {
            Ok(guard) => guard,
            Err(_) => return false,
        };

        let removed = if let Some(entry) = inner.providers.get_mut(content_id) {
            entry.remove(provider_node_id)
        } else {
            None
        };

        if removed.is_some() {
            if let Some(count) = inner.peer_counts.get_mut(provider_node_id) {
                *count = count.saturating_sub(1);
            }
            inner.total_records = inner.total_records.saturating_sub(1);

            let is_empty = inner
                .providers
                .get(content_id)
                .map(|e| e.is_empty())
                .unwrap_or(false);
            if is_empty {
                inner.providers.remove(content_id);
            }

            if let Some(conn) = &self.conn {
                if let Ok(c) = conn.lock() {
                    let _ = c.execute(
                        "DELETE FROM provider_cache WHERE content_id = ?1 AND provider_node_id = ?2",
                        params![content_id.to_hex(), provider_node_id.to_hex()],
                    );
                }
            }

            return true;
        }
        false
    }

    /// Sweeps through the registry, removing records that have exceeded their hard `expires_at`.
    pub fn expire(&self, now: Timestamp) -> usize {
        let mut inner = match self.inner.write() {
            Ok(guard) => guard,
            Err(_) => return 0,
        };

        let mut expired_list = Vec::new();

        for (cid, providers_map) in inner.providers.iter_mut() {
            let to_remove: Vec<NodeId> = providers_map
                .iter()
                .filter(|(_, r)| r.is_expired(now))
                .map(|(&pid, _)| pid)
                .collect();

            for pid in to_remove {
                providers_map.remove(&pid);
                expired_list.push((*cid, pid));
            }
        }

        for (cid, pid) in &expired_list {
            if let Some(count) = inner.peer_counts.get_mut(pid) {
                *count = count.saturating_sub(1);
            }
            inner.total_records = inner.total_records.saturating_sub(1);

            let _ = self.event_tx.send(ContentEvent::ProviderExpired {
                content_id: *cid,
                provider_node_id: *pid,
            });
        }

        inner.providers.retain(|_, map| !map.is_empty());

        // Clear expired rows from SQLite cache
        if let Some(conn) = &self.conn {
            if let Ok(c) = conn.lock() {
                let _ = c.execute(
                    "DELETE FROM provider_cache WHERE expires_at <= ?1",
                    params![now.as_millis() as i64],
                );
            }
        }

        expired_list.len()
    }

    /// Marks a provider as `Banned` or invalidated due to corrupted data delivery or invalid claims.
    pub fn invalidate(
        &self,
        content_id: &ContentId,
        provider_node_id: &NodeId,
        reason: &str,
    ) -> bool {
        let res = self.update(content_id, provider_node_id, |r| {
            r.availability_state = ProviderState::Banned;
            r.reliability_permille = 0;
        });

        if res.is_ok() {
            warn!(
                content_id = %content_id,
                provider = %provider_node_id,
                reason = %reason,
                "Provider explicitly invalidated and marked Banned"
            );
            let _ = self.event_tx.send(ContentEvent::ProviderInvalidated {
                content_id: *content_id,
                provider_node_id: *provider_node_id,
                reason: reason.to_string(),
            });
            true
        } else {
            false
        }
    }

    /// Handles peer expiration from discovery: marks all provider records for that peer as `Stale`.
    pub fn handle_peer_expired(&self, peer_id: &NodeId) -> usize {
        let mut inner = match self.inner.write() {
            Ok(guard) => guard,
            Err(_) => return 0,
        };

        let mut transitioned = 0;
        for (cid, providers_map) in inner.providers.iter_mut() {
            if let Some(record) = providers_map.get_mut(peer_id) {
                if record.availability_state == ProviderState::Active
                    || record.availability_state == ProviderState::Degraded
                {
                    record.availability_state = ProviderState::Stale;
                    transitioned += 1;
                    let _ = self.event_tx.send(ContentEvent::ProviderUpdated {
                        content_id: *cid,
                        provider_node_id: *peer_id,
                        state: ProviderState::Stale,
                    });
                }
            }
        }

        transitioned
    }

    /// Handles peer reappearance: restores `Stale` records for this peer back to `Active` if unexpired.
    pub fn handle_peer_discovered(&self, peer_id: &NodeId) -> usize {
        let now = Timestamp::now();
        let mut inner = match self.inner.write() {
            Ok(guard) => guard,
            Err(_) => return 0,
        };

        let mut restored = 0;
        for (cid, providers_map) in inner.providers.iter_mut() {
            if let Some(record) = providers_map.get_mut(peer_id) {
                if record.availability_state == ProviderState::Stale && !record.is_expired(now) {
                    record.availability_state = ProviderState::Active;
                    record.last_seen_at = now;
                    restored += 1;
                    let _ = self.event_tx.send(ContentEvent::ProviderUpdated {
                        content_id: *cid,
                        provider_node_id: *peer_id,
                        state: ProviderState::Active,
                    });
                }
            }
        }

        restored
    }

    /// Loads unexpired cached provider records from the SQLite database on startup.
    fn load_from_db(&self) -> Result<usize, ContentError> {
        let conn = match &self.conn {
            Some(c) => c,
            None => return Ok(0),
        };

        let now = Timestamp::now().as_millis() as i64;
        let c = conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;

        let mut stmt = c.prepare("SELECT record_json FROM provider_cache WHERE expires_at > ?1")?;

        let rows = stmt.query_map(params![now], |row| {
            let json: String = row.get(0)?;
            Ok(json)
        })?;

        let mut inner = self
            .inner
            .write()
            .map_err(|e| ContentError::LockError(e.to_string()))?;
        let mut count = 0;

        for json in rows.flatten() {
            if let Ok(mut record) = serde_json::from_str::<ProviderRecord>(&json) {
                // On restart, remote provider records are marked Stale until fresh re-discovery/ping
                record.availability_state = ProviderState::Stale;
                let cid = record.content_id;
                let pid = record.provider_node_id;

                let is_new = match inner.providers.get(&cid) {
                    Some(m) => !m.contains_key(&pid),
                    None => true,
                };
                if is_new {
                    *inner.peer_counts.entry(pid).or_insert(0) += 1;
                    inner.total_records += 1;
                    count += 1;
                }
                inner.providers.entry(cid).or_default().insert(pid, record);
            }
        }

        Ok(count)
    }

    /// Returns total active provider records across all contents.
    pub fn total_records_count(&self) -> usize {
        self.inner.read().map(|i| i.total_records).unwrap_or(0)
    }

    /// Returns the number of distinct contents tracked.
    pub fn tracked_contents_count(&self) -> usize {
        self.inner.read().map(|i| i.providers.len()).unwrap_or(0)
    }
}
