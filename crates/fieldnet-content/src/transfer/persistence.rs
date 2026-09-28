use rusqlite::{params, Connection, OptionalExtension};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tracing::debug;

use fieldnet_core::{NodeId, Priority};

use super::protocol::TransferId;
use super::session::TransferSession;
use super::state::TransferState;
use crate::error::ContentError;
use crate::identity::ContentId;
use crate::manifest::id::ManifestId;

/// Persistent SQLite store for transfer session state in `content.db`.
pub struct TransferPersistence {
    conn: Arc<Mutex<Connection>>,
}

impl TransferPersistence {
    /// Creates a new `TransferPersistence` instance wrapping the shared SQLite connection.
    pub fn new(conn: Arc<Mutex<Connection>>) -> Result<Self, ContentError> {
        let instance = Self { conn };
        instance.init_schema()?;
        Ok(instance)
    }

    /// Initializes the `content_transfers` table and indices.
    pub fn init_schema(&self) -> Result<(), ContentError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;

        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS content_transfers (
                transfer_id TEXT PRIMARY KEY,
                content_id TEXT NOT NULL,
                manifest_id TEXT,
                requester_node_id TEXT NOT NULL,
                provider_node_id TEXT NOT NULL,
                status TEXT NOT NULL,
                priority TEXT NOT NULL,
                verified_chunks_json TEXT NOT NULL,
                failed_chunks_json TEXT NOT NULL,
                total_chunks INTEGER,
                total_bytes INTEGER,
                verified_bytes INTEGER NOT NULL DEFAULT 0,
                retry_count INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                last_activity_at INTEGER NOT NULL,
                error_message TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_transfers_content ON content_transfers(content_id);
            CREATE INDEX IF NOT EXISTS idx_transfers_status ON content_transfers(status);
            "#,
        )?;

        debug!("TransferPersistence schema initialized");
        Ok(())
    }

    /// Persists or updates the complete state of a transfer session.
    pub fn save(&self, session: &TransferSession) -> Result<(), ContentError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;

        let verified_chunks_vec: Vec<u32> = session.verified_chunks.iter().copied().collect();
        let verified_json = serde_json::to_string(&verified_chunks_vec).map_err(|e| {
            ContentError::MetadataFailure(format!("Failed to serialize verified chunks: {}", e))
        })?;

        let failed_json = serde_json::to_string(&session.failed_chunks).map_err(|e| {
            ContentError::MetadataFailure(format!("Failed to serialize failed chunks: {}", e))
        })?;

        let priority_str = match session.priority {
            Priority::Low => "LOW",
            Priority::Normal => "NORMAL",
            Priority::High => "HIGH",
            Priority::Critical => "CRITICAL",
        };

        conn.execute(
            r#"
            INSERT INTO content_transfers (
                transfer_id, content_id, manifest_id, requester_node_id, provider_node_id,
                status, priority, verified_chunks_json, failed_chunks_json,
                total_chunks, total_bytes, verified_bytes, retry_count,
                created_at, updated_at, expires_at, last_activity_at, error_message
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)
            ON CONFLICT(transfer_id) DO UPDATE SET
                status = excluded.status,
                manifest_id = excluded.manifest_id,
                verified_chunks_json = excluded.verified_chunks_json,
                failed_chunks_json = excluded.failed_chunks_json,
                total_chunks = excluded.total_chunks,
                total_bytes = excluded.total_bytes,
                verified_bytes = excluded.verified_bytes,
                retry_count = excluded.retry_count,
                updated_at = excluded.updated_at,
                expires_at = excluded.expires_at,
                last_activity_at = excluded.last_activity_at,
                error_message = excluded.error_message
            "#,
            params![
                session.transfer_id.to_hex(),
                session.content_id.to_hex(),
                session.manifest_id.map(|m| m.to_hex()),
                session.requester_node_id.to_hex(),
                session.provider_node_id.to_hex(),
                session.status.as_str(),
                priority_str,
                verified_json,
                failed_json,
                session.total_chunks.map(|t| t as i64),
                session.total_bytes.map(|b| b as i64),
                session.verified_bytes as i64,
                session.retry_count as i64,
                session.created_at as i64,
                session.updated_at as i64,
                session.expires_at as i64,
                session.last_activity_at as i64,
                session.error_message,
            ],
        )?;

        Ok(())
    }

    /// Loads a specific transfer session by `TransferId`.
    pub fn load(&self, transfer_id: &TransferId) -> Result<Option<TransferSession>, ContentError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;

        let row = conn
            .query_row(
                r#"
                SELECT
                    transfer_id, content_id, manifest_id, requester_node_id, provider_node_id,
                    status, priority, verified_chunks_json, failed_chunks_json,
                    total_chunks, total_bytes, verified_bytes, retry_count,
                    created_at, updated_at, expires_at, last_activity_at, error_message
                FROM content_transfers
                WHERE transfer_id = ?1
                "#,
                params![transfer_id.to_hex()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, String>(8)?,
                        row.get::<_, Option<i64>>(9)?,
                        row.get::<_, Option<i64>>(10)?,
                        row.get::<_, i64>(11)?,
                        row.get::<_, i64>(12)?,
                        row.get::<_, i64>(13)?,
                        row.get::<_, i64>(14)?,
                        row.get::<_, i64>(15)?,
                        row.get::<_, i64>(16)?,
                        row.get::<_, Option<String>>(17)?,
                    ))
                },
            )
            .optional()?;

        let Some(data) = row else {
            return Ok(None);
        };

        Self::parse_row(data).map(Some)
    }

    /// Loads all transfer sessions regardless of state.
    pub fn load_all(&self) -> Result<Vec<TransferSession>, ContentError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;

        let mut stmt = conn.prepare(
            r#"
            SELECT
                transfer_id, content_id, manifest_id, requester_node_id, provider_node_id,
                status, priority, verified_chunks_json, failed_chunks_json,
                total_chunks, total_bytes, verified_bytes, retry_count,
                created_at, updated_at, expires_at, last_activity_at, error_message
            FROM content_transfers
            ORDER BY created_at DESC
            "#,
        )?;

        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, String>(8)?,
                row.get::<_, Option<i64>>(9)?,
                row.get::<_, Option<i64>>(10)?,
                row.get::<_, i64>(11)?,
                row.get::<_, i64>(12)?,
                row.get::<_, i64>(13)?,
                row.get::<_, i64>(14)?,
                row.get::<_, i64>(15)?,
                row.get::<_, i64>(16)?,
                row.get::<_, Option<String>>(17)?,
            ))
        })?;

        let mut sessions = Vec::new();
        for row in rows {
            let data = row?;
            sessions.push(Self::parse_row(data)?);
        }

        Ok(sessions)
    }

    /// Loads all active (incomplete/non-terminal) transfer sessions for recovery.
    pub fn load_active(&self) -> Result<Vec<TransferSession>, ContentError> {
        let all = self.load_all()?;
        Ok(all
            .into_iter()
            .filter(|s| !s.status.is_terminal())
            .collect())
    }

    /// Deletes a transfer record from the database.
    pub fn delete(&self, transfer_id: &TransferId) -> Result<(), ContentError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;

        conn.execute(
            "DELETE FROM content_transfers WHERE transfer_id = ?1",
            params![transfer_id.to_hex()],
        )?;
        Ok(())
    }

    #[allow(clippy::type_complexity)]
    fn parse_row(
        data: (
            String,
            String,
            Option<String>,
            String,
            String,
            String,
            String,
            String,
            String,
            Option<i64>,
            Option<i64>,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            Option<String>,
        ),
    ) -> Result<TransferSession, ContentError> {
        let (
            tid_str,
            cid_str,
            mid_str,
            req_str,
            prov_str,
            status_str,
            prio_str,
            verified_json,
            failed_json,
            total_chunks,
            total_bytes,
            verified_bytes,
            retry_count,
            created_at,
            updated_at,
            expires_at,
            last_activity_at,
            error_message,
        ) = data;

        let transfer_id = TransferId::from_hex(&tid_str)?;
        let content_id = ContentId::from_hex(&cid_str)?;
        let manifest_id = mid_str.as_deref().map(ManifestId::from_hex).transpose()?;
        let requester_node_id = req_str.parse::<NodeId>().map_err(|e| {
            ContentError::StorageFailure(format!("Invalid requester NodeId: {}", e))
        })?;
        let provider_node_id = prov_str
            .parse::<NodeId>()
            .map_err(|e| ContentError::StorageFailure(format!("Invalid provider NodeId: {}", e)))?;

        let status = TransferState::parse(&status_str).unwrap_or(TransferState::Failed);
        let priority = match prio_str.to_ascii_uppercase().as_str() {
            "LOW" => Priority::Low,
            "HIGH" => Priority::High,
            "CRITICAL" | "URGENT" => Priority::Critical,
            _ => Priority::Normal,
        };

        let verified_vec: Vec<u32> = serde_json::from_str(&verified_json).unwrap_or_default();
        let verified_chunks: HashSet<u32> = verified_vec.into_iter().collect();

        let failed_chunks: HashMap<u32, u8> =
            serde_json::from_str(&failed_json).unwrap_or_default();

        Ok(TransferSession {
            transfer_id,
            content_id,
            manifest_id,
            requester_node_id,
            provider_node_id,
            status,
            priority,
            manifest: None,
            verified_chunks,
            in_flight_chunks: HashSet::new(),
            failed_chunks,
            total_chunks: total_chunks.map(|t| t as u32),
            total_bytes: total_bytes.map(|b| b as u64),
            verified_bytes: verified_bytes as u64,
            retry_count: retry_count as u32,
            created_at: created_at as u64,
            updated_at: updated_at as u64,
            expires_at: expires_at as u64,
            last_activity_at: last_activity_at as u64,
            error_message,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_persistence_crud_and_recovery() {
        let conn = Connection::open_in_memory().unwrap();
        let shared = Arc::new(Mutex::new(conn));
        let persistence = TransferPersistence::new(shared).unwrap();

        let tid = TransferId::random();
        let cid = ContentId::from_bytes([9u8; 32]);
        let req = NodeId::from_bytes([1u8; 32]);
        let prov = NodeId::from_bytes([2u8; 32]);

        let mut session = TransferSession::new(tid, cid, req, prov, Priority::High, 600);
        session.transition_to(TransferState::Requested).unwrap();
        session.transition_to(TransferState::Negotiating).unwrap();
        session.transition_to(TransferState::Transferring).unwrap();
        session.total_chunks = Some(10);
        session.total_bytes = Some(10240);
        session.mark_chunk_verified(0, 1024);
        session.mark_chunk_verified(1, 1024);

        persistence.save(&session).unwrap();

        let loaded = persistence.load(&tid).unwrap().expect("Session not found");
        assert_eq!(loaded.transfer_id, tid);
        assert_eq!(loaded.content_id, cid);
        assert_eq!(loaded.status, TransferState::Transferring);
        assert_eq!(loaded.verified_chunks.len(), 2);
        assert!(loaded.verified_chunks.contains(&0));
        assert!(loaded.verified_chunks.contains(&1));
        assert_eq!(loaded.verified_bytes, 2048);

        let active = persistence.load_active().unwrap();
        assert_eq!(active.len(), 1);

        persistence.delete(&tid).unwrap();
        assert!(persistence.load(&tid).unwrap().is_none());
    }
}
