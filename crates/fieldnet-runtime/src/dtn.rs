use fieldnet_core::{MessageId, Timestamp};
use fieldnet_protocol::Message;
use rusqlite::{params, Connection};
use std::path::Path;
use std::sync::Mutex;
use thiserror::Error;
use tracing::{debug, info, warn};

pub const MAX_DTN_QUEUE_CAPACITY: usize = 1000;
pub const MAX_DTN_MESSAGE_BYTES: usize = 1024 * 1024; // 1 MB limit
pub const MAX_DTN_RETRIES: u32 = 10;
pub const INITIAL_BACKOFF_MILLIS: u64 = 500;

#[derive(Error, Debug)]
pub enum DtnError {
    #[error("SQLite database error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Message size {0} exceeds maximum limit {1}")]
    MessageTooLarge(usize, usize),

    #[error("DTN queue capacity exceeded ({0})")]
    QueueFull(usize),
}

pub struct DtnQueue {
    conn: Mutex<Connection>,
}

impl DtnQueue {
    pub fn open(path: &Path) -> Result<Self, DtnError> {
        let conn = Connection::open(path)?;

        // Configure WAL mode for high concurrency and crash resilience
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS dtn_bundles (
                message_id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL,
                destination_id TEXT NOT NULL,
                priority INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                retry_count INTEGER NOT NULL,
                next_retry_at INTEGER NOT NULL,
                serialized_message BLOB NOT NULL,
                status TEXT NOT NULL
            );",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_dtn_priority_retry 
             ON dtn_bundles(status, priority DESC, next_retry_at ASC);",
            [],
        )?;

        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Enqueues a message into persistent storage.
    pub fn enqueue(&self, message: &Message) -> Result<(), DtnError> {
        let serialized = serde_json::to_vec(message)?;
        if serialized.len() > MAX_DTN_MESSAGE_BYTES {
            return Err(DtnError::MessageTooLarge(
                serialized.len(),
                MAX_DTN_MESSAGE_BYTES,
            ));
        }

        let conn = self.conn.lock().unwrap();

        let count: usize = conn.query_row(
            "SELECT COUNT(*) FROM dtn_bundles WHERE status = 'QUEUED'",
            [],
            |r| r.get(0),
        )?;

        if count >= MAX_DTN_QUEUE_CAPACITY {
            // Attempt to evict expired bundles first
            let now = Timestamp::now().as_millis();
            let evicted = conn.execute(
                "DELETE FROM dtn_bundles WHERE status = 'QUEUED' AND expires_at <= ?1",
                params![now],
            )?;
            if evicted == 0 {
                return Err(DtnError::QueueFull(MAX_DTN_QUEUE_CAPACITY));
            }
        }

        let now = Timestamp::now().as_millis();
        conn.execute(
            "INSERT OR REPLACE INTO dtn_bundles (
                message_id, source_id, destination_id, priority, created_at, expires_at,
                retry_count, next_retry_at, serialized_message, status
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'QUEUED')",
            params![
                message.message_id.to_string(),
                message.source.to_string(),
                message.destination.to_string(),
                message.priority as u8,
                message.created_at,
                message.expires_at,
                0,
                now,
                serialized,
            ],
        )?;

        info!(
            "DTN Queue: persistently stored bundle {} for destination {} (priority: {:?})",
            message.message_id, message.destination, message.priority
        );
        Ok(())
    }

    /// Removes a confirmed/delivered bundle from persistent storage.
    pub fn remove(&self, message_id: &MessageId) -> Result<bool, DtnError> {
        let conn = self.conn.lock().unwrap();
        let rows = conn.execute(
            "DELETE FROM dtn_bundles WHERE message_id = ?1",
            params![message_id.to_string()],
        )?;
        Ok(rows > 0)
    }

    /// Retrieves runnable messages ordered by priority and retry schedule.
    pub fn get_runnable(&self, now: u64, limit: usize) -> Result<Vec<Message>, DtnError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT serialized_message FROM dtn_bundles 
             WHERE status = 'QUEUED' AND next_retry_at <= ?1 AND expires_at > ?1
             ORDER BY priority DESC, next_retry_at ASC LIMIT ?2",
        )?;

        let rows = stmt.query_map(params![now, limit], |row| {
            let blob: Vec<u8> = row.get(0)?;
            Ok(blob)
        })?;

        let mut messages = Vec::new();
        for blob_res in rows {
            let blob = blob_res?;
            if let Ok(msg) = serde_json::from_slice::<Message>(&blob) {
                messages.push(msg);
            }
        }

        Ok(messages)
    }

    /// Records a retry failure, applying exponential backoff or marking as failed.
    pub fn record_retry_failure(&self, message_id: &MessageId) -> Result<(), DtnError> {
        let conn = self.conn.lock().unwrap();
        let now = Timestamp::now().as_millis();

        let retry_info: Option<(u32, u64)> = conn
            .query_row(
                "SELECT retry_count, expires_at FROM dtn_bundles WHERE message_id = ?1",
                params![message_id.to_string()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .ok();

        if let Some((retries, expires_at)) = retry_info {
            let next_retries = retries + 1;
            if next_retries >= MAX_DTN_RETRIES || now >= expires_at {
                warn!(
                    "DTN Queue: bundle {} reached max retries ({}) or expired, marking FAILED",
                    message_id, next_retries
                );
                conn.execute(
                    "UPDATE dtn_bundles SET status = 'FAILED', retry_count = ?1 WHERE message_id = ?2",
                    params![next_retries, message_id.to_string()],
                )?;
            } else {
                let delay = INITIAL_BACKOFF_MILLIS.saturating_mul(1 << next_retries.min(6));
                let next_retry_at = now.saturating_add(delay);
                debug!(
                    "DTN Queue: scheduled retry {} for bundle {} in {}ms",
                    next_retries, message_id, delay
                );
                conn.execute(
                    "UPDATE dtn_bundles SET retry_count = ?1, next_retry_at = ?2 WHERE message_id = ?3",
                    params![next_retries, next_retry_at, message_id.to_string()],
                )?;
            }
        }

        Ok(())
    }

    /// Purges all expired messages.
    pub fn purge_expired(&self, now: u64) -> Result<usize, DtnError> {
        let conn = self.conn.lock().unwrap();
        let purged = conn.execute(
            "DELETE FROM dtn_bundles WHERE expires_at <= ?1",
            params![now],
        )?;
        if purged > 0 {
            debug!("DTN Queue: purged {} expired bundles", purged);
        }
        Ok(purged)
    }

    /// Checks if a bundle ID is in the queue.
    pub fn contains(&self, message_id: &MessageId) -> Result<bool, DtnError> {
        let conn = self.conn.lock().unwrap();
        let count: usize = conn.query_row(
            "SELECT COUNT(*) FROM dtn_bundles WHERE message_id = ?1 AND status = 'QUEUED'",
            params![message_id.to_string()],
            |r| r.get(0),
        )?;
        Ok(count > 0)
    }

    /// Total count of queued bundles.
    pub fn count(&self) -> Result<usize, DtnError> {
        let conn = self.conn.lock().unwrap();
        let count: usize = conn.query_row(
            "SELECT COUNT(*) FROM dtn_bundles WHERE status = 'QUEUED'",
            [],
            |r| r.get(0),
        )?;
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fieldnet_core::{NodeId, Priority};
    use fieldnet_identity::NodeIdentity;
    use fieldnet_protocol::MessageType;

    #[test]
    fn test_dtn_queue_persistence_and_priority() {
        let temp_dir = std::env::temp_dir().join("fieldnet_dtn_test");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let db_path = temp_dir.join("dtn_queue.db");
        let queue = DtnQueue::open(&db_path).unwrap();

        let id = NodeIdentity::generate();
        let dest = NodeId::from_bytes([9u8; 32]);

        // Message 1: Normal priority
        let msg_normal = Message::sign_and_build(
            &id,
            dest,
            MessageType::Data,
            b"Normal Message".to_vec(),
            16,
            60_000,
            1,
            Priority::Normal,
        )
        .unwrap();

        // Message 2: Critical priority
        let msg_crit = Message::sign_and_build(
            &id,
            dest,
            MessageType::Data,
            b"Critical Message".to_vec(),
            16,
            60_000,
            2,
            Priority::Critical,
        )
        .unwrap();

        queue.enqueue(&msg_normal).unwrap();
        queue.enqueue(&msg_crit).unwrap();

        assert_eq!(queue.count().unwrap(), 2);

        // Fetch runnable: Critical must precede Normal
        let now = Timestamp::now().as_millis();
        let runnable = queue.get_runnable(now + 10, 10).unwrap();
        assert_eq!(runnable.len(), 2);
        assert_eq!(runnable[0].message_id, msg_crit.message_id);
        assert_eq!(runnable[1].message_id, msg_normal.message_id);

        // Remove critical
        assert!(queue.remove(&msg_crit.message_id).unwrap());
        assert_eq!(queue.count().unwrap(), 1);

        // Close and reopen database: verifies persistence across restart!
        drop(queue);
        let queue_reopened = DtnQueue::open(&db_path).unwrap();
        assert_eq!(queue_reopened.count().unwrap(), 1);
        assert!(queue_reopened.contains(&msg_normal.message_id).unwrap());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
