use fieldnet_core::Timestamp;
use rusqlite::{params, Connection};
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tracing::{error, warn};

use crate::error::ServiceError;
use crate::model::{ServiceInstanceId, ServiceManifest};

/// Persistent SQLite storage engine for service manifests.
pub struct ServiceStore {
    conn: Arc<Mutex<Connection>>,
}

impl ServiceStore {
    /// Opens or creates the SQLite store at `db_path`.
    ///
    /// Includes automated quick_check and corruption recovery: if the database
    /// is detected to be corrupt, it is archived to `.corrupt.<timestamp>` and a fresh database is initialized.
    pub fn open(db_path: &Path) -> Result<Arc<Self>, ServiceError> {
        if let Some(parent) = db_path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                ServiceError::PersistenceError(format!(
                    "Failed to create database directory: {}",
                    e
                ))
            })?;
        }

        // Try opening with corruption recovery
        let conn = match Self::open_and_configure(db_path) {
            Ok(c) => c,
            Err(e) => {
                warn!(
                    "SQLite open failed or database corrupt ({}); attempting quarantine and recreate",
                    e
                );
                let timestamp = Timestamp::now().as_millis();
                let corrupt_path = db_path.with_extension(format!("corrupt.{}", timestamp));
                let _ = fs::rename(db_path, &corrupt_path);
                // Also remove -wal and -shm files if they exist
                let _ = fs::remove_file(db_path.with_extension("db-wal"));
                let _ = fs::remove_file(db_path.with_extension("db-shm"));

                Self::open_and_configure(db_path)?
            }
        };

        Ok(Arc::new(Self {
            conn: Arc::new(Mutex::new(conn)),
        }))
    }

    /// Opens an in-memory SQLite store (primarily for unit tests).
    pub fn open_in_memory() -> Result<Arc<Self>, ServiceError> {
        let conn = Connection::open_in_memory()?;
        Self::configure_connection(&conn)?;
        Self::init_schema(&conn)?;

        Ok(Arc::new(Self {
            conn: Arc::new(Mutex::new(conn)),
        }))
    }

    fn open_and_configure(path: &Path) -> Result<Connection, ServiceError> {
        let conn = Connection::open(path)?;
        Self::configure_connection(&conn)?;

        // Run integrity quick check
        {
            let mut check_stmt = conn.prepare("PRAGMA quick_check")?;
            let check_result: String = check_stmt.query_row([], |row| row.get(0))?;
            if check_result != "ok" {
                return Err(ServiceError::PersistenceError(format!(
                    "SQLite integrity check failed: {}",
                    check_result
                )));
            }
        }

        Self::init_schema(&conn)?;
        Ok(conn)
    }

    fn configure_connection(conn: &Connection) -> Result<(), ServiceError> {
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.pragma_update(None, "busy_timeout", 5000)?;
        Ok(())
    }

    fn init_schema(conn: &Connection) -> Result<(), ServiceError> {
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS local_services (
                instance_id TEXT PRIMARY KEY,
                service_id TEXT NOT NULL,
                manifest_json TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_local_service_id ON local_services(service_id);

            CREATE TABLE IF NOT EXISTS cached_remote_manifests (
                instance_id TEXT PRIMARY KEY,
                service_id TEXT NOT NULL,
                provider_node_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                manifest_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_cached_service_id ON cached_remote_manifests(service_id);
            CREATE INDEX IF NOT EXISTS idx_cached_provider ON cached_remote_manifests(provider_node_id);
            CREATE INDEX IF NOT EXISTS idx_cached_expires ON cached_remote_manifests(expires_at);
            "#,
        )?;
        Ok(())
    }

    /// Stores or replaces a locally declared manifest.
    pub fn save_local_manifest(
        &self,
        manifest: &ServiceManifest,
        now: Timestamp,
    ) -> Result<(), ServiceError> {
        let json = serde_json::to_string(manifest)?;
        let conn = self
            .conn
            .lock()
            .map_err(|e| ServiceError::LockError(e.to_string()))?;

        conn.execute(
            r#"
            INSERT OR REPLACE INTO local_services (instance_id, service_id, manifest_json, updated_at)
            VALUES (?1, ?2, ?3, ?4)
            "#,
            params![
                manifest.instance_id.as_str(),
                manifest.service_id.as_str(),
                json,
                now.as_millis() as i64
            ],
        )?;

        Ok(())
    }

    /// Deletes a local manifest by instance ID.
    pub fn delete_local_manifest(
        &self,
        instance_id: &ServiceInstanceId,
    ) -> Result<(), ServiceError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ServiceError::LockError(e.to_string()))?;

        conn.execute(
            "DELETE FROM local_services WHERE instance_id = ?1",
            params![instance_id.as_str()],
        )?;

        Ok(())
    }

    /// Loads all local manifests from the database on startup.
    pub fn load_all_local_manifests(&self) -> Result<Vec<ServiceManifest>, ServiceError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ServiceError::LockError(e.to_string()))?;

        let mut stmt = conn.prepare("SELECT manifest_json FROM local_services")?;
        let rows = stmt.query_map([], |row| {
            let json: String = row.get(0)?;
            Ok(json)
        })?;

        let mut manifests = Vec::new();
        for row in rows {
            let json = row?;
            match serde_json::from_str::<ServiceManifest>(&json) {
                Ok(m) => manifests.push(m),
                Err(e) => {
                    error!("Failed to deserialize local manifest from SQLite: {}", e);
                }
            }
        }

        Ok(manifests)
    }

    /// Caches a remote manifest received from mesh discovery.
    pub fn save_cached_manifest(&self, manifest: &ServiceManifest) -> Result<(), ServiceError> {
        let json = serde_json::to_string(manifest)?;
        let conn = self
            .conn
            .lock()
            .map_err(|e| ServiceError::LockError(e.to_string()))?;

        conn.execute(
            r#"
            INSERT OR REPLACE INTO cached_remote_manifests
                (instance_id, service_id, provider_node_id, sequence, expires_at, manifest_json)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            "#,
            params![
                manifest.instance_id.as_str(),
                manifest.service_id.as_str(),
                manifest.provider_node_id.to_hex(),
                manifest.sequence as i64,
                manifest.expires_at.as_millis() as i64,
                json
            ],
        )?;

        Ok(())
    }

    /// Deletes a cached remote manifest by instance ID.
    pub fn delete_cached_manifest(
        &self,
        instance_id: &ServiceInstanceId,
    ) -> Result<(), ServiceError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ServiceError::LockError(e.to_string()))?;

        conn.execute(
            "DELETE FROM cached_remote_manifests WHERE instance_id = ?1",
            params![instance_id.as_str()],
        )?;

        Ok(())
    }

    /// Loads all cached remote manifests from the database on startup.
    pub fn load_all_cached_manifests(&self) -> Result<Vec<ServiceManifest>, ServiceError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ServiceError::LockError(e.to_string()))?;

        let mut stmt = conn.prepare("SELECT manifest_json FROM cached_remote_manifests")?;
        let rows = stmt.query_map([], |row| {
            let json: String = row.get(0)?;
            Ok(json)
        })?;

        let mut manifests = Vec::new();
        for row in rows {
            let json = row?;
            match serde_json::from_str::<ServiceManifest>(&json) {
                Ok(m) => manifests.push(m),
                Err(e) => {
                    warn!("Failed to deserialize cached manifest from SQLite: {}", e);
                }
            }
        }

        Ok(manifests)
    }

    /// Deletes all cached remote manifests whose `expires_at <= now`.
    pub fn delete_expired_cached_manifests(&self, now: Timestamp) -> Result<usize, ServiceError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ServiceError::LockError(e.to_string()))?;

        let count = conn.execute(
            "DELETE FROM cached_remote_manifests WHERE expires_at <= ?1",
            params![now.as_millis() as i64],
        )?;

        Ok(count)
    }
}
