use rusqlite::Connection;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use tracing::{debug, error, info};

use crate::error::DataError;
use crate::migration::MigrationRunner;

/// SQLite Local Persistence Engine.
/// Configures WAL mode, crash recovery pragmas, and deterministic migrations.
pub struct Database {
    path: PathBuf,
    conn: Mutex<Connection>,
}

impl Database {
    /// Opens or connects to an existing SQLite database at the specified path.
    /// Executes required migrations and ensures WAL journaling and foreign keys.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DataError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        info!("Opening SQLite database at {}", path.display());
        let mut conn = Connection::open(&path)?;

        // Configure resilient SQLite PRAGMAs
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.pragma_update(None, "busy_timeout", 5000)?;

        // Apply migrations deterministically
        let applied = MigrationRunner::apply_all(&mut conn)?;
        if applied > 0 {
            info!("Database initialized: applied {} migrations", applied);
        } else {
            debug!("Database initialized: schema up to date");
        }

        Ok(Self {
            path,
            conn: Mutex::new(conn),
        })
    }

    /// Opens an in-memory database for testing purposes.
    pub fn in_memory() -> Result<Self, DataError> {
        let mut conn = Connection::open_in_memory()?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.pragma_update(None, "busy_timeout", 5000)?;

        MigrationRunner::apply_all(&mut conn)?;

        Ok(Self {
            path: PathBuf::from(":memory:"),
            conn: Mutex::new(conn),
        })
    }

    /// Returns the filesystem path to this database.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Executes an operation with a read-only or shared connection reference.
    pub fn with_connection<F, T>(&self, f: F) -> Result<T, DataError>
    where
        F: FnOnce(&Connection) -> Result<T, DataError>,
    {
        let conn = self
            .conn
            .lock()
            .map_err(|e| DataError::LockError(format!("Mutex poisoned: {}", e)))?;
        f(&conn)
    }

    /// Executes a closure within an atomic SQLite transaction.
    /// If the closure returns `Err`, the transaction is rolled back automatically.
    /// If `Ok`, the transaction is committed atomically.
    pub fn with_transaction<F, T>(&self, f: F) -> Result<T, DataError>
    where
        F: FnOnce(&mut rusqlite::Transaction) -> Result<T, DataError>,
    {
        let mut conn = self
            .conn
            .lock()
            .map_err(|e| DataError::LockError(format!("Mutex poisoned: {}", e)))?;

        let mut tx = conn.transaction().map_err(|e| {
            DataError::TransactionFailure(format!("Failed to begin transaction: {}", e))
        })?;

        match f(&mut tx) {
            Ok(result) => {
                tx.commit().map_err(|e| {
                    error!("Transaction commit failed: {}", e);
                    DataError::TransactionFailure(format!("Commit failed: {}", e))
                })?;
                Ok(result)
            }
            Err(err) => {
                // rusqlite rolls back on Drop, but logging the rollback explicitly is critical for observability
                debug!("Rolling back transaction due to error: {:?}", err);
                let _ = tx.rollback();
                Err(err)
            }
        }
    }
}
