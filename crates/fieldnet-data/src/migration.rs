use fieldnet_core::Timestamp;
use rusqlite::{params, Connection};
use tracing::{debug, error, info};

use crate::error::DataError;

pub struct Migration {
    pub version: u32,
    pub name: &'static str,
    pub sql: &'static str,
}

impl Migration {
    pub fn checksum(&self) -> String {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.version.to_le_bytes());
        hasher.update(self.name.as_bytes());
        hasher.update(self.sql.as_bytes());
        hasher.finalize().to_hex().to_string()
    }
}

/// Ordered list of authoritative schema migrations.
pub fn registered_migrations() -> Vec<Migration> {
    vec![Migration {
        version: 1,
        name: "initial_document_store",
        sql: "
            CREATE TABLE IF NOT EXISTS documents (
                document_id         BLOB PRIMARY KEY,
                document_type       TEXT NOT NULL,
                schema_version      INTEGER NOT NULL DEFAULT 1,
                owner_node_id       BLOB NOT NULL,
                version             INTEGER NOT NULL DEFAULT 1,
                created_at          INTEGER NOT NULL,
                updated_at          INTEGER NOT NULL,
                is_deleted          INTEGER NOT NULL DEFAULT 0,
                payload_bytes       BLOB NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_documents_type ON documents(document_type);
            CREATE INDEX IF NOT EXISTS idx_documents_updated ON documents(updated_at);
            CREATE INDEX IF NOT EXISTS idx_documents_deleted ON documents(is_deleted);
            CREATE INDEX IF NOT EXISTS idx_documents_owner ON documents(owner_node_id);
        ",
    },
    Migration {
        version: 2,
        name: "operation_log_and_causality",
        sql: "
            CREATE TABLE IF NOT EXISTS operations (
                op_id               BLOB PRIMARY KEY,
                document_id         BLOB NOT NULL,
                actor_node_id       BLOB NOT NULL,
                sequence            INTEGER NOT NULL,
                op_type             INTEGER NOT NULL,
                delta_bytes         BLOB NOT NULL,
                hlc_physical        INTEGER NOT NULL,
                hlc_logical         INTEGER NOT NULL,
                dependencies_bytes  BLOB NOT NULL,
                signature           BLOB NOT NULL,
                created_at          INTEGER NOT NULL,
                status              INTEGER NOT NULL DEFAULT 1,
                FOREIGN KEY(document_id) REFERENCES documents(document_id) ON DELETE CASCADE
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_ops_doc_actor_seq 
                ON operations(document_id, actor_node_id, sequence);
            CREATE INDEX IF NOT EXISTS idx_ops_doc_hlc 
                ON operations(document_id, hlc_physical, hlc_logical);
            CREATE INDEX IF NOT EXISTS idx_ops_status 
                ON operations(status);

            CREATE TABLE IF NOT EXISTS applied_operations (
                document_id         BLOB NOT NULL,
                op_id               BLOB NOT NULL,
                applied_at          INTEGER NOT NULL,
                PRIMARY KEY(document_id, op_id)
            );

            CREATE TABLE IF NOT EXISTS replica_state (
                document_id         BLOB PRIMARY KEY,
                state_vector_bytes  BLOB NOT NULL,
                latest_hlc_physical INTEGER NOT NULL,
                latest_hlc_logical  INTEGER NOT NULL,
                local_sequence      INTEGER NOT NULL DEFAULT 0,
                updated_at          INTEGER NOT NULL,
                FOREIGN KEY(document_id) REFERENCES documents(document_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS local_node_state (
                node_id             BLOB PRIMARY KEY,
                local_sequence      INTEGER NOT NULL DEFAULT 0,
                latest_hlc_physical INTEGER NOT NULL,
                latest_hlc_logical  INTEGER NOT NULL,
                updated_at          INTEGER NOT NULL
            );
        ",
    },
    Migration {
        version: 3,
        name: "crdt_tombstones_and_unapplied_ops",
        sql: "
            CREATE TABLE IF NOT EXISTS tombstones (
                entity_id           BLOB PRIMARY KEY,
                entity_type         TEXT NOT NULL,
                deleted_at          INTEGER NOT NULL,
                hlc_physical        INTEGER NOT NULL,
                hlc_logical         INTEGER NOT NULL,
                deleted_by          BLOB NOT NULL,
                gc_after            INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_tombstones_gc ON tombstones(gc_after);

            CREATE TABLE IF NOT EXISTS unapplied_operations (
                op_id               BLOB PRIMARY KEY,
                document_id         BLOB NOT NULL,
                actor_node_id       BLOB NOT NULL,
                missing_dep_op_id   BLOB NOT NULL,
                operation_bytes     BLOB NOT NULL,
                received_at         INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_unapplied_dep ON unapplied_operations(missing_dep_op_id);
        ",
    },
    Migration {
        version: 4,
        name: "peer_sync_and_sessions",
        sql: "
            CREATE TABLE IF NOT EXISTS peer_sync_state (
                peer_node_id        BLOB NOT NULL,
                document_id         BLOB NOT NULL,
                last_synced_seq     INTEGER NOT NULL,
                last_synced_hlc_p   INTEGER NOT NULL,
                last_synced_hlc_l   INTEGER NOT NULL,
                last_sync_time      INTEGER NOT NULL,
                sync_status         TEXT NOT NULL,
                PRIMARY KEY(peer_node_id, document_id)
            );
            CREATE INDEX IF NOT EXISTS idx_peer_sync_peer ON peer_sync_state(peer_node_id);

            CREATE TABLE IF NOT EXISTS sync_sessions (
                session_id          BLOB PRIMARY KEY,
                peer_node_id        BLOB NOT NULL,
                started_at          INTEGER NOT NULL,
                state               TEXT NOT NULL,
                bytes_sent          INTEGER NOT NULL DEFAULT 0,
                bytes_received      INTEGER NOT NULL DEFAULT 0,
                ops_sent            INTEGER NOT NULL DEFAULT 0,
                ops_received        INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS sync_cursors (
                peer_node_id        BLOB NOT NULL,
                document_id         BLOB NOT NULL,
                cursor_seq          INTEGER NOT NULL,
                cursor_hlc_p        INTEGER NOT NULL,
                cursor_hlc_l        INTEGER NOT NULL,
                PRIMARY KEY(peer_node_id, document_id)
            );
        ",
    },
    Migration {
        version: 5,
        name: "snapshots_and_compaction",
        sql: "
            CREATE TABLE IF NOT EXISTS document_snapshots (
                snapshot_id         BLOB PRIMARY KEY,
                document_id         BLOB NOT NULL,
                snapshot_version    INTEGER NOT NULL,
                compacted_up_to_seq INTEGER NOT NULL,
                state_vector_bytes  BLOB NOT NULL,
                hlc_physical        INTEGER NOT NULL,
                hlc_logical         INTEGER NOT NULL,
                crdt_type           TEXT NOT NULL,
                snapshot_payload    BLOB NOT NULL,
                state_hash          BLOB NOT NULL,
                schema_version      INTEGER NOT NULL,
                created_at          INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_snapshots_doc ON document_snapshots(document_id, snapshot_version DESC);

            CREATE TABLE IF NOT EXISTS compaction_records (
                compaction_id       BLOB PRIMARY KEY,
                document_id         BLOB NOT NULL,
                snapshot_id         BLOB NOT NULL,
                compacted_up_to_seq INTEGER NOT NULL,
                ops_removed         INTEGER NOT NULL,
                bytes_reclaimed     INTEGER NOT NULL,
                compacted_at        INTEGER NOT NULL,
                reason              TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_compaction_doc ON compaction_records(document_id);
        ",
    }]
}

pub struct MigrationRunner;

impl MigrationRunner {
    /// Applies all pending migrations deterministically in an atomic transaction.
    pub fn apply_all(conn: &mut Connection) -> Result<usize, DataError> {
        // Ensure migration tracking table exists
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                applied_at INTEGER NOT NULL,
                checksum TEXT NOT NULL
            );",
            [],
        )?;

        let migrations = registered_migrations();
        let mut applied_count = 0;

        for migration in migrations {
            let existing: Option<(String, String)> = conn
                .query_row(
                    "SELECT name, checksum FROM schema_migrations WHERE version = ?1",
                    params![migration.version],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .ok();

            if let Some((_name, recorded_checksum)) = existing {
                let current_checksum = migration.checksum();
                if recorded_checksum != current_checksum {
                    error!(
                        "Migration checksum mismatch for version {}: recorded={}, current={}",
                        migration.version, recorded_checksum, current_checksum
                    );
                    return Err(DataError::MigrationChecksumMismatch {
                        version: migration.version,
                        expected: recorded_checksum,
                        actual: current_checksum,
                    });
                }
                debug!(
                    "Migration {} ({}) is already applied and verified",
                    migration.version, migration.name
                );
                continue;
            }

            info!(
                "Applying schema migration {} ({})",
                migration.version, migration.name
            );

            let tx = conn.transaction().map_err(|e| DataError::MigrationFailed {
                version: migration.version,
                reason: format!("Failed to start transaction: {}", e),
            })?;

            tx.execute_batch(migration.sql).map_err(|e| {
                error!(
                    "Failed executing migration {} ({}): {}",
                    migration.version, migration.name, e
                );
                DataError::MigrationFailed {
                    version: migration.version,
                    reason: format!("SQL execution failed: {}", e),
                }
            })?;

            let checksum = migration.checksum();
            let applied_at = Timestamp::now().as_millis() as i64;

            tx.execute(
                "INSERT INTO schema_migrations (version, name, applied_at, checksum) VALUES (?1, ?2, ?3, ?4)",
                params![migration.version, migration.name, applied_at, checksum],
            ).map_err(|e| {
                DataError::MigrationFailed {
                    version: migration.version,
                    reason: format!("Failed to record migration metadata: {}", e),
                }
            })?;

            tx.commit().map_err(|e| DataError::MigrationFailed {
                version: migration.version,
                reason: format!("Failed to commit transaction: {}", e),
            })?;

            applied_count += 1;
            info!(
                "Successfully applied and committed migration {} ({})",
                migration.version, migration.name
            );
        }

        Ok(applied_count)
    }

    /// Returns list of applied migration versions.
    pub fn get_applied_versions(conn: &Connection) -> Result<Vec<u32>, DataError> {
        let mut stmt =
            conn.prepare("SELECT version FROM schema_migrations ORDER BY version ASC")?;
        let rows = stmt.query_map([], |row| row.get::<_, u32>(0))?;
        let mut versions = Vec::new();
        for r in rows {
            versions.push(r?);
        }
        Ok(versions)
    }
}
