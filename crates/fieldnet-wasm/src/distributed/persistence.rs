use fieldnet_content::identity::ContentId;
use fieldnet_core::{NodeId, Timestamp};
use rusqlite::{params, Connection};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use super::catalog::{DiscoveredPackageRecord, PackageCatalogState};
use super::messages::{PackageAdvertisement, PackageTransferChunk};
use super::transfer::{PackageTransferSession, TransferStatus};
use crate::error::ApplicationError;
use crate::id::{AppVersion, ApplicationId};

/// SQLite persistence manager for distributed package catalog, transfer sessions, and cached chunks.
#[derive(Clone)]
pub struct PackageStorageDb {
    conn: Arc<Mutex<Connection>>,
}

impl PackageStorageDb {
    pub fn new(conn: Arc<Mutex<Connection>>) -> Result<Self, ApplicationError> {
        let db = Self { conn };
        db.init_schema()?;
        Ok(db)
    }

    /// Initializes SQLite tables for distributed packages and transfers.
    pub fn init_schema(&self) -> Result<(), ApplicationError> {
        let conn = self.conn.lock().unwrap();

        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS package_catalog (
                package_hash TEXT PRIMARY KEY,
                application_id TEXT NOT NULL,
                version TEXT NOT NULL,
                package_id TEXT NOT NULL,
                publisher TEXT NOT NULL,
                manifest_hash TEXT NOT NULL,
                package_size INTEGER NOT NULL,
                advertisement_json TEXT NOT NULL,
                catalog_state TEXT NOT NULL,
                provider_node TEXT NOT NULL,
                first_discovered_at_ms INTEGER NOT NULL,
                last_seen_at_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS package_transfers (
                transfer_id TEXT PRIMARY KEY,
                package_hash TEXT NOT NULL,
                application_id TEXT NOT NULL,
                version TEXT NOT NULL,
                source_node TEXT NOT NULL,
                destination_node TEXT NOT NULL,
                total_chunks INTEGER NOT NULL,
                chunk_size INTEGER NOT NULL,
                package_size INTEGER NOT NULL,
                received_chunks_json TEXT NOT NULL,
                chunk_hashes_json TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                expires_at_ms INTEGER NOT NULL,
                failure_reason TEXT
            );

            CREATE TABLE IF NOT EXISTS package_chunks (
                transfer_id TEXT NOT NULL,
                chunk_index INTEGER NOT NULL,
                total_chunks INTEGER NOT NULL,
                chunk_hash TEXT NOT NULL,
                chunk_size INTEGER NOT NULL,
                chunk_data BLOB NOT NULL,
                PRIMARY KEY (transfer_id, chunk_index)
            );

            CREATE TABLE IF NOT EXISTS package_sharing_rules (
                application_id TEXT PRIMARY KEY,
                is_shared INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_transfers_status ON package_transfers (status);
            CREATE INDEX IF NOT EXISTS idx_catalog_app ON package_catalog (application_id);
            "#,
        )?;

        Ok(())
    }

    /// Saves or updates a discovered package record in the catalog database.
    pub fn save_discovered_package(
        &self,
        record: &DiscoveredPackageRecord,
    ) -> Result<(), ApplicationError> {
        let conn = self.conn.lock().unwrap();
        let ad = &record.advertisement;
        let ad_json = serde_json::to_string(ad)?;

        conn.execute(
            r#"
            INSERT INTO package_catalog (
                package_hash, application_id, version, package_id, publisher, manifest_hash,
                package_size, advertisement_json, catalog_state, provider_node,
                first_discovered_at_ms, last_seen_at_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
            ON CONFLICT(package_hash) DO UPDATE SET
                catalog_state = excluded.catalog_state,
                last_seen_at_ms = excluded.last_seen_at_ms,
                advertisement_json = excluded.advertisement_json
            "#,
            params![
                ad.package_hash.to_hex(),
                ad.application_id.as_str(),
                ad.version.to_string(),
                ad.package_id.as_str(),
                ad.publisher_id.to_hex(),
                ad.manifest_hash.to_hex(),
                ad.package_size as i64,
                ad_json,
                record.state.as_str(),
                ad.provider_node.to_string(),
                record.first_discovered_at.0 as i64,
                record.last_seen_at.0 as i64,
            ],
        )?;

        Ok(())
    }

    /// Loads all discovered package records from SQLite.
    pub fn load_all_discovered_packages(
        &self,
    ) -> Result<Vec<DiscoveredPackageRecord>, ApplicationError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            r#"
            SELECT advertisement_json, catalog_state, first_discovered_at_ms, last_seen_at_ms
            FROM package_catalog
            "#,
        )?;

        let rows = stmt.query_map([], |row| {
            let ad_json: String = row.get(0)?;
            let state_str: String = row.get(1)?;
            let first_ms: i64 = row.get(2)?;
            let last_ms: i64 = row.get(3)?;
            Ok((ad_json, state_str, first_ms, last_ms))
        })?;

        let mut list = Vec::new();
        for r in rows {
            let (ad_json, state_str, first_ms, last_ms) = r?;
            let ad: PackageAdvertisement = serde_json::from_str(&ad_json)?;
            let state = match state_str.as_str() {
                "local" => PackageCatalogState::Local,
                "discovered" => PackageCatalogState::Discovered,
                "available" => PackageCatalogState::Available,
                "partial" => PackageCatalogState::Partial,
                "complete" => PackageCatalogState::Complete,
                "verified" => PackageCatalogState::Verified,
                "installable" => PackageCatalogState::Installable,
                "installed" => PackageCatalogState::Installed,
                _ => PackageCatalogState::Discovered,
            };

            list.push(DiscoveredPackageRecord {
                advertisement: ad,
                state,
                first_discovered_at: Timestamp(first_ms as u64),
                last_seen_at: Timestamp(last_ms as u64),
            });
        }

        Ok(list)
    }

    /// Deletes a package record from the catalog database.
    pub fn delete_discovered_package(
        &self,
        package_hash: &ContentId,
    ) -> Result<(), ApplicationError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM package_catalog WHERE package_hash = ?1",
            params![package_hash.to_hex()],
        )?;
        Ok(())
    }

    /// Persists or updates a package transfer session.
    pub fn save_transfer_session(
        &self,
        session: &PackageTransferSession,
    ) -> Result<(), ApplicationError> {
        let conn = self.conn.lock().unwrap();

        let received_json = serde_json::to_string(&session.received_chunks)?;
        let hashes_json = serde_json::to_string(&session.chunk_hashes)?;

        conn.execute(
            r#"
            INSERT INTO package_transfers (
                transfer_id, package_hash, application_id, version, source_node, destination_node,
                total_chunks, chunk_size, package_size, received_chunks_json, chunk_hashes_json,
                status, created_at_ms, updated_at_ms, expires_at_ms, failure_reason
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)
            ON CONFLICT(transfer_id) DO UPDATE SET
                received_chunks_json = excluded.received_chunks_json,
                chunk_hashes_json = excluded.chunk_hashes_json,
                status = excluded.status,
                total_chunks = excluded.total_chunks,
                chunk_size = excluded.chunk_size,
                package_size = excluded.package_size,
                updated_at_ms = excluded.updated_at_ms,
                failure_reason = excluded.failure_reason
            "#,
            params![
                session.transfer_id,
                session.package_hash.to_hex(),
                session.application_id.as_str(),
                session.version.to_string(),
                session.source_node.to_string(),
                session.destination_node.to_string(),
                session.total_chunks as i64,
                session.chunk_size as i64,
                session.package_size as i64,
                received_json,
                hashes_json,
                session.status.as_str(),
                session.created_at.0 as i64,
                session.updated_at.0 as i64,
                session.expires_at.0 as i64,
                session.failure_reason,
            ],
        )?;

        Ok(())
    }

    /// Loads a package transfer session by transfer_id.
    pub fn load_transfer_session(
        &self,
        transfer_id: &str,
    ) -> Result<Option<PackageTransferSession>, ApplicationError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            r#"
            SELECT transfer_id, package_hash, application_id, version, source_node, destination_node,
                   total_chunks, chunk_size, package_size, received_chunks_json, chunk_hashes_json,
                   status, created_at_ms, updated_at_ms, expires_at_ms, failure_reason
            FROM package_transfers WHERE transfer_id = ?1
            "#,
        )?;

        let mut rows = stmt.query(params![transfer_id])?;
        if let Some(row) = rows.next()? {
            let s = parse_transfer_row(row)?;
            Ok(Some(s))
        } else {
            Ok(None)
        }
    }

    /// Loads all package transfer sessions.
    pub fn load_all_transfer_sessions(
        &self,
    ) -> Result<Vec<PackageTransferSession>, ApplicationError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            r#"
            SELECT transfer_id, package_hash, application_id, version, source_node, destination_node,
                   total_chunks, chunk_size, package_size, received_chunks_json, chunk_hashes_json,
                   status, created_at_ms, updated_at_ms, expires_at_ms, failure_reason
            FROM package_transfers
            ORDER BY created_at_ms DESC
            "#,
        )?;

        let rows = stmt.query_map([], parse_transfer_row)?;
        let mut sessions = Vec::new();
        for r in rows {
            sessions.push(r?);
        }
        Ok(sessions)
    }

    /// Saves received chunk binary payload to SQLite.
    pub fn save_chunk(
        &self,
        transfer_id: &str,
        chunk_index: u32,
        total_chunks: u32,
        chunk_hash: &ContentId,
        data: &[u8],
    ) -> Result<(), ApplicationError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            r#"
            INSERT INTO package_chunks (
                transfer_id, chunk_index, total_chunks, chunk_hash, chunk_size, chunk_data
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(transfer_id, chunk_index) DO UPDATE SET
                chunk_data = excluded.chunk_data,
                chunk_size = excluded.chunk_size,
                chunk_hash = excluded.chunk_hash
            "#,
            params![
                transfer_id,
                chunk_index as i64,
                total_chunks as i64,
                chunk_hash.to_hex(),
                data.len() as i64,
                data,
            ],
        )?;
        Ok(())
    }

    /// Loads all chunks for an active transfer.
    pub fn load_all_chunks_for_transfer(
        &self,
        transfer_id: &str,
        package_hash: &ContentId,
    ) -> Result<Vec<PackageTransferChunk>, ApplicationError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            r#"
            SELECT chunk_index, total_chunks, chunk_hash, chunk_size, chunk_data
            FROM package_chunks
            WHERE transfer_id = ?1
            ORDER BY chunk_index ASC
            "#,
        )?;

        let rows = stmt.query_map(params![transfer_id], |row| {
            let index: i64 = row.get(0)?;
            let total: i64 = row.get(1)?;
            let hash_str: String = row.get(2)?;
            let size: i64 = row.get(3)?;
            let data: Vec<u8> = row.get(4)?;
            Ok((index, total, hash_str, size, data))
        })?;

        let mut chunks = Vec::new();
        for r in rows {
            let (index, total, hash_str, size, data) = r?;
            let hash = ContentId::from_hex(&hash_str).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?;

            chunks.push(PackageTransferChunk {
                transfer_id: transfer_id.to_string(),
                package_hash: *package_hash,
                chunk_index: index as u32,
                total_chunks: total as u32,
                chunk_hash: hash,
                chunk_size: size as u32,
                data,
            });
        }

        Ok(chunks)
    }

    /// Deletes all cached chunk payloads for a transfer.
    pub fn delete_transfer_chunks(&self, transfer_id: &str) -> Result<(), ApplicationError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM package_chunks WHERE transfer_id = ?1",
            params![transfer_id],
        )?;
        Ok(())
    }

    /// Saves or updates a sharing policy rule for an application.
    pub fn save_sharing_rule(
        &self,
        app_id: &ApplicationId,
        is_shared: bool,
        now: Timestamp,
    ) -> Result<(), ApplicationError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            r#"
            INSERT INTO package_sharing_rules (application_id, is_shared, updated_at_ms)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(application_id) DO UPDATE SET
                is_shared = excluded.is_shared,
                updated_at_ms = excluded.updated_at_ms
            "#,
            params![app_id.as_str(), if is_shared { 1 } else { 0 }, now.0 as i64],
        )?;
        Ok(())
    }

    /// Loads all application sharing policy rules.
    pub fn load_sharing_rules(&self) -> Result<HashMap<ApplicationId, bool>, ApplicationError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt =
            conn.prepare("SELECT application_id, is_shared FROM package_sharing_rules")?;
        let rows = stmt.query_map([], |row| {
            let app_id_str: String = row.get(0)?;
            let shared_int: i64 = row.get(1)?;
            Ok((app_id_str, shared_int != 0))
        })?;

        let mut rules = HashMap::new();
        for r in rows {
            let (app_id_str, is_shared) = r?;
            if let Ok(app_id) = ApplicationId::new(app_id_str) {
                rules.insert(app_id, is_shared);
            }
        }
        Ok(rules)
    }

    /// Startup reconciliation: marks in-flight transfers as Interrupted and cleans up expired sessions.
    pub fn reconcile_startup(&self, now: Timestamp) -> Result<usize, ApplicationError> {
        let conn = self.conn.lock().unwrap();

        // 1. Mark in-flight active transfers as Interrupted
        let modified = conn.execute(
            r#"
            UPDATE package_transfers
            SET status = 'interrupted', updated_at_ms = ?1, failure_reason = 'Node restart during active transfer'
            WHERE status IN ('requested', 'accepted', 'transferring', 'completing', 'verifying')
            "#,
            params![now.0 as i64],
        )?;

        // 2. Mark expired uncompleted transfers as Failed
        conn.execute(
            r#"
            UPDATE package_transfers
            SET status = 'failed', updated_at_ms = ?1, failure_reason = 'Transfer session expired'
            WHERE status NOT IN ('completed', 'failed', 'cancelled') AND expires_at_ms < ?1
            "#,
            params![now.0 as i64],
        )?;

        Ok(modified)
    }
}

fn parse_transfer_row(row: &rusqlite::Row) -> Result<PackageTransferSession, rusqlite::Error> {
    let transfer_id: String = row.get(0)?;
    let hash_str: String = row.get(1)?;
    let app_id_str: String = row.get(2)?;
    let ver_str: String = row.get(3)?;
    let src_str: String = row.get(4)?;
    let dst_str: String = row.get(5)?;
    let total_chunks: i64 = row.get(6)?;
    let chunk_size: i64 = row.get(7)?;
    let package_size: i64 = row.get(8)?;
    let rec_json: String = row.get(9)?;
    let hashes_json: String = row.get(10)?;
    let status_str: String = row.get(11)?;
    let created_ms: i64 = row.get(12)?;
    let updated_ms: i64 = row.get(13)?;
    let expires_ms: i64 = row.get(14)?;
    let failure_reason: Option<String> = row.get(15)?;

    let package_hash = ContentId::from_hex(&hash_str).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Text, Box::new(e))
    })?;
    let application_id = ApplicationId::new(app_id_str).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Text, Box::new(e))
    })?;
    let version = ver_str.parse::<AppVersion>().map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Text, Box::new(e))
    })?;
    let source_node = NodeId::from_str(&src_str).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(4, rusqlite::types::Type::Text, Box::new(e))
    })?;
    let destination_node = NodeId::from_str(&dst_str).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(5, rusqlite::types::Type::Text, Box::new(e))
    })?;

    let received_chunks: HashSet<u32> = serde_json::from_str(&rec_json).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(9, rusqlite::types::Type::Text, Box::new(e))
    })?;
    let chunk_hashes: Vec<ContentId> = serde_json::from_str(&hashes_json).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(10, rusqlite::types::Type::Text, Box::new(e))
    })?;

    let status = match status_str.as_str() {
        "requested" => TransferStatus::Requested,
        "accepted" => TransferStatus::Accepted,
        "transferring" => TransferStatus::Transferring,
        "paused" => TransferStatus::Paused,
        "interrupted" => TransferStatus::Interrupted,
        "completing" => TransferStatus::Completing,
        "verifying" => TransferStatus::Verifying,
        "completed" => TransferStatus::Completed,
        "failed" => TransferStatus::Failed,
        "cancelled" => TransferStatus::Cancelled,
        _ => TransferStatus::Failed,
    };

    Ok(PackageTransferSession {
        transfer_id,
        package_hash,
        application_id,
        version,
        source_node,
        destination_node,
        total_chunks: total_chunks as u32,
        chunk_size: chunk_size as u32,
        package_size: package_size as u64,
        received_chunks,
        chunk_hashes,
        status,
        created_at: Timestamp(created_ms as u64),
        updated_at: Timestamp(updated_ms as u64),
        expires_at: Timestamp(expires_ms as u64),
        failure_reason,
    })
}
