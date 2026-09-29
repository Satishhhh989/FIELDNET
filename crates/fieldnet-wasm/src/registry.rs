use fieldnet_content::identity::ContentId;
use fieldnet_core::Timestamp;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::{Arc, Mutex};
use tracing::{debug, info};

use crate::dependencies::AppDependency;
use crate::error::ApplicationError;
use crate::id::{AppVersion, ApplicationId, PackageId, PublisherIdentity};
use crate::lifecycle::ApplicationState;
use crate::limits::ResourceLimits;
use crate::manifest::ApplicationManifest;
use crate::package::PackageItem;
use crate::permissions::AppPermission;

/// Verification status of the package's publisher signature.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SignatureStatus {
    Verified,
    Unsigned,
    Invalid,
}

impl SignatureStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Verified => "VERIFIED",
            Self::Unsigned => "UNSIGNED",
            Self::Invalid => "INVALID",
        }
    }
}

/// Comprehensive persistent record of an installed application in the node catalog.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ApplicationRecord {
    pub application_id: ApplicationId,
    pub installed_version: AppVersion,
    pub package_id: PackageId,
    pub package_hash: ContentId,
    pub manifest: ApplicationManifest,
    pub items: Vec<PackageItem>,
    pub publisher: PublisherIdentity,
    pub signature_status: SignatureStatus,
    pub lifecycle_state: ApplicationState,
    pub permissions: Vec<AppPermission>,
    pub resource_limits: ResourceLimits,
    pub dependencies: Vec<AppDependency>,
    pub installed_at: Timestamp,
    pub updated_at: Timestamp,
}

impl ApplicationRecord {
    /// Returns the package item corresponding to the manifest's declared entrypoint.
    pub fn entrypoint_item(&self) -> Result<&PackageItem, ApplicationError> {
        self.items
            .iter()
            .find(|it| it.path == self.manifest.entrypoint)
            .ok_or_else(|| {
                ApplicationError::IntegrityError(format!(
                    "Entrypoint '{}' not found in package items",
                    self.manifest.entrypoint
                ))
            })
    }
}

/// Historical package version preserved for deterministic rollback.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ApplicationHistoryRecord {
    pub id: i64,
    pub application_id: ApplicationId,
    pub version: AppVersion,
    pub package_id: PackageId,
    pub package_hash: ContentId,
    pub manifest: ApplicationManifest,
    pub items: Vec<PackageItem>,
    pub recorded_at: Timestamp,
}

/// Persistent, restart-resilient registry storing application state, manifests, and rollback history.
#[derive(Clone)]
pub struct ApplicationRegistry {
    conn: Arc<Mutex<Connection>>,
}

impl ApplicationRegistry {
    /// Opens or creates a persistent SQLite application registry database at `path`.
    pub fn new(path: &Path) -> Result<Self, ApplicationError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let conn = Connection::open(path)?;
        Self::init_with_connection(conn)
    }

    /// Creates an ephemeral in-memory registry database for testing.
    pub fn new_in_memory() -> Result<Self, ApplicationError> {
        let conn = Connection::open_in_memory()?;
        Self::init_with_connection(conn)
    }

    fn init_with_connection(conn: Connection) -> Result<Self, ApplicationError> {
        conn.execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS applications (
                application_id TEXT PRIMARY KEY,
                installed_version TEXT NOT NULL,
                package_id TEXT NOT NULL,
                package_hash TEXT NOT NULL,
                manifest_json TEXT NOT NULL,
                publisher TEXT NOT NULL,
                signature_status TEXT NOT NULL,
                lifecycle_state TEXT NOT NULL,
                permissions_json TEXT NOT NULL,
                limits_json TEXT NOT NULL,
                dependencies_json TEXT NOT NULL,
                items_json TEXT NOT NULL DEFAULT '[]',
                installed_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS application_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                application_id TEXT NOT NULL,
                version TEXT NOT NULL,
                package_id TEXT NOT NULL,
                package_hash TEXT NOT NULL,
                manifest_json TEXT NOT NULL,
                items_json TEXT NOT NULL DEFAULT '[]',
                recorded_at_ms INTEGER NOT NULL,
                FOREIGN KEY (application_id) REFERENCES applications(application_id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_app_history_app_id
                ON application_history(application_id, recorded_at_ms DESC);

            CREATE TABLE IF NOT EXISTS application_storage_usage (
                application_id TEXT NOT NULL,
                content_id TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                created_at_ms INTEGER NOT NULL,
                PRIMARY KEY (application_id, content_id),
                FOREIGN KEY (application_id) REFERENCES applications(application_id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_storage_usage_app_id
                ON application_storage_usage(application_id);
            "#,
        )?;

        // Non-destructive column additions for schema migrations
        let _ = conn.execute(
            "ALTER TABLE applications ADD COLUMN items_json TEXT NOT NULL DEFAULT '[]'",
            [],
        );
        let _ = conn.execute(
            "ALTER TABLE application_history ADD COLUMN items_json TEXT NOT NULL DEFAULT '[]'",
            [],
        );

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Registers a newly installed application record atomically.
    pub fn register(&self, record: &ApplicationRecord) -> Result<(), ApplicationError> {
        let conn = self.conn.lock().unwrap();

        let manifest_json = serde_json::to_string(&record.manifest)?;
        let permissions_json = serde_json::to_string(&record.permissions)?;
        let limits_json = serde_json::to_string(&record.resource_limits)?;
        let deps_json = serde_json::to_string(&record.dependencies)?;
        let items_json = serde_json::to_string(&record.items)?;

        conn.execute(
            r#"
            INSERT INTO applications (
                application_id,
                installed_version,
                package_id,
                package_hash,
                manifest_json,
                publisher,
                signature_status,
                lifecycle_state,
                permissions_json,
                limits_json,
                dependencies_json,
                items_json,
                installed_at_ms,
                updated_at_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
            "#,
            params![
                record.application_id.to_string(),
                record.installed_version.to_string(),
                record.package_id.to_string(),
                record.package_hash.to_hex(),
                manifest_json,
                record.publisher.to_string(),
                record.signature_status.as_str(),
                record.lifecycle_state.as_str(),
                permissions_json,
                limits_json,
                deps_json,
                items_json,
                record.installed_at.as_millis(),
                record.updated_at.as_millis(),
            ],
        )?;

        info!(
            application_id = %record.application_id,
            version = %record.installed_version,
            "Application registered in catalog"
        );
        Ok(())
    }

    /// Fetches an application record by its `ApplicationId`.
    pub fn get(
        &self,
        app_id: &ApplicationId,
    ) -> Result<Option<ApplicationRecord>, ApplicationError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            r#"
            SELECT
                application_id,
                installed_version,
                package_id,
                package_hash,
                manifest_json,
                publisher,
                signature_status,
                lifecycle_state,
                permissions_json,
                limits_json,
                dependencies_json,
                items_json,
                installed_at_ms,
                updated_at_ms
            FROM applications
            WHERE application_id = ?1
            "#,
        )?;

        let row = stmt
            .query_row(params![app_id.to_string()], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, String>(5)?,
                    r.get::<_, String>(6)?,
                    r.get::<_, String>(7)?,
                    r.get::<_, String>(8)?,
                    r.get::<_, String>(9)?,
                    r.get::<_, String>(10)?,
                    r.get::<_, String>(11)?,
                    r.get::<_, u64>(12)?,
                    r.get::<_, u64>(13)?,
                ))
            })
            .optional()?;

        match row {
            Some((
                app_id_str,
                ver_str,
                pkg_id_str,
                pkg_hash_str,
                manifest_json,
                pub_str,
                sig_status_str,
                state_str,
                perms_json,
                limits_json,
                deps_json,
                items_json,
                installed_at_ms,
                updated_at_ms,
            )) => {
                let parsed = ApplicationRecord {
                    application_id: app_id_str.parse()?,
                    installed_version: ver_str.parse()?,
                    package_id: pkg_id_str.parse()?,
                    package_hash: ContentId::from_hex(&pkg_hash_str)?,
                    manifest: serde_json::from_str(&manifest_json)?,
                    items: serde_json::from_str(&items_json).unwrap_or_default(),
                    publisher: pub_str.parse()?,
                    signature_status: match sig_status_str.as_str() {
                        "VERIFIED" => SignatureStatus::Verified,
                        "UNSIGNED" => SignatureStatus::Unsigned,
                        _ => SignatureStatus::Invalid,
                    },
                    lifecycle_state: state_str.parse()?,
                    permissions: serde_json::from_str(&perms_json)?,
                    resource_limits: serde_json::from_str(&limits_json)?,
                    dependencies: serde_json::from_str(&deps_json)?,
                    installed_at: Timestamp::from_millis(installed_at_ms),
                    updated_at: Timestamp::from_millis(updated_at_ms),
                };
                Ok(Some(parsed))
            }
            None => Ok(None),
        }
    }

    /// Lists all installed applications.
    pub fn list(&self) -> Result<Vec<ApplicationRecord>, ApplicationError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            r#"
            SELECT
                application_id,
                installed_version,
                package_id,
                package_hash,
                manifest_json,
                publisher,
                signature_status,
                lifecycle_state,
                permissions_json,
                limits_json,
                dependencies_json,
                items_json,
                installed_at_ms,
                updated_at_ms
            FROM applications
            ORDER BY application_id ASC
            "#,
        )?;

        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, String>(5)?,
                r.get::<_, String>(6)?,
                r.get::<_, String>(7)?,
                r.get::<_, String>(8)?,
                r.get::<_, String>(9)?,
                r.get::<_, String>(10)?,
                r.get::<_, String>(11)?,
                r.get::<_, u64>(12)?,
                r.get::<_, u64>(13)?,
            ))
        })?;

        let mut out = Vec::new();
        for res in rows {
            let (
                app_id_str,
                ver_str,
                pkg_id_str,
                pkg_hash_str,
                manifest_json,
                pub_str,
                sig_status_str,
                state_str,
                perms_json,
                limits_json,
                deps_json,
                items_json,
                installed_at_ms,
                updated_at_ms,
            ) = res?;

            let record = ApplicationRecord {
                application_id: app_id_str.parse()?,
                installed_version: ver_str.parse()?,
                package_id: pkg_id_str.parse()?,
                package_hash: ContentId::from_hex(&pkg_hash_str)?,
                manifest: serde_json::from_str(&manifest_json)?,
                items: serde_json::from_str(&items_json).unwrap_or_default(),
                publisher: pub_str.parse()?,
                signature_status: match sig_status_str.as_str() {
                    "VERIFIED" => SignatureStatus::Verified,
                    "UNSIGNED" => SignatureStatus::Unsigned,
                    _ => SignatureStatus::Invalid,
                },
                lifecycle_state: state_str.parse()?,
                permissions: serde_json::from_str(&perms_json)?,
                resource_limits: serde_json::from_str(&limits_json)?,
                dependencies: serde_json::from_str(&deps_json)?,
                installed_at: Timestamp::from_millis(installed_at_ms),
                updated_at: Timestamp::from_millis(updated_at_ms),
            };
            out.push(record);
        }

        Ok(out)
    }

    /// Filters installed applications by lifecycle state.
    pub fn list_by_states(
        &self,
        states: &[ApplicationState],
    ) -> Result<Vec<ApplicationRecord>, ApplicationError> {
        let all = self.list()?;
        Ok(all
            .into_iter()
            .filter(|app| states.contains(&app.lifecycle_state))
            .collect())
    }

    /// Atomically updates an existing application with a new package version,
    /// recording the previous version in the rollback history table.
    pub fn update_version(&self, new_record: &ApplicationRecord) -> Result<(), ApplicationError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        // 1. Fetch existing record to archive
        let existing: Option<(String, String, String, String, String)> = tx
            .query_row(
                "SELECT installed_version, package_id, package_hash, manifest_json, items_json FROM applications WHERE application_id = ?1",
                params![new_record.application_id.to_string()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .optional()?;

        let (old_ver, old_pkg_id, old_pkg_hash, old_manifest, old_items) = match existing {
            Some(ex) => ex,
            None => {
                return Err(ApplicationError::NotFound(format!(
                    "Cannot update application '{}': not found",
                    new_record.application_id
                )))
            }
        };

        // 2. Archive current version into history
        tx.execute(
            r#"
            INSERT INTO application_history (
                application_id, version, package_id, package_hash, manifest_json, items_json, recorded_at_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            "#,
            params![
                new_record.application_id.to_string(),
                old_ver,
                old_pkg_id,
                old_pkg_hash,
                old_manifest,
                old_items,
                Timestamp::now().as_millis(),
            ],
        )?;

        // 3. Update main applications record
        let manifest_json = serde_json::to_string(&new_record.manifest)?;
        let permissions_json = serde_json::to_string(&new_record.permissions)?;
        let limits_json = serde_json::to_string(&new_record.resource_limits)?;
        let deps_json = serde_json::to_string(&new_record.dependencies)?;
        let items_json = serde_json::to_string(&new_record.items)?;

        tx.execute(
            r#"
            UPDATE applications SET
                installed_version = ?1,
                package_id = ?2,
                package_hash = ?3,
                manifest_json = ?4,
                publisher = ?5,
                signature_status = ?6,
                lifecycle_state = ?7,
                permissions_json = ?8,
                limits_json = ?9,
                dependencies_json = ?10,
                items_json = ?11,
                updated_at_ms = ?12
            WHERE application_id = ?13
            "#,
            params![
                new_record.installed_version.to_string(),
                new_record.package_id.to_string(),
                new_record.package_hash.to_hex(),
                manifest_json,
                new_record.publisher.to_string(),
                new_record.signature_status.as_str(),
                new_record.lifecycle_state.as_str(),
                permissions_json,
                limits_json,
                deps_json,
                items_json,
                new_record.updated_at.as_millis(),
                new_record.application_id.to_string(),
            ],
        )?;

        tx.commit()?;
        info!(
            application_id = %new_record.application_id,
            from_version = %old_ver,
            to_version = %new_record.installed_version,
            "Application successfully updated with history archived"
        );
        Ok(())
    }

    /// Rollback the application to its immediately preceding valid package version.
    pub fn rollback(&self, app_id: &ApplicationId) -> Result<ApplicationRecord, ApplicationError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        // 1. Fetch latest history record
        let hist_row: Option<(i64, String, String, String, String, String)> = tx
            .query_row(
                r#"
                SELECT id, version, package_id, package_hash, manifest_json, items_json
                FROM application_history
                WHERE application_id = ?1
                ORDER BY recorded_at_ms DESC, id DESC
                LIMIT 1
                "#,
                params![app_id.to_string()],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                    ))
                },
            )
            .optional()?;

        let (hist_id, hist_ver, hist_pkg_id, hist_pkg_hash, hist_manifest_json, hist_items_json) =
            match hist_row {
                Some(h) => h,
                None => {
                    return Err(ApplicationError::RollbackError(format!(
                        "No previous versions recorded for application '{}'",
                        app_id
                    )))
                }
            };

        // 2. Parse restored manifest and items
        let manifest: ApplicationManifest = serde_json::from_str(&hist_manifest_json)?;
        let items: Vec<PackageItem> = serde_json::from_str(&hist_items_json).unwrap_or_default();
        let restored_version: AppVersion = hist_ver.parse()?;
        let restored_pkg_id: PackageId = hist_pkg_id.parse()?;
        let restored_pkg_hash = ContentId::from_hex(&hist_pkg_hash)?;

        let permissions_json = serde_json::to_string(&manifest.permissions)?;
        let limits_json = serde_json::to_string(&manifest.resource_limits)?;
        let deps_json = serde_json::to_string(&manifest.dependencies)?;
        let now = Timestamp::now();

        // 3. Update main record with restored version
        tx.execute(
            r#"
            UPDATE applications SET
                installed_version = ?1,
                package_id = ?2,
                package_hash = ?3,
                manifest_json = ?4,
                publisher = ?5,
                signature_status = ?6,
                lifecycle_state = ?7,
                permissions_json = ?8,
                limits_json = ?9,
                dependencies_json = ?10,
                items_json = ?11,
                updated_at_ms = ?12
            WHERE application_id = ?13
            "#,
            params![
                restored_version.to_string(),
                restored_pkg_id.to_string(),
                restored_pkg_hash.to_hex(),
                hist_manifest_json,
                manifest.publisher.to_string(),
                SignatureStatus::Verified.as_str(),
                ApplicationState::Installed.as_str(),
                permissions_json,
                limits_json,
                deps_json,
                hist_items_json,
                now.as_millis(),
                app_id.to_string(),
            ],
        )?;

        // 4. Delete the consumed history entry
        tx.execute(
            "DELETE FROM application_history WHERE id = ?1",
            params![hist_id],
        )?;

        tx.commit()?;

        info!(
            application_id = %app_id,
            restored_version = %restored_version,
            "Application successfully rolled back"
        );

        let restored_record = ApplicationRecord {
            application_id: app_id.clone(),
            installed_version: restored_version,
            package_id: restored_pkg_id,
            package_hash: restored_pkg_hash,
            manifest: manifest.clone(),
            items,
            publisher: manifest.publisher,
            signature_status: SignatureStatus::Verified,
            lifecycle_state: ApplicationState::Installed,
            permissions: manifest.permissions,
            resource_limits: manifest.resource_limits,
            dependencies: manifest.dependencies,
            installed_at: now,
            updated_at: now,
        };

        Ok(restored_record)
    }

    /// Fetches all historical versions recorded for an application.
    pub fn get_history(
        &self,
        app_id: &ApplicationId,
    ) -> Result<Vec<ApplicationHistoryRecord>, ApplicationError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            r#"
            SELECT id, application_id, version, package_id, package_hash, manifest_json, items_json, recorded_at_ms
            FROM application_history
            WHERE application_id = ?1
            ORDER BY recorded_at_ms DESC, id DESC
            "#,
        )?;

        let rows = stmt.query_map(params![app_id.to_string()], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, String>(5)?,
                r.get::<_, String>(6)?,
                r.get::<_, u64>(7)?,
            ))
        })?;

        let mut out = Vec::new();
        for r in rows {
            let (
                id,
                app_id_str,
                ver_str,
                pkg_id_str,
                pkg_hash_str,
                manifest_json,
                items_json,
                recorded_at_ms,
            ) = r?;
            out.push(ApplicationHistoryRecord {
                id,
                application_id: app_id_str.parse()?,
                version: ver_str.parse()?,
                package_id: pkg_id_str.parse()?,
                package_hash: ContentId::from_hex(&pkg_hash_str)?,
                manifest: serde_json::from_str(&manifest_json)?,
                items: serde_json::from_str(&items_json).unwrap_or_default(),
                recorded_at: Timestamp::from_millis(recorded_at_ms),
            });
        }

        Ok(out)
    }

    /// Updates the operational lifecycle state of an application, validating state machine transition.
    pub fn update_lifecycle_state(
        &self,
        app_id: &ApplicationId,
        target_state: ApplicationState,
    ) -> Result<ApplicationState, ApplicationError> {
        let conn = self.conn.lock().unwrap();

        // 1. Fetch current state
        let current_state_str: Option<String> = conn
            .query_row(
                "SELECT lifecycle_state FROM applications WHERE application_id = ?1",
                params![app_id.to_string()],
                |r| r.get(0),
            )
            .optional()?;

        let current_state: ApplicationState = match current_state_str {
            Some(s) => s.parse()?,
            None => {
                return Err(ApplicationError::NotFound(format!(
                    "Application '{}' not found",
                    app_id
                )))
            }
        };

        // 2. Validate transition
        let validated_state = current_state.transition_to(target_state)?;

        // 3. Persist new state
        conn.execute(
            "UPDATE applications SET lifecycle_state = ?1, updated_at_ms = ?2 WHERE application_id = ?3",
            params![
                validated_state.as_str(),
                Timestamp::now().as_millis(),
                app_id.to_string(),
            ],
        )?;

        debug!(
            application_id = %app_id,
            from = %current_state,
            to = %validated_state,
            "Application lifecycle state transitioned"
        );
        Ok(validated_state)
    }

    /// Updates the authorized capability permissions granted by node policy to this application.
    pub fn update_permissions(
        &self,
        app_id: &ApplicationId,
        permissions: Vec<AppPermission>,
    ) -> Result<(), ApplicationError> {
        let conn = self.conn.lock().unwrap();
        let perms_json = serde_json::to_string(&permissions)?;
        conn.execute(
            "UPDATE applications SET permissions_json = ?1, updated_at_ms = ?2 WHERE application_id = ?3",
            params![perms_json, Timestamp::now().as_millis(), app_id.to_string()],
        )?;
        Ok(())
    }

    /// Removes an application and its history completely from the catalog.
    pub fn remove(&self, app_id: &ApplicationId) -> Result<bool, ApplicationError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        tx.execute(
            "DELETE FROM application_history WHERE application_id = ?1",
            params![app_id.to_string()],
        )?;

        let count = tx.execute(
            "DELETE FROM applications WHERE application_id = ?1",
            params![app_id.to_string()],
        )?;

        tx.commit()?;
        Ok(count > 0)
    }

    /// Returns the total bytes stored by this application according to persistent records.
    pub fn get_storage_usage(&self, app_id: &ApplicationId) -> Result<u64, ApplicationError> {
        let conn = self.conn.lock().unwrap();
        let total: Option<i64> = conn
            .query_row(
                "SELECT SUM(size_bytes) FROM application_storage_usage WHERE application_id = ?1",
                params![app_id.to_string()],
                |r| r.get(0),
            )
            .optional()?
            .flatten();
        Ok(total.unwrap_or(0) as u64)
    }

    /// Records or updates an item stored by this application.
    /// If the item was already recorded for this app, replaces it (maintaining idempotency).
    pub fn record_storage_put(
        &self,
        app_id: &ApplicationId,
        content_id: &ContentId,
        size_bytes: u64,
    ) -> Result<(), ApplicationError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            r#"
            INSERT INTO application_storage_usage (
                application_id,
                content_id,
                size_bytes,
                created_at_ms
            ) VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(application_id, content_id) DO UPDATE SET
                size_bytes = excluded.size_bytes,
                created_at_ms = excluded.created_at_ms
            "#,
            params![
                app_id.to_string(),
                content_id.to_hex(),
                size_bytes as i64,
                Timestamp::now().as_millis(),
            ],
        )?;
        Ok(())
    }

    /// Records deletion of an item owned/referenced by this application.
    /// Returns true if an item was removed, false if not found.
    pub fn record_storage_delete(
        &self,
        app_id: &ApplicationId,
        content_id: &ContentId,
    ) -> Result<bool, ApplicationError> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute(
            "DELETE FROM application_storage_usage WHERE application_id = ?1 AND content_id = ?2",
            params![app_id.to_string(), content_id.to_hex()],
        )?;
        Ok(count > 0)
    }

    /// Checks if this application owns/references the specified content item.
    pub fn has_storage_item(
        &self,
        app_id: &ApplicationId,
        content_id: &ContentId,
    ) -> Result<bool, ApplicationError> {
        let conn = self.conn.lock().unwrap();
        let exists: Option<i64> = conn
            .query_row(
                "SELECT 1 FROM application_storage_usage WHERE application_id = ?1 AND content_id = ?2",
                params![app_id.to_string(), content_id.to_hex()],
                |r| r.get(0),
            )
            .optional()?;
        Ok(exists.is_some())
    }
}
