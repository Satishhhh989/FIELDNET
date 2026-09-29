use fieldnet_content::store::{ContentStore, PutOptions};
use fieldnet_core::Timestamp;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::info;

use crate::error::ApplicationError;
use crate::events::ApplicationEvent;
use crate::id::ApplicationId;
use crate::lifecycle::ApplicationState;
use crate::package::ApplicationPackage;
use crate::registry::{ApplicationRecord, ApplicationRegistry, SignatureStatus};

/// Pipeline orchestrator managing atomic installation, upgrades, and rollbacks of application packages.
pub struct ApplicationInstaller {
    registry: Arc<ApplicationRegistry>,
    content_store: Option<Arc<ContentStore>>,
    event_tx: broadcast::Sender<ApplicationEvent>,
}

impl ApplicationInstaller {
    /// Creates a new installer instance.
    pub fn new(
        registry: Arc<ApplicationRegistry>,
        content_store: Option<Arc<ContentStore>>,
        event_tx: broadcast::Sender<ApplicationEvent>,
    ) -> Self {
        Self {
            registry,
            content_store,
            event_tx,
        }
    }

    /// Returns a reference to the underlying registry.
    pub fn registry(&self) -> &Arc<ApplicationRegistry> {
        &self.registry
    }

    /// Executes the full atomic installation pipeline for an application package:
    ///
    /// 1. PACKAGE RECEIVED
    /// 2. PACKAGE INTEGRITY VERIFIED
    /// 3. MANIFEST PARSED & VALIDATED
    /// 4. SIGNATURE VERIFIED
    /// 5. DEPENDENCIES VALIDATED
    /// 6. CONTENT STORED IN CAS (optional if CAS provided)
    /// 7. APPLICATION REGISTERED ATOMICALLY
    /// 8. INSTALLED STATE & EVENT EMITTED
    pub async fn install(
        &self,
        package: &ApplicationPackage,
        item_payloads: &HashMap<String, Vec<u8>>,
    ) -> Result<ApplicationRecord, ApplicationError> {
        let app_id = &package.manifest.application_id;

        // 1. Emit discovered event
        let _ = self.event_tx.send(ApplicationEvent::Discovered {
            application_id: app_id.clone(),
            package_id: package.package_id.clone(),
            publisher: package.manifest.publisher,
        });

        // 2. Verify Package Content Integrity
        let computed_hash = package.verify_integrity(Some(item_payloads))?;

        // 3. Verify Publisher Signature
        package.verify_signature()?;

        // 4. Emit verified event
        let _ = self.event_tx.send(ApplicationEvent::Verified {
            application_id: app_id.clone(),
            package_id: package.package_id.clone(),
            package_hash: computed_hash,
        });

        // 5. Validate Dependencies
        for dep in &package.manifest.dependencies {
            dep.validate()?;
            // Note: In later builds, dependency satisfaction against the local registry will be resolved here.
        }

        // 6. Check if already installed
        let existing = self.registry.get(app_id)?;
        if let Some(existing_record) = existing {
            if existing_record.installed_version >= package.manifest.version {
                return Err(ApplicationError::AlreadyInstalled(format!(
                    "Application '{}' is already installed with version {} (cannot downgrade to {})",
                    app_id, existing_record.installed_version, package.manifest.version
                )));
            }

            // Route to update pipeline
            return self.update(package, item_payloads).await;
        }

        // 7. Emit installing event
        let _ = self.event_tx.send(ApplicationEvent::Installing {
            application_id: app_id.clone(),
            package_id: package.package_id.clone(),
            version: package.manifest.version.to_string(),
        });

        // 8. Store items in Content-Addressed Storage if configured
        if let Some(store) = &self.content_store {
            for item in &package.items {
                if let Some(bytes) = item_payloads.get(&item.path) {
                    let opts = PutOptions {
                        is_pinned: true,
                        mime_type: item.mime_type.clone(),
                        ..Default::default()
                    };
                    store.put(bytes.as_slice(), Some(opts)).await.map_err(|e| {
                        ApplicationError::ContentError(format!(
                            "Failed to commit package item '{}' into CAS: {}",
                            item.path, e
                        ))
                    })?;
                }
            }
        }

        let now = Timestamp::now();
        let record = ApplicationRecord {
            application_id: app_id.clone(),
            installed_version: package.manifest.version.clone(),
            package_id: package.package_id.clone(),
            package_hash: computed_hash,
            manifest: package.manifest.clone(),
            items: package.items.clone(),
            publisher: package.manifest.publisher,
            signature_status: SignatureStatus::Verified,
            lifecycle_state: ApplicationState::Installed,
            permissions: package.manifest.permissions.clone(),
            resource_limits: package.manifest.resource_limits.clone(),
            dependencies: package.manifest.dependencies.clone(),
            installed_at: now,
            updated_at: now,
        };

        // 9. Atomic Registry Insert
        self.registry.register(&record)?;

        // 10. Emit installed event
        let _ = self.event_tx.send(ApplicationEvent::Installed {
            application_id: app_id.clone(),
            package_id: package.package_id.clone(),
            version: package.manifest.version.to_string(),
        });

        info!(
            application_id = %app_id,
            version = %package.manifest.version,
            "Application successfully installed"
        );

        Ok(record)
    }

    /// Upgrades an existing application package, preserving the previous package in history.
    pub async fn update(
        &self,
        package: &ApplicationPackage,
        item_payloads: &HashMap<String, Vec<u8>>,
    ) -> Result<ApplicationRecord, ApplicationError> {
        let app_id = &package.manifest.application_id;

        let existing = self.registry.get(app_id)?.ok_or_else(|| {
            ApplicationError::NotFound(format!("Application '{}' not found", app_id))
        })?;

        // 1. Verify integrity & signature
        let computed_hash = package.verify_integrity(Some(item_payloads))?;
        package.verify_signature()?;

        // 2. Store new items in CAS if configured
        if let Some(store) = &self.content_store {
            for item in &package.items {
                if let Some(bytes) = item_payloads.get(&item.path) {
                    let opts = PutOptions {
                        is_pinned: true,
                        mime_type: item.mime_type.clone(),
                        ..Default::default()
                    };
                    store.put(bytes.as_slice(), Some(opts)).await.map_err(|e| {
                        ApplicationError::ContentError(format!(
                            "Failed to store updated item '{}' into CAS: {}",
                            item.path, e
                        ))
                    })?;
                }
            }
        }

        let now = Timestamp::now();
        let new_record = ApplicationRecord {
            application_id: app_id.clone(),
            installed_version: package.manifest.version.clone(),
            package_id: package.package_id.clone(),
            package_hash: computed_hash,
            manifest: package.manifest.clone(),
            items: package.items.clone(),
            publisher: package.manifest.publisher,
            signature_status: SignatureStatus::Verified,
            lifecycle_state: ApplicationState::Installed,
            permissions: package.manifest.permissions.clone(),
            resource_limits: package.manifest.resource_limits.clone(),
            dependencies: package.manifest.dependencies.clone(),
            installed_at: existing.installed_at,
            updated_at: now,
        };

        // 3. Atomically update version and archive old record
        self.registry.update_version(&new_record)?;

        // 4. Emit updated event
        let _ = self.event_tx.send(ApplicationEvent::Updated {
            application_id: app_id.clone(),
            from_version: existing.installed_version.to_string(),
            to_version: package.manifest.version.to_string(),
            package_id: package.package_id.clone(),
        });

        info!(
            application_id = %app_id,
            from = %existing.installed_version,
            to = %package.manifest.version,
            "Application successfully upgraded"
        );

        Ok(new_record)
    }

    /// Rolls back an application to its previous recorded version.
    pub async fn rollback(
        &self,
        app_id: &ApplicationId,
    ) -> Result<ApplicationRecord, ApplicationError> {
        let current = self.registry.get(app_id)?.ok_or_else(|| {
            ApplicationError::NotFound(format!("Application '{}' not found", app_id))
        })?;

        let restored = self.registry.rollback(app_id)?;

        // Emit rollback event
        let _ = self.event_tx.send(ApplicationEvent::Rollback {
            application_id: app_id.clone(),
            from_version: current.installed_version.to_string(),
            to_version: restored.installed_version.to_string(),
            package_id: restored.package_id.clone(),
        });

        info!(
            application_id = %app_id,
            from_version = %current.installed_version,
            restored_version = %restored.installed_version,
            "Application rolled back"
        );

        Ok(restored)
    }
}
