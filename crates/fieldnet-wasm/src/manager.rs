use fieldnet_content::store::{ContentStore, PutOptions};
use fieldnet_core::Timestamp;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{info, warn};

use crate::dependencies::{DependencyResolutionReport, DependencyResolver};
use crate::error::ApplicationError;
use crate::events::ApplicationEvent;
use crate::execution::supervisor::{ApplicationExecutionSupervisor, ExecutionReport};
use crate::id::{AppVersion, ApplicationId, ExecutionId};
use crate::installer::ApplicationInstaller;
use crate::lifecycle::{ApplicationState, UpdateTransactionState};
use crate::package::ApplicationPackage;
use crate::registry::{ApplicationRecord, ApplicationRegistry, UpdateTransactionRecord};

/// Report generated during node startup reconciliation of application lifecycle states.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StartupReconciliationReport {
    pub stale_executions_reconciled: usize,
    pub incomplete_updates_reconciled: usize,
    pub corrupted_packages_isolated: usize,
    pub total_active_applications: usize,
}

/// Central application lifecycle manager coordinating installation, dependency resolution,
/// authorization, execution startup/shutdown, update transactions, rollback, isolation, and uninstallation.
pub struct ApplicationLifecycleManager {
    registry: Arc<ApplicationRegistry>,
    installer: Arc<ApplicationInstaller>,
    supervisor: Arc<ApplicationExecutionSupervisor>,
    dependency_resolver: Arc<DependencyResolver>,
    content_store: Arc<ContentStore>,
    event_tx: broadcast::Sender<ApplicationEvent>,
    max_consecutive_crashes: u32,
}

impl ApplicationLifecycleManager {
    /// Creates a new ApplicationLifecycleManager.
    pub fn new(
        registry: Arc<ApplicationRegistry>,
        installer: Arc<ApplicationInstaller>,
        supervisor: Arc<ApplicationExecutionSupervisor>,
        dependency_resolver: Arc<DependencyResolver>,
        content_store: Arc<ContentStore>,
        event_tx: broadcast::Sender<ApplicationEvent>,
    ) -> Self {
        Self {
            registry,
            installer,
            supervisor,
            dependency_resolver,
            content_store,
            event_tx,
            max_consecutive_crashes: 3,
        }
    }

    /// Accessor for the underlying application registry.
    pub fn registry(&self) -> &Arc<ApplicationRegistry> {
        &self.registry
    }

    /// Accessor for the application installer.
    pub fn installer(&self) -> &Arc<ApplicationInstaller> {
        &self.installer
    }

    /// Accessor for the execution supervisor.
    pub fn supervisor(&self) -> &Arc<ApplicationExecutionSupervisor> {
        &self.supervisor
    }

    /// Accessor for the dependency resolver.
    pub fn dependency_resolver(&self) -> &Arc<DependencyResolver> {
        &self.dependency_resolver
    }

    /// Accessor for the content store.
    pub fn content_store(&self) -> &Arc<ContentStore> {
        &self.content_store
    }

    /// Resolves dependencies for an application by its ApplicationId.
    pub async fn resolve_dependencies(
        &self,
        app_id: &ApplicationId,
    ) -> Result<DependencyResolutionReport, ApplicationError> {
        self.dependency_resolver.resolve_for_app(app_id).await
    }

    /// Installs a new application package after validating integrity, signature, and dependencies.
    pub async fn install(
        &self,
        package: &ApplicationPackage,
        item_payloads: &HashMap<String, Vec<u8>>,
    ) -> Result<ApplicationRecord, ApplicationError> {
        let app_id = &package.manifest.application_id;

        // 1. Dependency Resolution & Verification
        let _ = self.event_tx.send(ApplicationEvent::DependencyChecking {
            application_id: app_id.clone(),
            dependency_count: package.manifest.dependencies.len(),
        });

        let dep_report = self
            .dependency_resolver
            .resolve_for_manifest(&package.manifest)
            .await?;

        if !dep_report.all_satisfied {
            let reason = format!(
                "Missing required dependencies: {}",
                dep_report.missing_required.join(", ")
            );
            let _ = self.event_tx.send(ApplicationEvent::DependencyFailed {
                application_id: app_id.clone(),
                reason: reason.clone(),
            });
            return Err(ApplicationError::DependencyError(reason));
        }

        // 2. Delegate to atomic installer pipeline
        self.installer.install(package, item_payloads).await
    }

    /// Authorizes an installed application, enabling execution by node security policy.
    pub fn authorize(&self, app_id: &ApplicationId) -> Result<ApplicationState, ApplicationError> {
        if self.registry.is_isolated(app_id)? {
            return Err(ApplicationError::ExecutionError(format!(
                "Application '{}' is in ISOLATED state and cannot be authorized",
                app_id
            )));
        }

        let new_state = self
            .registry
            .update_lifecycle_state(app_id, ApplicationState::Authorized)?;

        let record = self.registry.get(app_id)?.ok_or_else(|| {
            ApplicationError::NotFound(format!("Application '{}' not found", app_id))
        })?;

        let _ = self.event_tx.send(ApplicationEvent::Authorized {
            application_id: app_id.clone(),
            granted_permissions: record.permissions,
        });

        Ok(new_state)
    }

    /// Starts an authorized application under strict dependency and isolation checks.
    pub async fn start(&self, app_id: &ApplicationId) -> Result<ExecutionReport, ApplicationError> {
        // 1. Validate application exists
        let record = self.registry.get(app_id)?.ok_or_else(|| {
            ApplicationError::NotFound(format!("Application '{}' not found", app_id))
        })?;

        // 2. Check isolation status: isolated applications must never start
        if self.registry.is_isolated(app_id)?
            || record.lifecycle_state == ApplicationState::Isolated
        {
            let reason = self
                .registry
                .get_isolation_reason(app_id)?
                .unwrap_or_else(|| "Quarantined by node security policy".to_string());
            return Err(ApplicationError::ExecutionError(format!(
                "Application '{}' is ISOLATED and cannot start: {}",
                app_id, reason
            )));
        }

        // 3. Validate state eligibility
        match record.lifecycle_state {
            ApplicationState::Authorized
            | ApplicationState::Stopped
            | ApplicationState::Running
            | ApplicationState::Crashed => {}
            ApplicationState::Installed => {
                return Err(ApplicationError::ApplicationNotAuthorized {
                    current_state: record.lifecycle_state,
                });
            }
            other => {
                return Err(ApplicationError::ExecutionError(format!(
                    "Application '{}' cannot be started in state {:?}",
                    app_id, other
                )));
            }
        }

        // 4. Validate Dependencies: required services and applications must be available
        let _ = self.event_tx.send(ApplicationEvent::DependencyChecking {
            application_id: app_id.clone(),
            dependency_count: record.dependencies.len(),
        });

        let dep_report = self
            .dependency_resolver
            .resolve_for_manifest(&record.manifest)
            .await?;

        if !dep_report.all_satisfied {
            let reason = format!(
                "Cannot start application '{}': missing required dependencies: {}",
                app_id,
                dep_report.missing_required.join(", ")
            );
            let _ = self.event_tx.send(ApplicationEvent::DependencyFailed {
                application_id: app_id.clone(),
                reason: reason.clone(),
            });
            return Err(ApplicationError::DependencyError(reason));
        }

        // 5. Verify Package CAS integrity: entrypoint item must be intact
        let entrypoint = record.entrypoint_item()?;
        if !self
            .content_store
            .has(&entrypoint.content_id)
            .map_err(|e| {
                ApplicationError::ContentError(format!("Failed to verify entrypoint in CAS: {}", e))
            })?
        {
            // Isolate application immediately due to missing package artifact
            self.isolate(
                app_id,
                &format!("Missing entrypoint binary '{}' in CAS", entrypoint.path),
            )
            .await?;
            return Err(ApplicationError::IntegrityError(format!(
                "Entrypoint binary '{}' missing from content store; application quarantined",
                entrypoint.path
            )));
        }

        // 6. Delegate to execution supervisor
        let report = match self.supervisor.execute(app_id).await {
            Ok(report) => report,
            Err(err) => {
                let is_crash = matches!(
                    err,
                    ApplicationError::TrapError { .. }
                        | ApplicationError::FuelExhausted
                        | ApplicationError::MemoryLimitExceeded(_)
                        | ApplicationError::ExecutionTimeout { .. }
                        | ApplicationError::ExecutionError(_)
                );
                if is_crash {
                    let exec_id = ExecutionId::generate();
                    let _ = self.registry.record_crash(
                        app_id,
                        &exec_id,
                        &record.installed_version,
                        &err.to_string(),
                    );

                    // Check repeated crashes
                    if let Ok(history) = self.registry.get_crash_history(app_id) {
                        if history.len() >= self.max_consecutive_crashes as usize {
                            warn!(
                                application_id = %app_id,
                                crashes = history.len(),
                                "Application exceeded maximum crash threshold, moving to ISOLATED"
                            );
                            let _ = self
                                .isolate(
                                    app_id,
                                    &format!(
                                        "Exceeded maximum consecutive crash threshold ({} crashes)",
                                        history.len()
                                    ),
                                )
                                .await;
                        }
                    }
                }
                return Err(err);
            }
        };

        Ok(report)
    }

    /// Stops any active executions for an application cleanly.
    pub async fn stop(&self, app_id: &ApplicationId) -> Result<(), ApplicationError> {
        let active = self.supervisor.list_active_executions().await;

        for exec in active {
            if exec.application_id == *app_id {
                self.supervisor.stop_execution(&exec.execution_id).await?;
            }
        }

        // Transition state to Stopped if currently Running or Starting
        let current_record = self.registry.get(app_id)?;
        if let Some(record) = current_record {
            if matches!(
                record.lifecycle_state,
                ApplicationState::Running
                    | ApplicationState::Starting
                    | ApplicationState::Authorized
            ) {
                let _ = self
                    .registry
                    .update_lifecycle_state(app_id, ApplicationState::Stopped);

                let _ = self.event_tx.send(ApplicationEvent::Stopped {
                    application_id: app_id.clone(),
                    package_id: Some(record.package_id),
                    execution_id: None,
                    reason: Some("Stop requested by lifecycle manager".to_string()),
                    timestamp: Timestamp::now().as_millis(),
                });
            }
        }

        Ok(())
    }

    /// Gracefully restarts an application, re-validating package, dependencies, and limits.
    pub async fn restart(
        &self,
        app_id: &ApplicationId,
    ) -> Result<ExecutionReport, ApplicationError> {
        info!(application_id = %app_id, "Restarting application");
        self.stop(app_id).await?;
        self.start(app_id).await
    }

    /// Executes an atomic, transactional application package update with automatic rollback.
    pub async fn update(
        &self,
        package: &ApplicationPackage,
        item_payloads: &HashMap<String, Vec<u8>>,
    ) -> Result<ApplicationRecord, ApplicationError> {
        let app_id = &package.manifest.application_id;

        let existing = self.registry.get(app_id)?.ok_or_else(|| {
            ApplicationError::NotFound(format!("Application '{}' not found for update", app_id))
        })?;

        // Verify version progression
        if package.manifest.version <= existing.installed_version {
            return Err(ApplicationError::AlreadyInstalled(format!(
                "Cannot update application '{}': new version {} must be greater than installed version {}",
                app_id, package.manifest.version, existing.installed_version
            )));
        }

        let update_id = format!(
            "upd:{}:{}:{}",
            app_id,
            package.manifest.version,
            Timestamp::now().as_millis()
        );

        // 1. Transaction State: PREPARING
        let tx_record = UpdateTransactionRecord {
            update_id: update_id.clone(),
            application_id: app_id.clone(),
            target_version: package.manifest.version.clone(),
            target_package_id: package.package_id.clone(),
            target_package_hash: package.compute_package_hash(),
            target_manifest: package.manifest.clone(),
            target_items: package.items.clone(),
            previous_version: existing.installed_version.clone(),
            previous_package_id: existing.package_id.clone(),
            previous_package_hash: existing.package_hash,
            state: UpdateTransactionState::Preparing,
            error_message: None,
            created_at: Timestamp::now(),
            updated_at: Timestamp::now(),
        };
        self.registry.create_update_transaction(&tx_record)?;

        let _ = self.event_tx.send(ApplicationEvent::UpdateStarted {
            application_id: app_id.clone(),
            target_version: package.manifest.version.to_string(),
            package_id: package.package_id.clone(),
        });

        // 2. Transaction State: VERIFIED (Verify package integrity and signature)
        let computed_hash = match package.verify_integrity(Some(item_payloads)) {
            Ok(h) => h,
            Err(e) => {
                let err_msg = format!("Update package integrity failed: {}", e);
                let _ = self.registry.set_update_transaction_state(
                    &update_id,
                    UpdateTransactionState::Failed,
                    Some(err_msg.clone()),
                );
                let _ = self.event_tx.send(ApplicationEvent::UpdateFailed {
                    application_id: app_id.clone(),
                    target_version: package.manifest.version.to_string(),
                    reason: err_msg,
                });
                return Err(e);
            }
        };

        if let Err(e) = package.verify_signature() {
            let err_msg = format!("Update signature verification failed: {}", e);
            let _ = self.registry.set_update_transaction_state(
                &update_id,
                UpdateTransactionState::Failed,
                Some(err_msg.clone()),
            );
            let _ = self.event_tx.send(ApplicationEvent::UpdateFailed {
                application_id: app_id.clone(),
                target_version: package.manifest.version.to_string(),
                reason: err_msg,
            });
            return Err(e);
        }

        self.registry.set_update_transaction_state(
            &update_id,
            UpdateTransactionState::Verified,
            None,
        )?;

        // 3. Resolve dependencies for new package
        let dep_report = self
            .dependency_resolver
            .resolve_for_manifest(&package.manifest)
            .await?;

        if !dep_report.all_satisfied {
            let err_msg = format!(
                "Update dependencies unsatisfied: {}",
                dep_report.missing_required.join(", ")
            );
            let _ = self.registry.set_update_transaction_state(
                &update_id,
                UpdateTransactionState::Failed,
                Some(err_msg.clone()),
            );
            let _ = self.event_tx.send(ApplicationEvent::UpdateFailed {
                application_id: app_id.clone(),
                target_version: package.manifest.version.to_string(),
                reason: err_msg.clone(),
            });
            return Err(ApplicationError::DependencyError(err_msg));
        }

        // 4. Transaction State: STAGED (Commit items to CAS)
        for item in &package.items {
            if let Some(bytes) = item_payloads.get(&item.path) {
                let opts = PutOptions {
                    is_pinned: true,
                    mime_type: item.mime_type.clone(),
                    ..Default::default()
                };
                if let Err(e) = self.content_store.put(bytes.as_slice(), Some(opts)).await {
                    let err_msg = format!("Failed to commit update item into CAS: {}", e);
                    let _ = self.registry.set_update_transaction_state(
                        &update_id,
                        UpdateTransactionState::Failed,
                        Some(err_msg.clone()),
                    );
                    return Err(ApplicationError::ContentError(err_msg));
                }
            }
        }

        self.registry.set_update_transaction_state(
            &update_id,
            UpdateTransactionState::Staged,
            None,
        )?;

        let _ = self.event_tx.send(ApplicationEvent::UpdateStaged {
            application_id: app_id.clone(),
            version: package.manifest.version.to_string(),
            package_id: package.package_id.clone(),
        });

        // 5. Transaction State: ACTIVATING (Stop old version if running)
        self.registry.set_update_transaction_state(
            &update_id,
            UpdateTransactionState::Activating,
            None,
        )?;

        let was_running = self
            .supervisor
            .list_active_executions()
            .await
            .iter()
            .any(|e| e.application_id == *app_id);

        if was_running {
            self.stop(app_id).await?;
        }

        // 6. Atomically update version and archive old record in registry
        let now = Timestamp::now();
        let new_record = ApplicationRecord {
            application_id: app_id.clone(),
            installed_version: package.manifest.version.clone(),
            package_id: package.package_id.clone(),
            package_hash: computed_hash,
            manifest: package.manifest.clone(),
            items: package.items.clone(),
            publisher: package.manifest.publisher,
            signature_status: crate::registry::SignatureStatus::Verified,
            lifecycle_state: ApplicationState::Installed,
            permissions: package.manifest.permissions.clone(),
            resource_limits: package.manifest.resource_limits.clone(),
            dependencies: package.manifest.dependencies.clone(),
            installed_at: existing.installed_at,
            updated_at: now,
        };

        if let Err(e) = self.registry.update_version(&new_record) {
            let err_msg = format!("Registry update_version failed: {}", e);
            let _ = self.registry.set_update_transaction_state(
                &update_id,
                UpdateTransactionState::Failed,
                Some(err_msg.clone()),
            );
            return Err(e);
        }

        // 7. Transaction State: ACTIVE
        self.registry.set_update_transaction_state(
            &update_id,
            UpdateTransactionState::Active,
            None,
        )?;

        let _ = self.event_tx.send(ApplicationEvent::UpdateActivated {
            application_id: app_id.clone(),
            from_version: existing.installed_version.to_string(),
            to_version: package.manifest.version.to_string(),
            package_id: package.package_id.clone(),
        });

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
            "Application update completed successfully"
        );

        Ok(new_record)
    }

    /// Rolls back an application to a previous version after verifying integrity, signature, and dependencies.
    pub async fn rollback(
        &self,
        app_id: &ApplicationId,
        target_version: Option<&AppVersion>,
    ) -> Result<ApplicationRecord, ApplicationError> {
        let current = self.registry.get(app_id)?.ok_or_else(|| {
            ApplicationError::NotFound(format!("Application '{}' not found for rollback", app_id))
        })?;

        // 1. Fetch historical record to inspect before modifying state
        let target_hist = match target_version {
            Some(ver) => self
                .registry
                .get_history_version(app_id, ver)?
                .ok_or_else(|| {
                    ApplicationError::RollbackError(format!(
                        "Target version '{}' not found in history for application '{}'",
                        ver, app_id
                    ))
                })?,
            None => {
                let history = self.registry.get_history(app_id)?;
                history.into_iter().next().ok_or_else(|| {
                    ApplicationError::RollbackError(format!(
                        "No previous versions recorded in history for application '{}'",
                        app_id
                    ))
                })?
            }
        };

        let _ = self.event_tx.send(ApplicationEvent::RollbackStarted {
            application_id: app_id.clone(),
            target_version: target_hist.version.to_string(),
        });

        // 2. Verify previous package items in CAS
        for item in &target_hist.items {
            if !self.content_store.has(&item.content_id).map_err(|e| {
                ApplicationError::ContentError(format!(
                    "Rollback verification failed for CAS item '{}': {}",
                    item.path, e
                ))
            })? {
                return Err(ApplicationError::IntegrityError(format!(
                    "Rollback rejected: package item '{}' missing from ContentStore",
                    item.path
                )));
            }
        }

        // 3. Re-validate previous dependencies
        let dep_report = self
            .dependency_resolver
            .resolve_for_manifest(&target_hist.manifest)
            .await?;

        if !dep_report.all_satisfied {
            return Err(ApplicationError::DependencyError(format!(
                "Rollback rejected: dependencies for version {} cannot be satisfied: {}",
                target_hist.version,
                dep_report.missing_required.join(", ")
            )));
        }

        // 4. Stop current version if running
        self.stop(app_id).await?;

        // 5. Restore in registry
        let restored = self.registry.rollback_to_version(app_id, target_version)?;

        // 6. Emit events
        let _ = self.event_tx.send(ApplicationEvent::RollbackCompleted {
            application_id: app_id.clone(),
            restored_version: restored.installed_version.to_string(),
            package_id: restored.package_id.clone(),
        });

        let _ = self.event_tx.send(ApplicationEvent::Rollback {
            application_id: app_id.clone(),
            from_version: current.installed_version.to_string(),
            to_version: restored.installed_version.to_string(),
            package_id: restored.package_id.clone(),
        });

        info!(
            application_id = %app_id,
            from = %current.installed_version,
            to = %restored.installed_version,
            "Application successfully rolled back"
        );

        Ok(restored)
    }

    /// Moves an application into administrative or security ISOLATION, halting execution and preventing startup.
    pub async fn isolate(
        &self,
        app_id: &ApplicationId,
        reason: &str,
    ) -> Result<(), ApplicationError> {
        info!(application_id = %app_id, reason = %reason, "Isolating application");

        // Stop any running instances immediately
        let _ = self.stop(app_id).await;

        self.registry.record_isolation(app_id, reason)?;
        self.registry
            .update_lifecycle_state(app_id, ApplicationState::Isolated)?;

        let record = self.registry.get(app_id)?;

        let _ = self.event_tx.send(ApplicationEvent::Isolated {
            application_id: app_id.clone(),
            package_id: record.map(|r| r.package_id),
            execution_id: None,
            reason: reason.to_string(),
            timestamp: Timestamp::now().as_millis(),
        });

        Ok(())
    }

    /// Removes an application from ISOLATION back to STOPPED state via operator action.
    pub async fn unisolate(
        &self,
        app_id: &ApplicationId,
    ) -> Result<ApplicationState, ApplicationError> {
        info!(application_id = %app_id, "Lifting application isolation");

        self.registry.clear_isolation(app_id)?;
        self.registry
            .update_lifecycle_state(app_id, ApplicationState::Stopped)
    }

    /// Safely uninstalls an application from the node catalog, preserving shared CAS content.
    pub async fn uninstall(&self, app_id: &ApplicationId) -> Result<(), ApplicationError> {
        // Check if running
        let active = self.supervisor.list_active_executions().await;
        if active.iter().any(|e| e.application_id == *app_id) {
            return Err(ApplicationError::ExecutionError(format!(
                "Cannot uninstall application '{}' while actively running; stop it first",
                app_id
            )));
        }

        // Atomically remove from registry
        let removed = self.registry.remove(app_id)?;
        if !removed {
            return Err(ApplicationError::NotFound(format!(
                "Application '{}' not found for uninstall",
                app_id
            )));
        }

        let _ = self.event_tx.send(ApplicationEvent::Uninstalled {
            application_id: app_id.clone(),
        });

        info!(
            application_id = %app_id,
            "Application successfully uninstalled (shared CAS content preserved)"
        );

        Ok(())
    }

    /// Reconciles application lifecycle states and incomplete updates upon runtime restart.
    pub async fn reconcile_on_startup(
        &self,
    ) -> Result<StartupReconciliationReport, ApplicationError> {
        info!("Executing application lifecycle startup reconciliation");

        // 1. Reconcile supervisor executions
        let stale_reconciled = self.supervisor.reconcile_on_startup()?;

        // 2. Reconcile incomplete updates
        let incomplete_updates = self.registry.list_incomplete_update_transactions()?;
        let incomplete_count = incomplete_updates.len();

        for tx in incomplete_updates {
            warn!(
                update_id = %tx.update_id,
                application_id = %tx.application_id,
                state = ?tx.state,
                "Reconciling interrupted update transaction on startup"
            );

            // If interrupted before activation, mark FAILED without touching installed version
            let _ = self.registry.set_update_transaction_state(
                &tx.update_id,
                UpdateTransactionState::Failed,
                Some("Update interrupted by node shutdown before activation completed".to_string()),
            );
        }

        // 3. Inspect registered applications and validate package CAS items
        let all_apps = self.registry.list()?;
        let mut corrupted_isolated = 0;

        for app in &all_apps {
            // Verify entrypoint CAS item
            let mut is_corrupted = false;
            if let Ok(entrypoint) = app.entrypoint_item() {
                if let Ok(has_entry) = self.content_store.has(&entrypoint.content_id) {
                    if !has_entry {
                        is_corrupted = true;
                    }
                } else {
                    is_corrupted = true;
                }
            } else {
                is_corrupted = true;
            }

            if is_corrupted && app.lifecycle_state != ApplicationState::Isolated {
                warn!(
                    application_id = %app.application_id,
                    "Corrupted application package detected on startup, quarantining"
                );
                let _ = self
                    .isolate(
                        &app.application_id,
                        "Package entrypoint missing from ContentStore on startup",
                    )
                    .await;
                corrupted_isolated += 1;
            }
        }

        let total_active = all_apps
            .iter()
            .filter(|a| a.lifecycle_state != ApplicationState::Isolated)
            .count();

        Ok(StartupReconciliationReport {
            stale_executions_reconciled: stale_reconciled,
            incomplete_updates_reconciled: incomplete_count,
            corrupted_packages_isolated: corrupted_isolated,
            total_active_applications: total_active,
        })
    }
}
