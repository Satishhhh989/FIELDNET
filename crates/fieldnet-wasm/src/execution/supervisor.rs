use fieldnet_content::store::ContentStore;
use fieldnet_core::{NodeId, Timestamp};
use fieldnet_service::resolver::ServiceResolver;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, oneshot, RwLock};
use tracing::{info, warn};
use wasmtime::{Linker, Store, Val};

use crate::error::ApplicationError;
use crate::events::ApplicationEvent;
use crate::execution::cache::ModuleCache;
use crate::execution::engine::{WasmEngine, DEFAULT_EPOCH_TICK_INTERVAL_MS};
use crate::execution::store::StoreData;
use crate::host::context::HostContext;
use crate::host::events::HostEvents;
use crate::host::network::{ApplicationNetworkSender, HostNetwork};
use crate::host::permissions::CapabilityContext;
use crate::host::register_host_functions;
use crate::host::services::HostServices;
use crate::host::storage::HostStorage;
use crate::id::{ApplicationId, ExecutionId, PackageId};
use crate::lifecycle::ApplicationState;
use crate::registry::ApplicationRegistry;

/// Information about an active in-flight application execution.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActiveExecutionInfo {
    pub execution_id: ExecutionId,
    pub application_id: ApplicationId,
    pub package_id: PackageId,
    pub start_time: Timestamp,
    pub lifecycle_state: ApplicationState,
}

/// Comprehensive report generated at the termination of an application execution instance.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionReport {
    pub execution_id: ExecutionId,
    pub application_id: ApplicationId,
    pub package_id: PackageId,
    pub start_time: Timestamp,
    pub finish_time: Timestamp,
    pub duration_ms: u64,
    pub fuel_consumed: u64,
    pub exit_code: Option<i32>,
    pub final_state: ApplicationState,
    pub error: Option<String>,
}

/// Execution Supervisor orchestrating isolated Wasmtime execution, resource limit enforcement,
/// lifecycle transitions, trap isolation, and cancellation.
pub struct ApplicationExecutionSupervisor {
    engine: Arc<WasmEngine>,
    cache: Arc<ModuleCache>,
    registry: Arc<ApplicationRegistry>,
    content_store: Arc<ContentStore>,
    event_tx: broadcast::Sender<ApplicationEvent>,
    active_executions: Arc<RwLock<HashMap<ExecutionId, ActiveExecutionInfo>>>,
    cancellation_channels: Arc<RwLock<HashMap<ExecutionId, oneshot::Sender<()>>>>,
    service_resolver: Arc<std::sync::RwLock<Option<Arc<ServiceResolver>>>>,
    network_sender: Arc<std::sync::RwLock<Option<Arc<dyn ApplicationNetworkSender>>>>,
    local_node_id: Arc<std::sync::RwLock<Option<NodeId>>>,
}

impl ApplicationExecutionSupervisor {
    /// Creates a new execution supervisor.
    pub fn new(
        engine: Arc<WasmEngine>,
        cache: Arc<ModuleCache>,
        registry: Arc<ApplicationRegistry>,
        content_store: Arc<ContentStore>,
        event_tx: broadcast::Sender<ApplicationEvent>,
    ) -> Self {
        Self {
            engine,
            cache,
            registry,
            content_store,
            event_tx,
            active_executions: Arc::new(RwLock::new(HashMap::new())),
            cancellation_channels: Arc::new(RwLock::new(HashMap::new())),
            service_resolver: Arc::new(std::sync::RwLock::new(None)),
            network_sender: Arc::new(std::sync::RwLock::new(None)),
            local_node_id: Arc::new(std::sync::RwLock::new(None)),
        }
    }

    /// Builder method to configure a ServiceResolver.
    pub fn with_service_resolver(self, resolver: Arc<ServiceResolver>) -> Self {
        *self.service_resolver.write().unwrap() = Some(resolver);
        self
    }

    /// Builder method to configure an ApplicationNetworkSender.
    pub fn with_network_sender(self, sender: Arc<dyn ApplicationNetworkSender>) -> Self {
        *self.network_sender.write().unwrap() = Some(sender);
        self
    }

    /// Builder method to configure the local NodeId.
    pub fn with_node_id(self, node_id: NodeId) -> Self {
        *self.local_node_id.write().unwrap() = Some(node_id);
        self
    }

    /// Dynamically sets or updates the ServiceResolver.
    pub async fn set_service_resolver(&self, resolver: Arc<ServiceResolver>) {
        let mut guard = self.service_resolver.write().unwrap();
        *guard = Some(resolver);
    }

    /// Dynamically sets or updates the ApplicationNetworkSender.
    pub async fn set_network_sender(&self, sender: Arc<dyn ApplicationNetworkSender>) {
        let mut guard = self.network_sender.write().unwrap();
        *guard = Some(sender);
    }

    /// Dynamically sets or updates the local NodeId.
    pub async fn set_node_id(&self, node_id: NodeId) {
        let mut guard = self.local_node_id.write().unwrap();
        *guard = Some(node_id);
    }

    /// Accessor for the engine.
    pub fn engine(&self) -> &Arc<WasmEngine> {
        &self.engine
    }

    /// Accessor for the module cache.
    pub fn cache(&self) -> &Arc<ModuleCache> {
        &self.cache
    }

    /// Accessor for the application registry.
    pub fn registry(&self) -> &Arc<ApplicationRegistry> {
        &self.registry
    }

    /// Accessor for the content-addressed store.
    pub fn content_store(&self) -> &Arc<ContentStore> {
        &self.content_store
    }

    /// Returns a list of all currently active executions.
    pub async fn list_active_executions(&self) -> Vec<ActiveExecutionInfo> {
        let guard = self.active_executions.read().await;
        guard.values().cloned().collect()
    }

    /// Returns details for a specific active execution.
    pub async fn get_active_execution(
        &self,
        execution_id: &ExecutionId,
    ) -> Option<ActiveExecutionInfo> {
        let guard = self.active_executions.read().await;
        guard.get(execution_id).cloned()
    }

    /// Executes an authorized application, generating a unique `ExecutionId`.
    pub async fn execute(
        &self,
        app_id: &ApplicationId,
    ) -> Result<ExecutionReport, ApplicationError> {
        let execution_id = ExecutionId::generate();
        self.execute_with_id(app_id, execution_id).await
    }

    /// Executes an authorized application with an explicitly specified `ExecutionId`.
    pub async fn execute_with_id(
        &self,
        app_id: &ApplicationId,
        execution_id: ExecutionId,
    ) -> Result<ExecutionReport, ApplicationError> {
        // 1. Fetch Application Record from Registry
        let record = self.registry.get(app_id)?.ok_or_else(|| {
            ApplicationError::NotFound(format!("Application '{}' not found", app_id))
        })?;

        // 2. Validate Execution Eligibility
        match record.lifecycle_state {
            ApplicationState::Authorized
            | ApplicationState::Stopped
            | ApplicationState::Running => {
                // Eligible to start execution
            }
            ApplicationState::Installed => {
                return Err(ApplicationError::ApplicationNotAuthorized {
                    current_state: record.lifecycle_state,
                });
            }
            other => {
                return Err(ApplicationError::ExecutionError(format!(
                    "Application '{}' cannot be executed in state {:?}",
                    app_id, other
                )));
            }
        }

        // 3. Enforce Concurrency Limit
        let max_concurrency = record.resource_limits.max_concurrency as usize;
        {
            let active_guard = self.active_executions.read().await;
            let current_count = active_guard
                .values()
                .filter(|info| info.application_id == *app_id)
                .count();
            if current_count >= max_concurrency {
                return Err(ApplicationError::ConcurrencyLimitReached {
                    current: current_count,
                    max: max_concurrency,
                });
            }
        }

        // 4. Transition State to STARTING in Registry & Emit Event
        if record.lifecycle_state != ApplicationState::Running {
            self.registry
                .update_lifecycle_state(app_id, ApplicationState::Starting)?;
        }

        let start_time = Timestamp::now();
        let (cancel_tx, mut cancel_rx) = oneshot::channel::<()>();
        {
            let mut active_guard = self.active_executions.write().await;
            active_guard.insert(
                execution_id.clone(),
                ActiveExecutionInfo {
                    execution_id: execution_id.clone(),
                    application_id: app_id.clone(),
                    package_id: record.package_id.clone(),
                    start_time,
                    lifecycle_state: ApplicationState::Starting,
                },
            );

            let mut cancel_guard = self.cancellation_channels.write().await;
            cancel_guard.insert(execution_id.clone(), cancel_tx);
        }

        let _ = self.event_tx.send(ApplicationEvent::Starting {
            application_id: app_id.clone(),
            package_id: record.package_id.clone(),
            execution_id: execution_id.clone(),
            timestamp: start_time.as_millis(),
        });

        // 5. Pipeline: Locate Entrypoint & Fetch WASM Bytes from CAS
        let entrypoint_item = match record.entrypoint_item() {
            Ok(item) => item,
            Err(e) => {
                self.record_startup_failure(app_id, &record.package_id, &execution_id, &e)
                    .await;
                return Err(e);
            }
        };

        let wasm_bytes = match self
            .content_store
            .get_verified(&entrypoint_item.content_id)
            .await
        {
            Ok(bytes) => bytes,
            Err(e) => {
                let app_err = ApplicationError::ContentError(format!(
                    "Failed to fetch WASM binary from CAS for entrypoint '{}': {}",
                    entrypoint_item.path, e
                ));
                self.record_startup_failure(app_id, &record.package_id, &execution_id, &app_err)
                    .await;
                return Err(app_err);
            }
        };

        if let Err(e) = entrypoint_item.verify_bytes(&wasm_bytes) {
            self.record_startup_failure(app_id, &record.package_id, &execution_id, &e)
                .await;
            return Err(e);
        }

        // 6. Module Compilation or Content-Addressed Cache Retrieval
        let module = match self
            .cache
            .get(&record.package_hash, &entrypoint_item.content_id)
        {
            Some(m) => m,
            None => match self.engine.compile(&wasm_bytes) {
                Ok(m) => {
                    self.cache
                        .insert(record.package_hash, entrypoint_item.content_id, m.clone());
                    m
                }
                Err(e) => {
                    self.record_startup_failure(app_id, &record.package_id, &execution_id, &e)
                        .await;
                    return Err(e);
                }
            },
        };

        // 7. Configure Execution-Scoped CapabilityContext and HostContext
        let tokio_handle = tokio::runtime::Handle::current();

        let capabilities = CapabilityContext::new(
            record.manifest.permissions.clone(),
            record.permissions.clone(),
        );

        let storage = HostStorage::new(
            app_id.clone(),
            record.resource_limits.max_storage_bytes,
            self.content_store.clone(),
            self.registry.clone(),
            tokio_handle.clone(),
        );

        let service_res = self.service_resolver.read().unwrap().clone();
        let services =
            service_res.map(|res| HostServices::new(app_id.clone(), res, tokio_handle.clone()));

        let net_sender = self.network_sender.read().unwrap().clone();
        let local_node = *self.local_node_id.read().unwrap();
        let network = net_sender.map(|sender| {
            HostNetwork::new(
                app_id.clone(),
                execution_id.clone(),
                local_node,
                sender,
                tokio_handle.clone(),
            )
        });

        let events = HostEvents::new(app_id.clone(), execution_id.clone(), self.event_tx.clone());

        let host_context = HostContext::new(
            app_id.clone(),
            execution_id.clone(),
            capabilities,
            storage,
            services,
            network,
            events,
            tokio_handle,
        );

        // Configure Wasmtime Store & Resource Limits
        let store_data = StoreData::new(
            execution_id.clone(),
            app_id.clone(),
            record.package_id.clone(),
            &record.resource_limits,
        )
        .with_host_context(host_context);

        let mut store = Store::new(self.engine.engine(), store_data);
        store.limiter(|data| &mut data.limits);

        if let Err(e) = store.set_fuel(record.resource_limits.max_cpu_fuel) {
            let app_err =
                ApplicationError::ExecutionError(format!("Failed to configure fuel: {}", e));
            self.record_startup_failure(app_id, &record.package_id, &execution_id, &app_err)
                .await;
            return Err(app_err);
        }

        // Configure epoch deadline for execution timeout
        let ticks =
            (record.resource_limits.max_execution_time_ms / DEFAULT_EPOCH_TICK_INTERVAL_MS).max(1);
        store.set_epoch_deadline(ticks);

        // 8. Transition State to RUNNING & Emit Started Event
        if record.lifecycle_state != ApplicationState::Running {
            if let Err(e) = self
                .registry
                .update_lifecycle_state(app_id, ApplicationState::Running)
            {
                self.unregister_active(&execution_id).await;
                return Err(e);
            }
        }

        {
            let mut active_guard = self.active_executions.write().await;
            if let Some(info) = active_guard.get_mut(&execution_id) {
                info.lifecycle_state = ApplicationState::Running;
            }
        }

        let _ = self.event_tx.send(ApplicationEvent::Started {
            application_id: app_id.clone(),
            package_id: record.package_id.clone(),
            execution_id: execution_id.clone(),
            timestamp: start_time.as_millis(),
        });

        // 10. Instantiate WASM Module with registered capability-gated Host Environment APIs
        let mut linker = Linker::new(self.engine.engine());
        if let Err(e) = register_host_functions(&mut linker) {
            let app_err = ApplicationError::ExecutionError(format!(
                "Failed to register host functions: {}",
                e
            ));
            self.record_startup_failure(app_id, &record.package_id, &execution_id, &app_err)
                .await;
            return Err(app_err);
        }

        let instance = match linker.instantiate(&mut store, &module) {
            Ok(inst) => inst,
            Err(e) => {
                let finish_time = Timestamp::now();
                let remaining_fuel = store.get_fuel().unwrap_or(0);
                let fuel_consumed = record
                    .resource_limits
                    .max_cpu_fuel
                    .saturating_sub(remaining_fuel);
                let duration_ms = finish_time
                    .as_millis()
                    .saturating_sub(start_time.as_millis());

                let (app_err, desc) =
                    self.classify_trap(&e, record.resource_limits.max_execution_time_ms);
                self.unregister_active(&execution_id).await;
                let has_other_active = {
                    let active_guard = self.active_executions.read().await;
                    active_guard
                        .values()
                        .any(|info| info.application_id == *app_id)
                };
                if !has_other_active {
                    let _ = self
                        .registry
                        .update_lifecycle_state(app_id, ApplicationState::Crashed);
                }

                warn!(
                    application_id = %app_id,
                    execution_id = %execution_id,
                    fuel_consumed = fuel_consumed,
                    duration_ms = duration_ms,
                    error = %desc,
                    "Module instantiation failed"
                );

                let _ = self.event_tx.send(ApplicationEvent::Crashed {
                    application_id: app_id.clone(),
                    package_id: Some(record.package_id.clone()),
                    execution_id: Some(execution_id.clone()),
                    error: desc,
                    timestamp: finish_time.as_millis(),
                });

                return Err(app_err);
            }
        };

        // 11. Locate Entrypoint Export Function ("main" or "_start")
        let entrypoint_func = instance
            .get_func(&mut store, "main")
            .or_else(|| instance.get_func(&mut store, "_start"));

        let func = match entrypoint_func {
            Some(f) => f,
            None => {
                let finish_time = Timestamp::now();
                let remaining_fuel = store.get_fuel().unwrap_or(0);
                let fuel_consumed = record
                    .resource_limits
                    .max_cpu_fuel
                    .saturating_sub(remaining_fuel);
                let duration_ms = finish_time
                    .as_millis()
                    .saturating_sub(start_time.as_millis());

                let app_err = ApplicationError::EntrypointNotFound(
                    "Exported function 'main' or '_start' not found in module".to_string(),
                );

                self.unregister_active(&execution_id).await;
                let has_other_active = {
                    let active_guard = self.active_executions.read().await;
                    active_guard
                        .values()
                        .any(|info| info.application_id == *app_id)
                };
                if !has_other_active {
                    let _ = self
                        .registry
                        .update_lifecycle_state(app_id, ApplicationState::Crashed);
                }

                warn!(
                    application_id = %app_id,
                    execution_id = %execution_id,
                    fuel_consumed = fuel_consumed,
                    duration_ms = duration_ms,
                    "Entrypoint function not found in module"
                );

                let _ = self.event_tx.send(ApplicationEvent::Crashed {
                    application_id: app_id.clone(),
                    package_id: Some(record.package_id.clone()),
                    execution_id: Some(execution_id.clone()),
                    error: app_err.to_string(),
                    timestamp: finish_time.as_millis(),
                });

                return Err(app_err);
            }
        };

        // 12. Execute in Spawn Blocking to protect Tokio Worker Threads while allowing Epoch Interruption
        let engine_clone = self.engine.clone();
        let max_fuel = record.resource_limits.max_cpu_fuel;
        let max_time_ms = record.resource_limits.max_execution_time_ms;

        let execution_task = tokio::task::spawn_blocking(move || {
            let func_ty = func.ty(&store);
            let result_count = func_ty.results().len();
            let mut results = vec![Val::I32(0); result_count];
            let res = func.call(&mut store, &[], &mut results);
            let remaining_fuel = store.get_fuel().unwrap_or(0);
            let fuel_consumed = max_fuel.saturating_sub(remaining_fuel);

            let exit_code = match &res {
                Ok(_) => {
                    if results.is_empty() {
                        Some(0)
                    } else {
                        match results[0] {
                            Val::I32(code) => Some(code),
                            _ => Some(0),
                        }
                    }
                }
                Err(_) => None,
            };

            (res, fuel_consumed, exit_code)
        });

        // Monitor execution with cancellation select
        let (invoke_res, fuel_consumed, exit_code) = tokio::select! {
            task_res = execution_task => {
                match task_res {
                    Ok(tuple) => tuple,
                    Err(join_err) => {
                        (Err(wasmtime::Error::msg(format!("Execution task panicked: {}", join_err))), 0, None)
                    }
                }
            }
            _ = &mut cancel_rx => {
                // Immediate cancellation requested: advance epoch to interrupt running WASM
                engine_clone.advance_epoch(100_000);
                (Err(wasmtime::Error::msg("Execution cancelled by supervisor")), 0, None)
            }
        };

        let finish_time = Timestamp::now();
        let duration_ms = finish_time
            .as_millis()
            .saturating_sub(start_time.as_millis());

        // 13. Process Execution Result
        self.unregister_active(&execution_id).await;

        let has_other_active = {
            let active_guard = self.active_executions.read().await;
            active_guard
                .values()
                .any(|info| info.application_id == *app_id)
        };

        match invoke_res {
            Ok(_) => {
                if !has_other_active {
                    let _ = self
                        .registry
                        .update_lifecycle_state(app_id, ApplicationState::Stopped);
                }

                let _ = self.event_tx.send(ApplicationEvent::Stopped {
                    application_id: app_id.clone(),
                    package_id: Some(record.package_id.clone()),
                    execution_id: Some(execution_id.clone()),
                    reason: Some("Execution completed normally".to_string()),
                    timestamp: finish_time.as_millis(),
                });

                info!(
                    application_id = %app_id,
                    execution_id = %execution_id,
                    fuel_consumed = fuel_consumed,
                    duration_ms = duration_ms,
                    "Application execution completed successfully"
                );

                Ok(ExecutionReport {
                    execution_id,
                    application_id: app_id.clone(),
                    package_id: record.package_id,
                    start_time,
                    finish_time,
                    duration_ms,
                    fuel_consumed,
                    exit_code,
                    final_state: ApplicationState::Stopped,
                    error: None,
                })
            }
            Err(e) => {
                let (app_err, desc) = self.classify_trap(&e, max_time_ms);
                if !has_other_active {
                    let _ = self
                        .registry
                        .update_lifecycle_state(app_id, ApplicationState::Crashed);
                }

                let _ = self.event_tx.send(ApplicationEvent::Crashed {
                    application_id: app_id.clone(),
                    package_id: Some(record.package_id.clone()),
                    execution_id: Some(execution_id.clone()),
                    error: desc.clone(),
                    timestamp: finish_time.as_millis(),
                });

                warn!(
                    application_id = %app_id,
                    execution_id = %execution_id,
                    error = %desc,
                    "Application execution trapped or failed (host runtime unaffected)"
                );

                Err(app_err)
            }
        }
    }

    /// Stops an active execution instance cleanly.
    pub async fn stop_execution(&self, execution_id: &ExecutionId) -> Result<(), ApplicationError> {
        let mut cancel_guard = self.cancellation_channels.write().await;
        if let Some(tx) = cancel_guard.remove(execution_id) {
            let _ = tx.send(());
            // Trigger epoch interruption immediately
            self.engine.advance_epoch(100_000);
            info!(execution_id = %execution_id, "Execution cancellation triggered");
            Ok(())
        } else {
            Err(ApplicationError::ExecutionNotFound(
                execution_id.to_string(),
            ))
        }
    }

    /// Reconciles persisted lifecycle states upon node runtime startup.
    ///
    /// Any applications left in STARTING or RUNNING state from a previous session
    /// are transitioned according to lifecycle semantics:
    /// - STARTING -> CRASHED (startup did not complete before node shutdown)
    /// - RUNNING  -> STOPPED (in-flight execution cleanly halted on node restart)
    pub fn reconcile_on_startup(&self) -> Result<usize, ApplicationError> {
        let dangling = self
            .registry
            .list_by_states(&[ApplicationState::Starting, ApplicationState::Running])?;

        let mut count = 0;
        for app in dangling {
            match app.lifecycle_state {
                ApplicationState::Starting => {
                    self.registry
                        .update_lifecycle_state(&app.application_id, ApplicationState::Crashed)?;
                    let _ = self.event_tx.send(ApplicationEvent::Crashed {
                        application_id: app.application_id.clone(),
                        package_id: Some(app.package_id),
                        execution_id: None,
                        error: "Runtime restarted during application startup".to_string(),
                        timestamp: Timestamp::now().as_millis(),
                    });
                    count += 1;
                }
                ApplicationState::Running => {
                    self.registry
                        .update_lifecycle_state(&app.application_id, ApplicationState::Stopped)?;
                    let _ = self.event_tx.send(ApplicationEvent::Stopped {
                        application_id: app.application_id.clone(),
                        package_id: Some(app.package_id),
                        execution_id: None,
                        reason: Some("Runtime restarted while application was running".to_string()),
                        timestamp: Timestamp::now().as_millis(),
                    });
                    count += 1;
                }
                _ => {}
            }
        }

        if count > 0 {
            info!(
                reconciled_count = count,
                "Application lifecycle states reconciled on startup"
            );
        }
        Ok(count)
    }

    async fn unregister_active(&self, execution_id: &ExecutionId) {
        let mut active_guard = self.active_executions.write().await;
        active_guard.remove(execution_id);
        let mut cancel_guard = self.cancellation_channels.write().await;
        cancel_guard.remove(execution_id);
    }

    async fn record_startup_failure(
        &self,
        app_id: &ApplicationId,
        package_id: &PackageId,
        execution_id: &ExecutionId,
        err: &ApplicationError,
    ) {
        self.unregister_active(execution_id).await;
        let has_other_active = {
            let active_guard = self.active_executions.read().await;
            active_guard
                .values()
                .any(|info| info.application_id == *app_id)
        };
        if !has_other_active {
            let _ = self
                .registry
                .update_lifecycle_state(app_id, ApplicationState::Crashed);
        }
        let _ = self.event_tx.send(ApplicationEvent::Crashed {
            application_id: app_id.clone(),
            package_id: Some(package_id.clone()),
            execution_id: Some(execution_id.clone()),
            error: err.to_string(),
            timestamp: Timestamp::now().as_millis(),
        });
    }

    fn classify_trap(
        &self,
        err: &wasmtime::Error,
        max_execution_time_ms: u64,
    ) -> (ApplicationError, String) {
        let trap = err
            .downcast_ref::<wasmtime::Trap>()
            .or_else(|| err.root_cause().downcast_ref::<wasmtime::Trap>());

        if let Some(trap) = trap {
            match trap {
                wasmtime::Trap::OutOfFuel => (
                    ApplicationError::FuelExhausted,
                    "CPU fuel limit exhausted".to_string(),
                ),
                wasmtime::Trap::Interrupt => (
                    ApplicationError::ExecutionTimeout {
                        max_ms: max_execution_time_ms,
                    },
                    format!("Execution timed out after {} ms", max_execution_time_ms),
                ),
                wasmtime::Trap::MemoryOutOfBounds => (
                    ApplicationError::MemoryLimitExceeded(
                        "WebAssembly memory access out of bounds".to_string(),
                    ),
                    "WebAssembly memory access out of bounds".to_string(),
                ),
                wasmtime::Trap::UnreachableCodeReached => (
                    ApplicationError::TrapError {
                        trap: "unreachable".to_string(),
                        reason: "WebAssembly unreachable instruction executed".to_string(),
                    },
                    "WebAssembly unreachable instruction executed".to_string(),
                ),
                other => (
                    ApplicationError::TrapError {
                        trap: format!("{:?}", other),
                        reason: err.to_string(),
                    },
                    err.to_string(),
                ),
            }
        } else {
            let err_str = err.to_string();
            let lower = err_str.to_lowercase();
            if lower.contains("all fuel consumed") || lower.contains("out of fuel") {
                (
                    ApplicationError::FuelExhausted,
                    "CPU fuel limit exhausted".to_string(),
                )
            } else if lower.contains("interrupt") || lower.contains("epoch") {
                (
                    ApplicationError::ExecutionTimeout {
                        max_ms: max_execution_time_ms,
                    },
                    format!("Execution timed out after {} ms", max_execution_time_ms),
                )
            } else if lower.contains("unreachable") {
                (
                    ApplicationError::TrapError {
                        trap: "unreachable".to_string(),
                        reason: "WebAssembly unreachable instruction executed".to_string(),
                    },
                    "WebAssembly unreachable instruction executed".to_string(),
                )
            } else if lower.contains("out of bounds")
                || lower.contains("memory")
                || lower.contains("resource limit")
            {
                (
                    ApplicationError::MemoryLimitExceeded(err_str.clone()),
                    err_str,
                )
            } else if lower.contains("cancelled") {
                (
                    ApplicationError::ExecutionCancelled(err_str.clone()),
                    err_str,
                )
            } else {
                (ApplicationError::ExecutionError(err_str.clone()), err_str)
            }
        }
    }
}
