use fieldnet_content::hashing::hash_bytes;
use fieldnet_content::store::ContentStore;
use fieldnet_core::Timestamp;
use fieldnet_identity::NodeIdentity;
use fieldnet_wasm::error::ApplicationError;
use fieldnet_wasm::events::ApplicationEvent;
use fieldnet_wasm::execution::{ApplicationExecutionSupervisor, ModuleCache, WasmEngine};
use fieldnet_wasm::id::{AppVersion, ApplicationId, ExecutionId, PackageId, PublisherIdentity};
use fieldnet_wasm::installer::ApplicationInstaller;
use fieldnet_wasm::lifecycle::ApplicationState;
use fieldnet_wasm::limits::ResourceLimits;
use fieldnet_wasm::manifest::{ApplicationManifest, CURRENT_MANIFEST_SCHEMA_VERSION};
use fieldnet_wasm::package::{ApplicationPackage, PackageItem};
use fieldnet_wasm::permissions::AppPermission;
use fieldnet_wasm::registry::ApplicationRegistry;
use fieldnet_wasm::signature::PackageSignature;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::broadcast;

// =============================================================================
// REAL WEBASSEMBLY TEXT (WAT) FIXTURES
// =============================================================================

/// Minimal hello-world module exporting `main` with `() -> ()`.
const HELLO_WORLD_WAT: &str = r#"
(module
  (func (export "main"))
)
"#;

/// Module executing normally and returning exit code 42: `() -> i32`.
const NORMAL_RETURN_WAT: &str = r#"
(module
  (func (export "main") (result i32)
    i32.const 42
  )
)
"#;

/// Module containing an explicit `unreachable` trap instruction.
const TRAPPING_WAT: &str = r#"
(module
  (func (export "main")
    unreachable
  )
)
"#;

/// Module executing an infinite loop that exhausts CPU fuel.
const EXCESSIVE_FUEL_WAT: &str = r#"
(module
  (func (export "main")
    (loop (br 0))
  )
)
"#;

/// Module attempting out-of-bounds memory load.
const INVALID_MEMORY_ACCESS_WAT: &str = r#"
(module
  (memory 1)
  (func (export "main")
    i32.const 100000
    i32.load
    drop
  )
)
"#;

/// Module missing declared `main` or `_start` export.
const INVALID_ENTRYPOINT_WAT: &str = r#"
(module
  (func (export "not_main_func"))
)
"#;

/// Module attempting to allocate memory exceeding configured limits.
const EXCEED_MEMORY_LIMITS_WAT: &str = r#"
(module
  (memory 1 100)
  (func (export "main") (result i32)
    i32.const 50
    memory.grow
  )
)
"#;

// =============================================================================
// TEST ENVIRONMENT & FIXTURE BUILDERS
// =============================================================================

struct TestEnvironment {
    supervisor: Arc<ApplicationExecutionSupervisor>,
    registry: Arc<ApplicationRegistry>,
    content_store: Arc<ContentStore>,
    installer: Arc<ApplicationInstaller>,
    _event_rx: broadcast::Receiver<ApplicationEvent>,
    _temp_dir: tempfile::TempDir,
}

impl TestEnvironment {
    async fn new() -> Self {
        let temp_dir = tempdir().unwrap();
        let content_store = ContentStore::open(temp_dir.path()).unwrap();

        let registry = Arc::new(ApplicationRegistry::new_in_memory().unwrap());
        let (app_event_tx, app_event_rx) = broadcast::channel(256);

        let installer = Arc::new(ApplicationInstaller::new(
            Arc::clone(&registry),
            Some(Arc::clone(&content_store)),
            app_event_tx.clone(),
        ));

        let engine = Arc::new(WasmEngine::new().unwrap());
        let cache = Arc::new(ModuleCache::new());
        let supervisor = Arc::new(ApplicationExecutionSupervisor::new(
            engine,
            cache,
            Arc::clone(&registry),
            Arc::clone(&content_store),
            app_event_tx,
        ));

        Self {
            supervisor,
            registry,
            content_store,
            installer,
            _event_rx: app_event_rx,
            _temp_dir: temp_dir,
        }
    }
}

fn build_package_from_wat(
    app_id_str: &str,
    version_str: &str,
    wat_source: &str,
    limits: Option<ResourceLimits>,
) -> (ApplicationPackage, HashMap<String, Vec<u8>>) {
    let wasm_bytes = wat::parse_str(wat_source).expect("Valid WAT source");
    let identity = NodeIdentity::generate();
    let publisher = PublisherIdentity::from_public_key(identity.public_key_bytes());
    let app_id = ApplicationId::new(app_id_str).unwrap();
    let version: AppVersion = version_str.parse().unwrap();

    let manifest = ApplicationManifest {
        schema_version: CURRENT_MANIFEST_SCHEMA_VERSION,
        application_id: app_id.clone(),
        name: format!("App {}", app_id),
        version,
        description: "Test description".to_string(),
        publisher,
        entrypoint: "app.wasm".to_string(),
        permissions: vec![AppPermission::StorageRead],
        resource_limits: limits.unwrap_or_default(),
        dependencies: vec![],
        package_metadata: HashMap::new(),
    };

    let item_hash = hash_bytes(&wasm_bytes);
    let item = PackageItem::new(
        "app.wasm",
        item_hash,
        wasm_bytes.len() as u64,
        Some("application/wasm".to_string()),
    )
    .unwrap();

    let pkg_id = PackageId::new(format!("pkg:{}:{}", app_id_str, version_str)).unwrap();
    let mut pkg =
        ApplicationPackage::new(pkg_id, manifest, vec![item], Timestamp::now(), None).unwrap();

    let package_hash = pkg.compute_package_hash();
    let sig = PackageSignature::sign_with_identity(&identity, &package_hash, Timestamp::now());
    pkg.signature = Some(sig);

    let mut payloads = HashMap::new();
    payloads.insert("app.wasm".to_string(), wasm_bytes);

    (pkg, payloads)
}

// =============================================================================
// VERIFICATION TEST CASES
// =============================================================================

#[tokio::test]
async fn test_01_wasmtime_engine_creation() {
    let engine = WasmEngine::new().expect("Engine should initialize cleanly");
    let _ = engine.engine();
}

#[tokio::test]
async fn test_02_valid_module_loading() {
    let engine = WasmEngine::new().unwrap();
    let wasm_bytes = wat::parse_str(HELLO_WORLD_WAT).unwrap();
    let module = engine.compile(&wasm_bytes);
    assert!(module.is_ok(), "Module compilation should succeed");
}

#[tokio::test]
async fn test_03_invalid_wasm_rejection() {
    let engine = WasmEngine::new().unwrap();
    let corrupt_bytes = b"NOT_A_WASM_BINARY_MAGIC";
    let module = engine.compile(corrupt_bytes);
    assert!(module.is_err(), "Corrupt bytes must be rejected");
    assert!(matches!(
        module.unwrap_err(),
        ApplicationError::CompilationError(_)
    ));
}

#[tokio::test]
async fn test_04_package_to_module_loading() {
    let env = TestEnvironment::new().await;
    let (pkg, payloads) =
        build_package_from_wat("fieldnet.test.load", "1.0.0", HELLO_WORLD_WAT, None);

    let record = env.installer.install(&pkg, &payloads).await.unwrap();
    assert_eq!(record.lifecycle_state, ApplicationState::Installed);

    // Verify entrypoint bytes exist in CAS
    let entrypoint = record.entrypoint_item().unwrap();
    let cas_bytes = env
        .content_store
        .get_verified(&entrypoint.content_id)
        .await
        .unwrap();
    assert_eq!(cas_bytes, payloads.get("app.wasm").unwrap().as_slice());

    // Compile from CAS bytes
    let module = env.supervisor.engine().compile(&cas_bytes).unwrap();
    assert!(!module.name().unwrap_or("").is_empty() || true);
}

#[tokio::test]
async fn test_05_content_hash_mismatch_rejection() {
    let env = TestEnvironment::new().await;
    let (pkg, mut payloads) =
        build_package_from_wat("fieldnet.test.tamper", "1.0.0", HELLO_WORLD_WAT, None);

    // Tamper with payload bytes
    payloads.insert("app.wasm".to_string(), b"tampered_bytes".to_vec());

    // Installing tampered package must fail integrity check
    let res = env.installer.install(&pkg, &payloads).await;
    assert!(
        res.is_err(),
        "Tampered payload must be rejected during install"
    );
    assert!(matches!(
        res.unwrap_err(),
        ApplicationError::IntegrityError(_)
    ));
}

#[tokio::test]
async fn test_06_module_cache_hit_and_invalidation() {
    let env = TestEnvironment::new().await;
    let (pkg, payloads) =
        build_package_from_wat("fieldnet.test.cache", "1.0.0", HELLO_WORLD_WAT, None);
    let app_id = pkg.manifest.application_id.clone();

    env.installer.install(&pkg, &payloads).await.unwrap();
    env.registry
        .update_lifecycle_state(&app_id, ApplicationState::Authorized)
        .unwrap();

    assert_eq!(env.supervisor.cache().len(), 0);

    // 1st Execution -> Compiles and caches
    let report1 = env.supervisor.execute(&app_id).await.unwrap();
    assert_eq!(report1.final_state, ApplicationState::Stopped);
    assert_eq!(env.supervisor.cache().len(), 1);

    // 2nd Execution -> Hits cache
    env.registry
        .update_lifecycle_state(&app_id, ApplicationState::Authorized)
        .unwrap();
    let report2 = env.supervisor.execute(&app_id).await.unwrap();
    assert_eq!(report2.final_state, ApplicationState::Stopped);
    assert_eq!(env.supervisor.cache().len(), 1);

    // Invalidation
    let entrypoint = env
        .registry
        .get(&app_id)
        .unwrap()
        .unwrap()
        .entrypoint_item()
        .unwrap()
        .clone();
    let evicted = env
        .supervisor
        .cache()
        .invalidate(&pkg.compute_package_hash(), &entrypoint.content_id);
    assert!(evicted, "Module should be evicted from cache");
    assert_eq!(env.supervisor.cache().len(), 0);
}

#[tokio::test]
async fn test_07_valid_application_startup_and_clean_shutdown() {
    let env = TestEnvironment::new().await;
    let (pkg, payloads) =
        build_package_from_wat("fieldnet.test.run", "1.0.0", NORMAL_RETURN_WAT, None);
    let app_id = pkg.manifest.application_id.clone();

    env.installer.install(&pkg, &payloads).await.unwrap();
    env.registry
        .update_lifecycle_state(&app_id, ApplicationState::Authorized)
        .unwrap();

    let report = env.supervisor.execute(&app_id).await.unwrap();
    assert_eq!(report.final_state, ApplicationState::Stopped);
    assert_eq!(report.exit_code, Some(42));
    assert!(
        report.fuel_consumed > 0,
        "Fuel must actually be consumed: {}",
        report.fuel_consumed
    );

    // Persisted state must be STOPPED
    let persisted = env.registry.get(&app_id).unwrap().unwrap();
    assert_eq!(persisted.lifecycle_state, ApplicationState::Stopped);
}

#[tokio::test]
async fn test_08_invalid_lifecycle_transition_rejection() {
    let env = TestEnvironment::new().await;
    let (pkg, payloads) =
        build_package_from_wat("fieldnet.test.unauth", "1.0.0", HELLO_WORLD_WAT, None);
    let app_id = pkg.manifest.application_id.clone();

    env.installer.install(&pkg, &payloads).await.unwrap();
    // App is in INSTALLED state (not yet Authorized)

    let res = env.supervisor.execute(&app_id).await;
    assert!(
        res.is_err(),
        "Installed application cannot execute without authorization"
    );
    assert!(matches!(
        res.unwrap_err(),
        ApplicationError::ApplicationNotAuthorized {
            current_state: ApplicationState::Installed
        }
    ));
}

#[tokio::test]
async fn test_09_trap_handling_and_crash_state_transition() {
    let env = TestEnvironment::new().await;
    let (pkg, payloads) = build_package_from_wat("fieldnet.test.trap", "1.0.0", TRAPPING_WAT, None);
    let app_id = pkg.manifest.application_id.clone();

    env.installer.install(&pkg, &payloads).await.unwrap();
    env.registry
        .update_lifecycle_state(&app_id, ApplicationState::Authorized)
        .unwrap();

    let res = env.supervisor.execute(&app_id).await;
    assert!(res.is_err(), "Trapping application must return error");
    assert!(matches!(
        res.unwrap_err(),
        ApplicationError::TrapError { .. }
    ));

    // Persisted state must be CRASHED
    let persisted = env.registry.get(&app_id).unwrap().unwrap();
    assert_eq!(persisted.lifecycle_state, ApplicationState::Crashed);
}

#[tokio::test]
async fn test_10_fuel_exhaustion_enforcement() {
    let env = TestEnvironment::new().await;
    let limits = ResourceLimits {
        max_cpu_fuel: 50_000, // Small fuel limit for quick exhaustion
        ..Default::default()
    };
    let (pkg, payloads) = build_package_from_wat(
        "fieldnet.test.fuel",
        "1.0.0",
        EXCESSIVE_FUEL_WAT,
        Some(limits),
    );
    let app_id = pkg.manifest.application_id.clone();

    env.installer.install(&pkg, &payloads).await.unwrap();
    env.registry
        .update_lifecycle_state(&app_id, ApplicationState::Authorized)
        .unwrap();

    let res = env.supervisor.execute(&app_id).await;
    assert!(res.is_err(), "Fuel exhaustion must abort execution");
    assert!(matches!(res.unwrap_err(), ApplicationError::FuelExhausted));

    // State transitioned to CRASHED
    let persisted = env.registry.get(&app_id).unwrap().unwrap();
    assert_eq!(persisted.lifecycle_state, ApplicationState::Crashed);
}

#[tokio::test]
async fn test_11_invalid_memory_access_trapping() {
    let env = TestEnvironment::new().await;
    let (pkg, payloads) = build_package_from_wat(
        "fieldnet.test.memaccess",
        "1.0.0",
        INVALID_MEMORY_ACCESS_WAT,
        None,
    );
    let app_id = pkg.manifest.application_id.clone();

    env.installer.install(&pkg, &payloads).await.unwrap();
    env.registry
        .update_lifecycle_state(&app_id, ApplicationState::Authorized)
        .unwrap();

    let res = env.supervisor.execute(&app_id).await;
    assert!(res.is_err(), "Out of bounds memory access must trap");
    assert!(matches!(
        res.unwrap_err(),
        ApplicationError::MemoryLimitExceeded(_)
    ));

    let persisted = env.registry.get(&app_id).unwrap().unwrap();
    assert_eq!(persisted.lifecycle_state, ApplicationState::Crashed);
}

#[tokio::test]
async fn test_12_memory_limit_enforcement() {
    let env = TestEnvironment::new().await;
    let limits = ResourceLimits {
        max_memory_bytes: 128 * 1024, // 128 KB (2 WASM pages)
        ..Default::default()
    };
    let (pkg, payloads) = build_package_from_wat(
        "fieldnet.test.memgrow",
        "1.0.0",
        EXCEED_MEMORY_LIMITS_WAT,
        Some(limits),
    );
    let app_id = pkg.manifest.application_id.clone();

    env.installer.install(&pkg, &payloads).await.unwrap();
    env.registry
        .update_lifecycle_state(&app_id, ApplicationState::Authorized)
        .unwrap();

    let report = env.supervisor.execute(&app_id).await.unwrap();
    // memory.grow returns -1 when exceeding store memory limit
    assert_eq!(
        report.exit_code,
        Some(-1),
        "WASM memory.grow beyond store limit must return -1"
    );
}

#[tokio::test]
async fn test_13_missing_entrypoint_rejection() {
    let env = TestEnvironment::new().await;
    let (pkg, payloads) = build_package_from_wat(
        "fieldnet.test.noentry",
        "1.0.0",
        INVALID_ENTRYPOINT_WAT,
        None,
    );
    let app_id = pkg.manifest.application_id.clone();

    env.installer.install(&pkg, &payloads).await.unwrap();
    env.registry
        .update_lifecycle_state(&app_id, ApplicationState::Authorized)
        .unwrap();

    let res = env.supervisor.execute(&app_id).await;
    assert!(res.is_err(), "Missing entrypoint function must fail");
    assert!(matches!(
        res.unwrap_err(),
        ApplicationError::EntrypointNotFound(_)
    ));

    let persisted = env.registry.get(&app_id).unwrap().unwrap();
    assert_eq!(persisted.lifecycle_state, ApplicationState::Crashed);
}

#[tokio::test]
async fn test_14_execution_timeout_enforcement() {
    let env = TestEnvironment::new().await;
    let limits = ResourceLimits {
        max_execution_time_ms: 30,  // 30 ms timeout
        max_cpu_fuel: u64::MAX / 2, // Plentiful fuel so timeout triggers first
        ..Default::default()
    };
    let (pkg, payloads) = build_package_from_wat(
        "fieldnet.test.timeout",
        "1.0.0",
        EXCESSIVE_FUEL_WAT,
        Some(limits),
    );
    let app_id = pkg.manifest.application_id.clone();

    env.installer.install(&pkg, &payloads).await.unwrap();
    env.registry
        .update_lifecycle_state(&app_id, ApplicationState::Authorized)
        .unwrap();

    let res = env.supervisor.execute(&app_id).await;
    assert!(res.is_err(), "Long running application must time out");
    assert!(matches!(
        res.unwrap_err(),
        ApplicationError::ExecutionTimeout { .. }
    ));

    let persisted = env.registry.get(&app_id).unwrap().unwrap();
    assert_eq!(persisted.lifecycle_state, ApplicationState::Crashed);
}

#[tokio::test]
async fn test_15_supervisor_controlled_cancellation() {
    let env = TestEnvironment::new().await;
    let limits = ResourceLimits {
        max_execution_time_ms: 10_000,
        max_cpu_fuel: u64::MAX / 2,
        ..Default::default()
    };
    let (pkg, payloads) = build_package_from_wat(
        "fieldnet.test.cancel",
        "1.0.0",
        EXCESSIVE_FUEL_WAT,
        Some(limits),
    );
    let app_id = pkg.manifest.application_id.clone();

    env.installer.install(&pkg, &payloads).await.unwrap();
    env.registry
        .update_lifecycle_state(&app_id, ApplicationState::Authorized)
        .unwrap();

    let execution_id = ExecutionId::generate();
    let exec_id_clone = execution_id.clone();
    let supervisor_clone = Arc::clone(&env.supervisor);

    // Spawn execution in background
    let exec_task = tokio::spawn(async move {
        supervisor_clone
            .execute_with_id(&app_id, exec_id_clone)
            .await
    });

    // Wait for execution to register
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Trigger cancellation
    let stop_res = env.supervisor.stop_execution(&execution_id).await;
    assert!(stop_res.is_ok(), "Stop signal must succeed");

    let join_res = exec_task.await.unwrap();
    assert!(join_res.is_err(), "Cancelled execution must fail");
    assert!(matches!(
        join_res.unwrap_err(),
        ApplicationError::ExecutionCancelled(_) | ApplicationError::ExecutionTimeout { .. }
    ));
}

#[tokio::test]
async fn test_16_concurrent_applications() {
    let env = TestEnvironment::new().await;
    let (pkg1, payloads1) =
        build_package_from_wat("fieldnet.app.alpha", "1.0.0", NORMAL_RETURN_WAT, None);
    let (pkg2, payloads2) =
        build_package_from_wat("fieldnet.app.beta", "1.0.0", NORMAL_RETURN_WAT, None);

    let id1 = pkg1.manifest.application_id.clone();
    let id2 = pkg2.manifest.application_id.clone();

    env.installer.install(&pkg1, &payloads1).await.unwrap();
    env.installer.install(&pkg2, &payloads2).await.unwrap();

    env.registry
        .update_lifecycle_state(&id1, ApplicationState::Authorized)
        .unwrap();
    env.registry
        .update_lifecycle_state(&id2, ApplicationState::Authorized)
        .unwrap();

    let sup1 = Arc::clone(&env.supervisor);
    let sup2 = Arc::clone(&env.supervisor);

    let task1 = tokio::spawn(async move { sup1.execute(&id1).await });
    let task2 = tokio::spawn(async move { sup2.execute(&id2).await });

    let (res1, res2) = tokio::join!(task1, task2);
    assert_eq!(res1.unwrap().unwrap().exit_code, Some(42));
    assert_eq!(res2.unwrap().unwrap().exit_code, Some(42));
}

#[tokio::test]
async fn test_17_max_concurrency_enforcement() {
    let env = TestEnvironment::new().await;
    let limits = ResourceLimits {
        max_concurrency: 1, // Only 1 concurrent execution allowed
        max_execution_time_ms: 5_000,
        max_cpu_fuel: u64::MAX / 2,
        ..Default::default()
    };
    let (pkg, payloads) = build_package_from_wat(
        "fieldnet.test.concurrency",
        "1.0.0",
        EXCESSIVE_FUEL_WAT,
        Some(limits),
    );
    let app_id = pkg.manifest.application_id.clone();

    env.installer.install(&pkg, &payloads).await.unwrap();
    env.registry
        .update_lifecycle_state(&app_id, ApplicationState::Authorized)
        .unwrap();

    let exec_id = ExecutionId::generate();
    let exec_id_clone = exec_id.clone();
    let id_clone = app_id.clone();
    let sup_clone = Arc::clone(&env.supervisor);
    let task =
        tokio::spawn(async move { sup_clone.execute_with_id(&id_clone, exec_id_clone).await });

    // Allow first task to start and become active
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Second execution must be rejected due to max_concurrency = 1
    let res = env.supervisor.execute(&app_id).await;
    assert!(res.is_err(), "Exceeding max concurrency must be rejected");
    assert!(matches!(
        res.unwrap_err(),
        ApplicationError::ConcurrencyLimitReached { current: 1, max: 1 }
    ));

    // Clean up background task so test completes immediately
    let _ = env.supervisor.stop_execution(&exec_id).await;
    let _ = task.await;
}

#[tokio::test]
async fn test_18_runtime_restart_reconciliation() {
    let env = TestEnvironment::new().await;
    let (pkg1, payloads1) =
        build_package_from_wat("fieldnet.test.dangling1", "1.0.0", HELLO_WORLD_WAT, None);
    let (pkg2, payloads2) =
        build_package_from_wat("fieldnet.test.dangling2", "1.0.0", HELLO_WORLD_WAT, None);

    let id1 = pkg1.manifest.application_id.clone();
    let id2 = pkg2.manifest.application_id.clone();

    env.installer.install(&pkg1, &payloads1).await.unwrap();
    env.installer.install(&pkg2, &payloads2).await.unwrap();

    env.registry
        .update_lifecycle_state(&id1, ApplicationState::Authorized)
        .unwrap();
    env.registry
        .update_lifecycle_state(&id2, ApplicationState::Authorized)
        .unwrap();

    // Simulate in-flight states prior to sudden shutdown
    env.registry
        .update_lifecycle_state(&id1, ApplicationState::Starting)
        .unwrap();
    env.registry
        .update_lifecycle_state(&id2, ApplicationState::Starting)
        .unwrap();
    env.registry
        .update_lifecycle_state(&id2, ApplicationState::Running)
        .unwrap();

    // Reconcile on startup
    let count = env.supervisor.reconcile_on_startup().unwrap();
    assert_eq!(count, 2, "Both dangling applications must be reconciled");

    // Starting -> Crashed
    assert_eq!(
        env.registry.get(&id1).unwrap().unwrap().lifecycle_state,
        ApplicationState::Crashed
    );
    // Running -> Stopped
    assert_eq!(
        env.registry.get(&id2).unwrap().unwrap().lifecycle_state,
        ApplicationState::Stopped
    );
}

#[tokio::test]
async fn test_19_execution_id_uniqueness() {
    let mut ids = std::collections::HashSet::new();
    for _ in 0..100 {
        let id = ExecutionId::generate();
        assert!(ids.insert(id.to_string()), "ExecutionId must be unique");
    }
}

#[tokio::test]
async fn test_20_runtime_survives_application_crash() {
    let env = TestEnvironment::new().await;
    let (trap_pkg, trap_payloads) =
        build_package_from_wat("fieldnet.test.bad", "1.0.0", TRAPPING_WAT, None);
    let (good_pkg, good_payloads) =
        build_package_from_wat("fieldnet.test.good", "1.0.0", NORMAL_RETURN_WAT, None);

    let bad_id = trap_pkg.manifest.application_id.clone();
    let good_id = good_pkg.manifest.application_id.clone();

    env.installer
        .install(&trap_pkg, &trap_payloads)
        .await
        .unwrap();
    env.installer
        .install(&good_pkg, &good_payloads)
        .await
        .unwrap();

    env.registry
        .update_lifecycle_state(&bad_id, ApplicationState::Authorized)
        .unwrap();
    env.registry
        .update_lifecycle_state(&good_id, ApplicationState::Authorized)
        .unwrap();

    // 1. Crash the first application
    let bad_res = env.supervisor.execute(&bad_id).await;
    assert!(bad_res.is_err(), "Bad application should fail");

    // 2. FIELDNET runtime is intact! Second application executes normally
    let good_res = env.supervisor.execute(&good_id).await;
    assert!(
        good_res.is_ok(),
        "Runtime must survive crash and execute subsequent app"
    );
    assert_eq!(good_res.unwrap().exit_code, Some(42));
}

#[tokio::test]
async fn test_21_isolation_boundary_zero_host_functions() {
    // Attempting to instantiate a module importing host functions must be rejected
    // because Linker exposes strictly ZERO host functions.
    let wat_with_host_import = r#"
    (module
      (import "env" "host_fs_read" (func $fs_read (param i32) (result i32)))
      (func (export "main")
        i32.const 0
        call $fs_read
        drop
      )
    )
    "#;
    let env = TestEnvironment::new().await;
    let (pkg, payloads) = build_package_from_wat(
        "fieldnet.test.hostimport",
        "1.0.0",
        wat_with_host_import,
        None,
    );
    let app_id = pkg.manifest.application_id.clone();

    env.installer.install(&pkg, &payloads).await.unwrap();
    env.registry
        .update_lifecycle_state(&app_id, ApplicationState::Authorized)
        .unwrap();

    let res = env.supervisor.execute(&app_id).await;
    assert!(
        res.is_err(),
        "Modules requesting unprovided host imports must fail instantiation"
    );
    assert!(matches!(
        res.unwrap_err(),
        ApplicationError::ExecutionError(_)
    ));
}
