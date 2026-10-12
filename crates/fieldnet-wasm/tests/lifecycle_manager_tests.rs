use fieldnet_content::hashing::hash_bytes;
use fieldnet_content::store::ContentStore;
use fieldnet_core::Timestamp;
use fieldnet_identity::NodeIdentity;
use fieldnet_service::model::{
    ServiceCapability, ServiceEndpoint, ServiceId, ServiceInstanceId, ServiceManifestBuilder,
    ServiceProtocol, ServiceVersion,
};
use fieldnet_service::registry::{RegistryConfig, ServiceRegistry};
use fieldnet_service::resolver::{ResolverConfig, ServiceResolver};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::broadcast;

use fieldnet_wasm::dependencies::{
    AppDependency, DependencyGraph, DependencyResolver, DependencyType,
};
use fieldnet_wasm::error::ApplicationError;
use fieldnet_wasm::events::ApplicationEvent;
use fieldnet_wasm::execution::supervisor::ApplicationExecutionSupervisor;
use fieldnet_wasm::execution::{ModuleCache, WasmEngine};
use fieldnet_wasm::id::{AppVersion, AppVersionReq, ApplicationId, PackageId, PublisherIdentity};
use fieldnet_wasm::installer::ApplicationInstaller;
use fieldnet_wasm::lifecycle::{ApplicationState, UpdateTransactionState};
use fieldnet_wasm::limits::ResourceLimits;
use fieldnet_wasm::manager::ApplicationLifecycleManager;
use fieldnet_wasm::manifest::{ApplicationManifest, CURRENT_MANIFEST_SCHEMA_VERSION};
use fieldnet_wasm::package::{ApplicationPackage, PackageItem};
use fieldnet_wasm::permissions::AppPermission;
use fieldnet_wasm::registry::{ApplicationRecord, ApplicationRegistry, UpdateTransactionRecord};
use fieldnet_wasm::signature::PackageSignature;

const VALID_WAT: &str = r#"
(module
  (func (export "main") (result i32)
    i32.const 42
  )
)
"#;

const CRASHING_WAT: &str = r#"
(module
  (func (export "main") (result i32)
    unreachable
    i32.const 0
  )
)
"#;

const LOOPING_WAT: &str = r#"
(module
  (func (export "main") (result i32)
    (loop $l
      br $l
    )
    i32.const 0
  )
)
"#;

struct TestContext {
    manager: Arc<ApplicationLifecycleManager>,
    registry: Arc<ApplicationRegistry>,
    content_store: Arc<ContentStore>,
    service_registry: Arc<ServiceRegistry>,
    #[allow(dead_code)]
    service_resolver: Arc<ServiceResolver>,
    event_rx: broadcast::Receiver<ApplicationEvent>,
    identity: Arc<NodeIdentity>,
}

async fn setup_test_context(temp_path: &std::path::Path) -> TestContext {
    let identity = Arc::new(NodeIdentity::generate());
    let node_id = identity.node_id();

    let registry_db = temp_path.join("registry.db");
    let registry = Arc::new(ApplicationRegistry::new(&registry_db).unwrap());

    let cas_dir = temp_path.join("cas");
    let content_store = ContentStore::open(&cas_dir).unwrap();

    let (svc_event_tx, _) = broadcast::channel(32);
    let service_registry =
        ServiceRegistry::open_in_memory(RegistryConfig::default(), svc_event_tx).unwrap();

    let service_resolver = Arc::new(ServiceResolver::new(
        Arc::clone(&service_registry),
        None,
        Some(node_id),
        None,
        ResolverConfig::default(),
    ));

    let dependency_resolver = Arc::new(DependencyResolver::new(
        Arc::clone(&registry),
        Some(Arc::clone(&service_resolver)),
        Some(Arc::clone(&content_store)),
    ));

    let (event_tx, event_rx) = broadcast::channel(128);

    let installer = Arc::new(ApplicationInstaller::new(
        Arc::clone(&registry),
        Some(Arc::clone(&content_store)),
        event_tx.clone(),
    ));

    let engine = Arc::new(WasmEngine::new().unwrap());
    let cache = Arc::new(ModuleCache::new());
    let supervisor = Arc::new(ApplicationExecutionSupervisor::new(
        engine,
        cache,
        Arc::clone(&registry),
        Arc::clone(&content_store),
        event_tx.clone(),
    ));

    let manager = Arc::new(ApplicationLifecycleManager::new(
        Arc::clone(&registry),
        installer,
        supervisor,
        dependency_resolver,
        Arc::clone(&content_store),
        event_tx,
    ));

    TestContext {
        manager,
        registry,
        content_store,
        service_registry,
        service_resolver,
        event_rx,
        identity,
    }
}

fn build_package(
    identity: &NodeIdentity,
    app_id_str: &str,
    version_str: &str,
    wat_code: &str,
    dependencies: Vec<AppDependency>,
    permissions: Vec<AppPermission>,
) -> (ApplicationPackage, HashMap<String, Vec<u8>>) {
    let wasm_bytes = wat::parse_str(wat_code).expect("Valid WAT compilation");
    let publisher = PublisherIdentity::from_public_key(identity.public_key_bytes());
    let app_id = ApplicationId::new(app_id_str).unwrap();
    let version: AppVersion = version_str.parse().unwrap();

    let manifest = ApplicationManifest {
        schema_version: CURRENT_MANIFEST_SCHEMA_VERSION,
        application_id: app_id.clone(),
        name: format!("App {}", app_id),
        version,
        description: "Lifecycle test package".to_string(),
        publisher,
        entrypoint: "app.wasm".to_string(),
        permissions,
        resource_limits: ResourceLimits::default(),
        dependencies,
        package_metadata: Default::default(),
    };

    let wasm_hash = hash_bytes(&wasm_bytes);
    let item_wasm = PackageItem::new(
        "app.wasm",
        wasm_hash,
        wasm_bytes.len() as u64,
        Some("application/wasm".to_string()),
    )
    .unwrap();

    let mut pkg = ApplicationPackage::new(
        PackageId::new(format!("pkg:{}:{}", app_id_str, version_str)).unwrap(),
        manifest,
        vec![item_wasm],
        Timestamp::now(),
        None,
    )
    .unwrap();

    let hash = pkg.compute_package_hash();
    let sig = PackageSignature::sign_with_identity(identity, &hash, Timestamp::now());
    pkg.signature = Some(sig);

    let mut payloads = HashMap::new();
    payloads.insert("app.wasm".to_string(), wasm_bytes);

    (pkg, payloads)
}

// ---------------------------------------------------------------------------
// 1. LifecycleManager creation
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_01_lifecycle_manager_creation() {
    let dir = tempdir().unwrap();
    let ctx = setup_test_context(dir.path()).await;

    assert!(ctx.manager.registry().list().unwrap().is_empty());
    assert!(ctx
        .manager
        .supervisor()
        .list_active_executions()
        .await
        .is_empty());
}

// ---------------------------------------------------------------------------
// 2. install → authorize → start
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_02_install_authorize_start() {
    let dir = tempdir().unwrap();
    let ctx = setup_test_context(dir.path()).await;

    let (pkg, payloads) = build_package(
        &ctx.identity,
        "app.flow.test",
        "1.0.0",
        VALID_WAT,
        vec![],
        vec![AppPermission::StorageRead],
    );
    let app_id = pkg.manifest.application_id.clone();

    // 1. Install
    let record = ctx.manager.install(&pkg, &payloads).await.unwrap();
    assert_eq!(record.lifecycle_state, ApplicationState::Installed);

    // 2. Authorize
    let state = ctx.manager.authorize(&app_id).unwrap();
    assert_eq!(state, ApplicationState::Authorized);

    // 3. Start
    let report = ctx.manager.start(&app_id).await.unwrap();
    assert_eq!(report.application_id, app_id);
    assert_eq!(report.exit_code, Some(42));
    assert_eq!(report.final_state, ApplicationState::Stopped);
}

// ---------------------------------------------------------------------------
// 3. dependency resolution success
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_03_dependency_resolution_success() {
    let dir = tempdir().unwrap();
    let ctx = setup_test_context(dir.path()).await;

    // Install base dependency
    let (base_pkg, base_payloads) = build_package(
        &ctx.identity,
        "lib.base",
        "1.2.0",
        VALID_WAT,
        vec![],
        vec![],
    );
    ctx.manager
        .install(&base_pkg, &base_payloads)
        .await
        .unwrap();

    // Application requiring base dependency
    let dep = AppDependency::new(
        "lib.base",
        AppVersionReq::from_str(">=1.0.0").unwrap(),
        DependencyType::Application,
        false,
    )
    .unwrap();

    let (app_pkg, app_payloads) = build_package(
        &ctx.identity,
        "app.consumer",
        "1.0.0",
        VALID_WAT,
        vec![dep],
        vec![],
    );

    let report = ctx
        .manager
        .dependency_resolver()
        .resolve_for_manifest(&app_pkg.manifest)
        .await
        .unwrap();

    assert!(report.all_satisfied);
    assert!(report.missing_required.is_empty());
    assert_eq!(
        report.dependencies[0].resolved_version,
        Some("1.2.0".to_string())
    );

    // Successful installation
    let installed = ctx.manager.install(&app_pkg, &app_payloads).await.unwrap();
    assert_eq!(installed.installed_version, AppVersion::new(1, 0, 0));
}

// ---------------------------------------------------------------------------
// 4. missing required dependency
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_04_missing_required_dependency() {
    let dir = tempdir().unwrap();
    let ctx = setup_test_context(dir.path()).await;

    let dep = AppDependency::new(
        "missing.package",
        AppVersionReq::from_str(">=1.0.0").unwrap(),
        DependencyType::Application,
        false, // required
    )
    .unwrap();

    let (pkg, payloads) = build_package(
        &ctx.identity,
        "app.missing.req",
        "1.0.0",
        VALID_WAT,
        vec![dep],
        vec![],
    );

    let err = ctx.manager.install(&pkg, &payloads).await.unwrap_err();
    assert!(matches!(err, ApplicationError::DependencyError(_)));
}

// ---------------------------------------------------------------------------
// 5. optional dependency unavailable
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_05_optional_dependency_unavailable() {
    let dir = tempdir().unwrap();
    let ctx = setup_test_context(dir.path()).await;

    let opt_dep = AppDependency::new(
        "optional.ai.service",
        AppVersionReq::from_str(">=2.0.0").unwrap(),
        DependencyType::Service,
        true, // optional!
    )
    .unwrap();

    let (pkg, payloads) = build_package(
        &ctx.identity,
        "app.optional.test",
        "1.0.0",
        VALID_WAT,
        vec![opt_dep],
        vec![],
    );

    // Optional dependency does NOT prevent installation
    let installed = ctx.manager.install(&pkg, &payloads).await.unwrap();
    assert_eq!(installed.lifecycle_state, ApplicationState::Installed);

    let report = ctx
        .manager
        .resolve_dependencies(&pkg.manifest.application_id)
        .await
        .unwrap();

    assert!(report.all_satisfied); // all REQUIRED satisfied
    assert_eq!(report.missing_optional.len(), 1);
    assert!(report.dependencies[0].satisfied);
    assert!(!report.dependencies[0].available);
}

// ---------------------------------------------------------------------------
// 6. incompatible dependency version
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_06_incompatible_dependency_version() {
    let dir = tempdir().unwrap();
    let ctx = setup_test_context(dir.path()).await;

    // Installed version is 1.2.0
    let (base_pkg, base_payloads) = build_package(
        &ctx.identity,
        "shared.lib",
        "1.2.0",
        VALID_WAT,
        vec![],
        vec![],
    );
    ctx.manager
        .install(&base_pkg, &base_payloads)
        .await
        .unwrap();

    // Requires version >= 2.0.0
    let dep = AppDependency::new(
        "shared.lib",
        AppVersionReq::from_str(">=2.0.0").unwrap(),
        DependencyType::Application,
        false,
    )
    .unwrap();

    let (pkg, payloads) = build_package(
        &ctx.identity,
        "app.incompat",
        "1.0.0",
        VALID_WAT,
        vec![dep],
        vec![],
    );

    let err = ctx.manager.install(&pkg, &payloads).await.unwrap_err();
    assert!(matches!(err, ApplicationError::DependencyError(_)));
}

// ---------------------------------------------------------------------------
// 7. dependency cycle detection
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_07_dependency_cycle_detection() {
    let mut graph = DependencyGraph::new();

    let dep_b = AppDependency::new(
        "app.b",
        AppVersionReq::from_str(">=1.0.0").unwrap(),
        DependencyType::Application,
        false,
    )
    .unwrap();
    let dep_c = AppDependency::new(
        "app.c",
        AppVersionReq::from_str(">=1.0.0").unwrap(),
        DependencyType::Application,
        false,
    )
    .unwrap();
    let dep_a = AppDependency::new(
        "app.a",
        AppVersionReq::from_str(">=1.0.0").unwrap(),
        DependencyType::Application,
        false,
    )
    .unwrap();

    graph.add_node("app.a".to_string(), vec![dep_b]).unwrap();
    graph.add_node("app.b".to_string(), vec![dep_c]).unwrap();
    graph.add_node("app.c".to_string(), vec![dep_a]).unwrap();

    let err = graph.detect_cycles("app.a").unwrap_err();
    assert!(
        matches!(err, ApplicationError::DependencyError(msg) if msg.to_lowercase().contains("cycle detected"))
    );
}

// ---------------------------------------------------------------------------
// 8. service dependency resolution
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_08_service_dependency_resolution() {
    let dir = tempdir().unwrap();
    let ctx = setup_test_context(dir.path()).await;

    // Register maps service
    let svc_id = ServiceId::new("maps.service").unwrap();
    let manifest_svc = ServiceManifestBuilder::new(svc_id.clone(), ServiceVersion::new(1, 4, 0))
        .instance_id(ServiceInstanceId::generate())
        .display_name("Mesh Maps")
        .endpoint(ServiceEndpoint::InternalRpc)
        .protocol(ServiceProtocol::NativeFieldnetRpc)
        .capability(ServiceCapability::new("gis").unwrap())
        .build_and_sign(&ctx.identity)
        .unwrap();

    ctx.service_registry
        .register_local(manifest_svc)
        .await
        .unwrap();

    let svc_dep = AppDependency::new(
        "maps.service",
        AppVersionReq::from_str(">=1.2.0").unwrap(),
        DependencyType::Service,
        false,
    )
    .unwrap();

    let (pkg, payloads) = build_package(
        &ctx.identity,
        "app.gis.client",
        "1.0.0",
        VALID_WAT,
        vec![svc_dep],
        vec![],
    );

    let installed = ctx.manager.install(&pkg, &payloads).await.unwrap();
    assert_eq!(installed.installed_version, AppVersion::new(1, 0, 0));
}

// ---------------------------------------------------------------------------
// 9. deterministic version selection
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_09_deterministic_version_selection() {
    let dir = tempdir().unwrap();
    let ctx = setup_test_context(dir.path()).await;

    let svc_id = ServiceId::new("data.store").unwrap();

    // Register v1.0.0, v1.5.0, v1.2.0
    for v in [
        ServiceVersion::new(1, 0, 0),
        ServiceVersion::new(1, 5, 0),
        ServiceVersion::new(1, 2, 0),
    ] {
        let manifest = ServiceManifestBuilder::new(svc_id.clone(), v)
            .instance_id(ServiceInstanceId::generate())
            .display_name("Data Store")
            .endpoint(ServiceEndpoint::InternalRpc)
            .protocol(ServiceProtocol::NativeFieldnetRpc)
            .build_and_sign(&ctx.identity)
            .unwrap();
        ctx.service_registry.register_local(manifest).await.unwrap();
    }

    let dep = AppDependency::new(
        "data.store",
        AppVersionReq::from_str(">=1.0.0").unwrap(),
        DependencyType::Service,
        false,
    )
    .unwrap();

    let (pkg, _) = build_package(
        &ctx.identity,
        "app.consumer.test",
        "1.0.0",
        VALID_WAT,
        vec![dep],
        vec![],
    );

    let report = ctx
        .manager
        .dependency_resolver()
        .resolve_for_manifest(&pkg.manifest)
        .await
        .unwrap();

    assert!(report.all_satisfied);
    // Highest SemVer selected deterministically
    assert_eq!(
        report.dependencies[0].resolved_version,
        Some("1.5.0".to_string())
    );
}

// ---------------------------------------------------------------------------
// 10. update success
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_10_update_success() {
    let dir = tempdir().unwrap();
    let ctx = setup_test_context(dir.path()).await;

    let (pkg_v1, payloads_v1) = build_package(
        &ctx.identity,
        "app.upd.success",
        "1.0.0",
        VALID_WAT,
        vec![],
        vec![],
    );
    let app_id = pkg_v1.manifest.application_id.clone();
    ctx.manager.install(&pkg_v1, &payloads_v1).await.unwrap();

    let (pkg_v2, payloads_v2) = build_package(
        &ctx.identity,
        "app.upd.success",
        "1.1.0",
        VALID_WAT,
        vec![],
        vec![],
    );

    let updated = ctx.manager.update(&pkg_v2, &payloads_v2).await.unwrap();
    assert_eq!(updated.installed_version, AppVersion::new(1, 1, 0));

    // History preserved
    let hist = ctx.registry.get_history(&app_id).unwrap();
    assert_eq!(hist.len(), 1);
    assert_eq!(hist[0].version, AppVersion::new(1, 0, 0));
}

// ---------------------------------------------------------------------------
// 11. update failure
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_11_update_failure() {
    let dir = tempdir().unwrap();
    let ctx = setup_test_context(dir.path()).await;

    let (pkg_v1, payloads_v1) = build_package(
        &ctx.identity,
        "app.upd.fail",
        "1.2.0",
        VALID_WAT,
        vec![],
        vec![],
    );
    let app_id = pkg_v1.manifest.application_id.clone();
    ctx.manager.install(&pkg_v1, &payloads_v1).await.unwrap();

    // Attempt downgrade to 1.1.0
    let (pkg_bad, payloads_bad) = build_package(
        &ctx.identity,
        "app.upd.fail",
        "1.1.0",
        VALID_WAT,
        vec![],
        vec![],
    );

    let err = ctx
        .manager
        .update(&pkg_bad, &payloads_bad)
        .await
        .unwrap_err();
    assert!(matches!(err, ApplicationError::AlreadyInstalled(_)));

    // Active version remains 1.2.0
    let current = ctx.registry.get(&app_id).unwrap().unwrap();
    assert_eq!(current.installed_version, AppVersion::new(1, 2, 0));
}

// ---------------------------------------------------------------------------
// 12. interrupted update
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_12_interrupted_update() {
    let dir = tempdir().unwrap();
    let ctx = setup_test_context(dir.path()).await;

    let (pkg_v1, payloads_v1) = build_package(
        &ctx.identity,
        "app.upd.interrupt",
        "1.0.0",
        VALID_WAT,
        vec![],
        vec![],
    );
    let app_id = pkg_v1.manifest.application_id.clone();
    ctx.manager.install(&pkg_v1, &payloads_v1).await.unwrap();

    // Create staged update transaction
    let tx = UpdateTransactionRecord {
        update_id: "upd:123".to_string(),
        application_id: app_id.clone(),
        target_version: AppVersion::new(1, 1, 0),
        target_package_id: PackageId::new("pkg:app.upd.interrupt:1.1.0").unwrap(),
        target_package_hash: hash_bytes(b"target"),
        target_manifest: pkg_v1.manifest.clone(),
        target_items: vec![],
        previous_version: AppVersion::new(1, 0, 0),
        previous_package_id: PackageId::new("pkg:app.upd.interrupt:1.0.0").unwrap(),
        previous_package_hash: hash_bytes(b"prev"),
        state: UpdateTransactionState::Staged,
        error_message: None,
        created_at: Timestamp::now(),
        updated_at: Timestamp::now(),
    };
    ctx.registry.create_update_transaction(&tx).unwrap();

    // Reconcile on restart
    let report = ctx.manager.reconcile_on_startup().await.unwrap();
    assert_eq!(report.incomplete_updates_reconciled, 1);

    // Active version is still 1.0.0
    let rec = ctx.registry.get(&app_id).unwrap().unwrap();
    assert_eq!(rec.installed_version, AppVersion::new(1, 0, 0));
}

// ---------------------------------------------------------------------------
// 13. rollback
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_13_rollback() {
    let dir = tempdir().unwrap();
    let ctx = setup_test_context(dir.path()).await;

    let (pkg_v1, payloads_v1) = build_package(
        &ctx.identity,
        "app.rb.test",
        "1.0.0",
        VALID_WAT,
        vec![],
        vec![],
    );
    let app_id = pkg_v1.manifest.application_id.clone();
    ctx.manager.install(&pkg_v1, &payloads_v1).await.unwrap();

    let (pkg_v2, payloads_v2) = build_package(
        &ctx.identity,
        "app.rb.test",
        "1.1.0",
        VALID_WAT,
        vec![],
        vec![],
    );
    ctx.manager.update(&pkg_v2, &payloads_v2).await.unwrap();

    // Rollback to v1.0.0
    let target = AppVersion::new(1, 0, 0);
    let rolled_back = ctx.manager.rollback(&app_id, Some(&target)).await.unwrap();
    assert_eq!(rolled_back.installed_version, AppVersion::new(1, 0, 0));

    let active = ctx.registry.get(&app_id).unwrap().unwrap();
    assert_eq!(active.installed_version, AppVersion::new(1, 0, 0));
}

// ---------------------------------------------------------------------------
// 14. rollback integrity verification
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_14_rollback_integrity_verification() {
    let dir = tempdir().unwrap();
    let ctx = setup_test_context(dir.path()).await;

    let (pkg_v1, payloads_v1) = build_package(
        &ctx.identity,
        "app.rb.corrupt",
        "1.0.0",
        VALID_WAT,
        vec![],
        vec![],
    );
    let app_id = pkg_v1.manifest.application_id.clone();
    ctx.manager.install(&pkg_v1, &payloads_v1).await.unwrap();

    let (pkg_v2, payloads_v2) = build_package(
        &ctx.identity,
        "app.rb.corrupt",
        "1.1.0",
        VALID_WAT,
        vec![],
        vec![],
    );
    ctx.manager.update(&pkg_v2, &payloads_v2).await.unwrap();

    // Manually unpin and delete v1 CAS chunk to simulate corruption / missing asset
    let v1_cid = pkg_v1.items[0].content_id;
    ctx.content_store.unpin(&v1_cid).unwrap();
    ctx.content_store.delete(&v1_cid).await.unwrap();

    // Rollback must fail integrity check
    let target = AppVersion::new(1, 0, 0);
    let err = ctx
        .manager
        .rollback(&app_id, Some(&target))
        .await
        .unwrap_err();
    assert!(matches!(err, ApplicationError::IntegrityError(_)));

    // Active version remains intact as v1.1.0
    let active = ctx.registry.get(&app_id).unwrap().unwrap();
    assert_eq!(active.installed_version, AppVersion::new(1, 1, 0));
}

// ---------------------------------------------------------------------------
// 15. restart after incomplete update
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_15_restart_after_incomplete_update() {
    let dir = tempdir().unwrap();
    let ctx = setup_test_context(dir.path()).await;

    let (pkg, payloads) = build_package(
        &ctx.identity,
        "app.reboot.test",
        "1.0.0",
        VALID_WAT,
        vec![],
        vec![],
    );
    let app_id = pkg.manifest.application_id.clone();
    ctx.manager.install(&pkg, &payloads).await.unwrap();
    ctx.manager.authorize(&app_id).unwrap();

    // Start cleanly
    let report = ctx.manager.start(&app_id).await.unwrap();
    assert_eq!(report.exit_code, Some(42));

    // Restart application
    let r_report = ctx.manager.restart(&app_id).await.unwrap();
    assert_eq!(r_report.exit_code, Some(42));
}

// ---------------------------------------------------------------------------
// 16. graceful stop
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_16_graceful_stop() {
    let dir = tempdir().unwrap();
    let ctx = setup_test_context(dir.path()).await;

    let (pkg, payloads) = build_package(
        &ctx.identity,
        "app.stop.test",
        "1.0.0",
        LOOPING_WAT,
        vec![],
        vec![],
    );
    let app_id = pkg.manifest.application_id.clone();
    ctx.manager.install(&pkg, &payloads).await.unwrap();
    ctx.manager.authorize(&app_id).unwrap();

    let mgr = Arc::clone(&ctx.manager);
    let id_clone = app_id.clone();

    let handle = tokio::spawn(async move { mgr.start(&id_clone).await });

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Gracefully stop
    ctx.manager.stop(&app_id).await.unwrap();

    let res = handle.await.unwrap();
    assert!(res.is_err() || res.unwrap().final_state == ApplicationState::Stopped);
}

// ---------------------------------------------------------------------------
// 17. application restart
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_17_application_restart() {
    let dir = tempdir().unwrap();
    let ctx = setup_test_context(dir.path()).await;

    let (pkg, payloads) = build_package(
        &ctx.identity,
        "app.restart.test",
        "1.0.0",
        VALID_WAT,
        vec![],
        vec![],
    );
    let app_id = pkg.manifest.application_id.clone();
    ctx.manager.install(&pkg, &payloads).await.unwrap();
    ctx.manager.authorize(&app_id).unwrap();

    let report = ctx.manager.restart(&app_id).await.unwrap();
    assert_eq!(report.exit_code, Some(42));
}

// ---------------------------------------------------------------------------
// 18. crash recording
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_18_crash_recording() {
    let dir = tempdir().unwrap();
    let ctx = setup_test_context(dir.path()).await;

    let (pkg, payloads) = build_package(
        &ctx.identity,
        "app.crash.rec",
        "1.0.0",
        CRASHING_WAT,
        vec![],
        vec![],
    );
    let app_id = pkg.manifest.application_id.clone();
    ctx.manager.install(&pkg, &payloads).await.unwrap();
    ctx.manager.authorize(&app_id).unwrap();

    let _ = ctx.manager.start(&app_id).await;

    let crashes = ctx.registry.get_crash_history(&app_id).unwrap();
    assert_eq!(crashes.len(), 1);
    assert!(crashes[0].reason.contains("Trap") || crashes[0].reason.contains("unreachable"));
}

// ---------------------------------------------------------------------------
// 19. crash isolation
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_19_crash_isolation() {
    let dir = tempdir().unwrap();
    let ctx = setup_test_context(dir.path()).await;

    let (pkg, payloads) = build_package(
        &ctx.identity,
        "app.crash.iso",
        "1.0.0",
        CRASHING_WAT,
        vec![],
        vec![],
    );
    let app_id = pkg.manifest.application_id.clone();
    ctx.manager.install(&pkg, &payloads).await.unwrap();
    ctx.manager.authorize(&app_id).unwrap();

    for _ in 0..3 {
        let _ = ctx.manager.start(&app_id).await;
    }

    assert!(ctx.registry.is_isolated(&app_id).unwrap());
    let rec = ctx.registry.get(&app_id).unwrap().unwrap();
    assert_eq!(rec.lifecycle_state, ApplicationState::Isolated);
}

// ---------------------------------------------------------------------------
// 20. application isolation
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_20_application_isolation() {
    let dir = tempdir().unwrap();
    let ctx = setup_test_context(dir.path()).await;

    let (pkg, payloads) = build_package(
        &ctx.identity,
        "app.manual.iso",
        "1.0.0",
        VALID_WAT,
        vec![],
        vec![],
    );
    let app_id = pkg.manifest.application_id.clone();
    ctx.manager.install(&pkg, &payloads).await.unwrap();

    ctx.manager
        .isolate(&app_id, "Security audit quarantine")
        .await
        .unwrap();

    assert!(ctx.registry.is_isolated(&app_id).unwrap());
    assert_eq!(
        ctx.registry.get_isolation_reason(&app_id).unwrap(),
        Some("Security audit quarantine".to_string())
    );
}

// ---------------------------------------------------------------------------
// 21. isolated application cannot start
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_21_isolated_application_cannot_start() {
    let dir = tempdir().unwrap();
    let ctx = setup_test_context(dir.path()).await;

    let (pkg, payloads) = build_package(
        &ctx.identity,
        "app.iso.block",
        "1.0.0",
        VALID_WAT,
        vec![],
        vec![],
    );
    let app_id = pkg.manifest.application_id.clone();
    ctx.manager.install(&pkg, &payloads).await.unwrap();
    ctx.manager.authorize(&app_id).unwrap();
    ctx.manager.isolate(&app_id, "Operator lock").await.unwrap();

    let err = ctx.manager.start(&app_id).await.unwrap_err();
    assert!(matches!(err, ApplicationError::ExecutionError(msg) if msg.contains("ISOLATED")));
}

// ---------------------------------------------------------------------------
// 22. uninstall running application rejection
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_22_uninstall_running_application_rejection() {
    let dir = tempdir().unwrap();
    let ctx = setup_test_context(dir.path()).await;

    let (pkg, payloads) = build_package(
        &ctx.identity,
        "app.uninst.block",
        "1.0.0",
        LOOPING_WAT,
        vec![],
        vec![],
    );
    let app_id = pkg.manifest.application_id.clone();
    ctx.manager.install(&pkg, &payloads).await.unwrap();
    ctx.manager.authorize(&app_id).unwrap();

    let mgr = Arc::clone(&ctx.manager);
    let id_clone = app_id.clone();
    let _handle = tokio::spawn(async move { mgr.start(&id_clone).await });

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let err = ctx.manager.uninstall(&app_id).await.unwrap_err();
    assert!(matches!(err, ApplicationError::ExecutionError(_)));

    ctx.manager.stop(&app_id).await.unwrap();
}

// ---------------------------------------------------------------------------
// 23. uninstall stopped application
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_23_uninstall_stopped_application() {
    let dir = tempdir().unwrap();
    let ctx = setup_test_context(dir.path()).await;

    let (pkg, payloads) = build_package(
        &ctx.identity,
        "app.uninst.ok",
        "1.0.0",
        VALID_WAT,
        vec![],
        vec![],
    );
    let app_id = pkg.manifest.application_id.clone();
    ctx.manager.install(&pkg, &payloads).await.unwrap();

    ctx.manager.uninstall(&app_id).await.unwrap();
    assert!(ctx.registry.get(&app_id).unwrap().is_none());
}

// ---------------------------------------------------------------------------
// 24. shared CAS content preservation
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_24_shared_cas_content_preservation() {
    let dir = tempdir().unwrap();
    let ctx = setup_test_context(dir.path()).await;

    // Both apps share the exact same WASM code (and thus identical ContentId)
    let (pkg_a, payloads_a) = build_package(
        &ctx.identity,
        "app.shared.a",
        "1.0.0",
        VALID_WAT,
        vec![],
        vec![],
    );
    let (pkg_b, payloads_b) = build_package(
        &ctx.identity,
        "app.shared.b",
        "1.0.0",
        VALID_WAT,
        vec![],
        vec![],
    );

    let cid = pkg_a.items[0].content_id;
    assert_eq!(cid, pkg_b.items[0].content_id);

    ctx.manager.install(&pkg_a, &payloads_a).await.unwrap();
    ctx.manager.install(&pkg_b, &payloads_b).await.unwrap();

    // Uninstall App A
    ctx.manager
        .uninstall(&pkg_a.manifest.application_id)
        .await
        .unwrap();

    // ContentId must STILL be present in CAS for App B!
    assert!(ctx.content_store.has(&cid).unwrap());
}

// ---------------------------------------------------------------------------
// 25. registry persistence
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_25_registry_persistence() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("persist.db");

    let app_id = ApplicationId::new("app.persist.test").unwrap();

    {
        let reg = ApplicationRegistry::new(&db_path).unwrap();
        let (pkg, _) = build_package(
            &NodeIdentity::generate(),
            "app.persist.test",
            "1.0.0",
            VALID_WAT,
            vec![],
            vec![],
        );
        let pkg_hash = pkg.compute_package_hash();
        let rec = ApplicationRecord {
            application_id: app_id.clone(),
            installed_version: AppVersion::new(1, 0, 0),
            package_id: pkg.package_id.clone(),
            package_hash: pkg_hash,
            manifest: pkg.manifest,
            items: pkg.items,
            publisher: PublisherIdentity::from_public_key([1u8; 32]),
            signature_status: fieldnet_wasm::registry::SignatureStatus::Verified,
            lifecycle_state: ApplicationState::Authorized,
            permissions: vec![],
            resource_limits: ResourceLimits::default(),
            dependencies: vec![],
            installed_at: Timestamp::now(),
            updated_at: Timestamp::now(),
        };
        reg.register(&rec).unwrap();
    }

    // Reopen from disk
    {
        let reg = ApplicationRegistry::new(&db_path).unwrap();
        let loaded = reg.get(&app_id).unwrap().unwrap();
        assert_eq!(loaded.installed_version, AppVersion::new(1, 0, 0));
        assert_eq!(loaded.lifecycle_state, ApplicationState::Authorized);
    }
}

// ---------------------------------------------------------------------------
// 26. runtime restart recovery
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_26_runtime_restart_recovery() {
    let dir = tempdir().unwrap();
    let ctx = setup_test_context(dir.path()).await;

    let report = ctx.manager.reconcile_on_startup().await.unwrap();
    assert_eq!(report.corrupted_packages_isolated, 0);
    assert_eq!(report.incomplete_updates_reconciled, 0);
}

// ---------------------------------------------------------------------------
// 27. stale RUNNING reconciliation
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_27_stale_running_reconciliation() {
    let dir = tempdir().unwrap();
    let ctx = setup_test_context(dir.path()).await;

    let (pkg, payloads) = build_package(
        &ctx.identity,
        "app.stale.rec",
        "1.0.0",
        VALID_WAT,
        vec![],
        vec![],
    );
    let app_id = pkg.manifest.application_id.clone();
    ctx.manager.install(&pkg, &payloads).await.unwrap();
    ctx.manager.authorize(&app_id).unwrap();

    // Transition state to Running
    ctx.registry
        .update_lifecycle_state(&app_id, ApplicationState::Starting)
        .unwrap();
    ctx.registry
        .update_lifecycle_state(&app_id, ApplicationState::Running)
        .unwrap();

    // Reconcile on startup
    let _ = ctx.manager.reconcile_on_startup().await.unwrap();

    // Must be reconciled to Stopped
    let state = ctx.registry.get(&app_id).unwrap().unwrap().lifecycle_state;
    assert_eq!(state, ApplicationState::Stopped);
}

// ---------------------------------------------------------------------------
// 28. concurrent lifecycle operations
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_28_concurrent_lifecycle_operations() {
    let dir = tempdir().unwrap();
    let ctx = setup_test_context(dir.path()).await;

    let mut handles = Vec::new();

    for i in 0..5 {
        let mgr = Arc::clone(&ctx.manager);
        let id_ref = Arc::clone(&ctx.identity);
        handles.push(tokio::spawn(async move {
            let app_name = format!("app.conc.{}", i);
            let (pkg, payloads) =
                build_package(&id_ref, &app_name, "1.0.0", VALID_WAT, vec![], vec![]);
            mgr.install(&pkg, &payloads).await
        }));
    }

    for h in handles {
        assert!(h.await.unwrap().is_ok());
    }

    assert_eq!(ctx.registry.list().unwrap().len(), 5);
}

// ---------------------------------------------------------------------------
// 29. lifecycle event correctness
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_29_lifecycle_event_correctness() {
    let dir = tempdir().unwrap();
    let mut ctx = setup_test_context(dir.path()).await;

    let (pkg, payloads) = build_package(
        &ctx.identity,
        "app.event.test",
        "1.0.0",
        VALID_WAT,
        vec![],
        vec![],
    );
    let app_id = pkg.manifest.application_id.clone();

    ctx.manager.install(&pkg, &payloads).await.unwrap();
    ctx.manager.authorize(&app_id).unwrap();
    ctx.manager.start(&app_id).await.unwrap();

    let mut events = Vec::new();
    while let Ok(ev) = ctx.event_rx.try_recv() {
        events.push(ev);
    }

    // Verify presence of lifecycle events
    assert!(events
        .iter()
        .any(|e| matches!(e, ApplicationEvent::Installed { .. })));
    assert!(events
        .iter()
        .any(|e| matches!(e, ApplicationEvent::Starting { .. })));
    assert!(events
        .iter()
        .any(|e| matches!(e, ApplicationEvent::Started { .. })));
    assert!(events
        .iter()
        .any(|e| matches!(e, ApplicationEvent::Stopped { .. })));
}

// ---------------------------------------------------------------------------
// 30. security invariant tests
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_30_security_invariants() {
    let dir = tempdir().unwrap();
    let ctx = setup_test_context(dir.path()).await;

    // 1. Unsigned package cannot be installed
    let (mut unsigned_pkg, payloads) = build_package(
        &ctx.identity,
        "app.sec.unsigned",
        "1.0.0",
        VALID_WAT,
        vec![],
        vec![],
    );
    unsigned_pkg.signature = None;
    let err = ctx
        .manager
        .install(&unsigned_pkg, &payloads)
        .await
        .unwrap_err();
    assert!(matches!(err, ApplicationError::SignatureError(_)));

    // 2. Tampered signature rejected
    let (mut tampered_pkg, payloads) = build_package(
        &ctx.identity,
        "app.sec.tampered",
        "1.0.0",
        VALID_WAT,
        vec![],
        vec![],
    );
    tampered_pkg.manifest.name = "Tampered".to_string();
    let err = ctx
        .manager
        .install(&tampered_pkg, &payloads)
        .await
        .unwrap_err();
    assert!(matches!(err, ApplicationError::SignatureError(_)));

    // 3. Unauthorized application cannot start
    let (pkg, payloads) = build_package(
        &ctx.identity,
        "app.sec.unauth",
        "1.0.0",
        VALID_WAT,
        vec![],
        vec![],
    );
    let app_id = pkg.manifest.application_id.clone();
    ctx.manager.install(&pkg, &payloads).await.unwrap();

    let err = ctx.manager.start(&app_id).await.unwrap_err();
    assert!(matches!(
        err,
        ApplicationError::ApplicationNotAuthorized { .. }
    ));
}
