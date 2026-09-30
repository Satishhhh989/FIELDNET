use fieldnet_content::hashing::hash_bytes;
use fieldnet_core::Timestamp;
use fieldnet_identity::NodeIdentity;
use fieldnet_runtime::app::{
    AppDependency, AppPermission, AppVersion, AppVersionReq, ApplicationError, ApplicationEvent,
    ApplicationId, ApplicationManifest, ApplicationPackage, ApplicationState, DependencyGraph,
    DependencyType, PackageId, PackageItem, PackageSignature, PublisherIdentity, ResourceLimits,
    CURRENT_MANIFEST_SCHEMA_VERSION,
};
use fieldnet_runtime::service::{
    ServiceCapability, ServiceEndpoint, ServiceId, ServiceInstanceId, ServiceManifestBuilder,
    ServiceProtocol, ServiceVersion,
};
use fieldnet_runtime::{FieldnetRuntime, RuntimeConfig};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tempfile::tempdir;

fn create_test_identity() -> Arc<NodeIdentity> {
    Arc::new(NodeIdentity::generate())
}

fn build_package(
    publisher_identity: &NodeIdentity,
    app_id_str: &str,
    version_str: &str,
    wat_source: &str,
    permissions: Vec<AppPermission>,
    dependencies: Vec<AppDependency>,
) -> (ApplicationPackage, HashMap<String, Vec<u8>>) {
    let wasm_bytes = wat::parse_str(wat_source).expect("Failed to parse WAT fixture");
    let publisher = PublisherIdentity::from_public_key(publisher_identity.public_key_bytes());
    let app_id = ApplicationId::new(app_id_str).unwrap();
    let version: AppVersion = version_str.parse().unwrap();

    let manifest = ApplicationManifest {
        schema_version: CURRENT_MANIFEST_SCHEMA_VERSION,
        application_id: app_id,
        name: format!("Lifecycle App {}", app_id_str),
        version,
        description: "Lifecycle test application".to_string(),
        publisher,
        entrypoint: "main.wasm".to_string(),
        permissions,
        resource_limits: ResourceLimits::default(),
        dependencies,
        package_metadata: Default::default(),
    };

    let wasm_hash = hash_bytes(&wasm_bytes);
    let item_wasm = PackageItem::new(
        "main.wasm",
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

    let pkg_hash = pkg.compute_package_hash();
    let sig = PackageSignature::sign_with_identity(publisher_identity, &pkg_hash, Timestamp::now());
    pkg.signature = Some(sig);

    let mut payloads = HashMap::new();
    payloads.insert("main.wasm".to_string(), wasm_bytes);

    (pkg, payloads)
}

const VALID_WAT: &str = r#"
(module
    (memory (export "memory") 1)
    (func (export "main") (result i32)
        i32.const 0
    )
)
"#;

const CRASHING_WAT: &str = r#"
(module
    (memory (export "memory") 1)
    (func (export "main") (result i32)
        unreachable
    )
)
"#;

#[tokio::test]
async fn test_full_application_lifecycle_transitions_and_events() {
    let dir = tempdir().unwrap();
    let config = RuntimeConfig {
        data_dir: dir.path().to_path_buf(),
        ..Default::default()
    };

    let runtime = FieldnetRuntime::start(config).await.expect("Runtime start");
    let mut app_event_rx = runtime.subscribe_application_events();

    let identity = create_test_identity();
    let (pkg, payloads) = build_package(
        &identity,
        "lifecycle.test.app",
        "1.0.0",
        VALID_WAT,
        vec![AppPermission::StorageRead],
        vec![],
    );
    let app_id = pkg.manifest.application_id.clone();

    // 1. Install & Authorize
    let installed = runtime
        .install_application(&pkg, &payloads)
        .await
        .expect("Install success");
    assert_eq!(installed.lifecycle_state, ApplicationState::Installed);

    let authorized_state = runtime
        .authorize_application(&app_id)
        .expect("Authorize success");
    assert_eq!(authorized_state, ApplicationState::Authorized);

    // 2. Start Application
    let start_report = runtime
        .start_application(&app_id)
        .await
        .expect("Start success");
    assert_eq!(start_report.application_id, app_id);
    assert_eq!(start_report.exit_code, Some(0));

    // Verify Active Executions
    let _executions = runtime.list_application_executions().await;
    assert_eq!(start_report.final_state, ApplicationState::Stopped);

    // 3. Restart Application
    let restart_report = runtime
        .restart_application(&app_id)
        .await
        .expect("Restart success");
    assert_eq!(restart_report.application_id, app_id);
    assert_eq!(restart_report.exit_code, Some(0));

    // 4. Stop Application
    let stop_res = runtime.stop_application(&app_id).await;
    assert!(stop_res.is_ok());

    let state = runtime
        .application_registry()
        .get(&app_id)
        .unwrap()
        .unwrap()
        .lifecycle_state;
    assert_eq!(state, ApplicationState::Stopped);

    // 5. Isolate Application
    runtime
        .isolate_application(&app_id, "Security audit quarantine")
        .await
        .expect("Isolate success");

    let is_isolated = runtime.application_registry().is_isolated(&app_id).unwrap();
    assert!(is_isolated);

    let reason = runtime
        .application_registry()
        .get_isolation_reason(&app_id)
        .unwrap();
    assert_eq!(reason, Some("Security audit quarantine".to_string()));

    // Verify cannot start isolated app
    let err = runtime.start_application(&app_id).await.unwrap_err();
    assert!(matches!(err, ApplicationError::ExecutionError(msg) if msg.contains("ISOLATED")));

    // 6. Unisolate Application
    let restored_state = runtime
        .unisolate_application(&app_id)
        .await
        .expect("Unisolate success");
    assert_eq!(restored_state, ApplicationState::Stopped);
    assert!(!runtime.application_registry().is_isolated(&app_id).unwrap());

    // 7. Verify events received
    let mut events = Vec::new();
    while let Ok(ev) = app_event_rx.try_recv() {
        events.push(ev);
    }
    assert!(events
        .iter()
        .any(|e| matches!(e, ApplicationEvent::Isolated { .. })));

    runtime.shutdown();
}

#[tokio::test]
async fn test_dependency_resolution_and_conflict_detection() {
    let dir = tempdir().unwrap();
    let config = RuntimeConfig {
        data_dir: dir.path().to_path_buf(),
        ..Default::default()
    };

    let runtime = FieldnetRuntime::start(config).await.expect("Runtime start");

    // 1. Test missing required dependency
    let missing_dep = AppDependency::new(
        "dep.missing.service",
        AppVersionReq::from_str("^1.0.0").unwrap(),
        DependencyType::Service,
        false,
    )
    .unwrap();

    let identity = create_test_identity();
    let (pkg, _) = build_package(
        &identity,
        "app.with.dep",
        "1.0.0",
        VALID_WAT,
        vec![],
        vec![missing_dep],
    );

    let report = runtime
        .dependency_resolver()
        .resolve_for_manifest(&pkg.manifest)
        .await
        .expect("Resolution report");

    assert!(!report.all_satisfied);
    assert_eq!(report.missing_required.len(), 1);
    assert_eq!(report.missing_required[0], "dep.missing.service");

    // 2. Register required service and verify satisfaction
    let svc_id = ServiceId::new("dep.missing.service").unwrap();
    let instance_id = ServiceInstanceId::generate();
    let manifest_svc = ServiceManifestBuilder::new(svc_id, ServiceVersion::new(1, 2, 0))
        .instance_id(instance_id)
        .display_name("Dependency Service")
        .endpoint(ServiceEndpoint::InternalRpc)
        .protocol(ServiceProtocol::NativeFieldnetRpc)
        .capability(ServiceCapability::new("routing").unwrap())
        .build_and_sign(&runtime.identity())
        .unwrap();

    runtime
        .service_registry()
        .register_local(manifest_svc)
        .await
        .unwrap();

    let report_satisfied = runtime
        .dependency_resolver()
        .resolve_for_manifest(&pkg.manifest)
        .await
        .expect("Resolution report");

    assert!(report_satisfied.all_satisfied);
    assert!(report_satisfied.missing_required.is_empty());
    assert_eq!(
        report_satisfied.dependencies[0].resolved_version,
        Some("1.2.0".to_string())
    );

    // 3. Test dependency cycle detection
    let mut graph = DependencyGraph::new();
    let dep_b = AppDependency::new(
        "package-b",
        AppVersionReq::from_str(">=1.0.0").unwrap(),
        DependencyType::Application,
        false,
    )
    .unwrap();
    let dep_c = AppDependency::new(
        "package-c",
        AppVersionReq::from_str(">=1.0.0").unwrap(),
        DependencyType::Application,
        false,
    )
    .unwrap();
    let dep_a = AppDependency::new(
        "package-a",
        AppVersionReq::from_str(">=1.0.0").unwrap(),
        DependencyType::Application,
        false,
    )
    .unwrap();

    graph
        .add_node("package-a".to_string(), vec![dep_b])
        .unwrap();
    graph
        .add_node("package-b".to_string(), vec![dep_c])
        .unwrap();
    graph
        .add_node("package-c".to_string(), vec![dep_a])
        .unwrap();

    let cycle_result = graph.detect_cycles("package-a");
    assert!(cycle_result.is_err());
    let err = cycle_result.unwrap_err();
    assert!(
        matches!(err, ApplicationError::DependencyError(msg) if msg.contains("Dependency cycle detected"))
    );

    runtime.shutdown();
}

#[tokio::test]
async fn test_crash_recovery_and_auto_quarantine() {
    let dir = tempdir().unwrap();
    let config = RuntimeConfig {
        data_dir: dir.path().to_path_buf(),
        ..Default::default()
    };

    let runtime = FieldnetRuntime::start(config).await.expect("Runtime start");
    let mut app_event_rx = runtime.subscribe_application_events();

    let identity = create_test_identity();
    let (pkg, payloads) = build_package(
        &identity,
        "crashing.fault.app",
        "1.0.0",
        CRASHING_WAT,
        vec![],
        vec![],
    );
    let app_id = pkg.manifest.application_id.clone();

    runtime
        .install_application(&pkg, &payloads)
        .await
        .expect("Install success");
    runtime.authorize_application(&app_id).expect("Authorize");

    // Execute application 3 times; each will crash due to unreachable instruction
    for i in 1..=3 {
        let res = runtime.start_application(&app_id).await;
        assert!(res.is_err(), "Run {} must fail with crash", i);
    }

    // Verify 3 crashes recorded in crash history
    let crashes = runtime
        .application_registry()
        .get_crash_history(&app_id)
        .expect("Get crash history");
    assert_eq!(crashes.len(), 3);

    // Verify automatic quarantine triggered!
    let is_isolated = runtime
        .application_registry()
        .is_isolated(&app_id)
        .expect("Isolated check");
    assert!(is_isolated, "App must be auto-isolated after 3 crashes");

    let reason = runtime
        .application_registry()
        .get_isolation_reason(&app_id)
        .unwrap();
    assert!(reason.unwrap().contains("crash"));

    // Attempting a 4th start must be blocked by quarantine
    let err = runtime.start_application(&app_id).await.unwrap_err();
    assert!(matches!(err, ApplicationError::ExecutionError(msg) if msg.contains("ISOLATED")));

    // Verify Isolated event was emitted
    let mut received_isolated_event = false;
    while let Ok(ev) = app_event_rx.try_recv() {
        if matches!(ev, ApplicationEvent::Isolated { .. }) {
            received_isolated_event = true;
        }
    }
    assert!(received_isolated_event, "Isolated event must be emitted");

    runtime.shutdown();
}

#[tokio::test]
async fn test_transactional_update_and_atomic_rollback() {
    let dir = tempdir().unwrap();
    let config = RuntimeConfig {
        data_dir: dir.path().to_path_buf(),
        ..Default::default()
    };

    let runtime = FieldnetRuntime::start(config).await.expect("Runtime start");

    let identity = create_test_identity();
    let app_id_str = "update.rollback.app";

    // 1. Build v1.0.0
    let (pkg_v1, payloads_v1) =
        build_package(&identity, app_id_str, "1.0.0", VALID_WAT, vec![], vec![]);
    let app_id = pkg_v1.manifest.application_id.clone();

    runtime
        .install_application(&pkg_v1, &payloads_v1)
        .await
        .unwrap();
    runtime.authorize_application(&app_id).unwrap();

    let rec_v1 = runtime
        .application_registry()
        .get(&app_id)
        .unwrap()
        .unwrap();
    assert_eq!(rec_v1.installed_version, AppVersion::new(1, 0, 0));

    // 2. Perform Transactional Update to v1.1.0
    let (pkg_v2, payloads_v2) =
        build_package(&identity, app_id_str, "1.1.0", VALID_WAT, vec![], vec![]);

    let updated_rec = runtime
        .update_application(&pkg_v2, &payloads_v2)
        .await
        .expect("Update success");
    assert_eq!(updated_rec.installed_version, AppVersion::new(1, 1, 0));

    // Verify history has v1.0.0
    let history = runtime.application_registry().get_history(&app_id).unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].version, AppVersion::new(1, 0, 0));

    // 3. Atomic Rollback to v1.0.0
    let target_ver = AppVersion::new(1, 0, 0);
    let rolled_back_rec = runtime
        .rollback_application_to_version(&app_id, Some(&target_ver))
        .await
        .expect("Rollback success");
    assert_eq!(rolled_back_rec.installed_version, AppVersion::new(1, 0, 0));

    let active_rec = runtime
        .application_registry()
        .get(&app_id)
        .unwrap()
        .unwrap();
    assert_eq!(active_rec.installed_version, AppVersion::new(1, 0, 0));

    // 4. Uninstall Application
    runtime
        .uninstall_application(&app_id)
        .await
        .expect("Uninstall success");
    assert!(runtime
        .application_registry()
        .get(&app_id)
        .unwrap()
        .is_none());

    runtime.shutdown();
}

#[tokio::test]
async fn test_startup_reconciliation_of_interrupted_states() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();
    let config = RuntimeConfig {
        data_dir: data_dir.clone(),
        ..Default::default()
    };

    let identity = create_test_identity();
    let (pkg, payloads) = build_package(
        &identity,
        "reconcile.test.app",
        "1.0.0",
        VALID_WAT,
        vec![],
        vec![],
    );
    let app_id = pkg.manifest.application_id.clone();

    // 1. Install and simulate an interrupted running state in the database
    {
        let runtime = FieldnetRuntime::start(config.clone())
            .await
            .expect("Runtime start 1");

        runtime
            .install_application(&pkg, &payloads)
            .await
            .expect("Install success");
        runtime.authorize_application(&app_id).expect("Authorize");

        // Transition through Starting to Running to simulate abrupt node crash while running
        runtime
            .application_registry()
            .update_lifecycle_state(&app_id, ApplicationState::Starting)
            .unwrap();
        runtime
            .application_registry()
            .update_lifecycle_state(&app_id, ApplicationState::Running)
            .unwrap();

        // Shutdown runtime
        runtime.shutdown();
    }

    // 2. Restart runtime and verify reconciliation reset orphaned Running state to Stopped
    {
        let rebooted_runtime = FieldnetRuntime::start(config)
            .await
            .expect("Runtime reboot");

        let state = rebooted_runtime
            .application_registry()
            .get(&app_id)
            .unwrap()
            .unwrap()
            .lifecycle_state;

        assert_eq!(
            state,
            ApplicationState::Stopped,
            "Orphaned running state must be reconciled to Stopped on restart"
        );

        rebooted_runtime.shutdown();
    }
}
