use fieldnet_content::hashing::hash_bytes;
use fieldnet_core::Timestamp;
use fieldnet_identity::NodeIdentity;
use fieldnet_runtime::app::{
    AppPermission, AppVersion, ApplicationError, ApplicationEvent, ApplicationId,
    ApplicationManifest, ApplicationPackage, ApplicationState, ExecutionId, PackageId, PackageItem,
    PackageSignature, PublisherIdentity, ResourceLimits, CURRENT_MANIFEST_SCHEMA_VERSION,
};
use fieldnet_runtime::{FieldnetRuntime, RuntimeConfig};
use std::collections::HashMap;
use std::time::Duration;
use tempfile::tempdir;

fn build_test_package(
    publisher_identity: &NodeIdentity,
    app_id_str: &str,
    version_str: &str,
    wat_source: &str,
    limits: Option<ResourceLimits>,
) -> (ApplicationPackage, HashMap<String, Vec<u8>>) {
    let wasm_bytes = wat::parse_str(wat_source).expect("Failed to parse WAT test fixture");
    let publisher = PublisherIdentity::from_public_key(publisher_identity.public_key_bytes());
    let app_id = ApplicationId::new(app_id_str).unwrap();
    let version: AppVersion = version_str.parse().unwrap();

    let manifest = ApplicationManifest {
        schema_version: CURRENT_MANIFEST_SCHEMA_VERSION,
        application_id: app_id.clone(),
        name: format!("Fieldnet Test App {}", app_id_str),
        version,
        description: "Integration test WASM application".to_string(),
        publisher,
        entrypoint: "main.wasm".to_string(),
        permissions: vec![AppPermission::StorageRead],
        resource_limits: limits.unwrap_or_default(),
        dependencies: vec![],
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

#[tokio::test]
async fn test_fieldnet_runtime_wasm_execution_end_to_end() {
    let temp = tempdir().unwrap();
    let data_dir = temp.path().to_path_buf();

    let config = RuntimeConfig {
        data_dir: data_dir.clone(),
        tcp_port: 0,
        discovery_port: 0,
        auto_connect: false,
        beacon_interval: Duration::from_millis(500),
        allowed_peers: None,
        blocked_peers: None,
    };

    // 1. Start Runtime Node
    let runtime = FieldnetRuntime::start(config)
        .await
        .expect("Runtime must start");
    let publisher = NodeIdentity::generate();

    let wat_add = r#"
    (module
        (func (export "main") (result i32)
            (i32.add (i32.const 100) (i32.const 234))
        )
    )
    "#;

    let (pkg, payloads) =
        build_test_package(&publisher, "fieldnet.calc.add", "1.0.0", wat_add, None);
    let app_id = pkg.manifest.application_id.clone();

    // 2. Install application
    let record = runtime
        .install_application(&pkg, &payloads)
        .await
        .expect("Application must install successfully");
    assert_eq!(record.lifecycle_state, ApplicationState::Installed);

    // 3. Execution rejected before authorization
    let unauthorized_res = runtime.run_application(&app_id).await;
    assert!(
        matches!(
            unauthorized_res,
            Err(ApplicationError::ApplicationNotAuthorized { .. })
        ),
        "Execution must be rejected before authorization"
    );

    // 4. Authorize application
    runtime
        .authorize_application(&app_id)
        .expect("Application authorization must succeed");

    // 5. Subscribe to application events
    let mut event_rx = runtime.subscribe_application_events();

    // 6. Execute application via Runtime Supervisor
    let report = runtime
        .run_application(&app_id)
        .await
        .expect("Authorized application must execute cleanly");

    assert_eq!(report.exit_code, Some(334), "100 + 234 must equal 334");
    assert!(report.fuel_consumed > 0, "Execution must meter fuel");
    assert_eq!(report.final_state, ApplicationState::Stopped);

    // 7. Verify Lifecycle Events on bus
    let mut event_kinds = Vec::new();
    while let Ok(event) = event_rx.try_recv() {
        match event {
            ApplicationEvent::Starting { application_id, .. } if application_id == app_id => {
                event_kinds.push("starting");
            }
            ApplicationEvent::Started { application_id, .. } if application_id == app_id => {
                event_kinds.push("started");
            }
            ApplicationEvent::Stopped { application_id, .. } if application_id == app_id => {
                event_kinds.push("stopped");
            }
            _ => {}
        }
    }

    assert!(
        event_kinds.contains(&"starting"),
        "Starting event must be emitted"
    );
    assert!(
        event_kinds.contains(&"started"),
        "Started event must be emitted"
    );
    assert!(
        event_kinds.contains(&"stopped"),
        "Stopped event must be emitted"
    );

    runtime.shutdown();
}

#[tokio::test]
async fn test_fieldnet_runtime_crash_isolation() {
    let temp = tempdir().unwrap();
    let data_dir = temp.path().to_path_buf();

    let config = RuntimeConfig {
        data_dir: data_dir.clone(),
        tcp_port: 0,
        discovery_port: 0,
        auto_connect: false,
        beacon_interval: Duration::from_millis(500),
        allowed_peers: None,
        blocked_peers: None,
    };

    let runtime = FieldnetRuntime::start(config)
        .await
        .expect("Runtime must start");
    let publisher = NodeIdentity::generate();

    // Trap WAT program
    let wat_trap = r#"
    (module
        (func (export "main")
            unreachable
        )
    )
    "#;

    let (pkg_trap, payloads_trap) =
        build_test_package(&publisher, "fieldnet.faulty.trap", "1.0.0", wat_trap, None);
    let trap_app_id = pkg_trap.manifest.application_id.clone();

    runtime
        .install_application(&pkg_trap, &payloads_trap)
        .await
        .unwrap();
    runtime.authorize_application(&trap_app_id).unwrap();

    // Executing faulty application traps
    let trap_res = runtime.run_application(&trap_app_id).await;
    assert!(trap_res.is_err(), "Faulty app must trap");
    assert!(matches!(
        trap_res.unwrap_err(),
        ApplicationError::TrapError { .. }
    ));

    // Faulty app state is CRASHED
    let persisted_trap = runtime
        .application_registry()
        .get(&trap_app_id)
        .unwrap()
        .unwrap();
    assert_eq!(persisted_trap.lifecycle_state, ApplicationState::Crashed);

    // INVARIANT: Node runtime is 100% operational and isolated
    let wat_healthy = r#"
    (module
        (func (export "main") (result i32)
            (i32.const 999)
        )
    )
    "#;

    let (pkg_ok, payloads_ok) = build_test_package(
        &publisher,
        "fieldnet.healthy.app",
        "1.0.0",
        wat_healthy,
        None,
    );
    let healthy_app_id = pkg_ok.manifest.application_id.clone();

    runtime
        .install_application(&pkg_ok, &payloads_ok)
        .await
        .unwrap();
    runtime.authorize_application(&healthy_app_id).unwrap();

    let healthy_report = runtime
        .run_application(&healthy_app_id)
        .await
        .expect("Runtime must remain fully healthy and execute other applications");

    assert_eq!(healthy_report.exit_code, Some(999));
    assert_eq!(healthy_report.final_state, ApplicationState::Stopped);

    runtime.shutdown();
}

#[tokio::test]
async fn test_fieldnet_runtime_execution_cancellation() {
    let temp = tempdir().unwrap();
    let data_dir = temp.path().to_path_buf();

    let config = RuntimeConfig {
        data_dir: data_dir.clone(),
        tcp_port: 0,
        discovery_port: 0,
        auto_connect: false,
        beacon_interval: Duration::from_millis(500),
        allowed_peers: None,
        blocked_peers: None,
    };

    let runtime = FieldnetRuntime::start(config)
        .await
        .expect("Runtime must start");
    let publisher = NodeIdentity::generate();

    let wat_infinite = r#"
    (module
        (func (export "main")
            (loop (br 0))
        )
    )
    "#;

    let limits = ResourceLimits {
        max_execution_time_ms: 10_000,
        max_cpu_fuel: u64::MAX / 2,
        ..Default::default()
    };

    let (pkg, payloads) = build_test_package(
        &publisher,
        "fieldnet.long.loop",
        "1.0.0",
        wat_infinite,
        Some(limits),
    );
    let app_id = pkg.manifest.application_id.clone();

    runtime.install_application(&pkg, &payloads).await.unwrap();
    runtime.authorize_application(&app_id).unwrap();

    let exec_id = ExecutionId::generate();
    let exec_id_clone = exec_id.clone();
    let runtime_clone = runtime.clone();
    let id_clone = app_id.clone();

    let task = tokio::spawn(async move {
        runtime_clone
            .run_application_with_id(&id_clone, exec_id_clone)
            .await
    });

    // Wait for execution to register
    tokio::time::sleep(Duration::from_millis(50)).await;

    let active_list = runtime.list_application_executions().await;
    assert!(
        active_list.iter().any(|info| info.execution_id == exec_id),
        "Active execution must be listed"
    );

    // Stop execution via runtime
    let stop_res = runtime.stop_application_execution(&exec_id).await;
    assert!(stop_res.is_ok(), "Stop signal must succeed");

    let join_res = task.await.unwrap();
    assert!(join_res.is_err(), "Cancelled task must return error");
    assert!(matches!(
        join_res.unwrap_err(),
        ApplicationError::ExecutionCancelled(_) | ApplicationError::ExecutionTimeout { .. }
    ));

    runtime.shutdown();
}

#[tokio::test]
async fn test_fieldnet_runtime_restart_reconciliation() {
    let temp = tempdir().unwrap();
    let data_dir = temp.path().to_path_buf();

    let config = RuntimeConfig {
        data_dir: data_dir.clone(),
        tcp_port: 0,
        discovery_port: 0,
        auto_connect: false,
        beacon_interval: Duration::from_millis(500),
        allowed_peers: None,
        blocked_peers: None,
    };

    let publisher = NodeIdentity::generate();
    let wat = r#"
    (module
        (func (export "main") (result i32)
            (i32.const 123)
        )
    )
    "#;

    let (pkg_a, payloads_a) =
        build_test_package(&publisher, "fieldnet.reconcile.a", "1.0.0", wat, None);
    let (pkg_b, payloads_b) =
        build_test_package(&publisher, "fieldnet.reconcile.b", "1.0.0", wat, None);

    let id_a = pkg_a.manifest.application_id.clone();
    let id_b = pkg_b.manifest.application_id.clone();

    // 1. Session 1: Install apps and simulate abrupt termination
    {
        let runtime = FieldnetRuntime::start(config.clone())
            .await
            .expect("Runtime 1 must start");
        runtime
            .install_application(&pkg_a, &payloads_a)
            .await
            .unwrap();
        runtime
            .install_application(&pkg_b, &payloads_b)
            .await
            .unwrap();

        runtime.authorize_application(&id_a).unwrap();
        runtime.authorize_application(&id_b).unwrap();

        // Simulate dangling states left behind
        runtime
            .application_registry()
            .update_lifecycle_state(&id_a, ApplicationState::Starting)
            .unwrap();
        runtime
            .application_registry()
            .update_lifecycle_state(&id_b, ApplicationState::Starting)
            .unwrap();
        runtime
            .application_registry()
            .update_lifecycle_state(&id_b, ApplicationState::Running)
            .unwrap();

        runtime.shutdown();
    }

    // 2. Session 2: Boot runtime on same data dir - startup reconciliation must execute
    {
        let rebooted_runtime = FieldnetRuntime::start(config)
            .await
            .expect("Runtime 2 must start");

        let rec_a = rebooted_runtime
            .application_registry()
            .get(&id_a)
            .unwrap()
            .expect("App A must exist");
        assert_eq!(
            rec_a.lifecycle_state,
            ApplicationState::Crashed,
            "Dangling STARTING state must reconcile to CRASHED on node boot"
        );

        let rec_b = rebooted_runtime
            .application_registry()
            .get(&id_b)
            .unwrap()
            .expect("App B must exist");
        assert_eq!(
            rec_b.lifecycle_state,
            ApplicationState::Stopped,
            "Dangling RUNNING state must reconcile to STOPPED on node boot"
        );

        // App B is in STOPPED state and can be executed again immediately
        let report = rebooted_runtime
            .run_application(&id_b)
            .await
            .expect("Reconciled STOPPED app must be executable");
        assert_eq!(report.exit_code, Some(123));
        assert_eq!(report.final_state, ApplicationState::Stopped);

        rebooted_runtime.shutdown();
    }
}
