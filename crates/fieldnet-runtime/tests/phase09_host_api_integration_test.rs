use fieldnet_content::hashing::hash_bytes;
use fieldnet_core::Timestamp;
use fieldnet_identity::NodeIdentity;
use fieldnet_runtime::app::{
    AppPermission, AppVersion, ApplicationManifest, ApplicationPackage, GuestErrorCode, PackageId,
    PackageItem, PackageSignature, PublisherIdentity, ResourceLimits,
    CURRENT_MANIFEST_SCHEMA_VERSION,
};
use fieldnet_runtime::service::{
    ServiceCapability, ServiceEndpoint, ServiceId, ServiceInstanceId, ServiceManifestBuilder,
    ServiceProtocol, ServiceVersion,
};
use fieldnet_runtime::{FieldnetRuntime, RuntimeConfig};
use std::collections::HashMap;
use std::time::Duration;
use tempfile::tempdir;

fn build_package(
    publisher_identity: &NodeIdentity,
    app_id_str: &str,
    version_str: &str,
    wat_source: &str,
    permissions: Vec<AppPermission>,
    limits: Option<ResourceLimits>,
) -> (ApplicationPackage, HashMap<String, Vec<u8>>) {
    let wasm_bytes = wat::parse_str(wat_source).expect("Failed to parse WAT fixture");
    let publisher = PublisherIdentity::from_public_key(publisher_identity.public_key_bytes());
    let app_id = app_id_str.parse().unwrap();
    let version: AppVersion = version_str.parse().unwrap();

    let manifest = ApplicationManifest {
        schema_version: CURRENT_MANIFEST_SCHEMA_VERSION,
        application_id: app_id,
        name: format!("Host API App {}", app_id_str),
        version,
        description: "Integration test for Host Functions API".to_string(),
        publisher,
        entrypoint: "main.wasm".to_string(),
        permissions,
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
async fn test_runtime_host_api_storage_e2e() {
    let temp = tempdir().unwrap();
    let config = RuntimeConfig {
        data_dir: temp.path().to_path_buf(),
        tcp_port: 0,
        discovery_port: 0,
        auto_connect: false,
        beacon_interval: Duration::from_millis(500),
        allowed_peers: None,
        blocked_peers: None,
    };

    let runtime = FieldnetRuntime::start(config)
        .await
        .expect("Runtime starts");
    let publisher = NodeIdentity::generate();

    // WAT Guest: Stores 18 bytes "runtime_cas_test_1" -> gets back 18 bytes -> returns length
    let wat = r#"
    (module
        (import "fieldnet_v1" "storage_put" (func $put (param i32 i32 i32 i32) (result i32)))
        (import "fieldnet_v1" "storage_get" (func $get (param i32 i32 i32 i32) (result i32)))
        (memory (export "memory") 1)
        (data (i32.const 0) "runtime_cas_test_1")
        (func (export "main") (result i32)
            ;; Put: data_ptr=0, len=18, cid_ptr=100, max_len=128
            i32.const 0
            i32.const 18
            i32.const 100
            i32.const 128
            call $put
            drop

            ;; Get: cid_ptr=100, cid_len=64, out_ptr=200, max_len=128
            i32.const 100
            i32.const 64
            i32.const 200
            i32.const 128
            call $get
        )
    )
    "#;

    let perms = vec![AppPermission::StorageRead, AppPermission::StorageWrite];
    let (pkg, payloads) = build_package(
        &publisher,
        "test.storage.e2e",
        "1.0.0",
        wat,
        perms.clone(),
        None,
    );
    let app_id = pkg.manifest.application_id.clone();

    runtime.install_application(&pkg, &payloads).await.unwrap();
    runtime.authorize_application(&app_id).unwrap();

    let report = runtime.run_application(&app_id).await.unwrap();
    assert_eq!(
        report.exit_code,
        Some(18),
        "Storage put and get should match 18 bytes"
    );

    let usage = runtime
        .application_registry()
        .get_storage_usage(&app_id)
        .unwrap();
    assert_eq!(usage, 18, "Persistent quota accounting records 18 bytes");

    runtime.shutdown();
}

#[tokio::test]
async fn test_runtime_host_api_service_resolution_e2e() {
    let temp = tempdir().unwrap();
    let config = RuntimeConfig {
        data_dir: temp.path().to_path_buf(),
        tcp_port: 0,
        discovery_port: 0,
        auto_connect: false,
        beacon_interval: Duration::from_millis(500),
        allowed_peers: None,
        blocked_peers: None,
    };

    let runtime = FieldnetRuntime::start(config)
        .await
        .expect("Runtime starts");

    // Register a local service: "maps.service"
    let svc_id = ServiceId::new("maps.service").unwrap();
    let instance_id = ServiceInstanceId::generate();
    let manifest = ServiceManifestBuilder::new(svc_id.clone(), ServiceVersion::new(1, 0, 0))
        .instance_id(instance_id)
        .display_name("Runtime Maps Service")
        .endpoint(ServiceEndpoint::InternalRpc)
        .protocol(ServiceProtocol::NativeFieldnetRpc)
        .capability(ServiceCapability::new("routing").unwrap())
        .build_and_sign(&runtime.identity())
        .unwrap();

    runtime
        .service_registry()
        .register_local(manifest)
        .await
        .unwrap();

    let publisher = NodeIdentity::generate();
    let wat = r#"
    (module
        (import "fieldnet_v1" "service_describe" (func $describe (param i32 i32 i32 i32) (result i32)))
        (memory (export "memory") 1)
        (data (i32.const 0) "maps.service")
        (func (export "main") (result i32)
            i32.const 0
            i32.const 12
            i32.const 100
            i32.const 1024
            call $describe
        )
    )
    "#;

    let perms = vec![AppPermission::ServiceDiscover];
    let (pkg, payloads) = build_package(&publisher, "test.service.e2e", "1.0.0", wat, perms, None);
    let app_id = pkg.manifest.application_id.clone();

    runtime.install_application(&pkg, &payloads).await.unwrap();
    runtime.authorize_application(&app_id).unwrap();

    let report = runtime.run_application(&app_id).await.unwrap();
    assert!(
        report.exit_code.unwrap() > 0,
        "Service describe should return positive written bytes"
    );

    runtime.shutdown();
}

#[tokio::test]
async fn test_runtime_host_api_network_send_e2e() {
    let temp = tempdir().unwrap();
    let config = RuntimeConfig {
        data_dir: temp.path().to_path_buf(),
        tcp_port: 0,
        discovery_port: 0,
        auto_connect: false,
        beacon_interval: Duration::from_millis(500),
        allowed_peers: None,
        blocked_peers: None,
    };

    let runtime = FieldnetRuntime::start(config)
        .await
        .expect("Runtime starts");
    let dest_node = NodeIdentity::generate().node_id();
    let dest_str = dest_node.to_string();

    let publisher = NodeIdentity::generate();
    let wat = format!(
        r#"
        (module
            (import "fieldnet_v1" "network_send" (func $send (param i32 i32 i32 i32) (result i32)))
            (memory (export "memory") 1)
            (data (i32.const 0) "{}")
            (data (i32.const 100) "e2e runtime mesh message")
            (func (export "main") (result i32)
                i32.const 0
                i32.const {}
                i32.const 100
                i32.const 24
                call $send
            )
        )
        "#,
        dest_str,
        dest_str.len()
    );

    let perms = vec![AppPermission::NetworkSend];
    let (pkg, payloads) = build_package(&publisher, "test.network.e2e", "1.0.0", &wat, perms, None);
    let app_id = pkg.manifest.application_id.clone();

    runtime.install_application(&pkg, &payloads).await.unwrap();
    runtime.authorize_application(&app_id).unwrap();

    let report = runtime.run_application(&app_id).await.unwrap();
    assert_eq!(
        report.exit_code,
        Some(0),
        "Network send should return 0 (success)"
    );

    // Verify message was queued in DTN persistent queue
    let count = runtime.dtn_queue().count().unwrap();
    assert!(
        count >= 1,
        "Message should be durably queued for destination peer in DTN"
    );

    runtime.shutdown();
}

#[tokio::test]
async fn test_runtime_capability_separation_e2e() {
    let temp = tempdir().unwrap();
    let config = RuntimeConfig {
        data_dir: temp.path().to_path_buf(),
        tcp_port: 0,
        discovery_port: 0,
        auto_connect: false,
        beacon_interval: Duration::from_millis(500),
        allowed_peers: None,
        blocked_peers: None,
    };

    let runtime = FieldnetRuntime::start(config)
        .await
        .expect("Runtime starts");
    let publisher = NodeIdentity::generate();

    // App declaring only network.send attempts storage_put
    let wat = r#"
    (module
        (import "fieldnet_v1" "storage_put" (func $put (param i32 i32 i32 i32) (result i32)))
        (memory (export "memory") 1)
        (data (i32.const 0) "unauthorized write")
        (func (export "main") (result i32)
            i32.const 0
            i32.const 18
            i32.const 100
            i32.const 128
            call $put
        )
    )
    "#;

    let perms = vec![AppPermission::NetworkSend]; // Declares ONLY network.send
    let (pkg, payloads) =
        build_package(&publisher, "test.separation.e2e", "1.0.0", wat, perms, None);
    let app_id = pkg.manifest.application_id.clone();

    runtime.install_application(&pkg, &payloads).await.unwrap();
    runtime.authorize_application(&app_id).unwrap();

    let report = runtime.run_application(&app_id).await.unwrap();
    assert_eq!(
        report.exit_code,
        Some(GuestErrorCode::PermissionDenied.as_i32()),
        "network.send cannot authorize storage.write"
    );

    runtime.shutdown();
}
