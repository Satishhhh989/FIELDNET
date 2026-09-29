use async_trait::async_trait;
use fieldnet_content::hashing::hash_bytes;
use fieldnet_content::store::ContentStore;
use fieldnet_core::{NodeId, Timestamp};
use fieldnet_identity::NodeIdentity;
use fieldnet_service::model::{
    ServiceCapability, ServiceEndpoint, ServiceId, ServiceInstanceId, ServiceManifestBuilder,
    ServiceProtocol, ServiceVersion,
};
use fieldnet_service::registry::{RegistryConfig, ServiceRegistry};
use fieldnet_service::resolver::{ResolverConfig, ServiceResolver};
use fieldnet_wasm::error::ApplicationError;
use fieldnet_wasm::events::ApplicationEvent;
use fieldnet_wasm::execution::{ApplicationExecutionSupervisor, ModuleCache, WasmEngine};
use fieldnet_wasm::host::abi::FIELDNET_HOST_API_VERSION;
use fieldnet_wasm::host::errors::GuestErrorCode;
use fieldnet_wasm::host::network::{ApplicationNetworkMessage, ApplicationNetworkSender};
use fieldnet_wasm::id::{AppVersion, ApplicationId, PackageId, PublisherIdentity};
use fieldnet_wasm::installer::ApplicationInstaller;
use fieldnet_wasm::lifecycle::ApplicationState;
use fieldnet_wasm::limits::ResourceLimits;
use fieldnet_wasm::manifest::{ApplicationManifest, CURRENT_MANIFEST_SCHEMA_VERSION};
use fieldnet_wasm::package::{ApplicationPackage, PackageItem};
use fieldnet_wasm::permissions::AppPermission;
use fieldnet_wasm::registry::ApplicationRegistry;
use fieldnet_wasm::signature::PackageSignature;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::{broadcast, RwLock};

// =============================================================================
// RECORDING NETWORK SENDER (for testing network.send host calls)
// =============================================================================

#[derive(Default)]
struct RecordingNetworkSender {
    sent_messages: Arc<RwLock<Vec<ApplicationNetworkMessage>>>,
    send_count: AtomicUsize,
}

#[async_trait]
impl ApplicationNetworkSender for RecordingNetworkSender {
    async fn send_message(
        &self,
        msg: ApplicationNetworkMessage,
    ) -> Result<String, ApplicationError> {
        let count = self.send_count.fetch_add(1, Ordering::SeqCst);
        let msg_id = format!("msg-test-{}", count + 1);
        self.sent_messages.write().await.push(msg);
        Ok(msg_id)
    }
}

// =============================================================================
// TEST ENVIRONMENT HARNESS
// =============================================================================

struct HostTestHarness {
    supervisor: Arc<ApplicationExecutionSupervisor>,
    registry: Arc<ApplicationRegistry>,
    content_store: Arc<ContentStore>,
    installer: Arc<ApplicationInstaller>,
    _service_resolver: Arc<ServiceResolver>,
    network_sender: Arc<RecordingNetworkSender>,
    event_rx: broadcast::Receiver<ApplicationEvent>,
    local_node_id: NodeId,
    _temp_dir: tempfile::TempDir,
}

impl HostTestHarness {
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

        let local_identity = NodeIdentity::generate();
        let local_node_id = local_identity.node_id();

        // Configure Service Registry & Resolver
        let (svc_event_tx, _) = broadcast::channel(128);
        let svc_registry =
            ServiceRegistry::open_in_memory(RegistryConfig::default(), svc_event_tx.clone())
                .expect("Service registry opens");

        // Register a test service: "maps.service"
        let service_id = ServiceId::new("maps.service").unwrap();
        let instance_id = ServiceInstanceId::generate();
        let manifest =
            ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
                .instance_id(instance_id)
                .display_name("Maps Service")
                .endpoint(ServiceEndpoint::InternalRpc)
                .protocol(ServiceProtocol::NativeFieldnetRpc)
                .capability(ServiceCapability::new("maps.routing").unwrap())
                .build_and_sign(&local_identity)
                .unwrap();
        svc_registry.register_local(manifest).await.unwrap();

        let resolver_config = ResolverConfig::default();
        let service_resolver = Arc::new(ServiceResolver::new(
            svc_registry,
            None,
            Some(local_node_id),
            Some(svc_event_tx),
            resolver_config,
        ));

        let network_sender = Arc::new(RecordingNetworkSender::default());

        let engine = Arc::new(WasmEngine::new().unwrap());
        let cache = Arc::new(ModuleCache::new());
        let supervisor = Arc::new(
            ApplicationExecutionSupervisor::new(
                engine,
                cache,
                Arc::clone(&registry),
                Arc::clone(&content_store),
                app_event_tx,
            )
            .with_service_resolver(Arc::clone(&service_resolver))
            .with_network_sender(Arc::clone(&network_sender) as Arc<dyn ApplicationNetworkSender>)
            .with_node_id(local_node_id),
        );

        Self {
            supervisor,
            registry,
            content_store,
            installer,
            _service_resolver: service_resolver,
            network_sender,
            event_rx: app_event_rx,
            local_node_id,
            _temp_dir: temp_dir,
        }
    }

    async fn install_and_authorize(
        &self,
        app_id_str: &str,
        wat_source: &str,
        declared_permissions: Vec<AppPermission>,
        authorized_permissions: Vec<AppPermission>,
        limits: Option<ResourceLimits>,
    ) -> ApplicationId {
        let wasm_bytes = wat::parse_str(wat_source).expect("Valid WAT source");
        let identity = NodeIdentity::generate();
        let publisher = PublisherIdentity::from_public_key(identity.public_key_bytes());
        let app_id = ApplicationId::new(app_id_str).unwrap();
        let version: AppVersion = "1.0.0".parse().unwrap();

        let manifest = ApplicationManifest {
            schema_version: CURRENT_MANIFEST_SCHEMA_VERSION,
            application_id: app_id.clone(),
            name: format!("App {}", app_id),
            version: version.clone(),
            description: "Host test app".to_string(),
            publisher,
            entrypoint: "app.wasm".to_string(),
            permissions: declared_permissions,
            resource_limits: limits.unwrap_or_default(),
            dependencies: vec![],
            package_metadata: HashMap::new(),
        };

        let wasm_hash = hash_bytes(&wasm_bytes);
        let item = PackageItem::new(
            "app.wasm",
            wasm_hash,
            wasm_bytes.len() as u64,
            Some("application/wasm".to_string()),
        )
        .unwrap();

        let pkg_id = PackageId::new(format!("pkg:{}:1.0.0", app_id_str)).unwrap();
        let mut pkg =
            ApplicationPackage::new(pkg_id, manifest, vec![item], Timestamp::now(), None).unwrap();

        let digest = pkg.compute_package_hash();
        let sig = PackageSignature::sign_with_identity(&identity, &digest, Timestamp::now());
        pkg.signature = Some(sig);

        let mut blobs = HashMap::new();
        blobs.insert("app.wasm".to_string(), wasm_bytes);

        self.installer
            .install(&pkg, &blobs)
            .await
            .expect("Package installation succeeds");

        // Authorize with specified authorized_permissions
        self.registry
            .update_permissions(&app_id, authorized_permissions)
            .expect("Permissions update succeeds");
        self.registry
            .update_lifecycle_state(&app_id, ApplicationState::Authorized)
            .expect("Lifecycle authorization succeeds");

        app_id
    }
}

// =============================================================================
// TEST SUITE: 22+ WASM HOST API SECURITY AND INTEGRATION TESTS
// =============================================================================

/// 1. storage.read allowed: Wasm guest with storage.read capability checks if item exists in CAS.
#[tokio::test]
async fn test_01_storage_read_allowed() {
    let harness = HostTestHarness::new().await;

    // Put a dummy item in CAS
    let test_data = b"sample_read_content";
    let put_res = harness.content_store.put(test_data, None).await.unwrap();
    let cid_hex = put_res.content_id().to_hex();

    let wat = format!(
        r#"
        (module
          (import "fieldnet_v1" "storage_has" (func $has (param i32 i32) (result i32)))
          (memory (export "memory") 1)
          (data (i32.const 0) "{}")
          (func (export "main") (result i32)
            i32.const 0
            i32.const {}
            call $has
          )
        )
        "#,
        cid_hex,
        cid_hex.len()
    );

    let app_id = harness
        .install_and_authorize(
            "app.storage.reader",
            &wat,
            vec![AppPermission::StorageRead],
            vec![AppPermission::StorageRead],
            None,
        )
        .await;

    let report = harness.supervisor.execute(&app_id).await.unwrap();
    assert_eq!(
        report.exit_code,
        Some(1),
        "storage_has should return 1 (exists)"
    );
    assert_eq!(report.final_state, ApplicationState::Stopped);
}

/// 2. storage.read denied: Wasm guest without storage.read capability gets PERMISSION_DENIED (-2).
#[tokio::test]
async fn test_02_storage_read_denied() {
    let harness = HostTestHarness::new().await;

    let wat = r#"
        (module
          (import "fieldnet_v1" "storage_has" (func $has (param i32 i32) (result i32)))
          (memory (export "memory") 1)
          (data (i32.const 0) "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
          (func (export "main") (result i32)
            i32.const 0
            i32.const 64
            call $has
          )
        )
    "#;

    let app_id = harness
        .install_and_authorize(
            "app.unauth.reader",
            wat,
            vec![AppPermission::NetworkSend], // Declares only network.send, lacks storage.read
            vec![AppPermission::NetworkSend],
            None,
        )
        .await;

    let report = harness.supervisor.execute(&app_id).await.unwrap();
    assert_eq!(
        report.exit_code,
        Some(GuestErrorCode::PermissionDenied.as_i32()),
        "Should return PERMISSION_DENIED (-2)"
    );
}

/// 3. storage.write allowed: Wasm guest with storage.write stores data into CAS and receives CID length.
#[tokio::test]
async fn test_03_storage_write_allowed() {
    let harness = HostTestHarness::new().await;

    let wat = r#"
        (module
          (import "fieldnet_v1" "storage_put" (func $put (param i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 1)
          (data (i32.const 0) "hello from authorized wasm app")
          (func (export "main") (result i32)
            ;; data_ptr=0, data_len=30, out_cid_ptr=100, out_cid_max_len=128
            i32.const 0
            i32.const 30
            i32.const 100
            i32.const 128
            call $put
          )
        )
    "#;

    let app_id = harness
        .install_and_authorize(
            "app.storage.writer",
            wat,
            vec![AppPermission::StorageWrite],
            vec![AppPermission::StorageWrite],
            None,
        )
        .await;

    let report = harness.supervisor.execute(&app_id).await.unwrap();
    assert_eq!(
        report.exit_code,
        Some(64),
        "storage_put should write 64-char hex ContentId"
    );

    // Verify accounting in registry
    let usage = harness.registry.get_storage_usage(&app_id).unwrap();
    assert_eq!(usage, 30, "Persistent registry should record 30 bytes used");
}

/// 4. storage.write denied: Wasm guest declaring storage.read but not storage.write gets PERMISSION_DENIED (-2).
#[tokio::test]
async fn test_04_storage_write_denied() {
    let harness = HostTestHarness::new().await;

    let wat = r#"
        (module
          (import "fieldnet_v1" "storage_put" (func $put (param i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 1)
          (data (i32.const 0) "unauthorized write payload")
          (func (export "main") (result i32)
            i32.const 0
            i32.const 26
            i32.const 100
            i32.const 128
            call $put
          )
        )
    "#;

    let app_id = harness
        .install_and_authorize(
            "app.read.only",
            wat,
            vec![AppPermission::StorageRead], // Only storage.read
            vec![AppPermission::StorageRead],
            None,
        )
        .await;

    let report = harness.supervisor.execute(&app_id).await.unwrap();
    assert_eq!(
        report.exit_code,
        Some(GuestErrorCode::PermissionDenied.as_i32()),
        "Should return PERMISSION_DENIED (-2) on unpermitted write"
    );
}

/// 5. storage quota enforcement: Exceeding max_storage_bytes returns QUOTA_EXCEEDED (-5).
#[tokio::test]
async fn test_05_storage_quota_enforcement() {
    let harness = HostTestHarness::new().await;

    let limits = ResourceLimits {
        max_storage_bytes: 40, // Strict 40-byte storage quota
        ..Default::default()
    };

    let wat = r#"
        (module
          (import "fieldnet_v1" "storage_put" (func $put (param i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 1)
          ;; 50-byte string exceeding 40-byte quota
          (data (i32.const 0) "this payload is fifty bytes long and exceeds quota")
          (func (export "main") (result i32)
            i32.const 0
            i32.const 50
            i32.const 100
            i32.const 128
            call $put
          )
        )
    "#;

    let app_id = harness
        .install_and_authorize(
            "app.quota.test",
            wat,
            vec![AppPermission::StorageWrite],
            vec![AppPermission::StorageWrite],
            Some(limits),
        )
        .await;

    let report = harness.supervisor.execute(&app_id).await.unwrap();
    assert_eq!(
        report.exit_code,
        Some(GuestErrorCode::QuotaExceeded.as_i32()),
        "Should return QUOTA_EXCEEDED (-5)"
    );
}

/// 6. service.resolve allowed: Wasm guest with service.discover capability resolves "maps.service".
#[tokio::test]
async fn test_06_service_resolve_allowed() {
    let harness = HostTestHarness::new().await;

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

    let app_id = harness
        .install_and_authorize(
            "app.service.client",
            wat,
            vec![AppPermission::ServiceDiscover],
            vec![AppPermission::ServiceDiscover],
            None,
        )
        .await;

    let report = harness.supervisor.execute(&app_id).await.unwrap();
    assert!(
        report.exit_code.unwrap() > 0,
        "service_describe should return positive written JSON byte count"
    );
}

/// 7. service.resolve denied: Wasm guest without service.discover gets PERMISSION_DENIED (-2).
#[tokio::test]
async fn test_07_service_resolve_denied() {
    let harness = HostTestHarness::new().await;

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

    let app_id = harness
        .install_and_authorize(
            "app.service.denied",
            wat,
            vec![AppPermission::StorageRead], // No service.discover
            vec![AppPermission::StorageRead],
            None,
        )
        .await;

    let report = harness.supervisor.execute(&app_id).await.unwrap();
    assert_eq!(
        report.exit_code,
        Some(GuestErrorCode::PermissionDenied.as_i32()),
        "Should return PERMISSION_DENIED (-2)"
    );
}

/// 8. network.send allowed: Wasm guest with network.send transmits message to peer NodeId.
#[tokio::test]
async fn test_08_network_send_allowed() {
    let harness = HostTestHarness::new().await;
    let peer_node = NodeIdentity::generate().node_id();
    let peer_hex = peer_node.to_string();

    let wat = format!(
        r#"
        (module
          (import "fieldnet_v1" "network_send" (func $send (param i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 1)
          (data (i32.const 0) "{}")
          (data (i32.const 100) "ping from wasm mesh guest")
          (func (export "main") (result i32)
            i32.const 0
            i32.const {}
            i32.const 100
            i32.const 25
            call $send
          )
        )
        "#,
        peer_hex,
        peer_hex.len()
    );

    let app_id = harness
        .install_and_authorize(
            "app.mesh.sender",
            &wat,
            vec![AppPermission::NetworkSend],
            vec![AppPermission::NetworkSend],
            None,
        )
        .await;

    let report = harness.supervisor.execute(&app_id).await.unwrap();
    assert_eq!(
        report.exit_code,
        Some(0),
        "network_send should return 0 on success"
    );

    // Verify network message arrived in sender and identity is NOT spoofable
    let sent = harness.network_sender.sent_messages.read().await;
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].destination, peer_node);
    assert_eq!(sent[0].source_application, app_id);
    assert_eq!(sent[0].source_node, Some(harness.local_node_id));
    assert_eq!(sent[0].payload, b"ping from wasm mesh guest");
}

/// 9. network.send denied: Wasm guest without network.send gets PERMISSION_DENIED (-2).
#[tokio::test]
async fn test_09_network_send_denied() {
    let harness = HostTestHarness::new().await;
    let peer_hex = NodeIdentity::generate().node_id().to_string();

    let wat = format!(
        r#"
        (module
          (import "fieldnet_v1" "network_send" (func $send (param i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 1)
          (data (i32.const 0) "{}")
          (data (i32.const 100) "unauthorized packet")
          (func (export "main") (result i32)
            i32.const 0
            i32.const {}
            i32.const 100
            i32.const 19
            call $send
          )
        )
        "#,
        peer_hex,
        peer_hex.len()
    );

    let app_id = harness
        .install_and_authorize(
            "app.net.denied",
            &wat,
            vec![AppPermission::StorageWrite], // Lacks network.send
            vec![AppPermission::StorageWrite],
            None,
        )
        .await;

    let report = harness.supervisor.execute(&app_id).await.unwrap();
    assert_eq!(
        report.exit_code,
        Some(GuestErrorCode::PermissionDenied.as_i32()),
        "Should return PERMISSION_DENIED (-2)"
    );
}

/// 10. event.publish allowed: Wasm guest with events.publish emits custom event onto event bus.
#[tokio::test]
async fn test_10_event_publish_allowed() {
    let mut harness = HostTestHarness::new().await;

    let wat = r#"
        (module
          (import "fieldnet_v1" "event_publish" (func $pub (param i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 1)
          (data (i32.const 0) "heartbeat")
          (data (i32.const 50) "node-status-ok")
          (func (export "main") (result i32)
            i32.const 0
            i32.const 9
            i32.const 50
            i32.const 14
            call $pub
          )
        )
    "#;

    let app_id = harness
        .install_and_authorize(
            "app.event.publisher",
            wat,
            vec![AppPermission::EventsPublish],
            vec![AppPermission::EventsPublish],
            None,
        )
        .await;

    let report = harness.supervisor.execute(&app_id).await.unwrap();
    assert_eq!(
        report.exit_code,
        Some(0),
        "event_publish should return 0 on success"
    );

    // Verify canonical event received: "app.app.event.publisher.heartbeat"
    let mut received_custom = false;
    while let Ok(evt) = harness.event_rx.try_recv() {
        if let ApplicationEvent::Custom { topic, payload, .. } = evt {
            assert_eq!(topic, "app.app.event.publisher.heartbeat");
            assert_eq!(payload, b"node-status-ok");
            received_custom = true;
            break;
        }
    }
    assert!(
        received_custom,
        "Custom application event should be received on event bus"
    );
}

/// 11. event.publish denied: Wasm guest without events.publish gets PERMISSION_DENIED (-2).
#[tokio::test]
async fn test_11_event_publish_denied() {
    let harness = HostTestHarness::new().await;

    let wat = r#"
        (module
          (import "fieldnet_v1" "event_publish" (func $pub (param i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 1)
          (data (i32.const 0) "heartbeat")
          (data (i32.const 50) "payload")
          (func (export "main") (result i32)
            i32.const 0
            i32.const 9
            i32.const 50
            i32.const 7
            call $pub
          )
        )
    "#;

    let app_id = harness
        .install_and_authorize(
            "app.event.denied",
            wat,
            vec![AppPermission::StorageRead], // Lacks events.publish
            vec![AppPermission::StorageRead],
            None,
        )
        .await;

    let report = harness.supervisor.execute(&app_id).await.unwrap();
    assert_eq!(
        report.exit_code,
        Some(GuestErrorCode::PermissionDenied.as_i32()),
        "Should return PERMISSION_DENIED (-2)"
    );
}

/// 12. application identity cannot be spoofed: Attempting to publish into foreign or reserved event namespace is rejected.
#[tokio::test]
async fn test_12_application_identity_cannot_be_spoofed() {
    let harness = HostTestHarness::new().await;

    // Guest attempts to inject runtime lifecycle event "application.crashed"
    let wat_runtime_inject = r#"
        (module
          (import "fieldnet_v1" "event_publish" (func $pub (param i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 1)
          (data (i32.const 0) "application.crashed")
          (data (i32.const 50) "fake crash payload")
          (func (export "main") (result i32)
            i32.const 0
            i32.const 19
            i32.const 50
            i32.const 18
            call $pub
          )
        )
    "#;

    let app_id1 = harness
        .install_and_authorize(
            "app.spoof.attempt1",
            wat_runtime_inject,
            vec![AppPermission::EventsPublish],
            vec![AppPermission::EventsPublish],
            None,
        )
        .await;

    let report1 = harness.supervisor.execute(&app_id1).await.unwrap();
    assert_eq!(
        report1.exit_code,
        Some(GuestErrorCode::InvalidRequest.as_i32()),
        "Runtime-reserved namespace injection must be rejected with INVALID_REQUEST (-1)"
    );

    // Guest attempts to spoof another app's prefix: "app.victim_app.event"
    let wat_foreign_spoof = r#"
        (module
          (import "fieldnet_v1" "event_publish" (func $pub (param i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 1)
          (data (i32.const 0) "app.victim_app.event")
          (data (i32.const 50) "spoofed data")
          (func (export "main") (result i32)
            i32.const 0
            i32.const 20
            i32.const 50
            i32.const 12
            call $pub
          )
        )
    "#;

    let app_id2 = harness
        .install_and_authorize(
            "app.spoof.attempt2",
            wat_foreign_spoof,
            vec![AppPermission::EventsPublish],
            vec![AppPermission::EventsPublish],
            None,
        )
        .await;

    let report2 = harness.supervisor.execute(&app_id2).await.unwrap();
    assert_eq!(
        report2.exit_code,
        Some(GuestErrorCode::InvalidRequest.as_i32()),
        "Foreign application event namespace spoofing must be rejected with INVALID_REQUEST (-1)"
    );
}

/// 13. node identity cannot be accessed: Guest cannot retrieve private keys or NodeIdentity.
#[tokio::test]
async fn test_13_node_identity_cannot_be_accessed() {
    let harness = HostTestHarness::new().await;

    // Verify host ABI exports zero private key or node identity reflection functions
    let wat = r#"
        (module
          (import "fieldnet_v1" "get_node_private_key" (func $priv (result i32)))
          (func (export "main") (result i32)
            call $priv
          )
        )
    "#;

    let app_id = harness
        .install_and_authorize(
            "app.key.leaker",
            wat,
            vec![AppPermission::StorageRead],
            vec![AppPermission::StorageRead],
            None,
        )
        .await;

    let res = harness.supervisor.execute(&app_id).await;
    assert!(
        res.is_err(),
        "Module importing non-existent private key API must fail instantiation"
    );
}

/// 14. filesystem cannot be accessed: WASI and filesystem imports are rejected by Linker.
#[tokio::test]
async fn test_14_filesystem_cannot_be_accessed() {
    let harness = HostTestHarness::new().await;

    let wat = r#"
        (module
          (import "wasi_snapshot_preview1" "fd_write" (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (func (export "main") (result i32)
            i32.const 1
            i32.const 0
            i32.const 0
            i32.const 0
            call $fd_write
          )
        )
    "#;

    let app_id = harness
        .install_and_authorize(
            "app.wasi.fs",
            wat,
            vec![AppPermission::StorageRead],
            vec![AppPermission::StorageRead],
            None,
        )
        .await;

    let res = harness.supervisor.execute(&app_id).await;
    assert!(
        res.is_err(),
        "WASI filesystem imports must be strictly rejected"
    );
}

/// 15. raw socket access unavailable: Direct socket syscalls cannot be linked.
#[tokio::test]
async fn test_15_raw_socket_access_unavailable() {
    let harness = HostTestHarness::new().await;

    let wat = r#"
        (module
          (import "env" "socket" (func $socket (param i32 i32 i32) (result i32)))
          (func (export "main") (result i32)
            i32.const 2
            i32.const 1
            i32.const 0
            call $socket
          )
        )
    "#;

    let app_id = harness
        .install_and_authorize(
            "app.raw.socket",
            wat,
            vec![AppPermission::NetworkSend],
            vec![AppPermission::NetworkSend],
            None,
        )
        .await;

    let res = harness.supervisor.execute(&app_id).await;
    assert!(res.is_err(), "Raw socket imports must be strictly rejected");
}

/// 16. oversized host request rejected: Request exceeding MAX_REQUEST_PAYLOAD_BYTES returns INVALID_REQUEST (-1).
#[tokio::test]
async fn test_16_oversized_host_request_rejected() {
    let harness = HostTestHarness::new().await;

    let wat = r#"
        (module
          (import "fieldnet_v1" "storage_put" (func $put (param i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 1)
          (func (export "main") (result i32)
            ;; Requesting 20,000,000 bytes (> 10 MiB limit)
            i32.const 0
            i32.const 20000000
            i32.const 100
            i32.const 128
            call $put
          )
        )
    "#;

    let app_id = harness
        .install_and_authorize(
            "app.oversized",
            wat,
            vec![AppPermission::StorageWrite],
            vec![AppPermission::StorageWrite],
            None,
        )
        .await;

    let report = harness.supervisor.execute(&app_id).await.unwrap();
    assert_eq!(
        report.exit_code,
        Some(GuestErrorCode::InvalidRequest.as_i32()),
        "Oversized request must be rejected with INVALID_REQUEST (-1)"
    );
}

/// 17. malformed host request rejected: Malformed JSON or negative pointers return INVALID_REQUEST (-1).
#[tokio::test]
async fn test_17_malformed_host_request_rejected() {
    let harness = HostTestHarness::new().await;

    // Guest calls unified host_call with invalid JSON for ServiceResolve (opcode 5)
    let wat = r#"
        (module
          (import "fieldnet_v1" "host_call" (func $call (param i32 i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 1)
          (data (i32.const 0) "{ not valid json !! }")
          (func (export "main") (result i32)
            ;; op_code=5 (ServiceResolve), req_ptr=0, req_len=20, resp_ptr=100, resp_max_len=512
            i32.const 5
            i32.const 0
            i32.const 20
            i32.const 100
            i32.const 512
            call $call
          )
        )
    "#;

    let app_id = harness
        .install_and_authorize(
            "app.malformed",
            wat,
            vec![AppPermission::ServiceDiscover],
            vec![AppPermission::ServiceDiscover],
            None,
        )
        .await;

    let report = harness.supervisor.execute(&app_id).await.unwrap();
    assert_eq!(
        report.exit_code,
        Some(GuestErrorCode::InvalidRequest.as_i32()),
        "Malformed request JSON must return INVALID_REQUEST (-1)"
    );
}

/// 18. invalid content ID rejected: Non-hex or invalid length ContentId returns INVALID_ID (-4).
#[tokio::test]
async fn test_18_invalid_content_id_rejected() {
    let harness = HostTestHarness::new().await;

    let wat = r#"
        (module
          (import "fieldnet_v1" "storage_get" (func $get (param i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 1)
          (data (i32.const 0) "this_is_not_hex_nor_a_valid_blake3_hash!")
          (func (export "main") (result i32)
            i32.const 0
            i32.const 40
            i32.const 100
            i32.const 512
            call $get
          )
        )
    "#;

    let app_id = harness
        .install_and_authorize(
            "app.invalid.cid",
            wat,
            vec![AppPermission::StorageRead],
            vec![AppPermission::StorageRead],
            None,
        )
        .await;

    let report = harness.supervisor.execute(&app_id).await.unwrap();
    assert_eq!(
        report.exit_code,
        Some(GuestErrorCode::InvalidId.as_i32()),
        "Malformed ContentId must return INVALID_ID (-4)"
    );
}

/// 19. invalid service ID rejected: Illegal characters or malformed ServiceId returns INVALID_ID (-4).
#[tokio::test]
async fn test_19_invalid_service_id_rejected() {
    let harness = HostTestHarness::new().await;

    let wat = r#"
        (module
          (import "fieldnet_v1" "service_describe" (func $describe (param i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 1)
          (data (i32.const 0) "invalid:service:name")
          (func (export "main") (result i32)
            i32.const 0
            i32.const 20
            i32.const 100
            i32.const 512
            call $describe
          )
        )
    "#;

    let app_id = harness
        .install_and_authorize(
            "app.invalid.sid",
            wat,
            vec![AppPermission::ServiceDiscover],
            vec![AppPermission::ServiceDiscover],
            None,
        )
        .await;

    let report = harness.supervisor.execute(&app_id).await.unwrap();
    assert_eq!(
        report.exit_code,
        Some(GuestErrorCode::InvalidId.as_i32()),
        "Invalid ServiceId containing colons must return INVALID_ID (-4)"
    );
}

/// 20. runtime survives malicious host calls: Malicious pointers and invalid opcodes execute safely.
#[tokio::test]
async fn test_20_runtime_survives_malicious_host_calls() {
    let harness = HostTestHarness::new().await;

    let wat = r#"
        (module
          (import "fieldnet_v1" "host_call" (func $call (param i32 i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 1)
          (func (export "main") (result i32)
            ;; 1. Out-of-bounds negative pointers
            i32.const 1
            i32.const -1
            i32.const -100
            i32.const -5
            i32.const -10
            call $call
            drop

            ;; 2. Huge pointer past memory bounds
            i32.const 1
            i32.const 99999999
            i32.const 1000
            i32.const 99999999
            i32.const 1000
            call $call
            drop

            ;; 3. Unknown opcode
            i32.const 9999
            i32.const 0
            i32.const 0
            i32.const 0
            i32.const 0
            call $call
            drop

            i32.const 777
          )
        )
    "#;

    let app_id = harness
        .install_and_authorize(
            "app.malicious.caller",
            wat,
            vec![AppPermission::StorageWrite],
            vec![AppPermission::StorageWrite],
            None,
        )
        .await;

    let report = harness.supervisor.execute(&app_id).await.unwrap();
    assert_eq!(
        report.exit_code,
        Some(777),
        "Runtime must cleanly survive malicious host calls"
    );
    assert_eq!(report.final_state, ApplicationState::Stopped);
}

/// 21. host errors do not crash FIELDNET: Errors are cleanly mapped to guest return codes.
#[tokio::test]
async fn test_21_host_errors_do_not_crash_fieldnet() {
    let harness = HostTestHarness::new().await;

    let wat = r#"
        (module
          (import "fieldnet_v1" "storage_has" (func $has (param i32 i32) (result i32)))
          (memory (export "memory") 1)
          (data (i32.const 0) "not-hex")
          (func (export "main") (result i32)
            i32.const 0
            i32.const 7
            call $has
          )
        )
    "#;

    let app_id = harness
        .install_and_authorize(
            "app.error.isolation",
            wat,
            vec![AppPermission::StorageRead],
            vec![AppPermission::StorageRead],
            None,
        )
        .await;

    let report = harness.supervisor.execute(&app_id).await.unwrap();
    assert_eq!(
        report.exit_code,
        Some(GuestErrorCode::InvalidId.as_i32()),
        "Host error is cleanly reported to guest as return code without crashing supervisor"
    );
    assert_eq!(report.final_state, ApplicationState::Stopped);
}

/// 22. unauthorized host operation cannot bypass capability checks: Granular permissions are strictly isolated.
#[tokio::test]
async fn test_22_unauthorized_host_operation_cannot_bypass_capability_checks() {
    let harness = HostTestHarness::new().await;

    // Declares network.send only; attempts to call storage_put
    let wat = r#"
        (module
          (import "fieldnet_v1" "storage_put" (func $put (param i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 1)
          (data (i32.const 0) "data")
          (func (export "main") (result i32)
            i32.const 0
            i32.const 4
            i32.const 100
            i32.const 128
            call $put
          )
        )
    "#;

    let app_id = harness
        .install_and_authorize(
            "app.capability.separation",
            wat,
            vec![AppPermission::NetworkSend],
            vec![AppPermission::NetworkSend],
            None,
        )
        .await;

    let report = harness.supervisor.execute(&app_id).await.unwrap();
    assert_eq!(
        report.exit_code,
        Some(GuestErrorCode::PermissionDenied.as_i32()),
        "network.send permission must NOT confer storage.write authorization"
    );
}

/// 23. End-to-end test: storage.put -> CAS -> storage.get -> data matches.
#[tokio::test]
async fn test_23_end_to_end_storage_put_and_get() {
    let harness = HostTestHarness::new().await;

    let wat = r#"
        (module
          (import "fieldnet_v1" "storage_put" (func $put (param i32 i32 i32 i32) (result i32)))
          (import "fieldnet_v1" "storage_get" (func $get (param i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 1)
          (data (i32.const 0) "e2e_secret_payload_999")
          (func (export "main") (result i32)
            ;; 1. Store payload: data_ptr=0, data_len=22, out_cid_ptr=100, max_len=128
            i32.const 0
            i32.const 22
            i32.const 100
            i32.const 128
            call $put
            drop

            ;; 2. Read back stored payload: cid_ptr=100, cid_len=64, out_data_ptr=300, max_len=128
            i32.const 100
            i32.const 64
            i32.const 300
            i32.const 128
            call $get
          )
        )
    "#;

    let app_id = harness
        .install_and_authorize(
            "app.storage.e2e",
            wat,
            vec![AppPermission::StorageRead, AppPermission::StorageWrite],
            vec![AppPermission::StorageRead, AppPermission::StorageWrite],
            None,
        )
        .await;

    let report = harness.supervisor.execute(&app_id).await.unwrap();
    assert_eq!(
        report.exit_code,
        Some(22),
        "storage_get should read back exactly 22 bytes stored by storage_put"
    );
}

/// 24. host API version discovery returns FIELDNET_HOST_API_VERSION (1).
#[tokio::test]
async fn test_24_host_api_version_discovery() {
    let harness = HostTestHarness::new().await;

    let wat = r#"
        (module
          (import "fieldnet_v1" "host_api_version" (func $ver (result i32)))
          (func (export "main") (result i32)
            call $ver
          )
        )
    "#;

    let app_id = harness
        .install_and_authorize("app.version.discoverer", wat, vec![], vec![], None)
        .await;

    let report = harness.supervisor.execute(&app_id).await.unwrap();
    assert_eq!(
        report.exit_code,
        Some(FIELDNET_HOST_API_VERSION),
        "host_api_version should return 1"
    );
}
