use fieldnet_content::hashing::hash_bytes;
use fieldnet_core::Timestamp;
use fieldnet_identity::NodeIdentity;
use fieldnet_runtime::app::{
    AppPermission, AppVersion, ApplicationEvent, ApplicationId, ApplicationManifest,
    ApplicationPackage, ApplicationState, PackageId, PackageItem, PackageSignature,
    PublisherIdentity, ResourceLimits, SignatureStatus, CURRENT_MANIFEST_SCHEMA_VERSION,
};
use fieldnet_runtime::{FieldnetRuntime, RuntimeConfig};
use std::collections::HashMap;
use std::time::Duration;
use tempfile::tempdir;

fn create_signed_package(
    publisher_identity: &NodeIdentity,
    app_id_str: &str,
    version_str: &str,
    wasm_bytes: &[u8],
) -> (ApplicationPackage, HashMap<String, Vec<u8>>) {
    let publisher = PublisherIdentity::from_public_key(publisher_identity.public_key_bytes());
    let app_id = ApplicationId::new(app_id_str).unwrap();
    let version: AppVersion = version_str.parse().unwrap();

    let manifest = ApplicationManifest {
        schema_version: CURRENT_MANIFEST_SCHEMA_VERSION,
        application_id: app_id.clone(),
        name: "FIELDNET Secure Chat".to_string(),
        version,
        description: "Decentralized mesh chat application".to_string(),
        publisher,
        entrypoint: "app.wasm".to_string(),
        permissions: vec![
            AppPermission::StorageRead,
            AppPermission::StorageWrite,
            AppPermission::NetworkSend,
        ],
        resource_limits: ResourceLimits::default(),
        dependencies: vec![],
        package_metadata: Default::default(),
    };

    let wasm_hash = hash_bytes(wasm_bytes);
    let item_wasm = PackageItem::new(
        "app.wasm",
        wasm_hash,
        wasm_bytes.len() as u64,
        Some("application/wasm".to_string()),
    )
    .unwrap();

    let icon_bytes = b"PNG_ICON_TEST_ASSET";
    let icon_hash = hash_bytes(icon_bytes);
    let item_icon = PackageItem::new(
        "assets/icon.png",
        icon_hash,
        icon_bytes.len() as u64,
        Some("image/png".to_string()),
    )
    .unwrap();

    let mut pkg = ApplicationPackage::new(
        PackageId::new(format!("pkg:{}:{}", app_id_str, version_str)).unwrap(),
        manifest,
        vec![item_wasm, item_icon],
        Timestamp::now(),
        None,
    )
    .unwrap();

    let pkg_hash = pkg.compute_package_hash();
    let sig = PackageSignature::sign_with_identity(publisher_identity, &pkg_hash, Timestamp::now());
    pkg.signature = Some(sig);

    let mut payloads = HashMap::new();
    payloads.insert("app.wasm".to_string(), wasm_bytes.to_vec());
    payloads.insert("assets/icon.png".to_string(), icon_bytes.to_vec());

    (pkg, payloads)
}

#[tokio::test]
async fn test_phase09_full_application_lifecycle_and_package_system() {
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

    // =========================================================================
    // 1. Start FIELDNET Runtime Node
    // =========================================================================
    let runtime = FieldnetRuntime::start(config.clone())
        .await
        .expect("Runtime start");

    let mut app_event_rx = runtime.subscribe_application_events();

    // Verify initial application registry is empty
    let initial_apps = runtime.application_registry().list().unwrap();
    assert!(initial_apps.is_empty(), "Initial registry should be empty");

    // =========================================================================
    // 2. Build and Sign Application Package v1.0.0
    // =========================================================================
    let publisher = NodeIdentity::generate();
    let (pkg_v1, payloads_v1) = create_signed_package(
        &publisher,
        "fieldnet.secure.chat",
        "1.0.0",
        b"\0asm\x01\x00\x00\x00chat_binary_v1",
    );

    // Verify package integrity and signature prior to installation
    let computed_hash = pkg_v1.verify_integrity(Some(&payloads_v1)).unwrap();
    assert_eq!(computed_hash, pkg_v1.compute_package_hash());
    assert!(pkg_v1.verify_signature().is_ok());

    // =========================================================================
    // 3. Execute Atomic Installation Pipeline
    // =========================================================================
    let record = runtime
        .install_application(&pkg_v1, &payloads_v1)
        .await
        .expect("Installation must succeed");

    assert_eq!(record.application_id.as_str(), "fieldnet.secure.chat");
    assert_eq!(record.installed_version, AppVersion::new(1, 0, 0));
    assert_eq!(record.signature_status, SignatureStatus::Verified);
    assert_eq!(record.lifecycle_state, ApplicationState::Installed);

    // Verify events were emitted
    let mut collected_events = Vec::new();
    while let Ok(ev) = app_event_rx.try_recv() {
        collected_events.push(ev);
    }
    assert_eq!(collected_events.len(), 4);
    assert!(matches!(
        collected_events[0],
        ApplicationEvent::Discovered { .. }
    ));
    assert!(matches!(
        collected_events[1],
        ApplicationEvent::Verified { .. }
    ));
    assert!(matches!(
        collected_events[2],
        ApplicationEvent::Installing { .. }
    ));
    assert!(matches!(
        collected_events[3],
        ApplicationEvent::Installed { .. }
    ));

    // Verify items pinned in ContentStore
    let entrypoint_item = pkg_v1.entrypoint_item().unwrap();
    let cas_bytes = runtime
        .content_store()
        .get_verified(&entrypoint_item.content_id)
        .await
        .expect("Entrypoint must be stored in CAS");
    assert_eq!(cas_bytes, b"\0asm\x01\x00\x00\x00chat_binary_v1");

    // =========================================================================
    // 4. Lifecycle State Machine Validations
    // =========================================================================
    let app_id = record.application_id.clone();

    // Installed -> Authorized
    let state = runtime
        .application_registry()
        .update_lifecycle_state(&app_id, ApplicationState::Authorized)
        .unwrap();
    assert_eq!(state, ApplicationState::Authorized);

    // Authorized -> Running invalid (must go through Starting)
    assert!(runtime
        .application_registry()
        .update_lifecycle_state(&app_id, ApplicationState::Running)
        .is_err());

    // Authorized -> Starting -> Running -> Stopped
    runtime
        .application_registry()
        .update_lifecycle_state(&app_id, ApplicationState::Starting)
        .unwrap();
    runtime
        .application_registry()
        .update_lifecycle_state(&app_id, ApplicationState::Running)
        .unwrap();
    runtime
        .application_registry()
        .update_lifecycle_state(&app_id, ApplicationState::Stopped)
        .unwrap();

    // =========================================================================
    // 5. Upgrade Pipeline (v1.0.0 -> v1.1.0) with History Retention
    // =========================================================================
    let (pkg_v2, payloads_v2) = create_signed_package(
        &publisher,
        "fieldnet.secure.chat",
        "1.1.0",
        b"\0asm\x01\x00\x00\x00chat_binary_v2_enhanced",
    );

    let upgraded = runtime
        .install_application(&pkg_v2, &payloads_v2)
        .await
        .expect("Upgrade to v1.1.0 must succeed");

    assert_eq!(upgraded.installed_version, AppVersion::new(1, 1, 0));

    // Verify rollback history contains v1.0.0
    let history = runtime.application_registry().get_history(&app_id).unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].version, AppVersion::new(1, 0, 0));

    // =========================================================================
    // 6. Rollback Pipeline (v1.1.0 -> v1.0.0)
    // =========================================================================
    let rolled_back = runtime
        .rollback_application(&app_id)
        .await
        .expect("Rollback to v1.0.0 must succeed");

    assert_eq!(rolled_back.installed_version, AppVersion::new(1, 0, 0));
    let current = runtime
        .application_registry()
        .get(&app_id)
        .unwrap()
        .expect("App in registry");
    assert_eq!(current.installed_version, AppVersion::new(1, 0, 0));

    // Verify history consumed
    let remaining_history = runtime.application_registry().get_history(&app_id).unwrap();
    assert_eq!(remaining_history.len(), 0);

    // =========================================================================
    // 7. Restart Persistence Verification
    // =========================================================================
    runtime.shutdown();

    // Reopen runtime with identical configuration
    let rebooted_runtime = FieldnetRuntime::start(config)
        .await
        .expect("Reboot runtime start");

    let apps = rebooted_runtime
        .application_registry()
        .list()
        .expect("List apps after reboot");
    assert_eq!(apps.len(), 1);
    assert_eq!(apps[0].application_id.as_str(), "fieldnet.secure.chat");
    assert_eq!(apps[0].installed_version, AppVersion::new(1, 0, 0));
    assert_eq!(apps[0].signature_status, SignatureStatus::Verified);

    rebooted_runtime.shutdown();
}

#[tokio::test]
async fn test_phase09_security_failures_and_closed_fail_posture() {
    let temp = tempdir().unwrap();
    let data_dir = temp.path().to_path_buf();

    let config = RuntimeConfig {
        data_dir,
        tcp_port: 0,
        discovery_port: 0,
        auto_connect: false,
        beacon_interval: Duration::from_millis(500),
        allowed_peers: None,
        blocked_peers: None,
    };

    let runtime = FieldnetRuntime::start(config).await.expect("Runtime start");
    let publisher = NodeIdentity::generate();

    // 1. Corrupted package payload is rejected and not registered
    let (pkg_corrupt, mut payloads_corrupt) = create_signed_package(
        &publisher,
        "fieldnet.bad.corrupt",
        "1.0.0",
        b"\0asm\x01\x00\x00\x00authentic",
    );
    payloads_corrupt.insert("app.wasm".to_string(), b"tampered_payload".to_vec());

    assert!(runtime
        .install_application(&pkg_corrupt, &payloads_corrupt)
        .await
        .is_err());
    assert!(runtime
        .application_registry()
        .get(&pkg_corrupt.manifest.application_id)
        .unwrap()
        .is_none());

    // 2. Tampered signature is rejected and not registered
    let (mut pkg_tampered_sig, payloads_valid) = create_signed_package(
        &publisher,
        "fieldnet.bad.signature",
        "1.0.0",
        b"\0asm\x01\x00\x00\x00binary",
    );
    if let Some(sig) = &mut pkg_tampered_sig.signature {
        sig.signature_bytes[10] ^= 0xAA;
    }

    assert!(runtime
        .install_application(&pkg_tampered_sig, &payloads_valid)
        .await
        .is_err());
    assert!(runtime
        .application_registry()
        .get(&pkg_tampered_sig.manifest.application_id)
        .unwrap()
        .is_none());

    // 3. Forged publisher signature (signed by key B, but declared as key A) is rejected
    let foreign_publisher = NodeIdentity::generate();
    let (mut pkg_forged, payloads_forged) = create_signed_package(
        &publisher,
        "fieldnet.bad.forgery",
        "1.0.0",
        b"\0asm\x01\x00\x00\x00binary",
    );
    let hash = pkg_forged.compute_package_hash();
    let forged_sig =
        PackageSignature::sign_with_identity(&foreign_publisher, &hash, Timestamp::now());
    pkg_forged.signature = Some(forged_sig);

    assert!(runtime
        .install_application(&pkg_forged, &payloads_forged)
        .await
        .is_err());
    assert!(runtime
        .application_registry()
        .get(&pkg_forged.manifest.application_id)
        .unwrap()
        .is_none());

    runtime.shutdown();
}
