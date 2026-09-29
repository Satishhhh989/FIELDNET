use fieldnet_content::hashing::hash_bytes;
use fieldnet_content::store::ContentStore;
use fieldnet_core::Timestamp;
use fieldnet_identity::NodeIdentity;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::broadcast;

use fieldnet_wasm::events::ApplicationEvent;
use fieldnet_wasm::id::{AppVersion, ApplicationId, PackageId, PublisherIdentity};
use fieldnet_wasm::installer::ApplicationInstaller;
use fieldnet_wasm::limits::ResourceLimits;
use fieldnet_wasm::manifest::{ApplicationManifest, CURRENT_MANIFEST_SCHEMA_VERSION};
use fieldnet_wasm::package::{ApplicationPackage, PackageItem};
use fieldnet_wasm::permissions::AppPermission;
use fieldnet_wasm::registry::ApplicationRegistry;
use fieldnet_wasm::signature::PackageSignature;

fn build_package(
    identity: &NodeIdentity,
    app_id_str: &str,
    version_str: &str,
    wasm_content: &[u8],
) -> (ApplicationPackage, HashMap<String, Vec<u8>>) {
    let publisher = PublisherIdentity::from_public_key(identity.public_key_bytes());
    let app_id = ApplicationId::new(app_id_str).unwrap();
    let version: AppVersion = version_str.parse().unwrap();

    let manifest = ApplicationManifest {
        schema_version: CURRENT_MANIFEST_SCHEMA_VERSION,
        application_id: app_id.clone(),
        name: "Test App".to_string(),
        version,
        description: "Test".to_string(),
        publisher,
        entrypoint: "app.wasm".to_string(),
        permissions: vec![AppPermission::StorageRead],
        resource_limits: ResourceLimits::default(),
        dependencies: vec![],
        package_metadata: Default::default(),
    };

    let wasm_hash = hash_bytes(wasm_content);
    let item_wasm = PackageItem::new(
        "app.wasm",
        wasm_hash,
        wasm_content.len() as u64,
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
    payloads.insert("app.wasm".to_string(), wasm_content.to_vec());

    (pkg, payloads)
}

#[tokio::test]
async fn test_installation_pipeline_happy_path() {
    let temp = tempdir().unwrap();
    let registry = Arc::new(ApplicationRegistry::new_in_memory().unwrap());
    let content_store = ContentStore::open(temp.path()).unwrap();
    let (event_tx, mut event_rx) = broadcast::channel(32);

    let installer = ApplicationInstaller::new(
        Arc::clone(&registry),
        Some(Arc::clone(&content_store)),
        event_tx,
    );

    let identity = NodeIdentity::generate();
    let (pkg, payloads) = build_package(
        &identity,
        "fieldnet.chat",
        "1.0.0",
        b"\0asm\x01\x00\x00\x00chat_code",
    );

    let record = installer
        .install(&pkg, &payloads)
        .await
        .expect("Installation must succeed");

    assert_eq!(record.application_id.as_str(), "fieldnet.chat");
    assert_eq!(record.installed_version, AppVersion::new(1, 0, 0));

    // Verify events were emitted in proper chronological sequence
    let ev1 = event_rx.recv().await.unwrap();
    assert!(matches!(ev1, ApplicationEvent::Discovered { .. }));

    let ev2 = event_rx.recv().await.unwrap();
    assert!(matches!(ev2, ApplicationEvent::Verified { .. }));

    let ev3 = event_rx.recv().await.unwrap();
    assert!(matches!(ev3, ApplicationEvent::Installing { .. }));

    let ev4 = event_rx.recv().await.unwrap();
    assert!(matches!(ev4, ApplicationEvent::Installed { .. }));

    // Verify stored in registry
    let fetched = registry
        .get(&record.application_id)
        .unwrap()
        .expect("In registry");
    assert_eq!(fetched.installed_version, AppVersion::new(1, 0, 0));

    // Verify wasm payload stored in CAS
    let wasm_item = pkg.entrypoint_item().unwrap();
    let cas_bytes = content_store
        .get_verified(&wasm_item.content_id)
        .await
        .expect("In CAS");
    assert_eq!(cas_bytes, b"\0asm\x01\x00\x00\x00chat_code");
}

#[tokio::test]
async fn test_installation_atomicity_on_integrity_failure() {
    let registry = Arc::new(ApplicationRegistry::new_in_memory().unwrap());
    let (event_tx, _) = broadcast::channel(32);

    let installer = ApplicationInstaller::new(Arc::clone(&registry), None, event_tx);

    let identity = NodeIdentity::generate();
    let (pkg, mut payloads) = build_package(
        &identity,
        "fieldnet.chat",
        "1.0.0",
        b"\0asm\x01\x00\x00\x00good",
    );

    // Corrupt payload bytes
    payloads.insert("app.wasm".to_string(), b"corrupted".to_vec());

    let res = installer.install(&pkg, &payloads).await;
    assert!(res.is_err(), "Corrupt package installation must fail");

    // Must NOT be registered in catalog
    assert!(registry
        .get(&pkg.manifest.application_id)
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn test_installation_atomicity_on_signature_failure() {
    let registry = Arc::new(ApplicationRegistry::new_in_memory().unwrap());
    let (event_tx, _) = broadcast::channel(32);

    let installer = ApplicationInstaller::new(Arc::clone(&registry), None, event_tx);

    let identity = NodeIdentity::generate();
    let (mut pkg, payloads) = build_package(
        &identity,
        "fieldnet.chat",
        "1.0.0",
        b"\0asm\x01\x00\x00\x00good",
    );

    // Tamper with signature
    if let Some(sig) = &mut pkg.signature {
        sig.signature_bytes[0] ^= 0xFF;
    }

    let res = installer.install(&pkg, &payloads).await;
    assert!(res.is_err(), "Invalid signature must fail installation");

    // Must NOT be registered in catalog
    assert!(registry
        .get(&pkg.manifest.application_id)
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn test_duplicate_installation_and_upgrade_pipeline() {
    let registry = Arc::new(ApplicationRegistry::new_in_memory().unwrap());
    let (event_tx, mut event_rx) = broadcast::channel(32);

    let installer = ApplicationInstaller::new(Arc::clone(&registry), None, event_tx);

    let identity = NodeIdentity::generate();
    let (pkg_v1, payloads_v1) = build_package(
        &identity,
        "fieldnet.sensor",
        "1.0.0",
        b"\0asm\x01\x00\x00\x00v1",
    );
    let (pkg_v2, payloads_v2) = build_package(
        &identity,
        "fieldnet.sensor",
        "1.1.0",
        b"\0asm\x01\x00\x00\x00v2",
    );
    let (pkg_v0, payloads_v0) = build_package(
        &identity,
        "fieldnet.sensor",
        "0.9.0",
        b"\0asm\x01\x00\x00\x00v0",
    );

    // 1. Install v1.0.0
    installer.install(&pkg_v1, &payloads_v1).await.unwrap();

    // 2. Attempt duplicate install of v1.0.0 -> Rejected
    assert!(installer.install(&pkg_v1, &payloads_v1).await.is_err());

    // 3. Attempt downgrade to v0.9.0 -> Rejected
    assert!(installer.install(&pkg_v0, &payloads_v0).await.is_err());

    // 4. Upgrade to v1.1.0 -> Accepted
    let upgraded = installer
        .install(&pkg_v2, &payloads_v2)
        .await
        .expect("Upgrade must succeed");
    assert_eq!(upgraded.installed_version, AppVersion::new(1, 1, 0));

    // Verify history preserved for rollback
    let history = registry
        .get_history(&pkg_v1.manifest.application_id)
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].version, AppVersion::new(1, 0, 0));

    // Drain events and verify Updated event was emitted
    let mut found_updated = false;
    while let Ok(ev) = event_rx.try_recv() {
        if matches!(ev, ApplicationEvent::Updated { .. }) {
            found_updated = true;
        }
    }
    assert!(found_updated);

    // 5. Execute rollback to v1.0.0
    let restored = installer
        .rollback(&pkg_v1.manifest.application_id)
        .await
        .expect("Rollback must succeed");
    assert_eq!(restored.installed_version, AppVersion::new(1, 0, 0));
    assert_eq!(
        registry
            .get(&pkg_v1.manifest.application_id)
            .unwrap()
            .unwrap()
            .installed_version,
        AppVersion::new(1, 0, 0)
    );
}
