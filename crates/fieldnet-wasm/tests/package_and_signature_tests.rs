use fieldnet_content::hashing::hash_bytes;
use fieldnet_content::store::ContentStore;
use fieldnet_core::Timestamp;
use fieldnet_identity::NodeIdentity;
use std::collections::HashMap;
use tempfile::tempdir;

use fieldnet_wasm::cas::CasPackageStorage;
use fieldnet_wasm::id::{AppVersion, ApplicationId, PackageId, PublisherIdentity};
use fieldnet_wasm::limits::ResourceLimits;
use fieldnet_wasm::manifest::{ApplicationManifest, CURRENT_MANIFEST_SCHEMA_VERSION};
use fieldnet_wasm::package::{ApplicationPackage, PackageItem};
use fieldnet_wasm::permissions::AppPermission;
use fieldnet_wasm::signature::PackageSignature;

fn sample_manifest(identity: &NodeIdentity) -> ApplicationManifest {
    let publisher = PublisherIdentity::from_public_key(identity.public_key_bytes());
    ApplicationManifest {
        schema_version: CURRENT_MANIFEST_SCHEMA_VERSION,
        application_id: ApplicationId::new("fieldnet.calc").unwrap(),
        name: "Calculator".to_string(),
        version: AppVersion::new(1, 0, 0),
        description: "A fast sandboxed calculator app".to_string(),
        publisher,
        entrypoint: "app.wasm".to_string(),
        permissions: vec![AppPermission::StorageRead],
        resource_limits: ResourceLimits::default(),
        dependencies: vec![],
        package_metadata: Default::default(),
    }
}

#[test]
fn test_package_hash_determinism_and_item_sorting() {
    let identity = NodeIdentity::generate();
    let manifest = sample_manifest(&identity);

    let wasm_bytes = b"\0asm\x01\x00\x00\x00calc";
    let icon_bytes = b"PNG_ICON_BYTES";

    let wasm_hash = hash_bytes(wasm_bytes);
    let icon_hash = hash_bytes(icon_bytes);

    let item_wasm = PackageItem::new(
        "app.wasm",
        wasm_hash,
        wasm_bytes.len() as u64,
        Some("application/wasm".to_string()),
    )
    .unwrap();
    let item_icon = PackageItem::new(
        "assets/icon.png",
        icon_hash,
        icon_bytes.len() as u64,
        Some("image/png".to_string()),
    )
    .unwrap();

    // Package 1: items added in order [wasm, icon]
    let pkg1 = ApplicationPackage::new(
        PackageId::new("pkg:fieldnet.calc:1.0.0").unwrap(),
        manifest.clone(),
        vec![item_wasm.clone(), item_icon.clone()],
        Timestamp::from_millis(1000),
        None,
    )
    .unwrap();

    // Package 2: items added in reverse order [icon, wasm]
    let pkg2 = ApplicationPackage::new(
        PackageId::new("pkg:fieldnet.calc:1.0.0").unwrap(),
        manifest.clone(),
        vec![item_icon.clone(), item_wasm.clone()],
        Timestamp::from_millis(1000),
        None,
    )
    .unwrap();

    // Hash must be strictly identical regardless of input insertion order
    assert_eq!(pkg1.compute_package_hash(), pkg2.compute_package_hash());
}

#[test]
fn test_package_integrity_verification() {
    let identity = NodeIdentity::generate();
    let manifest = sample_manifest(&identity);

    let wasm_bytes = b"\0asm\x01\x00\x00\x00valid_binary".to_vec();
    let wasm_hash = hash_bytes(&wasm_bytes);

    let item_wasm = PackageItem::new(
        "app.wasm",
        wasm_hash,
        wasm_bytes.len() as u64,
        Some("application/wasm".to_string()),
    )
    .unwrap();

    let pkg = ApplicationPackage::new(
        PackageId::new("pkg:fieldnet.calc:1.0.0").unwrap(),
        manifest,
        vec![item_wasm],
        Timestamp::now(),
        None,
    )
    .unwrap();

    let mut payloads = HashMap::new();
    payloads.insert("app.wasm".to_string(), wasm_bytes.clone());

    // Valid payloads pass verification
    assert!(pkg.verify_integrity(Some(&payloads)).is_ok());

    // Corrupted payload fails verification
    let mut corrupt_payloads = HashMap::new();
    corrupt_payloads.insert(
        "app.wasm".to_string(),
        b"\0asm\x01\x00\x00\x00tampered".to_vec(),
    );
    assert!(pkg.verify_integrity(Some(&corrupt_payloads)).is_err());

    // Missing payload fails verification
    let empty_payloads = HashMap::new();
    assert!(pkg.verify_integrity(Some(&empty_payloads)).is_err());
}

#[test]
fn test_missing_entrypoint_rejected() {
    let identity = NodeIdentity::generate();
    let manifest = sample_manifest(&identity);

    let other_bytes = b"other file";
    let other_hash = hash_bytes(other_bytes);

    let item_other =
        PackageItem::new("other.txt", other_hash, other_bytes.len() as u64, None).unwrap();

    // Package missing "app.wasm" must fail construction
    let res = ApplicationPackage::new(
        PackageId::new("pkg:fieldnet.calc:1.0.0").unwrap(),
        manifest,
        vec![item_other],
        Timestamp::now(),
        None,
    );

    assert!(res.is_err());
}

#[test]
fn test_duplicate_item_paths_rejected() {
    let identity = NodeIdentity::generate();
    let manifest = sample_manifest(&identity);

    let wasm_bytes = b"\0asm\x01\x00\x00\x00valid";
    let wasm_hash = hash_bytes(wasm_bytes);

    let item_wasm = PackageItem::new("app.wasm", wasm_hash, wasm_bytes.len() as u64, None).unwrap();

    let res = ApplicationPackage::new(
        PackageId::new("pkg:fieldnet.calc:1.0.0").unwrap(),
        manifest,
        vec![item_wasm.clone(), item_wasm.clone()],
        Timestamp::now(),
        None,
    );

    assert!(res.is_err());
}

#[test]
fn test_package_signing_and_signature_verification() {
    let identity = NodeIdentity::generate();
    let manifest = sample_manifest(&identity);

    let wasm_bytes = b"\0asm\x01\x00\x00\x00bin";
    let wasm_hash = hash_bytes(wasm_bytes);

    let item_wasm = PackageItem::new("app.wasm", wasm_hash, wasm_bytes.len() as u64, None).unwrap();

    let mut pkg = ApplicationPackage::new(
        PackageId::new("pkg:fieldnet.calc:1.0.0").unwrap(),
        manifest,
        vec![item_wasm],
        Timestamp::from_millis(5000),
        None,
    )
    .unwrap();

    let pkg_hash = pkg.compute_package_hash();

    // 1. Sign package with publisher identity
    let signature =
        PackageSignature::sign_with_identity(&identity, &pkg_hash, Timestamp::from_millis(5100));
    assert!(signature.verify().is_ok());

    pkg.signature = Some(signature);

    // 2. Package signature verification passes
    assert!(pkg.verify_signature().is_ok());

    // 3. Forged signature from another key fails verification
    let foreign_identity = NodeIdentity::generate();
    let forged_sig = PackageSignature::sign_with_identity(
        &foreign_identity,
        &pkg_hash,
        Timestamp::from_millis(5100),
    );
    pkg.signature = Some(forged_sig);
    assert!(pkg.verify_signature().is_err()); // Publisher in sig doesn't match manifest publisher

    // 4. Tampered signature bytes fail verification
    let mut tampered_sig =
        PackageSignature::sign_with_identity(&identity, &pkg_hash, Timestamp::from_millis(5100));
    tampered_sig.signature_bytes[0] ^= 0xFF;
    pkg.signature = Some(tampered_sig);
    assert!(pkg.verify_signature().is_err());
}

#[tokio::test]
async fn test_cas_package_storage_and_retrieval() {
    let temp = tempdir().unwrap();
    let store = ContentStore::open(temp.path()).unwrap();

    let identity = NodeIdentity::generate();
    let manifest = sample_manifest(&identity);

    let wasm_bytes = b"\0asm\x01\x00\x00\x00wasm_cas_test".to_vec();
    let wasm_hash = hash_bytes(&wasm_bytes);

    let item_wasm = PackageItem::new(
        "app.wasm",
        wasm_hash,
        wasm_bytes.len() as u64,
        Some("application/wasm".to_string()),
    )
    .unwrap();

    let mut payloads = HashMap::new();
    payloads.insert("app.wasm".to_string(), wasm_bytes.clone());

    let mut pkg = ApplicationPackage::new(
        PackageId::new("pkg:fieldnet.calc:1.0.0").unwrap(),
        manifest,
        vec![item_wasm],
        Timestamp::now(),
        None,
    )
    .unwrap();

    let pkg_hash = pkg.compute_package_hash();
    let sig = PackageSignature::sign_with_identity(&identity, &pkg_hash, Timestamp::now());
    pkg.signature = Some(sig);

    // Store entire package into CAS
    let root_cid = CasPackageStorage::store_package(&store, &pkg, &payloads)
        .await
        .expect("CAS store package");

    // Load package back from CAS by root_cid
    let loaded_pkg = CasPackageStorage::load_package(&store, &root_cid)
        .await
        .expect("CAS load package");

    assert_eq!(pkg.package_id, loaded_pkg.package_id);
    assert_eq!(
        pkg.compute_package_hash(),
        loaded_pkg.compute_package_hash()
    );

    // Load item payloads back from CAS
    let loaded_payloads = CasPackageStorage::load_package_payloads(&store, &loaded_pkg)
        .await
        .expect("CAS load payloads");

    assert_eq!(loaded_payloads.get("app.wasm").unwrap(), &wasm_bytes);
}
