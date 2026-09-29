use std::str::FromStr;

use fieldnet_identity::NodeIdentity;
use fieldnet_wasm::dependencies::{AppDependency, DependencyType};
use fieldnet_wasm::id::{AppVersion, AppVersionReq, ApplicationId, PublisherIdentity};
use fieldnet_wasm::limits::ResourceLimits;
use fieldnet_wasm::manifest::{ApplicationManifest, CURRENT_MANIFEST_SCHEMA_VERSION};
use fieldnet_wasm::permissions::AppPermission;

#[test]
fn test_valid_application_id() {
    let valid_cases = [
        "fieldnet.chat",
        "com.acme.sensor-app",
        "org.mesh.relay_node.v1",
        "app.sub1.sub2",
        "a1.b2.c3",
    ];

    for case in valid_cases {
        let app_id = ApplicationId::new(case);
        assert!(app_id.is_ok(), "Expected '{}' to be valid", case);
        assert_eq!(app_id.unwrap().as_str(), case.to_ascii_lowercase());
    }
}

#[test]
fn test_invalid_application_id_rejection() {
    let invalid_cases = [
        "",                     // empty
        "ab",                   // too short (< 3 chars)
        "singleword",           // no dot separator
        ".leading.dot",         // leading dot
        "trailing.dot.",        // trailing dot
        "double..dot",          // consecutive dots
        "app/path/traversal",   // slashes
        "app\\windows\\path",   // backslashes
        "app:port:8080",        // colons
        "app with spaces",      // whitespace
        "app.-invalid-seg.pkg", // segment starts with hyphen
        "app.invalid-_seg.pkg", // segment ends with underscore
    ];

    for case in invalid_cases {
        let res = ApplicationId::new(case);
        assert!(res.is_err(), "Expected '{}' to be rejected", case);
    }
}

#[test]
fn test_publisher_identity() {
    let identity = NodeIdentity::generate();
    let pub_id = PublisherIdentity::from_public_key(identity.public_key_bytes());

    let hex_str = pub_id.to_hex();
    assert_eq!(hex_str.len(), 64);

    let parsed = PublisherIdentity::from_hex(&hex_str).expect("Valid hex parse");
    assert_eq!(pub_id, parsed);

    let formatted = pub_id.to_string();
    assert!(formatted.starts_with("pub:"));

    let parsed_formatted =
        PublisherIdentity::from_str(&formatted).expect("Valid pub: prefix parse");
    assert_eq!(pub_id, parsed_formatted);

    // Invalid publisher hex
    assert!(PublisherIdentity::from_hex("invalid-hex").is_err());
    assert!(PublisherIdentity::from_hex("1234").is_err());
}

#[test]
fn test_app_version_and_semver_ordering() {
    let v1 = AppVersion::new(1, 0, 0);
    let v2 = AppVersion::new(1, 1, 0);
    let v3 = AppVersion::new(2, 0, 0);
    let v1_alpha = AppVersion::with_prerelease(1, 0, 0, "alpha.1");

    assert!(v1 < v2);
    assert!(v2 < v3);
    assert!(v1_alpha < v1);

    let parsed: AppVersion = "1.2.3".parse().expect("Valid version string");
    assert_eq!(parsed, AppVersion::new(1, 2, 3));

    let parsed_pre: AppVersion = "2.0.1-rc.2".parse().expect("Valid prerelease");
    assert_eq!(parsed_pre, AppVersion::with_prerelease(2, 0, 1, "rc.2"));

    assert!("invalid".parse::<AppVersion>().is_err());
    assert!("1.2".parse::<AppVersion>().is_err());
    assert!("1.2.a".parse::<AppVersion>().is_err());
}

#[test]
fn test_version_requirement_matching() {
    let exact: AppVersionReq = "=1.2.0".parse().unwrap();
    assert!(exact.matches(&AppVersion::new(1, 2, 0)));
    assert!(!exact.matches(&AppVersion::new(1, 2, 1)));

    let at_least: AppVersionReq = ">=1.2.0".parse().unwrap();
    assert!(at_least.matches(&AppVersion::new(1, 2, 0)));
    assert!(at_least.matches(&AppVersion::new(1, 3, 0)));
    assert!(!at_least.matches(&AppVersion::new(1, 1, 9)));

    let compatible: AppVersionReq = "^1.2.0".parse().unwrap();
    assert!(compatible.matches(&AppVersion::new(1, 2, 0)));
    assert!(compatible.matches(&AppVersion::new(1, 3, 5)));
    assert!(!compatible.matches(&AppVersion::new(2, 0, 0)));
    assert!(!compatible.matches(&AppVersion::new(1, 1, 0)));

    let any: AppVersionReq = "*".parse().unwrap();
    assert!(any.matches(&AppVersion::new(99, 99, 99)));
}

#[test]
fn test_manifest_serialization_roundtrip() {
    let identity = NodeIdentity::generate();
    let publisher = PublisherIdentity::from_public_key(identity.public_key_bytes());
    let app_id = ApplicationId::new("fieldnet.test.app").unwrap();

    let manifest = ApplicationManifest {
        schema_version: CURRENT_MANIFEST_SCHEMA_VERSION,
        application_id: app_id,
        name: "Test Application".to_string(),
        version: AppVersion::new(1, 0, 0),
        description: "A test app for FIELDNET".to_string(),
        publisher,
        entrypoint: "app.wasm".to_string(),
        permissions: vec![AppPermission::StorageRead, AppPermission::NetworkSend],
        resource_limits: ResourceLimits::default(),
        dependencies: vec![AppDependency::new(
            "files.service",
            ">=1.0.0".parse().unwrap(),
            DependencyType::Service,
            false,
        )
        .unwrap()],
        package_metadata: Default::default(),
    };

    let json = manifest.to_json_pretty().expect("Manifest serialize");
    let deserialized = ApplicationManifest::from_json_str(&json).expect("Manifest deserialize");

    assert_eq!(manifest, deserialized);
}

#[test]
fn test_manifest_validation_rules() {
    let identity = NodeIdentity::generate();
    let publisher = PublisherIdentity::from_public_key(identity.public_key_bytes());
    let app_id = ApplicationId::new("fieldnet.test.app").unwrap();

    // Valid base
    let mut manifest = ApplicationManifest {
        schema_version: CURRENT_MANIFEST_SCHEMA_VERSION,
        application_id: app_id.clone(),
        name: "Valid App".to_string(),
        version: AppVersion::new(1, 0, 0),
        description: "Valid".to_string(),
        publisher,
        entrypoint: "app.wasm".to_string(),
        permissions: vec![AppPermission::StorageRead],
        resource_limits: ResourceLimits::default(),
        dependencies: vec![],
        package_metadata: Default::default(),
    };
    assert!(manifest.validate().is_ok());

    // Schema version 0 rejected
    manifest.schema_version = 0;
    assert!(manifest.validate().is_err());
    manifest.schema_version = CURRENT_MANIFEST_SCHEMA_VERSION;

    // Empty name rejected
    manifest.name = "   ".to_string();
    assert!(manifest.validate().is_err());
    manifest.name = "Valid App".to_string();

    // Empty entrypoint rejected
    manifest.entrypoint = "".to_string();
    assert!(manifest.validate().is_err());

    // Path traversal in entrypoint rejected
    manifest.entrypoint = "../app.wasm".to_string();
    assert!(manifest.validate().is_err());
    manifest.entrypoint = "bin/app.wasm".to_string();
    assert!(manifest.validate().is_err());

    // Non-wasm entrypoint rejected
    manifest.entrypoint = "app.exe".to_string();
    assert!(manifest.validate().is_err());
    manifest.entrypoint = "app.wasm".to_string();

    // Duplicate permissions rejected
    manifest.permissions = vec![AppPermission::StorageRead, AppPermission::StorageRead];
    assert!(manifest.validate().is_err());
    manifest.permissions = vec![AppPermission::StorageRead];

    // Duplicate dependencies rejected
    manifest.dependencies = vec![
        AppDependency::new(
            "service.a",
            "*".parse().unwrap(),
            DependencyType::Service,
            false,
        )
        .unwrap(),
        AppDependency::new(
            "service.a",
            ">=1.0.0".parse().unwrap(),
            DependencyType::Service,
            false,
        )
        .unwrap(),
    ];
    assert!(manifest.validate().is_err());
}

#[test]
fn test_permission_model_and_strict_validation() {
    let standard = AppPermission::StorageRead;
    assert!(standard.is_standard());
    assert_eq!(standard.as_str(), "storage.read");
    assert!(standard.validate_strict().is_ok());

    let custom = AppPermission::Custom("org.custom.perm".to_string());
    assert!(!custom.is_standard());
    assert!(custom.validate_strict().is_err());

    let parsed: AppPermission = "content.read".parse().unwrap();
    assert_eq!(parsed, AppPermission::ContentRead);

    // Invalid syntax for custom permission
    assert!("invalidformat".parse::<AppPermission>().is_err()); // No dot
    assert!("bad perm".parse::<AppPermission>().is_err()); // Space
}

#[test]
fn test_resource_limits_bounds_validation() {
    let valid = ResourceLimits::default();
    assert!(valid.validate().is_ok());

    let invalid_mem = ResourceLimits {
        max_memory_bytes: 0,
        ..Default::default()
    };
    assert!(invalid_mem.validate().is_err());

    let invalid_time = ResourceLimits {
        max_execution_time_ms: 0,
        ..Default::default()
    };
    assert!(invalid_time.validate().is_err());

    let invalid_concurrency = ResourceLimits {
        max_concurrency: 0,
        ..Default::default()
    };
    assert!(invalid_concurrency.validate().is_err());

    let excessive_concurrency = ResourceLimits {
        max_concurrency: 1000,
        ..Default::default()
    };
    assert!(excessive_concurrency.validate().is_err());
}
