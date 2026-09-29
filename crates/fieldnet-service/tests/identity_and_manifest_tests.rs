use fieldnet_core::{NodeId, Timestamp};
use fieldnet_identity::NodeIdentity;
use fieldnet_service::{
    ServiceCapability, ServiceEndpoint, ServiceId, ServiceInstanceId, ServiceManifestBuilder,
    ServicePermissions, ServiceProtocol, ServiceVersion, CURRENT_SERVICE_MANIFEST_SCHEMA_VERSION,
};
use std::str::FromStr;

#[test]
fn test_valid_service_id() {
    let valid_cases = [
        "files.service",
        "search.service",
        "maps.service",
        "ai.service",
        "fieldnet.search.v1",
        "my-org.telemetry_service",
        "alpha.beta.gamma.delta",
    ];

    for case in valid_cases {
        let id = ServiceId::new(case).expect("Should parse valid ServiceId");
        assert_eq!(id.as_str(), case);
        assert_eq!(id.to_string(), case);

        let parsed = ServiceId::from_str(case).expect("FromStr should succeed");
        assert_eq!(id, parsed);
    }
}

#[test]
fn test_invalid_service_id() {
    let invalid_cases = [
        "",                 // empty
        "a",                // too short
        "single",           // only one segment
        "files..service",   // consecutive dots
        ".files.service",   // leading dot
        "files.service.",   // trailing dot
        "-files.service",   // segment starting with hyphen
        "files-.service",   // segment ending with hyphen
        "_files.service",   // segment starting with underscore
        "files_.service",   // segment ending with underscore
        "files service",    // contains whitespace
        "files:service",    // contains colon
        "192.168.1.1:8080", // IP address
        "files/service",    // contains slash
        "FILES.SERVICE",    // uppercase (must be normalized or rejected)
    ];

    for case in invalid_cases {
        let result = ServiceId::new(case);
        if case == "FILES.SERVICE" {
            // "FILES.SERVICE" normalizes to "files.service"
            assert!(result.is_ok());
            assert_eq!(result.unwrap().as_str(), "files.service");
        } else {
            assert!(result.is_err(), "Expected error for case '{}'", case);
        }
    }
}

#[test]
fn test_service_instance_id() {
    let id1 = ServiceInstanceId::generate();
    assert_eq!(id1.as_str().len(), 32);

    let id2 = ServiceInstanceId::generate();
    assert_ne!(id1, id2);

    let valid_custom = ServiceInstanceId::new("inst-12345678").unwrap();
    assert_eq!(valid_custom.as_str(), "inst-12345678");

    // Invalid instance IDs
    assert!(ServiceInstanceId::new("").is_err());
    assert!(ServiceInstanceId::new("short").is_err()); // < 8 chars
    assert!(ServiceInstanceId::new("invalid!chars@").is_err());
}

#[test]
fn test_service_version_and_semver_compatibility() {
    let v1_2_3 = ServiceVersion::parse("1.2.3").unwrap();
    assert_eq!(v1_2_3.major, 1);
    assert_eq!(v1_2_3.minor, 2);
    assert_eq!(v1_2_3.patch, 3);
    assert_eq!(v1_2_3.to_string(), "1.2.3");

    let with_prefix = ServiceVersion::parse("v2.0.1").unwrap();
    assert_eq!(with_prefix.major, 2);

    // Incompatible versions
    assert!(ServiceVersion::parse("1.2").is_err());
    assert!(ServiceVersion::parse("invalid").is_err());

    // Caret compatibility tests (major > 0)
    let v1_0_0 = ServiceVersion::new(1, 0, 0);
    let v1_2_0 = ServiceVersion::new(1, 2, 0);
    let v1_2_5 = ServiceVersion::new(1, 2, 5);
    let v2_0_0 = ServiceVersion::new(2, 0, 0);

    assert!(v1_2_5.is_compatible_with(&v1_2_0));
    assert!(v1_2_0.is_compatible_with(&v1_0_0));
    assert!(!v1_2_0.is_compatible_with(&v1_2_5)); // older patch
    assert!(!v2_0_0.is_compatible_with(&v1_2_0)); // different major

    // Caret compatibility tests (major == 0)
    let v0_1_2 = ServiceVersion::new(0, 1, 2);
    let v0_1_5 = ServiceVersion::new(0, 1, 5);
    let v0_2_0 = ServiceVersion::new(0, 2, 0);

    assert!(v0_1_5.is_compatible_with(&v0_1_2));
    assert!(!v0_2_0.is_compatible_with(&v0_1_2)); // 0.x breaking change on minor
}

#[test]
fn test_service_capability_and_permissions() {
    let cap = ServiceCapability::new("files.read").unwrap();
    assert_eq!(cap.as_str(), "files.read");

    assert!(ServiceCapability::new("").is_err());
    assert!(ServiceCapability::new("invalid cap with spaces").is_err());

    let default_perms = ServicePermissions::default();
    assert!(default_perms.is_public);
    assert!(!default_perms.requires_capability_token);

    let token_perms = ServicePermissions::requires_token();
    assert!(!token_perms.is_public);
    assert!(token_perms.requires_capability_token);

    let role_perms = ServicePermissions::with_roles(vec!["admin".to_string()]);
    assert!(!role_perms.is_public);
    assert_eq!(role_perms.required_roles, vec!["admin".to_string()]);
}

#[test]
fn test_service_manifest_signing_and_verification() {
    let identity = NodeIdentity::generate();
    let service_id = ServiceId::new("search.service").unwrap();
    let version = ServiceVersion::new(1, 0, 0);

    let manifest = ServiceManifestBuilder::new(service_id.clone(), version)
        .display_name("Global Mesh Search")
        .description("Distributed search across documents")
        .capability(ServiceCapability::new("search.query").unwrap())
        .endpoint(ServiceEndpoint::InternalRpc)
        .protocol(ServiceProtocol::NativeFieldnetRpc)
        .ttl_seconds(300)
        .build_and_sign(&identity)
        .expect("Build and sign must succeed");

    assert_eq!(
        manifest.schema_version,
        CURRENT_SERVICE_MANIFEST_SCHEMA_VERSION
    );
    assert_eq!(manifest.service_id, service_id);
    assert_eq!(manifest.provider_node_id, identity.node_id());
    assert_eq!(manifest.provider_public_key, identity.public_key_bytes());
    assert_ne!(manifest.signature, [0u8; 64]);

    // Verification must succeed
    assert!(manifest.verify_signature().is_ok());

    // Basic structural validation
    let now = Timestamp::now();
    assert!(manifest.validate_basics(now).is_ok());
    assert!(!manifest.is_expired(now));
}

#[test]
fn test_manifest_signature_tamper_detection() {
    let identity = NodeIdentity::generate();
    let service_id = ServiceId::new("files.service").unwrap();
    let version = ServiceVersion::new(1, 0, 0);

    let mut manifest = ServiceManifestBuilder::new(service_id, version)
        .display_name("File Storage Service")
        .build_and_sign(&identity)
        .unwrap();

    assert!(manifest.verify_signature().is_ok());

    // 1. Tamper with sequence number
    manifest.sequence += 1;
    assert!(manifest.verify_signature().is_err());
    manifest.sequence -= 1; // restore

    // 2. Tamper with endpoint
    manifest.endpoint = ServiceEndpoint::LocalPort(9999);
    assert!(manifest.verify_signature().is_err());
    manifest.endpoint = ServiceEndpoint::InternalRpc; // restore

    // 3. Tamper with provider NodeId
    manifest.provider_node_id = NodeId::from_bytes([0xaa; 32]);
    assert!(manifest.verify_signature().is_err());
    manifest.provider_node_id = identity.node_id(); // restore

    // 4. Tamper with signature bytes
    manifest.signature[0] ^= 0xff;
    assert!(manifest.verify_signature().is_err());
}

#[test]
fn test_expired_manifest_validation() {
    let identity = NodeIdentity::generate();
    let service_id = ServiceId::new("maps.service").unwrap();
    let version = ServiceVersion::new(2, 0, 0);

    let now = Timestamp::now();
    // Created 100 seconds ago with 50 second TTL -> already expired 50 seconds ago
    let past_created = now.sub_seconds(100);

    let manifest = ServiceManifestBuilder::new(service_id, version)
        .created_at(past_created)
        .ttl_seconds(50)
        .build_and_sign(&identity)
        .unwrap();

    assert!(manifest.verify_signature().is_ok()); // cryptographic signature is valid
    assert!(manifest.is_expired(now)); // but it is expired

    // validate_basics must fail
    assert!(manifest.validate_basics(now).is_err());
}
