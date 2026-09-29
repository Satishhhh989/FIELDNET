use fieldnet_content::hashing::hash_bytes;
use fieldnet_core::Timestamp;
use fieldnet_identity::NodeIdentity;
use tempfile::tempdir;

use fieldnet_wasm::id::{AppVersion, ApplicationId, PackageId, PublisherIdentity};
use fieldnet_wasm::lifecycle::ApplicationState;
use fieldnet_wasm::limits::ResourceLimits;
use fieldnet_wasm::manifest::{ApplicationManifest, CURRENT_MANIFEST_SCHEMA_VERSION};
use fieldnet_wasm::permissions::AppPermission;
use fieldnet_wasm::registry::{ApplicationRecord, ApplicationRegistry, SignatureStatus};

fn sample_record(app_id_str: &str, ver_str: &str) -> ApplicationRecord {
    let identity = NodeIdentity::generate();
    let publisher = PublisherIdentity::from_public_key(identity.public_key_bytes());
    let app_id = ApplicationId::new(app_id_str).unwrap();
    let version: AppVersion = ver_str.parse().unwrap();

    let manifest = ApplicationManifest {
        schema_version: CURRENT_MANIFEST_SCHEMA_VERSION,
        application_id: app_id.clone(),
        name: format!("App {}", app_id),
        version: version.clone(),
        description: "Test desc".to_string(),
        publisher,
        entrypoint: "app.wasm".to_string(),
        permissions: vec![AppPermission::StorageRead],
        resource_limits: ResourceLimits::default(),
        dependencies: vec![],
        package_metadata: Default::default(),
    };

    let fake_hash = hash_bytes(b"package_content");

    ApplicationRecord {
        application_id: app_id,
        installed_version: version,
        package_id: PackageId::new(format!("pkg:{}:{}", app_id_str, ver_str)).unwrap(),
        package_hash: fake_hash,
        manifest,
        items: vec![],
        publisher,
        signature_status: SignatureStatus::Verified,
        lifecycle_state: ApplicationState::Installed,
        permissions: vec![AppPermission::StorageRead],
        resource_limits: ResourceLimits::default(),
        dependencies: vec![],
        installed_at: Timestamp::now(),
        updated_at: Timestamp::now(),
    }
}

#[test]
fn test_valid_lifecycle_transitions() {
    let s = ApplicationState::Discovered;
    assert_eq!(
        s.transition_to(ApplicationState::Verified).unwrap(),
        ApplicationState::Verified
    );

    let s = ApplicationState::Verified;
    assert_eq!(
        s.transition_to(ApplicationState::Installed).unwrap(),
        ApplicationState::Installed
    );

    let s = ApplicationState::Installed;
    assert_eq!(
        s.transition_to(ApplicationState::Authorized).unwrap(),
        ApplicationState::Authorized
    );

    let s = ApplicationState::Authorized;
    assert_eq!(
        s.transition_to(ApplicationState::Starting).unwrap(),
        ApplicationState::Starting
    );

    let s = ApplicationState::Starting;
    assert_eq!(
        s.transition_to(ApplicationState::Running).unwrap(),
        ApplicationState::Running
    );

    let s = ApplicationState::Running;
    assert_eq!(
        s.transition_to(ApplicationState::Suspended).unwrap(),
        ApplicationState::Suspended
    );
    assert_eq!(
        s.transition_to(ApplicationState::Stopped).unwrap(),
        ApplicationState::Stopped
    );
    assert_eq!(
        s.transition_to(ApplicationState::Crashed).unwrap(),
        ApplicationState::Crashed
    );
    assert_eq!(
        s.transition_to(ApplicationState::Isolated).unwrap(),
        ApplicationState::Isolated
    );

    let s = ApplicationState::Isolated;
    assert_eq!(
        s.transition_to(ApplicationState::Stopped).unwrap(),
        ApplicationState::Stopped
    );
}

#[test]
fn test_invalid_lifecycle_transitions_rejected() {
    // Cannot jump from Discovered directly to Running
    assert!(ApplicationState::Discovered
        .transition_to(ApplicationState::Running)
        .is_err());

    // Cannot jump from Installed directly to Running
    assert!(ApplicationState::Installed
        .transition_to(ApplicationState::Running)
        .is_err());

    // Cannot jump from Crashed to Suspended
    assert!(ApplicationState::Crashed
        .transition_to(ApplicationState::Suspended)
        .is_err());

    // Cannot jump from Isolated to Running
    assert!(ApplicationState::Isolated
        .transition_to(ApplicationState::Running)
        .is_err());
}

#[test]
fn test_registry_persistence_and_restart_recovery() {
    let temp = tempdir().unwrap();
    let db_path = temp.path().join("apps.db");

    let record1 = sample_record("fieldnet.chat", "1.0.0");
    let record2 = sample_record("fieldnet.sensor", "2.1.0");

    // Scope 1: Write records to SQLite database
    {
        let registry = ApplicationRegistry::new(&db_path).unwrap();
        registry.register(&record1).unwrap();
        registry.register(&record2).unwrap();

        let list = registry.list().unwrap();
        assert_eq!(list.len(), 2);
    }

    // Scope 2: Reopen database from disk simulating node restart
    {
        let restored_registry = ApplicationRegistry::new(&db_path).unwrap();
        let list = restored_registry.list().unwrap();
        assert_eq!(list.len(), 2);

        let chat = restored_registry
            .get(&ApplicationId::new("fieldnet.chat").unwrap())
            .unwrap()
            .expect("Chat app found");
        assert_eq!(chat.installed_version, AppVersion::new(1, 0, 0));
        assert_eq!(chat.signature_status, SignatureStatus::Verified);

        let sensor = restored_registry
            .get(&ApplicationId::new("fieldnet.sensor").unwrap())
            .unwrap()
            .expect("Sensor app found");
        assert_eq!(sensor.installed_version, AppVersion::new(2, 1, 0));
    }
}

#[test]
fn test_registry_update_and_rollback() {
    let registry = ApplicationRegistry::new_in_memory().unwrap();
    let app_id = ApplicationId::new("fieldnet.calc").unwrap();

    let v1 = sample_record("fieldnet.calc", "1.0.0");
    registry.register(&v1).unwrap();

    // Verify initial version
    let current = registry.get(&app_id).unwrap().unwrap();
    assert_eq!(current.installed_version, AppVersion::new(1, 0, 0));

    // Update to v1.1.0
    let v2 = sample_record("fieldnet.calc", "1.1.0");
    registry.update_version(&v2).unwrap();

    let current = registry.get(&app_id).unwrap().unwrap();
    assert_eq!(current.installed_version, AppVersion::new(1, 1, 0));

    let history = registry.get_history(&app_id).unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].version, AppVersion::new(1, 0, 0));

    // Update to v2.0.0
    let v3 = sample_record("fieldnet.calc", "2.0.0");
    registry.update_version(&v3).unwrap();

    let current = registry.get(&app_id).unwrap().unwrap();
    assert_eq!(current.installed_version, AppVersion::new(2, 0, 0));

    let history = registry.get_history(&app_id).unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].version, AppVersion::new(1, 1, 0));
    assert_eq!(history[1].version, AppVersion::new(1, 0, 0));

    // Rollback: reverts to v1.1.0
    let restored1 = registry.rollback(&app_id).unwrap();
    assert_eq!(restored1.installed_version, AppVersion::new(1, 1, 0));
    assert_eq!(
        registry.get(&app_id).unwrap().unwrap().installed_version,
        AppVersion::new(1, 1, 0)
    );

    // Rollback again: reverts to v1.0.0
    let restored2 = registry.rollback(&app_id).unwrap();
    assert_eq!(restored2.installed_version, AppVersion::new(1, 0, 0));
    assert_eq!(
        registry.get(&app_id).unwrap().unwrap().installed_version,
        AppVersion::new(1, 0, 0)
    );

    // Third rollback fails because no further history remains
    assert!(registry.rollback(&app_id).is_err());
}

#[test]
fn test_registry_lifecycle_state_transitions() {
    let registry = ApplicationRegistry::new_in_memory().unwrap();
    let app_id = ApplicationId::new("fieldnet.state.app").unwrap();

    let record = sample_record("fieldnet.state.app", "1.0.0");
    registry.register(&record).unwrap();

    // Installed -> Authorized is valid
    let new_state = registry
        .update_lifecycle_state(&app_id, ApplicationState::Authorized)
        .unwrap();
    assert_eq!(new_state, ApplicationState::Authorized);
    assert_eq!(
        registry.get(&app_id).unwrap().unwrap().lifecycle_state,
        ApplicationState::Authorized
    );

    // Authorized -> Running is invalid (must go through Starting)
    assert!(registry
        .update_lifecycle_state(&app_id, ApplicationState::Running)
        .is_err());
}
