use fieldnet_core::Timestamp;
use fieldnet_identity::NodeIdentity;
use fieldnet_service::{
    RegistryConfig, ServiceCapability, ServiceId, ServiceInstanceId, ServiceManifestBuilder,
    ServiceRegistry, ServiceState, ServiceVersion,
};
use std::fs;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::broadcast;

#[tokio::test]
async fn test_sqlite_persistence_and_restart() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("services.db");

    let identity_local = NodeIdentity::generate();
    let identity_remote = NodeIdentity::generate();

    let service_id = ServiceId::new("database.service").unwrap();
    let local_inst = ServiceInstanceId::generate();
    let remote_inst = ServiceInstanceId::generate();

    let local_manifest =
        ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
            .instance_id(local_inst.clone())
            .display_name("Local Database")
            .build_and_sign(&identity_local)
            .unwrap();

    let remote_manifest =
        ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
            .instance_id(remote_inst.clone())
            .display_name("Remote Database")
            .ttl_seconds(600)
            .build_and_sign(&identity_remote)
            .unwrap();

    // 1. Initial run: Register local and remote instances
    {
        let (event_tx, _) = broadcast::channel(64);
        let registry =
            ServiceRegistry::open(RegistryConfig::default(), Some(db_path.clone()), event_tx)
                .expect("Open registry on disk must succeed");

        registry.register_local(local_manifest).await.unwrap();
        registry
            .register_remote(remote_manifest, Timestamp::now())
            .await
            .unwrap();

        assert_eq!(registry.count_instances().await, 2);
    } // Registry dropped here (simulating process termination)

    assert!(db_path.exists(), "services.db must exist on disk");

    // 2. Restart run: Reopen registry from same db_path
    {
        let (event_tx, _) = broadcast::channel(64);
        let registry =
            ServiceRegistry::open(RegistryConfig::default(), Some(db_path.clone()), event_tx)
                .expect("Reopening registry on disk must succeed");

        assert_eq!(registry.count_instances().await, 2);

        // Verify local instance was restored
        let local_rec = registry.get_instance(&local_inst).await.unwrap();
        assert!(local_rec.is_local);
        assert_eq!(local_rec.state(), ServiceState::Available);

        // Verify remote instance was restored
        let remote_rec = registry.get_instance(&remote_inst).await.unwrap();
        assert!(!remote_rec.is_local);
        assert_eq!(remote_rec.state(), ServiceState::Available);

        // 3. Remove local instance and verify deletion persists
        registry.unregister(&local_inst).await.unwrap();
        assert_eq!(registry.count_instances().await, 1);
    } // Registry dropped again

    // 4. Third run: Verify deleted instance is gone
    {
        let (event_tx, _) = broadcast::channel(64);
        let registry =
            ServiceRegistry::open(RegistryConfig::default(), Some(db_path.clone()), event_tx)
                .unwrap();

        assert_eq!(registry.count_instances().await, 1);
        assert!(registry.get_instance(&local_inst).await.is_none());
        assert!(registry.get_instance(&remote_inst).await.is_some());
    }
}

#[tokio::test]
async fn test_sqlite_corruption_recovery() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("services.db");

    // 1. Create a corrupt database file with random garbage bytes
    fs::write(&db_path, b"CORRUPTED_NON_SQLITE_GARBAGE_HEADER_DATA").unwrap();

    let (event_tx, _) = broadcast::channel(64);

    // 2. Opening registry must NOT panic or fail; it should quarantine and recreate
    let registry =
        ServiceRegistry::open(RegistryConfig::default(), Some(db_path.clone()), event_tx)
            .expect("Registry should recover from corrupt SQLite file");

    assert_eq!(registry.count_instances().await, 0);

    // Verify fresh database is operational
    let identity = NodeIdentity::generate();
    let manifest = ServiceManifestBuilder::new(
        ServiceId::new("recovery.service").unwrap(),
        ServiceVersion::new(1, 0, 0),
    )
    .build_and_sign(&identity)
    .unwrap();

    assert!(registry.register_local(manifest).await.is_ok());
    assert_eq!(registry.count_instances().await, 1);

    // Verify quarantine file exists in directory
    let entries: Vec<_> = fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
        .collect();
    assert!(
        entries.iter().any(|name| name.contains("corrupt")),
        "Corrupted file must be quarantined with .corrupt extension"
    );
}

#[tokio::test]
async fn test_registry_capacity_limits() {
    let config = RegistryConfig {
        max_instances_per_service: 2, // only 2 instances allowed for one service
        max_total_instances: 5,
        ..Default::default()
    };

    let (event_tx, _) = broadcast::channel(64);
    let registry = ServiceRegistry::open_in_memory(config, event_tx).unwrap();

    let service_id = ServiceId::new("capacity.service").unwrap();

    let id1 = NodeIdentity::generate();
    let m1 = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .build_and_sign(&id1)
        .unwrap();

    let id2 = NodeIdentity::generate();
    let m2 = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .build_and_sign(&id2)
        .unwrap();

    let id3 = NodeIdentity::generate();
    let m3 = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .build_and_sign(&id3)
        .unwrap();

    assert!(registry.register_remote(m1, Timestamp::now()).await.is_ok());
    assert!(registry.register_remote(m2, Timestamp::now()).await.is_ok());

    // Third instance exceeds max_instances_per_service (2)
    let res3 = registry.register_remote(m3, Timestamp::now()).await;
    assert!(res3.is_err());
    let err_str = res3.unwrap_err().to_string();
    assert!(err_str.contains("limit reached"));
}

#[tokio::test]
async fn test_concurrent_registry_access() {
    let (event_tx, _) = broadcast::channel(128);
    let registry = ServiceRegistry::open_in_memory(RegistryConfig::default(), event_tx).unwrap();

    let mut handles = Vec::new();

    // Spawn 16 concurrent tasks doing registrations, queries, and unregistrations
    for task_idx in 0..16 {
        let reg = Arc::clone(&registry);
        let handle = tokio::spawn(async move {
            let identity = NodeIdentity::generate();
            let service_id =
                ServiceId::new(format!("concurrent.service_{}", task_idx % 4)).unwrap();
            let instance_id = ServiceInstanceId::generate();

            let manifest =
                ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
                    .instance_id(instance_id.clone())
                    .capability(ServiceCapability::new("concurrent.task").unwrap())
                    .build_and_sign(&identity)
                    .unwrap();

            // Register
            reg.register_remote(manifest, Timestamp::now())
                .await
                .unwrap();

            // Query
            let rec = reg.get_instance(&instance_id).await.expect("Must exist");
            assert_eq!(rec.service_id(), &service_id);

            // Record telemetry
            reg.record_call_success(&instance_id, 10, Timestamp::now())
                .await;

            // Unregister every other task
            if task_idx % 2 == 0 {
                reg.unregister(&instance_id).await.unwrap();
                assert!(reg.get_instance(&instance_id).await.is_none());
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle
            .await
            .expect("Concurrent task must complete without panic");
    }

    // 8 tasks should remain (the odd numbered ones)
    assert_eq!(registry.count_instances().await, 8);
}
