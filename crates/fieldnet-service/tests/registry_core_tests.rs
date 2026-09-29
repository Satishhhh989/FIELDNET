use fieldnet_core::Timestamp;
use fieldnet_identity::NodeIdentity;
use fieldnet_service::{
    RegistryConfig, ServiceCapability, ServiceEndpoint, ServiceEvent, ServiceId, ServiceInstanceId,
    ServiceManifestBuilder, ServiceProtocol, ServiceRegistry, ServiceState, ServiceVersion,
};
use tokio::sync::broadcast;

#[tokio::test]
async fn test_local_service_registration_and_lookup() {
    let (event_tx, mut event_rx) = broadcast::channel(64);
    let registry = ServiceRegistry::open_in_memory(RegistryConfig::default(), event_tx)
        .expect("Registry should initialize in memory");

    let identity = NodeIdentity::generate();
    let service_id = ServiceId::new("files.service").unwrap();
    let instance_id = ServiceInstanceId::generate();

    let manifest = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .instance_id(instance_id.clone())
        .display_name("Local File Store")
        .endpoint(ServiceEndpoint::InternalRpc)
        .protocol(ServiceProtocol::NativeFieldnetRpc)
        .capability(ServiceCapability::new("files.read").unwrap())
        .build_and_sign(&identity)
        .unwrap();

    registry.register_local(manifest.clone()).await.unwrap();

    // Verify lookup
    let record = registry
        .get_instance(&instance_id)
        .await
        .expect("Instance must be found");
    assert_eq!(record.service_id(), &service_id);
    assert_eq!(record.instance_id(), &instance_id);
    assert_eq!(record.provider_node_id(), &identity.node_id());
    assert!(record.is_local);
    assert_eq!(record.state(), ServiceState::Available);

    // Verify events emitted
    let ev1 = event_rx.recv().await.unwrap();
    assert_eq!(ev1.event_name(), "service.registered");
    let ev2 = event_rx.recv().await.unwrap();
    assert_eq!(ev2.event_name(), "service.available");
}

#[tokio::test]
async fn test_multiple_providers_for_same_service() {
    let (event_tx, _event_rx) = broadcast::channel(64);
    let registry = ServiceRegistry::open_in_memory(RegistryConfig::default(), event_tx).unwrap();

    let service_id = ServiceId::new("search.service").unwrap();

    // Provider A
    let identity_a = NodeIdentity::generate();
    let instance_a = ServiceInstanceId::generate();
    let manifest_a = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .instance_id(instance_a.clone())
        .display_name("Search Provider A")
        .build_and_sign(&identity_a)
        .unwrap();

    // Provider B
    let identity_b = NodeIdentity::generate();
    let instance_b = ServiceInstanceId::generate();
    let manifest_b = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 1, 0))
        .instance_id(instance_b.clone())
        .display_name("Search Provider B")
        .build_and_sign(&identity_b)
        .unwrap();

    registry.register_local(manifest_a).await.unwrap();
    registry
        .register_remote(manifest_b, Timestamp::now())
        .await
        .unwrap();

    // Both instances must coexist under search.service
    let instances = registry.get_service_instances(&service_id).await;
    assert_eq!(instances.len(), 2);

    let providers: Vec<_> = instances.iter().map(|i| *i.provider_node_id()).collect();
    assert!(providers.contains(&identity_a.node_id()));
    assert!(providers.contains(&identity_b.node_id()));

    let callable = registry.list_callable_instances(&service_id).await;
    assert_eq!(callable.len(), 2);
}

#[tokio::test]
async fn test_duplicate_instance_handling() {
    let (event_tx, mut event_rx) = broadcast::channel(64);
    let registry = ServiceRegistry::open_in_memory(RegistryConfig::default(), event_tx).unwrap();

    let identity = NodeIdentity::generate();
    let service_id = ServiceId::new("maps.service").unwrap();
    let instance_id = ServiceInstanceId::generate();

    // 1. Initial manifest with sequence = 1
    let manifest_v1 = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .instance_id(instance_id.clone())
        .sequence(1)
        .display_name("Maps Initial")
        .build_and_sign(&identity)
        .unwrap();

    registry
        .register_remote(manifest_v1.clone(), Timestamp::now())
        .await
        .unwrap();

    // Drain initial events
    let _ = event_rx.recv().await.unwrap(); // Authenticated
    let _ = event_rx.recv().await.unwrap(); // Registered
    let _ = event_rx.recv().await.unwrap(); // Available

    // 2. Case A: Same sequence from same provider -> Idempotent refresh
    let refresh_result = registry
        .register_remote(manifest_v1, Timestamp::now())
        .await;
    assert!(refresh_result.is_ok());

    // 3. Case B: Higher sequence (2) from same provider -> Update accepted
    let manifest_v2 = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .instance_id(instance_id.clone())
        .sequence(2)
        .display_name("Maps Updated")
        .build_and_sign(&identity)
        .unwrap();

    registry
        .register_remote(manifest_v2, Timestamp::now())
        .await
        .unwrap();
    let ev_auth = event_rx.recv().await.unwrap();
    assert_eq!(ev_auth.event_name(), "service.authenticated");
    let ev_upd = event_rx.recv().await.unwrap();
    assert_eq!(ev_upd.event_name(), "service.updated");

    // 4. Case C: Lower sequence from same provider -> Rejected with StaleSequence
    let manifest_stale =
        ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
            .instance_id(instance_id.clone())
            .sequence(1)
            .display_name("Maps Stale")
            .build_and_sign(&identity)
            .unwrap();

    let stale_res = registry
        .register_remote(manifest_stale, Timestamp::now())
        .await;
    assert!(stale_res.is_err());

    // 5. Case D: Same instance ID claimed by a DIFFERENT provider -> Rejected with ConflictingProvider
    let imposter_identity = NodeIdentity::generate();
    let imposter_manifest =
        ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
            .instance_id(instance_id.clone()) // same instance ID
            .sequence(3)
            .display_name("Imposter Maps")
            .build_and_sign(&imposter_identity)
            .unwrap();

    let imposter_res = registry
        .register_remote(imposter_manifest, Timestamp::now())
        .await;
    assert!(imposter_res.is_err());
    let err_str = imposter_res.unwrap_err().to_string();
    assert!(err_str.contains("Conflicting provider"));
}

#[tokio::test]
async fn test_service_removal() {
    let (event_tx, mut event_rx) = broadcast::channel(64);
    let registry = ServiceRegistry::open_in_memory(RegistryConfig::default(), event_tx).unwrap();

    let identity = NodeIdentity::generate();
    let service_id = ServiceId::new("ai.service").unwrap();
    let instance_id = ServiceInstanceId::generate();

    let manifest = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .instance_id(instance_id.clone())
        .display_name("AI Inference")
        .build_and_sign(&identity)
        .unwrap();

    registry.register_local(manifest).await.unwrap();
    assert!(registry.get_instance(&instance_id).await.is_some());

    // Unregister
    registry.unregister(&instance_id).await.unwrap();
    assert!(registry.get_instance(&instance_id).await.is_none());

    // Verify Removed event
    while let Ok(ev) = event_rx.recv().await {
        if ev.event_name() == "service.removed" {
            assert_eq!(ev.instance_id(), &instance_id);
            return;
        }
    }
    panic!("Did not receive service.removed event");
}

#[tokio::test]
async fn test_provider_availability_and_recovery() {
    let (event_tx, _event_rx) = broadcast::channel(64);
    let registry = ServiceRegistry::open_in_memory(RegistryConfig::default(), event_tx).unwrap();

    let provider = NodeIdentity::generate();
    let service_id = ServiceId::new("sensor.service").unwrap();
    let inst1 = ServiceInstanceId::generate();
    let inst2 = ServiceInstanceId::generate();

    let m1 = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .instance_id(inst1.clone())
        .build_and_sign(&provider)
        .unwrap();

    let m2 = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .instance_id(inst2.clone())
        .build_and_sign(&provider)
        .unwrap();

    registry
        .register_remote(m1, Timestamp::now())
        .await
        .unwrap();
    registry
        .register_remote(m2, Timestamp::now())
        .await
        .unwrap();

    assert_eq!(registry.list_callable_instances(&service_id).await.len(), 2);

    // 1. Peer disconnected: Mark provider unavailable
    let affected = registry
        .mark_provider_unavailable(&provider.node_id(), "Peer TCP dropped")
        .await;
    assert_eq!(affected, 2);

    // Instances must still exist, but are NOT callable
    assert_eq!(registry.get_service_instances(&service_id).await.len(), 2);
    assert_eq!(registry.list_callable_instances(&service_id).await.len(), 0);

    let rec1 = registry.get_instance(&inst1).await.unwrap();
    assert_eq!(rec1.state(), ServiceState::Unavailable);

    // 2. Peer reconnected: Mark provider available
    let recovered = registry.mark_provider_available(&provider.node_id()).await;
    assert_eq!(recovered, 2);

    assert_eq!(registry.list_callable_instances(&service_id).await.len(), 2);
    let rec1_recov = registry.get_instance(&inst1).await.unwrap();
    assert_eq!(rec1_recov.state(), ServiceState::Available);
}

#[tokio::test]
async fn test_service_expiration_and_sweep() {
    let (event_tx, mut event_rx) = broadcast::channel(64);
    let registry = ServiceRegistry::open_in_memory(RegistryConfig::default(), event_tx).unwrap();

    let provider = NodeIdentity::generate();
    let service_id = ServiceId::new("ephemeral.service").unwrap();
    let instance_id = ServiceInstanceId::generate();

    let now = Timestamp::now();
    let m = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .instance_id(instance_id.clone())
        .created_at(now)
        .ttl_seconds(2) // 2 second TTL
        .build_and_sign(&provider)
        .unwrap();

    registry.register_remote(m, now).await.unwrap();
    assert!(registry.get_instance(&instance_id).await.is_some());

    // Advance time by 3 seconds
    let future_now = now.add_seconds(3);

    // Sweep expired
    let swept = registry.sweep_expired(future_now).await;
    assert_eq!(swept, 1);

    // Instance should be completely gone
    assert!(registry.get_instance(&instance_id).await.is_none());

    // Verify Expired event
    let mut received_expired = false;
    while let Ok(ev) = event_rx.recv().await {
        if let ServiceEvent::Expired {
            instance_id: id, ..
        } = ev
        {
            assert_eq!(id, instance_id);
            received_expired = true;
            break;
        }
    }
    assert!(received_expired, "Should emit service.expired event");
}
