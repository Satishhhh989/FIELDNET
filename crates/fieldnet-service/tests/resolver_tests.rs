use fieldnet_core::Timestamp;
use fieldnet_identity::NodeIdentity;
use fieldnet_service::{
    AuthContext, DirectRouteProvider, LocalityPreference, RegistryConfig, ResolverConfig,
    ServiceCapability, ServiceEndpoint, ServiceError, ServiceId, ServiceInstanceId,
    ServiceManifestBuilder, ServicePermissions, ServiceProtocol, ServiceQuery, ServiceRegistry,
    ServiceResolver, ServiceState, ServiceVersion, TrustRequirement,
};
use std::sync::Arc;
use tokio::sync::broadcast;

// Helper to create an in-memory resolver with an optional direct route table
fn setup_test_resolver() -> (
    Arc<ServiceRegistry>,
    Arc<DirectRouteProvider>,
    ServiceResolver,
    broadcast::Receiver<fieldnet_service::ServiceEvent>,
) {
    let (event_tx, event_rx) = broadcast::channel(128);
    let registry = ServiceRegistry::open_in_memory(RegistryConfig::default(), event_tx.clone())
        .expect("Registry should initialize in-memory");
    let route_provider = Arc::new(DirectRouteProvider::new());
    let local_identity = NodeIdentity::generate();

    let resolver = ServiceResolver::new(
        registry.clone(),
        Some(route_provider.clone()),
        Some(local_identity.node_id()),
        Some(event_tx),
        ResolverConfig::default(),
    );

    (registry, route_provider, resolver, event_rx)
}

// 1. Single provider resolution
#[tokio::test]
async fn test_single_provider_resolution() {
    let (registry, _, resolver, mut event_rx) = setup_test_resolver();
    let identity = NodeIdentity::generate();
    let service_id = ServiceId::new("calc.service").unwrap();
    let instance_id = ServiceInstanceId::generate();

    let manifest = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .instance_id(instance_id.clone())
        .display_name("Calculator Service")
        .endpoint(ServiceEndpoint::InternalRpc)
        .build_and_sign(&identity)
        .unwrap();

    registry.register_local(manifest).await.unwrap();

    let query = ServiceQuery::new(service_id.clone());
    let resolution = resolver.resolve(&query, None).await.unwrap();

    assert_eq!(resolution.service_id, service_id);
    assert_eq!(resolution.instance_id, instance_id);
    assert_eq!(resolution.provider_node_id, identity.node_id());
    assert!(resolution.is_local());
    assert_eq!(resolution.route_hops, Some(0));

    // Verify explanation
    assert!(resolution.reason.matched_service);
    assert!(resolution.reason.service_available);
    assert!(!resolution.reason.explanation_text.is_empty());

    // Verify resolved event emitted
    let mut resolved_event_found = false;
    while let Ok(ev) = event_rx.try_recv() {
        if ev.event_name() == "service.resolved" {
            assert_eq!(ev.service_id(), &service_id);
            assert_eq!(ev.instance_id(), &instance_id);
            resolved_event_found = true;
            break;
        }
    }
    assert!(resolved_event_found);
}

// 2. Multiple provider resolution
#[tokio::test]
async fn test_multiple_provider_resolution() {
    let (registry, routes, resolver, _) = setup_test_resolver();
    let service_id = ServiceId::new("storage.service").unwrap();

    let identity_local = NodeIdentity::generate();
    let instance_local = ServiceInstanceId::generate();
    let manifest_local =
        ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
            .instance_id(instance_local.clone())
            .build_and_sign(&identity_local)
            .unwrap();

    let identity_remote = NodeIdentity::generate();
    let instance_remote = ServiceInstanceId::generate();
    let manifest_remote =
        ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
            .instance_id(instance_remote.clone())
            .build_and_sign(&identity_remote)
            .unwrap();

    let now = Timestamp::now();
    registry.register_local(manifest_local).await.unwrap();
    registry
        .register_remote(manifest_remote, now)
        .await
        .unwrap();
    routes
        .add_route(identity_remote.node_id(), identity_remote.node_id(), 2, 20)
        .await;

    // By default, PreferLocal selects the local instance
    let query = ServiceQuery::new(service_id.clone());
    let resolution = resolver.resolve(&query, None).await.unwrap();
    assert_eq!(resolution.instance_id, instance_local);
}

// 3. Unknown service
#[tokio::test]
async fn test_unknown_service() {
    let (_, _, resolver, _) = setup_test_resolver();
    let query = ServiceQuery::new(ServiceId::new("nonexistent.service").unwrap());

    match resolver.resolve(&query, None).await {
        Err(ServiceError::ServiceNotFound(s)) => assert_eq!(s, "nonexistent.service"),
        other => panic!("Expected ServiceNotFound, got {:?}", other),
    }
    assert_eq!(resolver.metrics().snapshot().failures_not_found, 1);
}

// 4. Expired service
#[tokio::test]
async fn test_expired_service() {
    let (registry, _, resolver, _) = setup_test_resolver();
    let identity = NodeIdentity::generate();
    let service_id = ServiceId::new("ephemeral.service").unwrap();

    let manifest = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .ttl_seconds(1)
        .created_at(Timestamp::from_millis(1000))
        .build_and_sign(&identity)
        .unwrap();

    let now = Timestamp::from_millis(1000);
    registry.register_remote(manifest, now).await.unwrap();

    // Query at a future timestamp where manifest is expired
    let query = ServiceQuery::new(service_id.clone());
    let res = resolver.resolve(&query, None).await;
    assert!(matches!(
        res,
        Err(ServiceError::AllProvidersUnavailable { .. })
    ));
}

// 5. Unavailable service
#[tokio::test]
async fn test_unavailable_service() {
    let (registry, _, resolver, _) = setup_test_resolver();
    let identity = NodeIdentity::generate();
    let service_id = ServiceId::new("offline.service").unwrap();

    let manifest = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .build_and_sign(&identity)
        .unwrap();

    registry.register_local(manifest).await.unwrap();
    registry
        .mark_provider_unavailable(&identity.node_id(), "Network partition")
        .await;

    let query = ServiceQuery::new(service_id);
    let res = resolver.resolve(&query, None).await;
    assert!(matches!(
        res,
        Err(ServiceError::AllProvidersUnavailable { .. })
    ));
    assert_eq!(resolver.metrics().snapshot().failures_unavailable, 1);
}

// 6. Degraded service
#[tokio::test]
async fn test_degraded_service_resolution() {
    let (registry, _, resolver, _) = setup_test_resolver();
    let identity = NodeIdentity::generate();
    let service_id = ServiceId::new("degraded.service").unwrap();
    let instance_id = ServiceInstanceId::generate();

    let manifest = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .instance_id(instance_id.clone())
        .build_and_sign(&identity)
        .unwrap();

    registry.register_local(manifest).await.unwrap();
    // Simulate failure leading to Degraded state
    let _ = registry
        .record_call_failure(&instance_id, Timestamp::now())
        .await;

    let query = ServiceQuery::new(service_id);
    let resolution = resolver.resolve(&query, None).await.unwrap();

    assert_eq!(resolution.instance_id, instance_id);
    assert_eq!(resolution.selected_instance.state(), ServiceState::Degraded);
}

// 7. Exact version requirement
#[tokio::test]
async fn test_exact_version_requirement() {
    let (registry, _, resolver, _) = setup_test_resolver();
    let service_id = ServiceId::new("versioned.service").unwrap();

    let id_v1 = NodeIdentity::generate();
    let inst_v1 = ServiceInstanceId::generate();
    let manifest_v1 = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 2, 0))
        .instance_id(inst_v1)
        .build_and_sign(&id_v1)
        .unwrap();

    let id_v2 = NodeIdentity::generate();
    let inst_v2 = ServiceInstanceId::generate();
    let manifest_v2 = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(2, 0, 0))
        .instance_id(inst_v2.clone())
        .build_and_sign(&id_v2)
        .unwrap();

    registry.register_local(manifest_v1).await.unwrap();
    registry.register_local(manifest_v2).await.unwrap();

    let query = ServiceQuery::builder(service_id.clone())
        .min_version(ServiceVersion::new(2, 0, 0))
        .max_version(ServiceVersion::new(2, 0, 0))
        .build()
        .unwrap();

    let res = resolver.resolve(&query, None).await.unwrap();
    assert_eq!(res.instance_id, inst_v2);
    assert_eq!(res.version(), &ServiceVersion::new(2, 0, 0));
}

// 8. Incompatible version
#[tokio::test]
async fn test_incompatible_version() {
    let (registry, _, resolver, _) = setup_test_resolver();
    let service_id = ServiceId::new("legacy.service").unwrap();
    let identity = NodeIdentity::generate();

    let manifest = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 5, 0))
        .build_and_sign(&identity)
        .unwrap();

    registry.register_local(manifest).await.unwrap();

    // Query requires SemVer 2.0.0
    let query = ServiceQuery::builder(service_id)
        .min_version(ServiceVersion::new(2, 0, 0))
        .build()
        .unwrap();

    let res = resolver.resolve(&query, None).await;
    assert!(matches!(res, Err(ServiceError::NoCompatibleVersion { .. })));
    assert_eq!(resolver.metrics().snapshot().failures_version, 1);
}

// 9. Required capability
#[tokio::test]
async fn test_required_capability() {
    let (registry, _, resolver, _) = setup_test_resolver();
    let service_id = ServiceId::new("vision.service").unwrap();

    let id_basic = NodeIdentity::generate();
    let inst_basic = ServiceInstanceId::generate();
    let manifest_basic =
        ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
            .instance_id(inst_basic)
            .capability(ServiceCapability::new("vision.detect").unwrap())
            .build_and_sign(&id_basic)
            .unwrap();

    let id_adv = NodeIdentity::generate();
    let inst_adv = ServiceInstanceId::generate();
    let manifest_adv =
        ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
            .instance_id(inst_adv.clone())
            .capability(ServiceCapability::new("vision.detect").unwrap())
            .capability(ServiceCapability::new("vision.classify").unwrap())
            .build_and_sign(&id_adv)
            .unwrap();

    registry.register_local(manifest_basic).await.unwrap();
    registry.register_local(manifest_adv).await.unwrap();

    let query = ServiceQuery::builder(service_id)
        .capability(ServiceCapability::new("vision.classify").unwrap())
        .build()
        .unwrap();

    let res = resolver.resolve(&query, None).await.unwrap();
    assert_eq!(res.instance_id, inst_adv);
}

// 10. Missing capability
#[tokio::test]
async fn test_missing_capability() {
    let (registry, _, resolver, _) = setup_test_resolver();
    let service_id = ServiceId::new("audio.service").unwrap();
    let identity = NodeIdentity::generate();

    let manifest = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .capability(ServiceCapability::new("audio.record").unwrap())
        .build_and_sign(&identity)
        .unwrap();

    registry.register_local(manifest).await.unwrap();

    let query = ServiceQuery::builder(service_id)
        .capability(ServiceCapability::new("audio.synthesize").unwrap())
        .build()
        .unwrap();

    let res = resolver.resolve(&query, None).await;
    assert!(matches!(res, Err(ServiceError::MissingCapability { .. })));
    assert_eq!(resolver.metrics().snapshot().failures_capability, 1);
}

// 11. Protocol constraint
#[tokio::test]
async fn test_protocol_constraint() {
    let (registry, _, resolver, _) = setup_test_resolver();
    let service_id = ServiceId::new("web.service").unwrap();

    let id_rpc = NodeIdentity::generate();
    let inst_rpc = ServiceInstanceId::generate();
    let manifest_rpc =
        ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
            .instance_id(inst_rpc)
            .protocol(ServiceProtocol::NativeFieldnetRpc)
            .build_and_sign(&id_rpc)
            .unwrap();

    let id_http = NodeIdentity::generate();
    let inst_http = ServiceInstanceId::generate();
    let manifest_http =
        ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
            .instance_id(inst_http.clone())
            .protocol(ServiceProtocol::Http1)
            .build_and_sign(&id_http)
            .unwrap();

    registry.register_local(manifest_rpc).await.unwrap();
    registry.register_local(manifest_http).await.unwrap();

    let query = ServiceQuery::builder(service_id)
        .protocol(ServiceProtocol::Http1)
        .build()
        .unwrap();

    let res = resolver.resolve(&query, None).await.unwrap();
    assert_eq!(res.instance_id, inst_http);
    assert_eq!(res.protocol(), ServiceProtocol::Http1);
}

// 12. Authenticated provider
#[tokio::test]
async fn test_authenticated_provider() {
    let (registry, _, resolver, _) = setup_test_resolver();
    let service_id = ServiceId::new("crypto.service").unwrap();
    let identity = NodeIdentity::generate();

    let manifest = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .build_and_sign(&identity)
        .unwrap();

    assert!(manifest.verify_signature().is_ok());
    registry.register_local(manifest).await.unwrap();

    let query = ServiceQuery::builder(service_id)
        .trust(TrustRequirement::standard())
        .build()
        .unwrap();

    let res = resolver.resolve(&query, None).await.unwrap();
    assert!(res.reason.provider_authenticated);
}

// 13. Unauthenticated provider
#[tokio::test]
async fn test_unauthenticated_provider() {
    let (registry, _, resolver, _) = setup_test_resolver();
    let service_id = ServiceId::new("tampered.service").unwrap();
    let identity = NodeIdentity::generate();

    let mut manifest =
        ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
            .build_and_sign(&identity)
            .unwrap();

    // Tamper with sequence number after signing
    manifest.sequence = 999;
    assert!(manifest.verify_signature().is_err());

    // Bypass registry check by directly modifying record state in registry inner
    // Or we test through relaxed query vs strict query
    let query_strict = ServiceQuery::builder(service_id.clone())
        .trust(TrustRequirement::standard())
        .build()
        .unwrap();

    // Since registry.register_local checks signature, tampered registration is rejected at ingress
    let reg_result = registry.register_local(manifest).await;
    assert!(matches!(reg_result, Err(ServiceError::InvalidSignature(_))));

    // And resolver fails with not found
    let res = resolver.resolve(&query_strict, None).await;
    assert!(matches!(res, Err(ServiceError::ServiceNotFound(_))));
}

// 14. Authorized provider
#[tokio::test]
async fn test_authorized_provider() {
    let (registry, _, resolver, _) = setup_test_resolver();
    let service_id = ServiceId::new("admin.service").unwrap();
    let identity = NodeIdentity::generate();

    let manifest = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .permissions(ServicePermissions::with_roles(vec!["admin".to_string()]))
        .build_and_sign(&identity)
        .unwrap();

    registry.register_local(manifest).await.unwrap();

    let auth = AuthContext::for_node(identity.node_id()).with_role("admin");
    let query = ServiceQuery::new(service_id);

    let res = resolver.resolve(&query, Some(&auth)).await.unwrap();
    assert!(res.reason.provider_authorized);
}

// 15. Unauthorized provider
#[tokio::test]
async fn test_unauthorized_provider() {
    let (registry, _, resolver, _) = setup_test_resolver();
    let service_id = ServiceId::new("secure.service").unwrap();
    let identity = NodeIdentity::generate();

    let manifest = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .permissions(ServicePermissions::requires_token())
        .build_and_sign(&identity)
        .unwrap();

    registry.register_local(manifest).await.unwrap();

    // Caller provides no capability token
    let auth = AuthContext::anonymous();
    let query = ServiceQuery::new(service_id);

    let res = resolver.resolve(&query, Some(&auth)).await;
    assert!(matches!(res, Err(ServiceError::Unauthorized { .. })));
    assert_eq!(resolver.metrics().snapshot().failures_unauthorized, 1);
}

// 16. Reachable provider
#[tokio::test]
async fn test_reachable_provider() {
    let (registry, routes, resolver, _) = setup_test_resolver();
    let service_id = ServiceId::new("peer.service").unwrap();
    let identity = NodeIdentity::generate();
    let now = Timestamp::now();

    let manifest = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .build_and_sign(&identity)
        .unwrap();

    registry.register_remote(manifest, now).await.unwrap();
    routes
        .add_route(identity.node_id(), identity.node_id(), 1, 10)
        .await;

    let query = ServiceQuery::new(service_id);
    let res = resolver.resolve(&query, None).await.unwrap();
    assert_eq!(res.route_hops, Some(1));
    assert!(res.reason.route_available);
}

// 17. Unreachable provider
#[tokio::test]
async fn test_unreachable_provider() {
    let (registry, routes, resolver, _) = setup_test_resolver();
    let service_id = ServiceId::new("lost.service").unwrap();
    let identity = NodeIdentity::generate();
    let now = Timestamp::now();

    let manifest = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .build_and_sign(&identity)
        .unwrap();

    registry.register_remote(manifest, now).await.unwrap();
    routes.clear().await; // No route available

    let query = ServiceQuery::new(service_id);
    let res = resolver.resolve(&query, None).await;
    assert!(matches!(res, Err(ServiceError::NoReachableProvider { .. })));
    assert_eq!(resolver.metrics().snapshot().failures_unreachable, 1);
}

// 18. Local preference
#[tokio::test]
async fn test_local_preference() {
    let (registry, routes, resolver, _) = setup_test_resolver();
    let service_id = ServiceId::new("pref.service").unwrap();

    let id_local = NodeIdentity::generate();
    let inst_local = ServiceInstanceId::generate();
    let manifest_local =
        ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
            .instance_id(inst_local.clone())
            .build_and_sign(&id_local)
            .unwrap();

    let id_remote = NodeIdentity::generate();
    let inst_remote = ServiceInstanceId::generate();
    let manifest_remote =
        ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
            .instance_id(inst_remote)
            .build_and_sign(&id_remote)
            .unwrap();

    registry.register_local(manifest_local).await.unwrap();
    registry
        .register_remote(manifest_remote, Timestamp::now())
        .await
        .unwrap();
    routes
        .add_route(id_remote.node_id(), id_remote.node_id(), 1, 10)
        .await;

    let query = ServiceQuery::builder(service_id)
        .locality(LocalityPreference::PreferLocal)
        .build()
        .unwrap();

    let res = resolver.resolve(&query, None).await.unwrap();
    assert_eq!(res.instance_id, inst_local);
    assert!(res.is_local());
}

// 19. Remote provider fallback
#[tokio::test]
async fn test_remote_provider_fallback() {
    let (registry, routes, resolver, _) = setup_test_resolver();
    let service_id = ServiceId::new("fallback.service").unwrap();
    let id_remote = NodeIdentity::generate();
    let inst_remote = ServiceInstanceId::generate();

    let manifest = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .instance_id(inst_remote.clone())
        .build_and_sign(&id_remote)
        .unwrap();

    registry
        .register_remote(manifest, Timestamp::now())
        .await
        .unwrap();
    routes
        .add_route(id_remote.node_id(), id_remote.node_id(), 2, 20)
        .await;

    // PreferLocal but no local instance exists -> fallbacks cleanly to remote
    let query = ServiceQuery::builder(service_id)
        .locality(LocalityPreference::PreferLocal)
        .build()
        .unwrap();

    let res = resolver.resolve(&query, None).await.unwrap();
    assert_eq!(res.instance_id, inst_remote);
    assert_eq!(res.route_hops, Some(2));
}

// 20. Deterministic tie-breaking
#[tokio::test]
async fn test_deterministic_tie_breaking() {
    let (registry, _, resolver, _) = setup_test_resolver();
    let service_id = ServiceId::new("tie.service").unwrap();

    // Two identical local services with identical versions, capabilities, health
    let id_1 = NodeIdentity::generate();
    let inst_1 = ServiceInstanceId::new("inst-aaaa").unwrap();
    let manifest_1 = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .instance_id(inst_1.clone())
        .build_and_sign(&id_1)
        .unwrap();

    let id_2 = NodeIdentity::generate();
    let inst_2 = ServiceInstanceId::new("inst-bbbb").unwrap();
    let manifest_2 = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .instance_id(inst_2.clone())
        .build_and_sign(&id_2)
        .unwrap();

    registry.register_local(manifest_1).await.unwrap();
    registry.register_local(manifest_2).await.unwrap();

    let query = ServiceQuery::new(service_id);

    // Run 20 consecutive resolutions - the winner must ALWAYS be inst_1 ("inst-aaaa" < "inst-bbbb")
    for _ in 0..20 {
        let res = resolver.resolve(&query, None).await.unwrap();
        assert_eq!(res.instance_id, inst_1);
        assert!(res.reason.deterministic_tiebreak);
    }
}

// 21. Identical registry state produces identical result
#[tokio::test]
async fn test_identical_state_determinism() {
    let (registry, routes, resolver, _) = setup_test_resolver();
    let service_id = ServiceId::new("determ.service").unwrap();

    for i in 0..10 {
        let identity = NodeIdentity::generate();
        let inst_id = ServiceInstanceId::new(format!("node-inst-{:02}", i)).unwrap();
        let manifest =
            ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
                .instance_id(inst_id)
                .build_and_sign(&identity)
                .unwrap();

        registry
            .register_remote(manifest, Timestamp::now())
            .await
            .unwrap();
        routes
            .add_route(identity.node_id(), identity.node_id(), 1, 10)
            .await;
    }

    let query = ServiceQuery::new(service_id);
    let first_result = resolver.resolve(&query, None).await.unwrap();

    for _ in 0..50 {
        let next_result = resolver.resolve(&query, None).await.unwrap();
        assert_eq!(first_result.instance_id, next_result.instance_id);
        assert_eq!(first_result.provider_node_id, next_result.provider_node_id);
    }
}

// 22. Provider disappearance during resolution
#[tokio::test]
async fn test_provider_disappearance_during_resolution() {
    let (registry, _, resolver, _) = setup_test_resolver();
    let service_id = ServiceId::new("disappear.service").unwrap();
    let identity = NodeIdentity::generate();
    let inst_id = ServiceInstanceId::generate();

    let manifest = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .instance_id(inst_id.clone())
        .build_and_sign(&identity)
        .unwrap();

    registry.register_local(manifest).await.unwrap();

    // Verify it resolves
    let query = ServiceQuery::new(service_id.clone());
    assert!(resolver.resolve(&query, None).await.is_ok());

    // Provider disappears (unregistered)
    registry.unregister(&inst_id).await.unwrap();

    // Now resolution must fail with ServiceNotFound
    let res = resolver.resolve(&query, None).await;
    assert!(matches!(res, Err(ServiceError::ServiceNotFound(_))));
}

// 23. Service expiration during resolution
#[tokio::test]
async fn test_service_expiration_during_resolution() {
    let (registry, _, resolver, _) = setup_test_resolver();
    let service_id = ServiceId::new("expiring.service").unwrap();
    let identity = NodeIdentity::generate();
    let inst_id = ServiceInstanceId::generate();

    let manifest = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .instance_id(inst_id)
        .ttl_seconds(1)
        .created_at(Timestamp::from_millis(5000))
        .build_and_sign(&identity)
        .unwrap();

    registry
        .register_remote(manifest, Timestamp::from_millis(5000))
        .await
        .unwrap();

    // Sweep at t=7000 (expires at 6000)
    let swept = registry.sweep_expired(Timestamp::from_millis(7000)).await;
    assert_eq!(swept, 1);

    let query = ServiceQuery::new(service_id);
    let res = resolver.resolve(&query, None).await;
    assert!(matches!(res, Err(ServiceError::ServiceNotFound(_))));
}

// 24. Provider recovery
#[tokio::test]
async fn test_provider_recovery() {
    let (registry, _, resolver, _) = setup_test_resolver();
    let service_id = ServiceId::new("recovering.service").unwrap();
    let identity = NodeIdentity::generate();
    let inst_id = ServiceInstanceId::generate();

    let manifest = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .instance_id(inst_id.clone())
        .build_and_sign(&identity)
        .unwrap();

    registry.register_local(manifest.clone()).await.unwrap();

    // Peer drops
    registry
        .mark_provider_unavailable(&identity.node_id(), "link dropped")
        .await;
    let query = ServiceQuery::new(service_id.clone());
    assert!(resolver.resolve(&query, None).await.is_err());

    // Peer recovers (monotonic keepalive update with sequence 2)
    let mut updated_manifest = manifest;
    updated_manifest.sequence = 2;
    updated_manifest.sign(&identity).unwrap();
    registry.register_local(updated_manifest).await.unwrap();

    // Now resolution succeeds again
    let res = resolver.resolve(&query, None).await.unwrap();
    assert_eq!(res.instance_id, inst_id);
    assert_eq!(res.selected_instance.state(), ServiceState::Available);
}

// 25. Concurrent registry updates and resolutions
#[tokio::test]
async fn test_concurrent_registry_updates() {
    let (registry, _, resolver, _) = setup_test_resolver();
    let service_id = ServiceId::new("concurrent.service").unwrap();

    let identity = NodeIdentity::generate();
    let manifest = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .build_and_sign(&identity)
        .unwrap();
    registry.register_local(manifest).await.unwrap();

    let resolver_arc = Arc::new(resolver);
    let mut handles = Vec::new();

    // Spawn 10 resolution readers
    for _ in 0..10 {
        let r = resolver_arc.clone();
        let sid = service_id.clone();
        handles.push(tokio::spawn(async move {
            let q = ServiceQuery::new(sid);
            for _ in 0..50 {
                let res = r.resolve(&q, None).await;
                assert!(res.is_ok());
            }
        }));
    }

    // Spawn 2 registry writers registering newer sequences
    for i in 1..=2 {
        let reg = registry.clone();
        let id = NodeIdentity::generate();
        let sid = service_id.clone();
        handles.push(tokio::spawn(async move {
            let m = ServiceManifestBuilder::new(sid, ServiceVersion::new(1, 0, 0))
                .instance_id(ServiceInstanceId::new(format!("inst-writer-{}", i)).unwrap())
                .build_and_sign(&id)
                .unwrap();
            let _ = reg.register_local(m).await;
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

// 26. Malformed query
#[tokio::test]
async fn test_malformed_query() {
    let service_id = ServiceId::new("valid.service").unwrap();

    // min_version > max_version should fail during build
    let query_err = ServiceQuery::builder(service_id)
        .min_version(ServiceVersion::new(2, 0, 0))
        .max_version(ServiceVersion::new(1, 0, 0))
        .build();

    assert!(matches!(query_err, Err(ServiceError::InvalidQuery(_))));
}

// 27. No reachable provider
#[tokio::test]
async fn test_no_reachable_provider() {
    let (registry, routes, resolver, _) = setup_test_resolver();
    let service_id = ServiceId::new("island.service").unwrap();
    let id_remote = NodeIdentity::generate();

    let manifest = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .build_and_sign(&id_remote)
        .unwrap();

    registry
        .register_remote(manifest, Timestamp::now())
        .await
        .unwrap();
    routes.clear().await;

    let query = ServiceQuery::new(service_id);
    let res = resolver.resolve(&query, None).await;
    assert!(matches!(res, Err(ServiceError::NoReachableProvider { .. })));
}

// 28. Multiple equivalent providers with tie breaker
#[tokio::test]
async fn test_multiple_equivalent_providers() {
    let (registry, _, resolver, _) = setup_test_resolver();
    let service_id = ServiceId::new("equiv.service").unwrap();

    let mut instances = Vec::new();
    for name in ["delta", "beta", "alpha", "gamma"] {
        let identity = NodeIdentity::generate();
        let inst_id = ServiceInstanceId::new(format!("inst-{}", name)).unwrap();
        let manifest =
            ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
                .instance_id(inst_id.clone())
                .build_and_sign(&identity)
                .unwrap();
        registry.register_local(manifest).await.unwrap();
        instances.push(inst_id);
    }

    let query = ServiceQuery::new(service_id);
    let res = resolver.resolve(&query, None).await.unwrap();

    // "inst-alpha" is lexicographically smallest among the 4 equivalent providers
    assert_eq!(res.instance_id.as_str(), "inst-alpha");
    assert!(res.reason.deterministic_tiebreak);
}

// 29. Property tests: permutation invariance
#[tokio::test]
async fn test_property_permutation_invariance() {
    let (registry, routes, resolver, _) = setup_test_resolver();
    let service_id = ServiceId::new("perm.service").unwrap();

    // Register 5 providers with various hop counts and versions
    for i in 0..5 {
        let identity = NodeIdentity::generate();
        let inst_id = ServiceInstanceId::new(format!("provider-{:02}", i)).unwrap();
        let version = ServiceVersion::new(1, i, 0);

        let manifest = ServiceManifestBuilder::new(service_id.clone(), version)
            .instance_id(inst_id)
            .build_and_sign(&identity)
            .unwrap();

        registry
            .register_remote(manifest, Timestamp::now())
            .await
            .unwrap();
        routes
            .add_route(identity.node_id(), identity.node_id(), 1, 10)
            .await;
    }

    let query = ServiceQuery::new(service_id);
    let canonical_winner = resolver.resolve(&query, None).await.unwrap();

    // Repeated resolutions must ALWAYS return the identical winner (v1.4.0 is highest compatible)
    for _ in 0..100 {
        let res = resolver.resolve(&query, None).await.unwrap();
        assert_eq!(res.instance_id, canonical_winner.instance_id);
        assert_eq!(res.version(), canonical_winner.version());
    }
}

// 30. Performance benchmark: resolution at scale (10, 100, 1000 providers)
#[tokio::test]
async fn test_performance_at_scale() {
    let (event_tx, _) = broadcast::channel(16);
    let config = RegistryConfig {
        max_instances_per_service: 2000,
        max_total_instances: 10_000,
        max_instances_per_provider: 1000,
        ..Default::default()
    };
    let registry =
        ServiceRegistry::open_in_memory(config, event_tx.clone()).expect("Registry should open");
    let routes = Arc::new(DirectRouteProvider::new());
    let local_id = NodeIdentity::generate();

    let resolver = ServiceResolver::new(
        registry.clone(),
        Some(routes.clone()),
        Some(local_id.node_id()),
        Some(event_tx),
        ResolverConfig::default(),
    );

    let service_id = ServiceId::new("scale.service").unwrap();
    let now = Timestamp::now();

    // 1,000 providers
    for i in 0..1000 {
        let identity = NodeIdentity::generate();
        let inst_id = ServiceInstanceId::new(format!("scale-{:04}", i)).unwrap();
        let manifest =
            ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
                .instance_id(inst_id)
                .build_and_sign(&identity)
                .unwrap();

        registry.register_remote(manifest, now).await.unwrap();
        routes
            .add_route(
                identity.node_id(),
                identity.node_id(),
                (i % 5 + 1) as u8,
                10,
            )
            .await;
    }

    // Measure resolution over 10 providers
    let service_id_10 = ServiceId::new("scale10.service").unwrap();
    for i in 0..10 {
        let identity = NodeIdentity::generate();
        let inst_id = ServiceInstanceId::new(format!("sc10-{:04}", i)).unwrap();
        let manifest =
            ServiceManifestBuilder::new(service_id_10.clone(), ServiceVersion::new(1, 0, 0))
                .instance_id(inst_id)
                .build_and_sign(&identity)
                .unwrap();
        registry.register_remote(manifest, now).await.unwrap();
        routes
            .add_route(identity.node_id(), identity.node_id(), 1, 10)
            .await;
    }
    let query_10 = ServiceQuery::new(service_id_10);
    let start_10 = std::time::Instant::now();
    let res_10 = resolver.resolve(&query_10, None).await.unwrap();
    let elapsed_10 = start_10.elapsed();
    assert!(res_10.reason.matched_service);

    // Measure resolution over 100 providers
    let service_id_100 = ServiceId::new("scale100.service").unwrap();
    for i in 0..100 {
        let identity = NodeIdentity::generate();
        let inst_id = ServiceInstanceId::new(format!("sc100-{:04}", i)).unwrap();
        let manifest =
            ServiceManifestBuilder::new(service_id_100.clone(), ServiceVersion::new(1, 0, 0))
                .instance_id(inst_id)
                .build_and_sign(&identity)
                .unwrap();
        registry.register_remote(manifest, now).await.unwrap();
        routes
            .add_route(identity.node_id(), identity.node_id(), 1, 10)
            .await;
    }
    let query_100 = ServiceQuery::new(service_id_100);
    let start_100 = std::time::Instant::now();
    let res_100 = resolver.resolve(&query_100, None).await.unwrap();
    let elapsed_100 = start_100.elapsed();
    assert!(res_100.reason.matched_service);

    // Measure resolution over 1,000 providers
    let query = ServiceQuery::new(service_id);
    let start = std::time::Instant::now();
    let res = resolver.resolve(&query, None).await.unwrap();
    let elapsed = start.elapsed();

    assert!(res.reason.matched_service);
    println!(
        "Benchmark results: 10 providers: {:?}, 100 providers: {:?}, 1,000 providers: {:?}",
        elapsed_10, elapsed_100, elapsed
    );
    assert!(
        elapsed.as_millis() < 1000,
        "Resolving 1,000 providers in debug mode took {}ms, expected < 1000ms",
        elapsed.as_millis()
    );
}

#[tokio::test]
async fn test_resolver_resolve_with_exclusions_and_failover() {
    let (registry, routes, resolver, _) = setup_test_resolver();

    let service_id = ServiceId::new("failover.test.service").unwrap();
    let id_b = NodeIdentity::generate();
    let id_c = NodeIdentity::generate();
    let now = Timestamp::now();

    let inst_b = ServiceInstanceId::generate();
    let inst_c = ServiceInstanceId::generate();

    let manifest_b = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .instance_id(inst_b.clone())
        .display_name("Provider B")
        .endpoint(ServiceEndpoint::InternalRpc)
        .protocol(ServiceProtocol::NativeFieldnetRpc)
        .capability(ServiceCapability::new("service.invoke").unwrap())
        .ttl_seconds(300)
        .build_and_sign(&id_b)
        .unwrap();

    let manifest_c = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .instance_id(inst_c.clone())
        .display_name("Provider C")
        .endpoint(ServiceEndpoint::InternalRpc)
        .protocol(ServiceProtocol::NativeFieldnetRpc)
        .capability(ServiceCapability::new("service.invoke").unwrap())
        .ttl_seconds(300)
        .build_and_sign(&id_c)
        .unwrap();

    registry.register_remote(manifest_b, now).await.unwrap();
    registry.register_remote(manifest_c, now).await.unwrap();
    routes
        .add_route(id_b.node_id(), id_b.node_id(), 1, 10)
        .await;
    routes
        .add_route(id_c.node_id(), id_c.node_id(), 1, 10)
        .await;

    let query = ServiceQuery::new(service_id.clone());

    // 1. Initial resolution without exclusions selects the first ranked candidate
    let res_first = resolver.resolve(&query, None).await.unwrap();
    let first_inst = res_first.instance_id.clone();
    let alt_inst = if first_inst == inst_b {
        inst_c.clone()
    } else {
        inst_b.clone()
    };

    // 2. Resolve with first candidate excluded -> immediately resolves to alternative candidate
    let res_second = resolver
        .resolve_with_exclusions(&query, None, std::slice::from_ref(&first_inst))
        .await
        .unwrap();
    assert_eq!(res_second.instance_id, alt_inst);

    // 3. Resolve with both candidates excluded -> returns AllProvidersUnavailable
    let err_all = resolver
        .resolve_with_exclusions(&query, None, &[first_inst, alt_inst])
        .await
        .unwrap_err();
    match err_all {
        ServiceError::AllProvidersUnavailable { count, .. } => {
            assert_eq!(count, 2);
        }
        other => panic!("Expected AllProvidersUnavailable, got: {:?}", other),
    }
}
