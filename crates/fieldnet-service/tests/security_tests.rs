use fieldnet_core::{MessageId, NodeId, Timestamp};
use fieldnet_identity::NodeIdentity;
use fieldnet_service::{
    RegistryConfig, ServiceAdvertisementHandler, ServiceCapability, ServiceEndpoint, ServiceError,
    ServiceId, ServiceInstanceId, ServiceManifest, ServiceManifestBuilder, ServiceMessage,
    ServiceProtocol, ServiceQueryResponseWire, ServiceQueryWire, ServiceRegistry, ServiceVersion,
    ServiceWithdrawal,
};
use std::sync::Arc;
use tokio::sync::broadcast;

fn create_valid_manifest(
    identity: &NodeIdentity,
    service_id_str: &str,
    seq: u64,
    ttl_secs: u32,
) -> ServiceManifest {
    let service_id = ServiceId::new(service_id_str).unwrap();
    let instance_id = ServiceInstanceId::generate();
    ServiceManifestBuilder::new(service_id, ServiceVersion::new(1, 0, 0))
        .instance_id(instance_id)
        .endpoint(ServiceEndpoint::InternalRpc)
        .protocol(ServiceProtocol::NativeFieldnetRpc)
        .sequence(seq)
        .ttl_seconds(ttl_secs)
        .capability(ServiceCapability::new("data.read").unwrap())
        .build_and_sign(identity)
        .expect("Manifest must sign cleanly")
}

#[tokio::test]
async fn test_security_01_valid_signed_advertisement() {
    let (event_tx, _event_rx) = broadcast::channel(64);
    let registry =
        ServiceRegistry::open_in_memory(RegistryConfig::default(), event_tx.clone()).unwrap();
    let handler = ServiceAdvertisementHandler::new(
        NodeId::from_bytes([0xAA; 32]),
        Arc::clone(&registry),
        event_tx,
    );

    let identity = NodeIdentity::generate();
    let manifest = create_valid_manifest(&identity, "files.service", 1, 120);
    let now = Timestamp::now();

    let res = handler
        .handle_inbound_message(
            &identity.node_id(),
            ServiceMessage::Advertise(manifest.clone()),
            now,
        )
        .await;

    assert!(
        res.is_ok(),
        "Valid advertisement must be accepted: {:?}",
        res
    );
    let record = registry.get_instance(&manifest.instance_id).await;
    assert!(record.is_some(), "Service instance must be registered");
    assert_eq!(
        record.unwrap().manifest.provider_node_id,
        identity.node_id()
    );
}

#[tokio::test]
async fn test_security_02_invalid_signature() {
    let (event_tx, _event_rx) = broadcast::channel(64);
    let registry =
        ServiceRegistry::open_in_memory(RegistryConfig::default(), event_tx.clone()).unwrap();
    let handler = ServiceAdvertisementHandler::new(
        NodeId::from_bytes([0xAA; 32]),
        Arc::clone(&registry),
        event_tx,
    );

    let identity = NodeIdentity::generate();
    let mut manifest = create_valid_manifest(&identity, "files.service", 1, 120);
    let now = Timestamp::now();

    // Corrupt the signature
    manifest.signature[0] ^= 0xFF;

    let res = handler
        .handle_inbound_message(
            &identity.node_id(),
            ServiceMessage::Advertise(manifest.clone()),
            now,
        )
        .await;

    assert!(matches!(res, Err(ServiceError::InvalidSignature(_))));
    assert!(registry.get_instance(&manifest.instance_id).await.is_none());
}

#[tokio::test]
async fn test_security_03_wrong_provider_node_id() {
    let (event_tx, _event_rx) = broadcast::channel(64);
    let registry =
        ServiceRegistry::open_in_memory(RegistryConfig::default(), event_tx.clone()).unwrap();
    let handler = ServiceAdvertisementHandler::new(
        NodeId::from_bytes([0xAA; 32]),
        Arc::clone(&registry),
        event_tx,
    );

    let identity_actual = NodeIdentity::generate();
    let identity_imposter = NodeIdentity::generate();
    let manifest = create_valid_manifest(&identity_actual, "files.service", 1, 120);
    let now = Timestamp::now();

    // Imposter peer forwards or claims to be sender
    let res = handler
        .handle_inbound_message(
            &identity_imposter.node_id(),
            ServiceMessage::Advertise(manifest.clone()),
            now,
        )
        .await;

    assert!(matches!(
        res,
        Err(ServiceError::MismatchedProviderKey { .. })
    ));
    assert!(registry.get_instance(&manifest.instance_id).await.is_none());
}

#[tokio::test]
async fn test_security_04_expired_advertisement() {
    let (event_tx, _event_rx) = broadcast::channel(64);
    let registry =
        ServiceRegistry::open_in_memory(RegistryConfig::default(), event_tx.clone()).unwrap();
    let handler = ServiceAdvertisementHandler::new(
        NodeId::from_bytes([0xAA; 32]),
        Arc::clone(&registry),
        event_tx,
    );

    let identity = NodeIdentity::generate();
    let manifest = create_valid_manifest(&identity, "files.service", 1, 60);

    // Simulated time is 100 seconds in the future
    let future_time = manifest.expires_at.add_seconds(10);

    let res = handler
        .handle_inbound_message(
            &identity.node_id(),
            ServiceMessage::Advertise(manifest.clone()),
            future_time,
        )
        .await;

    assert!(matches!(res, Err(ServiceError::InvalidExpiration { .. })));
    assert!(registry.get_instance(&manifest.instance_id).await.is_none());
}

#[tokio::test]
async fn test_security_05_malformed_advertisement() {
    let malformed_bytes = b"{\"not_a_valid_service_manifest\": true}";
    let res = ServiceMessage::from_bytes(malformed_bytes);
    assert!(res.is_err(), "Malformed payload must fail deserialization");
}

#[tokio::test]
async fn test_security_06_replayed_advertisement() {
    let (event_tx, _event_rx) = broadcast::channel(64);
    let registry =
        ServiceRegistry::open_in_memory(RegistryConfig::default(), event_tx.clone()).unwrap();
    let handler = ServiceAdvertisementHandler::new(
        NodeId::from_bytes([0xAA; 32]),
        Arc::clone(&registry),
        event_tx,
    );

    let identity = NodeIdentity::generate();
    let manifest = create_valid_manifest(&identity, "files.service", 1, 120);
    let now = Timestamp::now();

    // First ingestion succeeds
    let res1 = handler
        .handle_inbound_message(
            &identity.node_id(),
            ServiceMessage::Advertise(manifest.clone()),
            now,
        )
        .await;
    assert!(res1.is_ok());

    // Replayed ingestion with identical sequence is suppressed
    let res2 = handler
        .handle_inbound_message(
            &identity.node_id(),
            ServiceMessage::Advertise(manifest.clone()),
            now,
        )
        .await;
    assert!(res2.is_ok());
    assert!(
        res2.unwrap().is_none(),
        "Duplicate was suppressed without error"
    );
}

#[tokio::test]
async fn test_security_07_stale_update() {
    let (event_tx, _event_rx) = broadcast::channel(64);
    let registry =
        ServiceRegistry::open_in_memory(RegistryConfig::default(), event_tx.clone()).unwrap();
    let handler = ServiceAdvertisementHandler::new(
        NodeId::from_bytes([0xAA; 32]),
        Arc::clone(&registry),
        event_tx,
    );

    let identity = NodeIdentity::generate();
    let service_id = ServiceId::new("files.service").unwrap();
    let instance_id = ServiceInstanceId::generate();
    let now = Timestamp::now();

    // Ingest seq 5
    let manifest_v5 = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .instance_id(instance_id.clone())
        .endpoint(ServiceEndpoint::InternalRpc)
        .sequence(5)
        .build_and_sign(&identity)
        .unwrap();

    let res = handler
        .handle_inbound_message(
            &identity.node_id(),
            ServiceMessage::Advertise(manifest_v5),
            now,
        )
        .await;
    assert!(res.is_ok());

    // Now try to inject stale seq 3 for the same instance
    let manifest_v3 = ServiceManifestBuilder::new(service_id, ServiceVersion::new(1, 0, 0))
        .instance_id(instance_id.clone())
        .endpoint(ServiceEndpoint::InternalRpc)
        .sequence(3)
        .build_and_sign(&identity)
        .unwrap();

    let res_stale = handler
        .handle_inbound_message(
            &identity.node_id(),
            ServiceMessage::Advertise(manifest_v3),
            now,
        )
        .await;

    assert!(matches!(res_stale, Err(ServiceError::StaleSequence { .. })));
    let current = registry.get_instance(&instance_id).await.unwrap();
    assert_eq!(
        current.manifest.sequence, 5,
        "Registry must retain newer sequence 5"
    );
}

#[tokio::test]
async fn test_security_08_conflicting_instance_id() {
    let (event_tx, _event_rx) = broadcast::channel(64);
    let registry =
        ServiceRegistry::open_in_memory(RegistryConfig::default(), event_tx.clone()).unwrap();
    let handler = ServiceAdvertisementHandler::new(
        NodeId::from_bytes([0xAA; 32]),
        Arc::clone(&registry),
        event_tx,
    );

    let identity_a = NodeIdentity::generate();
    let identity_b = NodeIdentity::generate();
    let service_id = ServiceId::new("files.service").unwrap();
    let instance_id = ServiceInstanceId::generate();
    let now = Timestamp::now();

    // Identity A registers instance
    let manifest_a = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .instance_id(instance_id.clone())
        .endpoint(ServiceEndpoint::InternalRpc)
        .build_and_sign(&identity_a)
        .unwrap();

    let res_a = handler
        .handle_inbound_message(
            &identity_a.node_id(),
            ServiceMessage::Advertise(manifest_a),
            now,
        )
        .await;
    assert!(res_a.is_ok());

    // Identity B attempts to overwrite with the same instance_id with incremented sequence
    let manifest_b = ServiceManifestBuilder::new(service_id, ServiceVersion::new(1, 0, 0))
        .instance_id(instance_id.clone())
        .endpoint(ServiceEndpoint::InternalRpc)
        .sequence(2)
        .build_and_sign(&identity_b)
        .unwrap();

    let res_b = handler
        .handle_inbound_message(
            &identity_b.node_id(),
            ServiceMessage::Advertise(manifest_b),
            now,
        )
        .await;

    assert!(matches!(
        res_b,
        Err(ServiceError::ConflictingProvider { .. })
    ));
    let current = registry.get_instance(&instance_id).await.unwrap();
    assert_eq!(current.manifest.provider_node_id, identity_a.node_id());
}

#[tokio::test]
async fn test_security_09_unauthorized_provider_and_withdrawal_hijack() {
    let (event_tx, _event_rx) = broadcast::channel(64);
    let registry =
        ServiceRegistry::open_in_memory(RegistryConfig::default(), event_tx.clone()).unwrap();
    let handler = ServiceAdvertisementHandler::new(
        NodeId::from_bytes([0xAA; 32]),
        Arc::clone(&registry),
        event_tx,
    );

    let identity_owner = NodeIdentity::generate();
    let identity_attacker = NodeIdentity::generate();
    let manifest = create_valid_manifest(&identity_owner, "files.service", 1, 120);
    let now = Timestamp::now();

    // Ingest legitimate owner's service
    handler
        .handle_inbound_message(
            &identity_owner.node_id(),
            ServiceMessage::Advertise(manifest.clone()),
            now,
        )
        .await
        .unwrap();

    // 1. Attacker tries to withdraw owner's service from attacker's peer connection
    let mut attacker_withdrawal = ServiceWithdrawal {
        service_id: manifest.service_id.clone(),
        instance_id: manifest.instance_id.clone(),
        provider_node_id: identity_owner.node_id(), // claims owner ID
        sequence: 2,
        timestamp: now,
        signature: [0u8; 64],
    };
    // Sign with attacker key
    let signing_bytes = attacker_withdrawal.canonical_signing_bytes();
    attacker_withdrawal.signature = identity_attacker.sign(&signing_bytes);

    let res_mismatch = handler
        .handle_inbound_message(
            &identity_attacker.node_id(),
            ServiceMessage::Withdraw(attacker_withdrawal.clone()),
            now,
        )
        .await;
    assert!(matches!(
        res_mismatch,
        Err(ServiceError::MismatchedProviderKey { .. })
    ));

    // 2. Even if attacker forged the sender peer id header to match owner, signature verification fails against cached owner key
    let res_sig = handler
        .handle_inbound_message(
            &identity_owner.node_id(),
            ServiceMessage::Withdraw(attacker_withdrawal),
            now,
        )
        .await;
    assert!(matches!(res_sig, Err(ServiceError::InvalidSignature(_))));

    // Owner's service remains safe and unmolested
    assert!(registry.get_instance(&manifest.instance_id).await.is_some());
}

#[tokio::test]
async fn test_security_10_unauthenticated_peer_and_forged_fields() {
    let (event_tx, _event_rx) = broadcast::channel(64);
    let registry =
        ServiceRegistry::open_in_memory(RegistryConfig::default(), event_tx.clone()).unwrap();
    let handler = ServiceAdvertisementHandler::new(
        NodeId::from_bytes([0xAA; 32]),
        Arc::clone(&registry),
        event_tx,
    );

    let identity = NodeIdentity::generate();
    let now = Timestamp::now();

    // 11. Forged ServiceId
    let mut manifest1 = create_valid_manifest(&identity, "files.service", 1, 120);
    manifest1.service_id = ServiceId::new("admin.service").unwrap();
    let res1 = handler
        .handle_inbound_message(
            &identity.node_id(),
            ServiceMessage::Advertise(manifest1),
            now,
        )
        .await;
    assert!(matches!(res1, Err(ServiceError::InvalidSignature(_))));

    // 12. Forged capabilities
    let mut manifest2 = create_valid_manifest(&identity, "files.service", 1, 120);
    manifest2
        .capabilities
        .push(ServiceCapability::new("root.access").unwrap());
    let res2 = handler
        .handle_inbound_message(
            &identity.node_id(),
            ServiceMessage::Advertise(manifest2),
            now,
        )
        .await;
    assert!(matches!(res2, Err(ServiceError::InvalidSignature(_))));

    // 13. Forged endpoint
    let mut manifest3 = create_valid_manifest(&identity, "files.service", 1, 120);
    manifest3.endpoint = ServiceEndpoint::LocalPort(9999);
    let res3 = handler
        .handle_inbound_message(
            &identity.node_id(),
            ServiceMessage::Advertise(manifest3),
            now,
        )
        .await;
    assert!(matches!(res3, Err(ServiceError::InvalidSignature(_))));

    // 14. Forged expiration
    let mut manifest4 = create_valid_manifest(&identity, "files.service", 1, 120);
    manifest4.expires_at = manifest4.expires_at.add_seconds(86400); // 1 day longer
    let res4 = handler
        .handle_inbound_message(
            &identity.node_id(),
            ServiceMessage::Advertise(manifest4),
            now,
        )
        .await;
    assert!(matches!(res4, Err(ServiceError::InvalidSignature(_))));
}

#[tokio::test]
async fn test_security_11_forged_query_response() {
    let (event_tx, _event_rx) = broadcast::channel(64);
    let registry =
        ServiceRegistry::open_in_memory(RegistryConfig::default(), event_tx.clone()).unwrap();
    let local_node = NodeId::from_bytes([0xAA; 32]);
    let handler = ServiceAdvertisementHandler::new(local_node, Arc::clone(&registry), event_tx);

    let responder_id = NodeIdentity::generate();
    let imposter_id = NodeIdentity::generate();
    let legitimate_provider = NodeIdentity::generate();
    let manifest = create_valid_manifest(&legitimate_provider, "query.service", 1, 120);
    let now = Timestamp::now();

    // Imposter peer claims to respond on behalf of responder_id
    let response_wire = ServiceQueryResponseWire {
        query_id: MessageId::compute(&responder_id.node_id(), 1, now.as_millis(), b"q"),
        manifests: vec![manifest],
        responder_node_id: responder_id.node_id(), // claims to be responder_id
        timestamp: now,
    };

    let res = handler
        .handle_inbound_message(
            &imposter_id.node_id(), // but actual peer is imposter_id
            ServiceMessage::QueryResponse(response_wire),
            now,
        )
        .await;

    assert!(matches!(
        res,
        Err(ServiceError::MismatchedProviderKey { .. })
    ));
}

#[tokio::test]
async fn test_security_12_duplicate_and_expired_queries() {
    let (event_tx, _event_rx) = broadcast::channel(64);
    let registry =
        ServiceRegistry::open_in_memory(RegistryConfig::default(), event_tx.clone()).unwrap();
    let local_node = NodeId::from_bytes([0xAA; 32]);
    let handler = ServiceAdvertisementHandler::new(local_node, Arc::clone(&registry), event_tx);

    let requester = NodeIdentity::generate();
    let service_id = ServiceId::new("search.service").unwrap();
    let now = Timestamp::now();
    let query_id = MessageId::compute(&requester.node_id(), 1, now.as_millis(), b"search");

    // 1. Mismatched requester identity
    let imposter = NodeIdentity::generate();
    let query_mismatch = ServiceQueryWire {
        query_id,
        service_id: service_id.clone(),
        min_version: None,
        required_capabilities: vec![],
        requester_node_id: requester.node_id(),
        timestamp: now,
    };
    let res_mismatch = handler
        .handle_inbound_message(
            &imposter.node_id(),
            ServiceMessage::Query(query_mismatch),
            now,
        )
        .await;
    assert!(matches!(
        res_mismatch,
        Err(ServiceError::MismatchedProviderKey { .. })
    ));

    // 2. Expired query
    let old_query = ServiceQueryWire {
        query_id,
        service_id: service_id.clone(),
        min_version: None,
        required_capabilities: vec![],
        requester_node_id: requester.node_id(),
        timestamp: now.sub_seconds(100),
    };
    let res_expired = handler
        .handle_inbound_message(&requester.node_id(), ServiceMessage::Query(old_query), now)
        .await;
    assert!(matches!(res_expired, Err(ServiceError::InvalidQuery(_))));

    // 3. Duplicate query suppression
    let valid_query = ServiceQueryWire {
        query_id,
        service_id,
        min_version: None,
        required_capabilities: vec![],
        requester_node_id: requester.node_id(),
        timestamp: now,
    };
    let res1 = handler
        .handle_inbound_message(
            &requester.node_id(),
            ServiceMessage::Query(valid_query.clone()),
            now,
        )
        .await;
    assert!(res1.is_ok());

    let res2 = handler
        .handle_inbound_message(
            &requester.node_id(),
            ServiceMessage::Query(valid_query),
            now,
        )
        .await;
    assert!(res2.is_ok());
    assert_eq!(res2.unwrap(), None, "Duplicate query must be suppressed");
}
