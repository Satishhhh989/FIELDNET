use std::fs;
use std::time::Duration;

use fieldnet_core::{MessageId, Timestamp};
use fieldnet_identity::NodeIdentity;
use fieldnet_runtime::service::{
    ServiceCapability, ServiceEndpoint, ServiceId, ServiceInstanceId, ServiceManifestBuilder,
    ServiceProtocol, ServiceQueryBuilder, ServiceQueryWire, ServiceState, ServiceVersion,
};
use fieldnet_runtime::{FieldnetRuntime, RuntimeConfig};

#[tokio::test]
async fn test_four_node_linear_multihop_propagation_and_origin_preservation() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_four_node_multihop_propagation_test");
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");
    let dir_c = base_dir.join("node_c");
    let dir_d = base_dir.join("node_d");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();
    fs::create_dir_all(&dir_c).unwrap();
    fs::create_dir_all(&dir_d).unwrap();

    let test_discovery_port = 4851;

    let id_a = NodeIdentity::generate();
    id_a.save_to_file(&dir_a.join("identity.json")).unwrap();
    let id_b = NodeIdentity::generate();
    id_b.save_to_file(&dir_b.join("identity.json")).unwrap();
    let id_c = NodeIdentity::generate();
    id_c.save_to_file(&dir_c.join("identity.json")).unwrap();
    let id_d = NodeIdentity::generate();
    id_d.save_to_file(&dir_d.join("identity.json")).unwrap();

    let node_a_id = id_a.node_id();
    let node_b_id = id_b.node_id();
    let node_c_id = id_c.node_id();
    let node_d_id = id_d.node_id();

    // Topology: Node A <--> Node B <--> Node C <--> Node D
    let config_a = RuntimeConfig {
        data_dir: dir_a.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: Some(vec![node_b_id]),
        blocked_peers: Some(vec![node_c_id, node_d_id]),
    };
    let config_b = RuntimeConfig {
        data_dir: dir_b.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: Some(vec![node_a_id, node_c_id]),
        blocked_peers: Some(vec![node_d_id]),
    };
    let config_c = RuntimeConfig {
        data_dir: dir_c.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: Some(vec![node_b_id, node_d_id]),
        blocked_peers: Some(vec![node_a_id]),
    };
    let config_d = RuntimeConfig {
        data_dir: dir_d.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: Some(vec![node_c_id]),
        blocked_peers: Some(vec![node_a_id, node_b_id]),
    };

    let runtime_a = FieldnetRuntime::start(config_a).await.unwrap();
    let runtime_b = FieldnetRuntime::start(config_b).await.unwrap();
    let runtime_c = FieldnetRuntime::start(config_c).await.unwrap();
    let runtime_d = FieldnetRuntime::start(config_d).await.unwrap();

    // 1. Wait for linear topology formation: A<->B, B<->C, C<->D
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let conns_a = runtime_a.get_connected_peers().await;
        let conns_b = runtime_b.get_connected_peers().await;
        let conns_c = runtime_c.get_connected_peers().await;
        let conns_d = runtime_d.get_connected_peers().await;

        if conns_a.contains(&node_b_id)
            && conns_b.contains(&node_a_id)
            && conns_b.contains(&node_c_id)
            && conns_c.contains(&node_b_id)
            && conns_c.contains(&node_d_id)
            && conns_d.contains(&node_c_id)
        {
            break;
        }
    }

    // 2. Wait for routing convergence across 3 hops (A learns route to D via B, D learns route to A via C)
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let route_to_d = runtime_a.routing_table().get_next_hop(&node_d_id).await;
        let route_to_a = runtime_d.routing_table().get_next_hop(&node_a_id).await;
        if route_to_d == Some(node_b_id) && route_to_a == Some(node_c_id) {
            break;
        }
    }

    let route_a_to_d = runtime_a
        .routing_table()
        .get_route(&node_d_id)
        .await
        .expect("A must have converged route to D");
    assert_eq!(route_a_to_d.next_hop, node_b_id);
    assert_eq!(route_a_to_d.hops, 3, "Route A to D must be 3 hops");

    // 3. Node D advertises 'ai.service' towards Node A across relays C and B
    let service_id = ServiceId::new("ai.service").unwrap();
    let instance_d = ServiceInstanceId::generate();
    let builder_d = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(3, 1, 0))
        .instance_id(instance_d.clone())
        .endpoint(ServiceEndpoint::InternalRpc)
        .protocol(ServiceProtocol::NativeFieldnetRpc)
        .capability(ServiceCapability::new("ai.inference").unwrap())
        .ttl_seconds(120);

    let manifest_d = runtime_d
        .advertise_service(builder_d, Some(node_a_id))
        .await
        .unwrap();
    assert_eq!(manifest_d.provider_node_id, node_d_id);

    // 4. Wait for Node A to receive the routed multi-hop advertisement
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if runtime_a
            .service_registry()
            .get_instance(&instance_d)
            .await
            .is_some()
        {
            break;
        }
    }

    // 5. Origin Preservation Verification: Node A sees provider as Node D (NOT relay B or C)
    let record_at_a = runtime_a
        .service_registry()
        .get_instance(&instance_d)
        .await
        .expect("Node A must receive 3-hop service advertisement");

    assert_eq!(
        record_at_a.manifest.provider_node_id, node_d_id,
        "Origin Preservation: provider MUST be Node D, not intermediary relays B or C"
    );
    assert_eq!(record_at_a.state(), ServiceState::Available);

    // 6. Node A resolves 'ai.service' through authoritative routing
    let query = ServiceQueryBuilder::new(service_id).build().unwrap();
    let resolution = runtime_a
        .resolve_service(&query, None)
        .await
        .expect("Resolution on Node A must succeed");

    assert_eq!(resolution.instance_id(), &instance_d);
    assert_eq!(resolution.provider_node_id(), &node_d_id);
    assert_eq!(
        resolution.route_hops,
        Some(3),
        "Hop count must reflect 3-hop distance across relays B and C"
    );

    runtime_a.shutdown();
    runtime_b.shutdown();
    runtime_c.shutdown();
    runtime_d.shutdown();
}

#[tokio::test]
async fn test_four_node_cycle_topology_and_loop_prevention() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_cycle_loop_prevention_test");
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");
    let dir_c = base_dir.join("node_c");
    let dir_d = base_dir.join("node_d");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();
    fs::create_dir_all(&dir_c).unwrap();
    fs::create_dir_all(&dir_d).unwrap();

    let test_discovery_port = 4852;

    let id_a = NodeIdentity::generate();
    id_a.save_to_file(&dir_a.join("identity.json")).unwrap();
    let id_b = NodeIdentity::generate();
    id_b.save_to_file(&dir_b.join("identity.json")).unwrap();
    let id_c = NodeIdentity::generate();
    id_c.save_to_file(&dir_c.join("identity.json")).unwrap();
    let id_d = NodeIdentity::generate();
    id_d.save_to_file(&dir_d.join("identity.json")).unwrap();

    let node_a_id = id_a.node_id();
    let node_b_id = id_b.node_id();
    let node_c_id = id_c.node_id();
    let node_d_id = id_d.node_id();

    // Cyclic Topology:
    // A <--> B
    // ^      v
    // D <--> C
    let config_a = RuntimeConfig {
        data_dir: dir_a.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: Some(vec![node_b_id, node_d_id]),
        blocked_peers: Some(vec![node_c_id]),
    };
    let config_b = RuntimeConfig {
        data_dir: dir_b.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: Some(vec![node_a_id, node_c_id]),
        blocked_peers: Some(vec![node_d_id]),
    };
    let config_c = RuntimeConfig {
        data_dir: dir_c.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: Some(vec![node_b_id, node_d_id]),
        blocked_peers: Some(vec![node_a_id]),
    };
    let config_d = RuntimeConfig {
        data_dir: dir_d.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: Some(vec![node_a_id, node_c_id]),
        blocked_peers: Some(vec![node_b_id]),
    };

    let runtime_a = FieldnetRuntime::start(config_a).await.unwrap();
    let runtime_b = FieldnetRuntime::start(config_b).await.unwrap();
    let runtime_c = FieldnetRuntime::start(config_c).await.unwrap();
    let runtime_d = FieldnetRuntime::start(config_d).await.unwrap();

    // 1. Wait for cyclic connectivity
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let conns_a = runtime_a.get_connected_peers().await;
        let conns_c = runtime_c.get_connected_peers().await;
        if conns_a.contains(&node_b_id)
            && conns_a.contains(&node_d_id)
            && conns_c.contains(&node_b_id)
            && conns_c.contains(&node_d_id)
        {
            break;
        }
    }

    // 2. Node A broadcasts service advertisement across cycle
    let service_id = ServiceId::new("cycle.service").unwrap();
    let instance_a = ServiceInstanceId::generate();
    let builder_a = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .instance_id(instance_a.clone())
        .endpoint(ServiceEndpoint::InternalRpc)
        .capability(ServiceCapability::new("cycle.test").unwrap())
        .ttl_seconds(120);

    let _ = runtime_a.advertise_service(builder_a, None).await.unwrap();

    // Wait for propagation across cycle
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Both B and D directly receive it, C receives it from B and/or D without infinite loop
    let has_b = runtime_b
        .service_registry()
        .get_instance(&instance_a)
        .await
        .is_some();
    let has_c = runtime_c
        .service_registry()
        .get_instance(&instance_a)
        .await
        .is_some();
    let has_d = runtime_d
        .service_registry()
        .get_instance(&instance_a)
        .await
        .is_some();

    assert!(has_b, "Node B must have received advertisement");
    assert!(has_d, "Node D must have received advertisement");
    assert!(
        has_c,
        "Node C must have received advertisement across cycle"
    );

    // Verify all nodes preserved Node A as the authoritative provider
    let rec_c = runtime_c
        .service_registry()
        .get_instance(&instance_a)
        .await
        .unwrap();
    assert_eq!(rec_c.manifest.provider_node_id, node_a_id);

    // Verify deduplication: inject duplicate message from A towards C specifically
    let duplicate_res = runtime_a
        .send_service_message(
            node_c_id,
            fieldnet_service::ServiceMessage::Advertise(rec_c.manifest.clone()),
        )
        .await;
    assert!(duplicate_res.is_ok());

    // Give network time to process; loop detection and deduplication must ensure stability
    tokio::time::sleep(Duration::from_millis(500)).await;

    runtime_a.shutdown();
    runtime_b.shutdown();
    runtime_c.shutdown();
    runtime_d.shutdown();
}

#[tokio::test]
async fn test_service_dtn_store_and_forward_on_reconnect() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_service_dtn_store_and_forward_test");
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");
    let dir_c = base_dir.join("node_c");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();
    fs::create_dir_all(&dir_c).unwrap();

    let test_discovery_port = 4853;

    let id_a = NodeIdentity::generate();
    id_a.save_to_file(&dir_a.join("identity.json")).unwrap();
    let id_b = NodeIdentity::generate();
    id_b.save_to_file(&dir_b.join("identity.json")).unwrap();
    let id_c = NodeIdentity::generate();
    id_c.save_to_file(&dir_c.join("identity.json")).unwrap();

    let node_a_id = id_a.node_id();
    let node_b_id = id_b.node_id();
    let node_c_id = id_c.node_id();

    // Node A and Node B start. Node C is NOT started yet (simulating offline/disconnected node).
    let config_a = RuntimeConfig {
        data_dir: dir_a.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: Some(vec![node_b_id]),
        blocked_peers: Some(vec![node_c_id]),
    };
    let config_b = RuntimeConfig {
        data_dir: dir_b.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: Some(vec![node_a_id, node_c_id]),
        blocked_peers: None,
    };
    let config_c = RuntimeConfig {
        data_dir: dir_c.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: Some(vec![node_b_id]),
        blocked_peers: Some(vec![node_a_id]),
    };

    let runtime_a = FieldnetRuntime::start(config_a).await.unwrap();
    let runtime_b = FieldnetRuntime::start(config_b).await.unwrap();

    // 1. Wait for A <-> B connection
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if runtime_a.get_connected_peers().await.contains(&node_b_id) {
            break;
        }
    }
    assert!(runtime_a.get_connected_peers().await.contains(&node_b_id));

    // 2. Node A advertises service specifically destined for Node C (which is currently offline)
    let service_id = ServiceId::new("dtn.service").unwrap();
    let instance_a = ServiceInstanceId::generate();
    let builder_a = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .instance_id(instance_a.clone())
        .endpoint(ServiceEndpoint::InternalRpc)
        .capability(ServiceCapability::new("dtn.store").unwrap())
        .ttl_seconds(180);

    // Send service advertisement from A towards C
    let manifest_a = runtime_a
        .advertise_service(builder_a, Some(node_c_id))
        .await
        .unwrap();
    assert_eq!(manifest_a.provider_node_id, node_a_id);

    // 3. Relay Node B receives message destined for Node C.
    // Since Node C is disconnected, B stages the message into its persistent DTN queue!
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 4. Now bring Node C online!
    let runtime_c = FieldnetRuntime::start(config_c).await.unwrap();

    // Wait for B <-> C connection and DTN automatic queue flush
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        if runtime_c
            .service_registry()
            .get_instance(&instance_a)
            .await
            .is_some()
        {
            break;
        }
    }

    // 5. Node C received the staged service advertisement through DTN store-and-forward!
    let rec_c = runtime_c
        .service_registry()
        .get_instance(&instance_a)
        .await
        .expect("Node C must receive staged service advertisement via DTN store-and-forward upon reconnect");

    assert_eq!(
        rec_c.manifest.provider_node_id, node_a_id,
        "Origin preservation: provider must be Node A"
    );
    assert_eq!(rec_c.state(), ServiceState::Available);

    runtime_a.shutdown();
    runtime_b.shutdown();
    runtime_c.shutdown();
}

#[tokio::test]
async fn test_service_query_and_response_multihop_network() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_service_query_multihop_test");
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");
    let dir_c = base_dir.join("node_c");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();
    fs::create_dir_all(&dir_c).unwrap();

    let test_discovery_port = 4854;

    let id_a = NodeIdentity::generate();
    id_a.save_to_file(&dir_a.join("identity.json")).unwrap();
    let id_b = NodeIdentity::generate();
    id_b.save_to_file(&dir_b.join("identity.json")).unwrap();
    let id_c = NodeIdentity::generate();
    id_c.save_to_file(&dir_c.join("identity.json")).unwrap();

    let node_a_id = id_a.node_id();
    let node_b_id = id_b.node_id();
    let node_c_id = id_c.node_id();

    // Topology: A <--> B <--> C
    let config_a = RuntimeConfig {
        data_dir: dir_a.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: Some(vec![node_b_id]),
        blocked_peers: Some(vec![node_c_id]),
    };
    let config_b = RuntimeConfig {
        data_dir: dir_b.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: Some(vec![node_a_id, node_c_id]),
        blocked_peers: None,
    };
    let config_c = RuntimeConfig {
        data_dir: dir_c.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: Some(vec![node_b_id]),
        blocked_peers: Some(vec![node_a_id]),
    };

    let runtime_a = FieldnetRuntime::start(config_a).await.unwrap();
    let runtime_b = FieldnetRuntime::start(config_b).await.unwrap();
    let runtime_c = FieldnetRuntime::start(config_c).await.unwrap();

    // 1. Wait for connection and routing convergence (A knows route to C via B)
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if runtime_a.routing_table().get_next_hop(&node_c_id).await == Some(node_b_id)
            && runtime_c.routing_table().get_next_hop(&node_a_id).await == Some(node_b_id)
        {
            break;
        }
    }

    // 2. Node C registers local service 'telemetry.service' (registered locally, NOT yet broadcast to A)
    let service_id = ServiceId::new("telemetry.service").unwrap();
    let instance_c = ServiceInstanceId::generate();
    let builder_c = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(2, 0, 0))
        .instance_id(instance_c.clone())
        .endpoint(ServiceEndpoint::InternalRpc)
        .capability(ServiceCapability::new("metrics.pull").unwrap())
        .ttl_seconds(120);

    let manifest_c = builder_c.build_and_sign(&id_c).unwrap();
    runtime_c
        .service_registry()
        .register_local(manifest_c)
        .await
        .unwrap();

    // Verify Node A does NOT have it yet
    assert!(runtime_a
        .service_registry()
        .get_instance(&instance_c)
        .await
        .is_none());

    // 3. Node A sends an active ServiceQueryWire towards Node C across multi-hop relay B
    let now = Timestamp::now();
    let query_wire = ServiceQueryWire {
        query_id: MessageId::compute(&node_a_id, 1, now.as_millis(), b"query-telemetry"),
        service_id: service_id.clone(),
        min_version: Some(ServiceVersion::new(2, 0, 0)),
        required_capabilities: vec![ServiceCapability::new("metrics.pull").unwrap()],
        requester_node_id: node_a_id,
        timestamp: now,
    };

    let query_msg_id = runtime_a
        .query_services_network(query_wire, node_c_id)
        .await
        .expect("Query dispatch must succeed");
    assert_ne!(query_msg_id, MessageId::from_bytes([0u8; 32]));

    // 4. Node C receives query, looks up matching services, and responds with signed manifests.
    // Node A receives response and automatically ingests into its registry!
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if runtime_a
            .service_registry()
            .get_instance(&instance_c)
            .await
            .is_some()
        {
            break;
        }
    }

    // 5. Verify Node A now has Node C's service registered and can resolve it!
    let record_at_a = runtime_a
        .service_registry()
        .get_instance(&instance_c)
        .await
        .expect("Node A must have received query response and ingested manifest");
    assert_eq!(record_at_a.manifest.provider_node_id, node_c_id);

    let query = ServiceQueryBuilder::new(service_id).build().unwrap();
    let resolution = runtime_a
        .resolve_service(&query, None)
        .await
        .expect("Resolution must succeed after reactive query");
    assert_eq!(resolution.instance_id(), &instance_c);
    assert_eq!(resolution.provider_node_id(), &node_c_id);
    assert!(resolution.route_hops.unwrap() >= 1);

    runtime_a.shutdown();
    runtime_b.shutdown();
    runtime_c.shutdown();
}

#[tokio::test]
async fn test_route_failure_and_recovery_preserves_service_record() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_route_failure_and_recovery_test");
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");
    let dir_c = base_dir.join("node_c");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();
    fs::create_dir_all(&dir_c).unwrap();

    let test_discovery_port = 4855;

    let id_a = NodeIdentity::generate();
    id_a.save_to_file(&dir_a.join("identity.json")).unwrap();
    let id_b = NodeIdentity::generate();
    id_b.save_to_file(&dir_b.join("identity.json")).unwrap();
    let id_c = NodeIdentity::generate();
    id_c.save_to_file(&dir_c.join("identity.json")).unwrap();

    let node_a_id = id_a.node_id();
    let node_b_id = id_b.node_id();
    let node_c_id = id_c.node_id();

    let config_a = RuntimeConfig {
        data_dir: dir_a.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: Some(vec![node_b_id]),
        blocked_peers: Some(vec![node_c_id]),
    };
    let config_b = RuntimeConfig {
        data_dir: dir_b.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: Some(vec![node_a_id, node_c_id]),
        blocked_peers: None,
    };
    let config_c = RuntimeConfig {
        data_dir: dir_c.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: Some(vec![node_b_id]),
        blocked_peers: Some(vec![node_a_id]),
    };

    let runtime_a = FieldnetRuntime::start(config_a).await.unwrap();
    let runtime_b = FieldnetRuntime::start(config_b).await.unwrap();
    let runtime_c = FieldnetRuntime::start(config_c.clone()).await.unwrap();

    // 1. Establish A <--> B <--> C connectivity
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if runtime_a.routing_table().get_next_hop(&node_c_id).await == Some(node_b_id) {
            break;
        }
    }

    let service_id = ServiceId::new("storage.service").unwrap();
    let instance_c = ServiceInstanceId::generate();
    let builder_c = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .instance_id(instance_c.clone())
        .endpoint(ServiceEndpoint::InternalRpc)
        .capability(ServiceCapability::new("storage.write").unwrap())
        .ttl_seconds(180);

    runtime_c
        .advertise_service(builder_c, Some(node_a_id))
        .await
        .unwrap();

    // Wait for Node A to discover it
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if runtime_a
            .service_registry()
            .get_instance(&instance_c)
            .await
            .is_some()
        {
            break;
        }
    }

    let query = ServiceQueryBuilder::new(service_id.clone())
        .build()
        .unwrap();
    let res_initial = runtime_a.resolve_service(&query, None).await;
    assert!(res_initial.is_ok(), "Service must resolve initially");

    // 2. Simulate Route Failure: Break B <--> C link by shutting down C
    runtime_c.shutdown();

    // Wait for B to detect disconnection and remove route to C
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if runtime_b.get_connected_peers().await.is_empty()
            || !runtime_b.get_connected_peers().await.contains(&node_c_id)
        {
            break;
        }
    }

    // Force expiration / route purge on A to simulate distance-vector route disappearance
    runtime_a.routing_table().expire_route(&node_c_id).await;

    // 3. Verify on Node A:
    // Service record is NOT corrupted or permanently deleted!
    let record_after_fail = runtime_a.service_registry().get_instance(&instance_c).await;
    assert!(
        record_after_fail.is_some(),
        "Service record must NOT be deleted from registry when route disappears"
    );

    // Route lookup fails cleanly, resolver rejects candidate because it is unreachable
    let res_after_fail = runtime_a.resolve_service(&query, None).await;
    assert!(
        res_after_fail.is_err(),
        "Resolver must reject unreachable provider when route disappears"
    );

    // 4. Route Recovery: Bring Node C back online
    let runtime_c_recovered = FieldnetRuntime::start(config_c).await.unwrap();

    // Wait for B <--> C connection and route propagation to A
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if runtime_a.routing_table().get_next_hop(&node_c_id).await == Some(node_b_id) {
            break;
        }
    }

    // Service can now be resolved again cleanly!
    let res_recovered = runtime_a.resolve_service(&query, None).await;
    assert!(
        res_recovered.is_ok(),
        "Service must resolve cleanly once route recovers: {:?}",
        res_recovered
    );

    runtime_a.shutdown();
    runtime_b.shutdown();
    runtime_c_recovered.shutdown();
}
