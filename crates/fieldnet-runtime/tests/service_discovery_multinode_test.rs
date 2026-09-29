use std::fs;
use std::time::Duration;

use fieldnet_identity::NodeIdentity;
use fieldnet_runtime::service::{
    ServiceCapability, ServiceEndpoint, ServiceId, ServiceInstanceId, ServiceManifestBuilder,
    ServiceProtocol, ServiceQueryBuilder, ServiceState, ServiceVersion,
};
use fieldnet_runtime::{FieldnetRuntime, RuntimeConfig};

#[tokio::test]
async fn test_two_node_real_service_discovery_and_resolution() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_two_node_service_discovery_test");
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();

    let test_discovery_port = 4810;

    let id_a = NodeIdentity::generate();
    id_a.save_to_file(&dir_a.join("identity.json")).unwrap();
    let id_b = NodeIdentity::generate();
    id_b.save_to_file(&dir_b.join("identity.json")).unwrap();

    let node_a_id = id_a.node_id();
    let node_b_id = id_b.node_id();

    let config_a = RuntimeConfig {
        data_dir: dir_a.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: None,
        blocked_peers: None,
    };

    let config_b = RuntimeConfig {
        data_dir: dir_b.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: None,
        blocked_peers: None,
    };

    let runtime_a = FieldnetRuntime::start(config_a).await.unwrap();
    let runtime_b = FieldnetRuntime::start(config_b).await.unwrap();

    // 1. Wait for real TCP mutual connection
    let mut connected = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let peers_a = runtime_a.get_connected_peers().await;
        let peers_b = runtime_b.get_connected_peers().await;
        if peers_a.contains(&node_b_id) && peers_b.contains(&node_a_id) {
            connected = true;
            break;
        }
    }
    assert!(
        connected,
        "Nodes A and B must establish mutual TCP connection"
    );

    // 2. Node B advertises 'files.service'
    let service_id = ServiceId::new("files.service").unwrap();
    let instance_b = ServiceInstanceId::generate();
    let builder_b = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .instance_id(instance_b.clone())
        .display_name("Node B Distributed File Store")
        .endpoint(ServiceEndpoint::InternalRpc)
        .protocol(ServiceProtocol::NativeFieldnetRpc)
        .capability(ServiceCapability::new("files.read").unwrap())
        .capability(ServiceCapability::new("files.write").unwrap())
        .ttl_seconds(120);

    let manifest_b = runtime_b.advertise_service(builder_b, None).await.unwrap();
    assert_eq!(manifest_b.provider_node_id, node_b_id);

    // 3. Node A receives advertisement over the wire and registers it
    let mut discovered = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Some(record) = runtime_a.service_registry().get_instance(&instance_b).await {
            assert_eq!(record.manifest.provider_node_id, node_b_id);
            assert_eq!(record.state(), ServiceState::Available);
            discovered = true;
            break;
        }
    }
    assert!(
        discovered,
        "Node A must receive and register Node B's advertisement over TCP"
    );

    // 4. Node A resolves 'files.service'
    let query = ServiceQueryBuilder::new(service_id.clone())
        .min_version(ServiceVersion::new(1, 0, 0))
        .capability(ServiceCapability::new("files.read").unwrap())
        .build()
        .unwrap();

    let resolution = runtime_a
        .resolve_service(&query, None)
        .await
        .expect("Resolution must succeed");
    assert_eq!(resolution.instance_id(), &instance_b);
    assert_eq!(resolution.provider_node_id(), &node_b_id);
    assert_eq!(resolution.route_hops, Some(1));
    assert!(!resolution.is_local());

    runtime_a.shutdown();
    runtime_b.shutdown();
}

#[tokio::test]
async fn test_multi_provider_deterministic_resolution_and_failover() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_multi_provider_test");
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");
    let dir_c = base_dir.join("node_c");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();
    fs::create_dir_all(&dir_c).unwrap();

    let test_discovery_port = 4811;

    let id_a = NodeIdentity::generate();
    id_a.save_to_file(&dir_a.join("identity.json")).unwrap();
    let id_b = NodeIdentity::generate();
    id_b.save_to_file(&dir_b.join("identity.json")).unwrap();
    let id_c = NodeIdentity::generate();
    id_c.save_to_file(&dir_c.join("identity.json")).unwrap();

    let _node_a_id = id_a.node_id();
    let node_b_id = id_b.node_id();
    let node_c_id = id_c.node_id();

    let config_a = RuntimeConfig {
        data_dir: dir_a.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: None,
        blocked_peers: None,
    };
    let config_b = RuntimeConfig {
        data_dir: dir_b.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: None,
        blocked_peers: None,
    };
    let config_c = RuntimeConfig {
        data_dir: dir_c.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: None,
        blocked_peers: None,
    };

    let runtime_a = FieldnetRuntime::start(config_a).await.unwrap();
    let runtime_b = FieldnetRuntime::start(config_b).await.unwrap();
    let runtime_c = FieldnetRuntime::start(config_c).await.unwrap();

    // Wait for all 3 nodes to interconnect
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let peers_a = runtime_a.get_connected_peers().await;
        if peers_a.contains(&node_b_id) && peers_a.contains(&node_c_id) {
            break;
        }
    }

    // Both Node B and Node C provide 'compute.service'
    let service_id = ServiceId::new("compute.service").unwrap();
    let instance_b = ServiceInstanceId::generate();
    let instance_c = ServiceInstanceId::generate();

    runtime_b
        .advertise_service(
            ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
                .instance_id(instance_b.clone())
                .endpoint(ServiceEndpoint::InternalRpc)
                .capability(ServiceCapability::new("compute.run").unwrap()),
            None,
        )
        .await
        .unwrap();

    runtime_c
        .advertise_service(
            ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
                .instance_id(instance_c.clone())
                .endpoint(ServiceEndpoint::InternalRpc)
                .capability(ServiceCapability::new("compute.run").unwrap()),
            None,
        )
        .await
        .unwrap();

    // Wait for Node A to discover both instances
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let instances = runtime_a
            .service_registry()
            .get_service_instances(&service_id)
            .await;
        if instances.len() >= 2 {
            break;
        }
    }

    let instances = runtime_a
        .service_registry()
        .get_service_instances(&service_id)
        .await;
    assert_eq!(
        instances.len(),
        2,
        "Both Node B and Node C instances must coexist"
    );

    // Resolve query on Node A: Deterministic selection must pick one consistently
    let query = ServiceQueryBuilder::new(service_id.clone())
        .build()
        .unwrap();
    let res1 = runtime_a.resolve_service(&query, None).await.unwrap();
    let res2 = runtime_a.resolve_service(&query, None).await.unwrap();
    assert_eq!(
        res1.instance_id(),
        res2.instance_id(),
        "Resolution must be deterministic"
    );

    let selected_first = res1.instance_id().clone();
    let unselected = if selected_first == instance_b {
        instance_c.clone()
    } else {
        instance_b.clone()
    };

    // If Node B was selected first, terminate Node B
    if selected_first == instance_b {
        runtime_b.shutdown();
    } else {
        runtime_c.shutdown();
    }

    // Wait for disconnected peer's service to transition to Unavailable
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Some(record) = runtime_a
            .service_registry()
            .get_instance(&selected_first)
            .await
        {
            if record.state() == ServiceState::Unavailable {
                break;
            }
        }
    }

    // Resolve again on Node A: Must seamlessly failover to the remaining available provider!
    let res3 = runtime_a
        .resolve_service(&query, None)
        .await
        .expect("Failover resolution must succeed");
    assert_eq!(
        res3.instance_id(),
        &unselected,
        "Resolution must failover to the surviving provider"
    );

    runtime_a.shutdown();
    if selected_first == instance_b {
        runtime_c.shutdown();
    } else {
        runtime_b.shutdown();
    }
}

#[tokio::test]
async fn test_provider_disconnect_and_reconnect_lifecycle() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_reconnect_lifecycle_test");
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();

    let test_discovery_port = 4812;

    let id_a = NodeIdentity::generate();
    id_a.save_to_file(&dir_a.join("identity.json")).unwrap();
    let id_b = NodeIdentity::generate();
    id_b.save_to_file(&dir_b.join("identity.json")).unwrap();

    let node_b_id = id_b.node_id();

    let config_a = RuntimeConfig {
        data_dir: dir_a.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: None,
        blocked_peers: None,
    };
    let config_b = RuntimeConfig {
        data_dir: dir_b.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: None,
        blocked_peers: None,
    };

    let runtime_a = FieldnetRuntime::start(config_a.clone()).await.unwrap();
    let runtime_b = FieldnetRuntime::start(config_b.clone()).await.unwrap();

    // Wait for connection
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if runtime_a.get_connected_peers().await.contains(&node_b_id) {
            break;
        }
    }

    let service_id = ServiceId::new("database.service").unwrap();
    let instance_b = ServiceInstanceId::generate();

    runtime_b
        .advertise_service(
            ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
                .instance_id(instance_b.clone())
                .endpoint(ServiceEndpoint::InternalRpc),
            None,
        )
        .await
        .unwrap();

    // Wait for Node A to receive it
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if runtime_a
            .service_registry()
            .get_instance(&instance_b)
            .await
            .is_some()
        {
            break;
        }
    }

    assert_eq!(
        runtime_a
            .service_registry()
            .get_instance(&instance_b)
            .await
            .unwrap()
            .state(),
        ServiceState::Available
    );

    // 1. Node B goes offline (disconnects)
    runtime_b.shutdown();

    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Some(record) = runtime_a.service_registry().get_instance(&instance_b).await {
            if record.state() == ServiceState::Unavailable {
                break;
            }
        }
    }

    let record_after_disconnect = runtime_a
        .service_registry()
        .get_instance(&instance_b)
        .await
        .unwrap();
    assert_eq!(record_after_disconnect.state(), ServiceState::Unavailable);

    // 2. Node B restarts with same data_dir and identity
    let runtime_b2 = FieldnetRuntime::start(config_b).await.unwrap();

    // Wait for reconnection and service re-advertisement
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Some(record) = runtime_a.service_registry().get_instance(&instance_b).await {
            if record.state() == ServiceState::Available {
                break;
            }
        }
    }

    let record_after_reconnect = runtime_a
        .service_registry()
        .get_instance(&instance_b)
        .await
        .unwrap();
    assert_eq!(
        record_after_reconnect.state(),
        ServiceState::Available,
        "Service must transition back to Available upon provider reconnection and re-advertisement"
    );

    runtime_a.shutdown();
    runtime_b2.shutdown();
}

#[tokio::test]
async fn test_service_update_and_explicit_withdrawal_across_network() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_update_and_withdrawal_test");
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();

    let test_discovery_port = 4813;

    let id_a = NodeIdentity::generate();
    id_a.save_to_file(&dir_a.join("identity.json")).unwrap();
    let id_b = NodeIdentity::generate();
    id_b.save_to_file(&dir_b.join("identity.json")).unwrap();

    let node_b_id = id_b.node_id();

    let config_a = RuntimeConfig {
        data_dir: dir_a.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: None,
        blocked_peers: None,
    };
    let config_b = RuntimeConfig {
        data_dir: dir_b.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: None,
        blocked_peers: None,
    };

    let runtime_a = FieldnetRuntime::start(config_a).await.unwrap();
    let runtime_b = FieldnetRuntime::start(config_b).await.unwrap();

    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if runtime_a.get_connected_peers().await.contains(&node_b_id) {
            break;
        }
    }

    let service_id = ServiceId::new("weather.service").unwrap();
    let instance_b = ServiceInstanceId::generate();

    // 1. Initial advertisement with seq 1
    runtime_b
        .advertise_service(
            ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
                .instance_id(instance_b.clone())
                .endpoint(ServiceEndpoint::InternalRpc)
                .capability(ServiceCapability::new("weather.temp").unwrap()),
            None,
        )
        .await
        .unwrap();

    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if runtime_a
            .service_registry()
            .get_instance(&instance_b)
            .await
            .is_some()
        {
            break;
        }
    }

    let record1 = runtime_a
        .service_registry()
        .get_instance(&instance_b)
        .await
        .unwrap();
    assert_eq!(record1.manifest.sequence, 1);
    assert_eq!(record1.manifest.capabilities.len(), 1);

    // 2. Node B updates the service: adds 'weather.forecast' capability
    runtime_b
        .update_service(
            &instance_b,
            |m| {
                m.capabilities
                    .push(ServiceCapability::new("weather.forecast").unwrap());
            },
            None,
        )
        .await
        .unwrap();

    // Node A must receive the update and increment sequence to 2
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Some(record) = runtime_a.service_registry().get_instance(&instance_b).await {
            if record.manifest.sequence == 2 {
                break;
            }
        }
    }

    let record2 = runtime_a
        .service_registry()
        .get_instance(&instance_b)
        .await
        .unwrap();
    assert_eq!(record2.manifest.sequence, 2);
    assert_eq!(record2.manifest.capabilities.len(), 2);

    // 3. Node B explicitly withdraws the service
    let withdrawal = runtime_b.withdraw_service(&instance_b, None).await.unwrap();
    assert_eq!(withdrawal.sequence, 3);

    // Node A must process withdrawal and remove/deactivate instance
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if runtime_a
            .service_registry()
            .get_instance(&instance_b)
            .await
            .is_none()
        {
            break;
        }
    }

    assert!(
        runtime_a
            .service_registry()
            .get_instance(&instance_b)
            .await
            .is_none(),
        "Withdrawn service must be removed from Node A's registry"
    );

    // Resolver on Node A must now fail
    let query = ServiceQueryBuilder::new(service_id).build().unwrap();
    let res = runtime_a.resolve_service(&query, None).await;
    assert!(
        res.is_err(),
        "Resolution must fail after service is withdrawn"
    );

    runtime_a.shutdown();
    runtime_b.shutdown();
}

#[tokio::test]
async fn test_three_node_multihop_service_discovery_and_origin_preservation() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_multihop_discovery_test");
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");
    let dir_c = base_dir.join("node_c");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();
    fs::create_dir_all(&dir_c).unwrap();

    let id_a = NodeIdentity::generate();
    id_a.save_to_file(&dir_a.join("identity.json")).unwrap();
    let id_b = NodeIdentity::generate();
    id_b.save_to_file(&dir_b.join("identity.json")).unwrap();
    let id_c = NodeIdentity::generate();
    id_c.save_to_file(&dir_c.join("identity.json")).unwrap();

    let node_a_id = id_a.node_id();
    let node_b_id = id_b.node_id();
    let node_c_id = id_c.node_id();

    let test_discovery_port = 4814;

    // Topology: Node A <--> Node B <--> Node C (Node A and Node C cannot communicate directly)
    let config_a = RuntimeConfig {
        data_dir: dir_a.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: Some(vec![node_b_id]),
        blocked_peers: Some(vec![node_c_id]), // block direct C
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
        blocked_peers: Some(vec![node_a_id]), // block direct A
    };

    let runtime_a = FieldnetRuntime::start(config_a).await.unwrap();
    let runtime_b = FieldnetRuntime::start(config_b).await.unwrap();
    let runtime_c = FieldnetRuntime::start(config_c).await.unwrap();

    // Wait for linear topology formation: A <-> B and B <-> C
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let conns_a = runtime_a.get_connected_peers().await;
        let conns_c = runtime_c.get_connected_peers().await;
        if conns_a.contains(&node_b_id) && conns_c.contains(&node_b_id) {
            break;
        }
    }

    // Wait for routing convergence (A learns route to C via B)
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let next_hop = runtime_a.routing_table().get_next_hop(&node_c_id).await;
        if next_hop == Some(node_b_id) {
            break;
        }
    }

    assert_eq!(
        runtime_a.routing_table().get_next_hop(&node_c_id).await,
        Some(node_b_id),
        "Route to C must exist with next hop B"
    );

    // Node C advertises 'maps.service' specifically towards Node A across multi-hop relay Node B
    let service_id = ServiceId::new("maps.service").unwrap();
    let instance_c = ServiceInstanceId::generate();
    let builder_c = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(2, 0, 0))
        .instance_id(instance_c.clone())
        .endpoint(ServiceEndpoint::InternalRpc)
        .capability(ServiceCapability::new("maps.tiles").unwrap());

    // Advertise on C and route towards Node A
    let manifest_c = runtime_c
        .advertise_service(builder_c, Some(node_a_id))
        .await
        .unwrap();
    assert_eq!(manifest_c.provider_node_id, node_c_id);

    // Wait for Node A to receive the routed multi-hop advertisement
    for _ in 0..60 {
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

    // Verify Origin Preservation: Node A sees provider as Node C (NOT relay Node B)
    let record_at_a = runtime_a
        .service_registry()
        .get_instance(&instance_c)
        .await
        .expect("Node A must receive multi-hop service advertisement");

    assert_eq!(
        record_at_a.manifest.provider_node_id, node_c_id,
        "Origin Preservation: provider MUST be Node C, not intermediary Node B"
    );

    // Node A resolves 'maps.service' through resolver
    let query = ServiceQueryBuilder::new(service_id).build().unwrap();
    let resolution = runtime_a
        .resolve_service(&query, None)
        .await
        .expect("Resolution must succeed");
    assert_eq!(resolution.instance_id(), &instance_c);
    assert_eq!(resolution.provider_node_id(), &node_c_id);
    assert!(
        resolution.route_hops.unwrap() >= 1,
        "Hop count must reflect multi-hop route"
    );

    runtime_a.shutdown();
    runtime_b.shutdown();
    runtime_c.shutdown();
}

#[tokio::test]
async fn test_service_sqlite_persistence_across_restart() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_persistence_service_test");
    let dir = base_dir.join("node_persist");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir).unwrap();

    let id = NodeIdentity::generate();
    id.save_to_file(&dir.join("identity.json")).unwrap();

    let config = RuntimeConfig {
        data_dir: dir.clone(),
        tcp_port: 0,
        discovery_port: 4817,
        auto_connect: false,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: None,
        blocked_peers: None,
    };

    let service_id = ServiceId::new("auth.service").unwrap();
    let instance_id = ServiceInstanceId::generate();

    // 1. Start node and advertise service
    {
        let runtime = FieldnetRuntime::start(config.clone()).await.unwrap();
        let builder = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
            .instance_id(instance_id.clone())
            .display_name("Persistent Local Auth")
            .endpoint(ServiceEndpoint::InternalRpc)
            .capability(ServiceCapability::new("auth.tokens").unwrap());

        runtime.advertise_service(builder, None).await.unwrap();

        let rec = runtime
            .service_registry()
            .get_instance(&instance_id)
            .await
            .unwrap();
        assert!(rec.is_local);
        assert_eq!(rec.state(), ServiceState::Available);

        runtime.shutdown();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // 2. Restart node with same SQLite storage directory
    {
        let runtime2 = FieldnetRuntime::start(config).await.unwrap();

        // Verify local service manifest was restored from SQLite
        let rec2 = runtime2
            .service_registry()
            .get_instance(&instance_id)
            .await
            .expect("Local service must be restored from SQLite");
        assert!(rec2.is_local);
        assert_eq!(rec2.state(), ServiceState::Available);

        // Verify resolver can resolve the restored local service
        let query = ServiceQueryBuilder::new(service_id).build().unwrap();
        let resolution = runtime2
            .resolve_service(&query, None)
            .await
            .expect("Restored service must resolve");
        assert!(resolution.is_local());
        assert_eq!(resolution.instance_id(), &instance_id);

        runtime2.shutdown();
    }
}

#[tokio::test]
async fn test_service_event_bus_emission() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_service_events_test");
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();

    let test_discovery_port = 4818;

    let id_a = NodeIdentity::generate();
    id_a.save_to_file(&dir_a.join("identity.json")).unwrap();
    let id_b = NodeIdentity::generate();
    id_b.save_to_file(&dir_b.join("identity.json")).unwrap();

    let node_b_id = id_b.node_id();

    let config_a = RuntimeConfig {
        data_dir: dir_a.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: None,
        blocked_peers: None,
    };
    let config_b = RuntimeConfig {
        data_dir: dir_b.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: None,
        blocked_peers: None,
    };

    let runtime_a = FieldnetRuntime::start(config_a).await.unwrap();
    let mut event_rx_a = runtime_a.subscribe_service_events();

    let runtime_b = FieldnetRuntime::start(config_b).await.unwrap();

    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if runtime_a.get_connected_peers().await.contains(&node_b_id) {
            break;
        }
    }

    let service_id = ServiceId::new("stream.service").unwrap();
    let instance_b = ServiceInstanceId::generate();

    // 1. Node B advertises -> Node A receives Discovered / Available
    runtime_b
        .advertise_service(
            ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
                .instance_id(instance_b.clone())
                .endpoint(ServiceEndpoint::InternalRpc),
            None,
        )
        .await
        .unwrap();

    let mut got_discovered = false;
    for _ in 0..20 {
        if let Ok(Ok(event)) =
            tokio::time::timeout(Duration::from_millis(200), event_rx_a.recv()).await
        {
            if event.event_name() == "service.discovered"
                || event.event_name() == "service.available"
            {
                got_discovered = true;
                break;
            }
        }
    }
    assert!(
        got_discovered,
        "Event bus must emit service.discovered or service.available"
    );

    // 2. Node B updates -> Node A receives Updated
    runtime_b
        .update_service(&instance_b, |m| m.sequence += 1, None)
        .await
        .unwrap();

    let mut got_updated = false;
    for _ in 0..20 {
        if let Ok(Ok(event)) =
            tokio::time::timeout(Duration::from_millis(200), event_rx_a.recv()).await
        {
            if event.event_name() == "service.updated" {
                got_updated = true;
                break;
            }
        }
    }
    assert!(got_updated, "Event bus must emit service.updated");

    // 3. Node B withdraws -> Node A receives Withdrawn
    runtime_b.withdraw_service(&instance_b, None).await.unwrap();

    let mut got_withdrawn = false;
    for _ in 0..20 {
        if let Ok(Ok(event)) =
            tokio::time::timeout(Duration::from_millis(200), event_rx_a.recv()).await
        {
            if event.event_name() == "service.withdrawn" {
                got_withdrawn = true;
                break;
            }
        }
    }
    assert!(got_withdrawn, "Event bus must emit service.withdrawn");

    runtime_a.shutdown();
    runtime_b.shutdown();
}
