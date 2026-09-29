use std::fs;
use std::time::Duration;

use fieldnet_identity::NodeIdentity;
use fieldnet_runtime::service::{
    ServiceEndpoint, ServiceId, ServiceInstanceId, ServiceManifestBuilder, ServiceQueryBuilder,
    ServiceState, ServiceVersion,
};
use fieldnet_runtime::{FieldnetRuntime, RuntimeConfig};

#[tokio::test]
async fn test_network_partition_and_reconvergence() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_partition_convergence_test");
    let dir_a = base_dir.join("cluster_a");
    let dir_b = base_dir.join("cluster_b");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();

    let test_discovery_port = 4819;

    let id_a = NodeIdentity::generate();
    id_a.save_to_file(&dir_a.join("identity.json")).unwrap();
    let id_b = NodeIdentity::generate();
    id_b.save_to_file(&dir_b.join("identity.json")).unwrap();

    let _node_a_id = id_a.node_id();
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
    let runtime_b = FieldnetRuntime::start(config_b.clone()).await.unwrap();

    // 1. Establish initial connectivity
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if runtime_a.get_connected_peers().await.contains(&node_b_id) {
            break;
        }
    }

    let service_id = ServiceId::new("finance.service").unwrap();
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

    // Verify Node A discovered Cluster B's service
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

    let query = ServiceQueryBuilder::new(service_id.clone())
        .build()
        .unwrap();
    let res_before = runtime_a.resolve_service(&query, None).await;
    assert!(
        res_before.is_ok(),
        "Service in Cluster B must be resolvable before partition"
    );

    // 2. Simulate Network Partition: Node B disappears / network link broken
    runtime_b.shutdown();

    // Wait for Node A to detect disconnection and transition service to Unavailable
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Some(record) = runtime_a.service_registry().get_instance(&instance_b).await {
            if record.state() == ServiceState::Unavailable {
                break;
            }
        }
    }

    let record_partitioned = runtime_a
        .service_registry()
        .get_instance(&instance_b)
        .await
        .expect("Record must still exist in registry");
    assert_eq!(
        record_partitioned.state(),
        ServiceState::Unavailable,
        "During partition, services in Cluster B must NOT appear available/reachable from Cluster A"
    );

    // Resolver on Cluster A must refuse to route to the partitioned provider
    let res_partitioned = runtime_a.resolve_service(&query, None).await;
    assert!(
        res_partitioned.is_err(),
        "Resolver must reject partitioned, unreachable provider"
    );

    // 3. Heal Network Partition: Cluster B returns
    let runtime_b_healed = FieldnetRuntime::start(config_b).await.unwrap();

    // Wait for connectivity to restore and advertisements to re-propagate / converge
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Some(record) = runtime_a.service_registry().get_instance(&instance_b).await {
            if record.state() == ServiceState::Available {
                break;
            }
        }
    }

    let record_healed = runtime_a
        .service_registry()
        .get_instance(&instance_b)
        .await
        .expect("Record must exist");
    assert_eq!(
        record_healed.state(),
        ServiceState::Available,
        "After partition heals, service must converge back to Available"
    );

    // Resolver must now successfully resolve newly reachable provider in Cluster B
    let res_healed = runtime_a.resolve_service(&query, None).await;
    assert!(
        res_healed.is_ok(),
        "Resolver must successfully resolve newly reachable provider after partition heals"
    );

    runtime_a.shutdown();
    runtime_b_healed.shutdown();
}
