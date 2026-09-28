//! Integration tests for real network synchronization:
//! - TEST 11: Multi-Hop Synchronization (A -> B -> C relay).
//! - TEST 12: DTN-Delayed Synchronization (offline modification, store-and-forward delivery).

use fieldnet_identity::NodeIdentity;
use fieldnet_runtime::{FieldnetRuntime, RuntimeConfig};
use std::fs;
use std::time::Duration;

/// TEST 11: Multi-Hop Synchronization across intermediate relay node B.
/// Topology: Node A <-> Node B <-> Node C.
/// Node A and Node C are physically separated and communicate only via Relay B.
#[tokio::test]
async fn test_11_multihop_sync_via_relay() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_multihop_sync_test");
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");
    let dir_c = base_dir.join("node_c");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();
    fs::create_dir_all(&dir_c).unwrap();

    let test_discovery_port = 4883;

    let id_a = NodeIdentity::generate();
    id_a.save_to_file(&dir_a.join("identity.json")).unwrap();
    let id_b = NodeIdentity::generate();
    id_b.save_to_file(&dir_b.join("identity.json")).unwrap();
    let id_c = NodeIdentity::generate();
    id_c.save_to_file(&dir_c.join("identity.json")).unwrap();

    let node_a_id = id_a.node_id();
    let node_b_id = id_b.node_id();
    let node_c_id = id_c.node_id();

    // Node A connects only to Node B
    let config_a = RuntimeConfig {
        data_dir: dir_a.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: Some(vec![node_b_id]),
        blocked_peers: None,
    };

    // Node B acts as relay, connecting to both Node A and Node C
    let config_b = RuntimeConfig {
        data_dir: dir_b.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: Some(vec![node_a_id, node_c_id]),
        blocked_peers: None,
    };

    // Node C connects only to Node B
    let config_c = RuntimeConfig {
        data_dir: dir_c.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: Some(vec![node_b_id]),
        blocked_peers: None,
    };

    let runtime_a = FieldnetRuntime::start(config_a)
        .await
        .expect("Failed to start Node A");
    let runtime_b = FieldnetRuntime::start(config_b)
        .await
        .expect("Failed to start Node B");
    let runtime_c = FieldnetRuntime::start(config_c)
        .await
        .expect("Failed to start Node C");

    // 1. Wait for topology: A <-> B <-> C
    let mut topology_ready = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let conns_a = runtime_a.get_connected_peers().await;
        let conns_b = runtime_b.get_connected_peers().await;
        let conns_c = runtime_c.get_connected_peers().await;

        if conns_a.contains(&node_b_id)
            && conns_b.contains(&node_a_id)
            && conns_b.contains(&node_c_id)
            && conns_c.contains(&node_b_id)
        {
            topology_ready = true;
            break;
        }
    }
    assert!(topology_ready, "Topology A <-> B <-> C failed to form");

    // Invariant: A and C must not be directly connected
    assert!(!runtime_a.get_connected_peers().await.contains(&node_c_id));
    assert!(!runtime_c.get_connected_peers().await.contains(&node_a_id));

    // 2. Wait for routing convergence (A -> C via B, C -> A via B)
    let mut route_converged = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let next_a_to_c = runtime_a.routing_table().get_next_hop(&node_c_id).await;
        let next_c_to_a = runtime_c.routing_table().get_next_hop(&node_a_id).await;

        if next_a_to_c == Some(node_b_id) && next_c_to_a == Some(node_b_id) {
            route_converged = true;
            break;
        }
    }
    assert!(route_converged, "Routing failed to converge across relay B");

    // 3. Node A creates a document locally
    let doc_a = runtime_a
        .document_store()
        .create_sync("relay_doc", b"MultiHop Sync Success".to_vec())
        .expect("Failed to create document on Node A");

    assert!(runtime_c
        .document_store()
        .get_sync(&doc_a.document_id)
        .unwrap()
        .is_none());

    // 4. Node C synchronizes with Node A (addressed logically across relay B)
    let _ = runtime_c
        .synchronize_with(node_a_id)
        .await
        .expect("Failed to initiate sync C -> A");

    // 5. Verify convergence at Node C
    let mut replicated = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        if let Ok(Some(doc_c)) = runtime_c.document_store().get_sync(&doc_a.document_id) {
            if doc_c.state == b"MultiHop Sync Success".to_vec() {
                replicated = true;
                break;
            }
        }
    }

    assert!(
        replicated,
        "Document failed to replicate from A to C across intermediate relay B"
    );

    // Cleanup
    runtime_a.shutdown();
    runtime_b.shutdown();
    runtime_c.shutdown();
}

/// TEST 12: DTN-Delayed Synchronization.
/// Node A modifies document while Node B is offline.
/// Sync message is queued in SQLite DTN storage.
/// When Node B powers up and connects, DTN bundle is delivered and state converges.
#[tokio::test]
async fn test_12_dtn_delayed_sync() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_dtn_sync_test");
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();

    let test_discovery_port = 4884;

    let id_a = NodeIdentity::generate();
    id_a.save_to_file(&dir_a.join("identity.json")).unwrap();
    let id_b = NodeIdentity::generate();
    id_b.save_to_file(&dir_b.join("identity.json")).unwrap();

    let node_a_id = id_a.node_id();
    let node_b_id = id_b.node_id();

    // Start Node A while Node B is completely OFFLINE
    let config_a = RuntimeConfig {
        data_dir: dir_a.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: Some(vec![node_b_id]),
        blocked_peers: None,
    };

    let runtime_a = FieldnetRuntime::start(config_a)
        .await
        .expect("Failed to start Node A");

    // Node A creates document locally while isolated
    let doc_a = runtime_a
        .document_store()
        .create_sync("dtn_stream", b"Store and Forward Payload".to_vec())
        .expect("Failed to create document locally on Node A");

    // Initiate sync towards offline Node B
    // Since B is unreachable, sync message will be queued into DTN persistent storage
    let _ = runtime_a.synchronize_with(node_b_id).await;

    // Verify DTN queue has pending bundle
    let mut dtn_count = 0;
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        dtn_count = runtime_a.dtn_queue().count().unwrap_or(0);
        if dtn_count > 0 {
            break;
        }
    }
    println!(
        "Pending DTN bundles at Node A before Node B start: {}",
        dtn_count
    );
    assert!(
        dtn_count > 0,
        "Sync message must be stored in DTN queue while remote peer is offline"
    );

    // Now start Node B
    let config_b = RuntimeConfig {
        data_dir: dir_b.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: Some(vec![node_a_id]),
        blocked_peers: None,
    };

    let runtime_b = FieldnetRuntime::start(config_b)
        .await
        .expect("Failed to start Node B");

    // Wait for connection and DTN dispatch
    let mut converged = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        if let Ok(Some(doc_b)) = runtime_b.document_store().get_sync(&doc_a.document_id) {
            if doc_b.state == b"Store and Forward Payload".to_vec() {
                converged = true;
                break;
            }
        }
    }

    assert!(
        converged,
        "DTN delayed sync failed to converge document on Node B after reconnection"
    );

    // Cleanup
    runtime_a.shutdown();
    runtime_b.shutdown();
}
