use std::fs;
use std::time::Duration;

use fieldnet_content::{ContentResolution, ProviderState, TrustTier};
use fieldnet_identity::NodeIdentity;
use fieldnet_runtime::{FieldnetRuntime, RuntimeConfig};

#[tokio::test]
async fn test_three_node_real_content_discovery_and_provider_lifecycle() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_content_discovery_multinode_test");
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");
    let dir_c = base_dir.join("node_c");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();
    fs::create_dir_all(&dir_c).unwrap();

    let test_discovery_port = 4991;

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

    let runtime_a = FieldnetRuntime::start(config_a)
        .await
        .expect("Failed to start Node A");
    let runtime_b = FieldnetRuntime::start(config_b)
        .await
        .expect("Failed to start Node B");
    let runtime_c = FieldnetRuntime::start(config_c)
        .await
        .expect("Failed to start Node C");

    println!("Node A: {}", node_a_id);
    println!("Node B: {}", node_b_id);
    println!("Node C: {}", node_c_id);

    // 1. Wait for peer discovery and mutual connections between nodes
    println!("Waiting for mesh connectivity across Nodes A, B, and C...");
    let mut mesh_connected = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let conns_a = runtime_a.get_connected_peers().await;
        let conns_b = runtime_b.get_connected_peers().await;
        let conns_c = runtime_c.get_connected_peers().await;

        if conns_a.contains(&node_b_id)
            && conns_a.contains(&node_c_id)
            && conns_b.contains(&node_a_id)
            && conns_b.contains(&node_c_id)
            && conns_c.contains(&node_a_id)
            && conns_c.contains(&node_b_id)
        {
            mesh_connected = true;
            break;
        }
    }
    assert!(
        mesh_connected,
        "Nodes A, B, and C failed to form full mesh connectivity"
    );
    println!("Full mesh connectivity established!");

    // 2. Node A stores content X
    let content_payload =
        b"FIELDNET Phase 07 Build 03 Distributed Content Discovery Verification Payload";
    let put_res = runtime_a
        .content_store()
        .put(&content_payload[..], None)
        .await
        .unwrap();
    let content_x = put_res.content_id();
    println!("Node A stored content X: {}", content_x);

    // Node A resolution is Local
    let res_a = runtime_a.content_resolve(&content_x).unwrap();
    assert!(matches!(res_a, ContentResolution::Local { .. }));

    // Before advertisement, Node B and C do not know about content X
    assert_eq!(runtime_b.content_find_providers(&content_x).len(), 0);
    assert_eq!(runtime_c.content_find_providers(&content_x).len(), 0);

    // 3. Node A advertises content X to the network
    println!("Node A broadcasting content advertisement for X...");
    runtime_a
        .content_advertise_local(&content_x, None)
        .await
        .unwrap();

    // 4. Node B and Node C receive the advertisement and record Node A as provider
    println!("Waiting for Node B and Node C to receive advertisement...");
    let mut discovered_b = false;
    let mut discovered_c = false;
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        if !discovered_b {
            let providers_b = runtime_b.content_find_providers(&content_x);
            if providers_b.iter().any(|p| p.provider_node_id == node_a_id) {
                discovered_b = true;
            }
        }
        if !discovered_c {
            let providers_c = runtime_c.content_find_providers(&content_x);
            if providers_c.iter().any(|p| p.provider_node_id == node_a_id) {
                discovered_c = true;
            }
        }
        if discovered_b && discovered_c {
            break;
        }
    }

    assert!(
        discovered_b,
        "Node B failed to discover Node A as provider for content X"
    );
    assert!(
        discovered_c,
        "Node C failed to discover Node A as provider for content X"
    );
    println!("Nodes B and C successfully discovered Node A as provider for content X!");

    // Check resolution on Node B -> Remote with Node A
    let res_b = runtime_b.content_resolve(&content_x).unwrap();
    match res_b {
        ContentResolution::Remote {
            content_id,
            providers,
        } => {
            assert_eq!(content_id, content_x);
            assert_eq!(providers.len(), 1);
            assert_eq!(providers[0].provider_node_id, node_a_id);
            assert_eq!(providers[0].trust_tier, TrustTier::Authorized);
            assert_eq!(providers[0].availability_state, ProviderState::Active);
            assert_eq!(
                providers[0].content_size,
                Some(content_payload.len() as u64)
            );
        }
        other => panic!("Expected Remote resolution on Node B, got {:?}", other),
    }

    // 5. Node B sends an active ContentQuery for content X
    println!("Node B querying network specifically for content X...");
    let query = runtime_b
        .content_query_network(&content_x, None, Some(node_a_id))
        .await
        .unwrap();
    assert_eq!(query.content_id, content_x);
    assert_eq!(query.requester, node_b_id);

    // 6. Multiple Providers: Node C also stores content X and advertises it
    println!("Node C storing content X and advertising as second provider...");
    runtime_c
        .content_store()
        .put(&content_payload[..], None)
        .await
        .unwrap();
    runtime_c
        .content_advertise_local(&content_x, None)
        .await
        .unwrap();

    // Node B should now resolve BOTH Node A and Node C as providers for content X
    let mut both_discovered = false;
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let providers = runtime_b.content_find_providers(&content_x);
        let has_a = providers.iter().any(|p| p.provider_node_id == node_a_id);
        let has_c = providers.iter().any(|p| p.provider_node_id == node_c_id);
        if has_a && has_c {
            both_discovered = true;
            break;
        }
    }
    assert!(
        both_discovered,
        "Node B failed to resolve multiple providers (Node A and Node C) for content X"
    );
    println!("Node B successfully resolved multiple providers (A and C) for content X!");

    // 7. Node A disappears / shuts down
    println!("Shutting down Node A...");
    runtime_a.shutdown();

    // Wait for Node B to detect peer disconnection and mark Node A as Stale
    println!("Waiting for Node B to mark disconnected Node A as Stale...");
    let mut a_marked_stale = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let status_a = runtime_b.content_provider_status(&content_x, &node_a_id);
        if let Some(status) = status_a {
            if status == ProviderState::Stale || status == ProviderState::Expired {
                a_marked_stale = true;
                break;
            }
        }
    }
    assert!(
        a_marked_stale,
        "Node B failed to transition disconnected Node A to Stale/Expired"
    );
    println!("Node A successfully transitioned to Stale/Expired on Node B!");

    // Node B's active find_providers must no longer consider A an active provider,
    // but Node C MUST REMAIN AVAILABLE!
    let active_providers_on_b = runtime_b.content_find_providers(&content_x);
    assert!(
        !active_providers_on_b
            .iter()
            .any(|p| p.provider_node_id == node_a_id),
        "Node A must not be returned as an active provider after disappearing"
    );
    assert!(
        active_providers_on_b
            .iter()
            .any(|p| p.provider_node_id == node_c_id),
        "Node C must remain available as an active provider for content X"
    );

    // 8. Explicit expiration sweep: Node A's stale record is purged from Node B
    let far_future = fieldnet_core::Timestamp::from_millis(
        fieldnet_core::Timestamp::now().as_millis() + 400_000,
    );
    runtime_b.content_provider_registry().expire(far_future);
    assert_eq!(
        runtime_b.content_provider_status(&content_x, &node_a_id),
        None,
        "Node A should be completely purged from registry after hard expiration"
    );

    println!("REAL MULTI-NODE CONTENT DISCOVERY TEST COMPLETE AND VERIFIED!");

    runtime_b.shutdown();
    runtime_c.shutdown();
}

#[tokio::test]
async fn test_multihop_and_dtn_content_advertisement_propagation() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_content_dtn_test");
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");
    let dir_c = base_dir.join("node_c");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();
    fs::create_dir_all(&dir_c).unwrap();

    let test_discovery_port = 4992;

    let id_a = NodeIdentity::generate();
    id_a.save_to_file(&dir_a.join("identity.json")).unwrap();
    let id_b = NodeIdentity::generate();
    id_b.save_to_file(&dir_b.join("identity.json")).unwrap();
    let id_c = NodeIdentity::generate();
    id_c.save_to_file(&dir_c.join("identity.json")).unwrap();

    let node_a_id = id_a.node_id();
    let node_b_id = id_b.node_id();
    let node_c_id = id_c.node_id();

    // Linear topology: A <-> B <-> C
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

    // 1. Wait for linear topology convergence
    println!("Waiting for linear topology (A <-> B <-> C)...");
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
    assert!(topology_ready, "Linear topology failed to form");
    println!("Linear topology established!");

    // Wait for route advertisements to exchange
    tokio::time::sleep(Duration::from_millis(600)).await;

    // 2. Node A stores content Z and sends advertisement targeted to Node C (2 hops away)
    let content_payload = b"Multi-hop DTN advertisement test payload";
    let put_res = runtime_a
        .content_store()
        .put(&content_payload[..], None)
        .await
        .unwrap();
    let content_z = put_res.content_id();

    println!("Node A dispatching 2-hop advertisement for content Z to Node C...");
    runtime_a
        .content_advertise_local(&content_z, Some(node_c_id))
        .await
        .unwrap();

    // 3. Node C should receive the multi-hop advertisement via relay Node B
    println!("Waiting for Node C to receive routed advertisement...");
    let mut received_by_c = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let providers_on_c = runtime_c.content_find_providers(&content_z);
        if providers_on_c
            .iter()
            .any(|p| p.provider_node_id == node_a_id)
        {
            received_by_c = true;
            break;
        }
    }
    assert!(
        received_by_c,
        "Node C failed to receive multi-hop content advertisement from Node A via Node B"
    );
    println!("Node C successfully received multi-hop advertisement!");

    // Verify Node C resolution
    let res_c = runtime_c.content_resolve(&content_z).unwrap();
    match res_c {
        ContentResolution::Remote {
            content_id,
            providers,
        } => {
            assert_eq!(content_id, content_z);
            assert_eq!(providers[0].provider_node_id, node_a_id);
        }
        other => panic!("Expected Remote resolution on Node C, got {:?}", other),
    }

    // 4. Test DTN Queue Staging: Node C disconnects
    println!("Shutting down Node C to test DTN queue staging on intermediate relay Node B...");
    runtime_c.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Node A advertises a second content W destined for disconnected Node C
    let content_w_bytes = b"DTN queued advertisement payload";
    let put_res_w = runtime_a
        .content_store()
        .put(&content_w_bytes[..], None)
        .await
        .unwrap();
    let content_w = put_res_w.content_id();

    runtime_a
        .content_advertise_local(&content_w, Some(node_c_id))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Relay Node B must have staged the message for Node C in its DTN queue
    let dtn_count_b = runtime_b.dtn_queue().count().unwrap();
    assert!(
        dtn_count_b > 0,
        "Node B should have staged the unroutable FILE message in DTN queue"
    );
    println!(
        "Node B successfully staged unroutable FILE message in DTN queue (queue count: {})",
        dtn_count_b
    );

    runtime_a.shutdown();
    runtime_b.shutdown();
}
