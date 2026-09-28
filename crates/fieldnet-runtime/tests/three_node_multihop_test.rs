use fieldnet_identity::NodeIdentity;
use fieldnet_protocol::AckStatus;
use fieldnet_runtime::{FieldnetRuntime, MessageDeliveryStatus, RuntimeConfig};
use std::fs;
use std::time::Duration;

#[tokio::test]
async fn test_three_node_multihop_end_to_end_delivery() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_three_node_test");
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");
    let dir_c = base_dir.join("node_c");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();
    fs::create_dir_all(&dir_c).unwrap();

    // Dedicated discovery port for isolated 3-node network
    let test_discovery_port = 4881;

    // Pre-generate identities so we can establish a strict linear topology: A <-> B <-> C
    let id_a = NodeIdentity::generate();
    id_a.save_to_file(&dir_a.join("identity.json")).unwrap();
    let id_b = NodeIdentity::generate();
    id_b.save_to_file(&dir_b.join("identity.json")).unwrap();
    let id_c = NodeIdentity::generate();
    id_c.save_to_file(&dir_c.join("identity.json")).unwrap();

    let node_a_id = id_a.node_id();
    let node_b_id = id_b.node_id();
    let node_c_id = id_c.node_id();

    println!("Node A: {}", node_a_id);
    println!("Node B: {} (Intermediate Relay)", node_b_id);
    println!("Node C: {}", node_c_id);

    // Node A connects only to Node B
    let config_a = RuntimeConfig {
        data_dir: dir_a.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(200),
        allowed_peers: Some(vec![node_b_id]),
        blocked_peers: Some(vec![node_c_id]),
    };

    // Node B acts as intermediate relay, connecting to both Node A and Node C
    let config_b = RuntimeConfig {
        data_dir: dir_b.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(200),
        allowed_peers: Some(vec![node_a_id, node_c_id]),
        blocked_peers: None,
    };

    // Node C connects only to Node B
    let config_c = RuntimeConfig {
        data_dir: dir_c.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(200),
        allowed_peers: Some(vec![node_b_id]),
        blocked_peers: Some(vec![node_a_id]),
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

    // 1. Verify Topology Formation: A <-> B and B <-> C
    println!("Waiting for linear topology convergence (A <-> B <-> C)...");
    let mut topology_ready = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let conns_a = runtime_a.get_connected_peers().await;
        let conns_b = runtime_b.get_connected_peers().await;
        let conns_c = runtime_c.get_connected_peers().await;

        let a_connected_to_b = conns_a.contains(&node_b_id);
        let b_connected_to_a = conns_b.contains(&node_a_id);
        let b_connected_to_c = conns_b.contains(&node_c_id);
        let c_connected_to_b = conns_c.contains(&node_b_id);

        if a_connected_to_b && b_connected_to_a && b_connected_to_c && c_connected_to_b {
            topology_ready = true;
            break;
        }
    }
    assert!(topology_ready, "Topology A <-> B <-> C failed to form");

    // Verify A and C are NEVER directly connected
    assert!(
        !runtime_a.get_connected_peers().await.contains(&node_c_id),
        "Invariant violated: Node A must not connect directly to Node C"
    );
    assert!(
        !runtime_c.get_connected_peers().await.contains(&node_a_id),
        "Invariant violated: Node C must not connect directly to Node A"
    );

    // 2. Verify Distance-Vector Route Exchange
    println!("Waiting for routing convergence (A learns route to C via B)...");
    let mut route_converged = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let next_hop_for_c_at_a = runtime_a.routing_table().get_next_hop(&node_c_id).await;
        let next_hop_for_a_at_c = runtime_c.routing_table().get_next_hop(&node_a_id).await;

        if next_hop_for_c_at_a == Some(node_b_id) && next_hop_for_a_at_c == Some(node_b_id) {
            route_converged = true;
            break;
        }
    }
    assert!(
        route_converged,
        "Routing failed to converge: A -> C via B, C -> A via B"
    );

    // Verify distance metrics
    let route_to_c = runtime_a
        .routing_table()
        .get_route(&node_c_id)
        .await
        .unwrap();
    assert_eq!(route_to_c.hops, 2, "Route from A to C must be 2 hops");
    assert_eq!(route_to_c.next_hop, node_b_id, "Next hop must be B");

    // 3. Application at Node A sends DATA message destined strictly for Node C
    // The application only specifies destination = C, never manually specifying B!
    let payload = "FIELDNET Phase 05 Multi-Hop Verification Payload";
    println!("Node A sending message to Node C through relay B...");
    let message_id = runtime_a
        .send_data_message(node_c_id, payload)
        .await
        .expect("Failed to send message from A to C");

    // 4. Verify Delivery at Destination (Node C)
    println!("Waiting for message delivery at Node C...");
    let mut delivered_msg = None;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let received = runtime_c.get_received_messages().await;
        if let Some(item) = received.iter().find(|m| m.message.message_id == message_id) {
            delivered_msg = Some(item.clone());
            break;
        }
    }

    let received_item = delivered_msg.expect("Message from A was not received at destination C");
    let received_msg = received_item.message;

    // Verify message integrity and forwarding invariants
    assert_eq!(received_msg.source, node_a_id, "Source must be Node A");
    assert_eq!(
        received_msg.destination, node_c_id,
        "Destination must be Node C"
    );
    assert_eq!(
        received_msg.payload,
        payload.as_bytes(),
        "Payload bytes must match exactly"
    );
    assert_eq!(
        received_msg.ttl, 15,
        "TTL must be decremented from 16 to 15 by relay Node B"
    );

    // Originator signature must remain valid despite multi-hop relaying
    let pubkey_a = id_a.public_key_bytes();
    assert!(
        received_msg.verify_signature(&pubkey_a).is_ok(),
        "Originator Ed25519 signature must be preserved intact across relays"
    );

    // 5. Verify Intermediate Relay (Node B)
    // Node B forwarded the message, so B's application layer MUST NOT receive it
    let received_at_b = runtime_b.get_received_messages().await;
    assert!(
        received_at_b.is_empty(),
        "Intermediate relay Node B must not deliver transit traffic to local application"
    );

    // 6. Verify End-to-End ACK Propagation: C -> B -> A
    println!("Waiting for end-to-end ACK to reach Node A...");
    let ack_result = runtime_a
        .wait_for_ack(message_id, Duration::from_secs(10))
        .await
        .expect("ACK did not arrive at originator Node A");

    match ack_result {
        MessageDeliveryStatus::AckReceived { status, .. } => {
            assert_eq!(status, AckStatus::Received, "ACK status must be Received");
        }
        other => panic!("Unexpected delivery status at Node A: {:?}", other),
    }

    // Verify DTN queue on Node A is now clean (message cleared upon receiving ACK)
    let queue_count = runtime_a.dtn_queue().count().unwrap();
    assert_eq!(
        queue_count, 0,
        "Originator DTN queue must be empty after ACK receipt"
    );

    println!("Three-node multi-hop integration test PASSED!");

    // Clean shutdown
    runtime_a.shutdown();
    runtime_b.shutdown();
    runtime_c.shutdown();
}
