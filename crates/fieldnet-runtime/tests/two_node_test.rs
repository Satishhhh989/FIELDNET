use fieldnet_protocol::MessageType;
use fieldnet_runtime::{FieldnetRuntime, MessageDeliveryStatus, RuntimeConfig};
use std::fs;
use std::time::Duration;

#[tokio::test]
async fn test_two_real_nodes_secure_message_exchange() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_two_node_test");
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();

    // Use a dedicated UDP discovery port for this test to prevent collision
    let test_discovery_port = 4567;

    // 1. Configure and start Node A
    let config_a = RuntimeConfig {
        data_dir: dir_a.clone(),
        tcp_port: 0, // dynamic ephemeral TCP port
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(200),
        allowed_peers: None,
        blocked_peers: None,
    };
    let runtime_a = FieldnetRuntime::start(config_a)
        .await
        .expect("Failed to start Runtime A");
    let node_a_id = runtime_a.node_id();

    // 2. Configure and start Node B
    let config_b = RuntimeConfig {
        data_dir: dir_b.clone(),
        tcp_port: 0, // dynamic ephemeral TCP port
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(200),
        allowed_peers: None,
        blocked_peers: None,
    };
    let runtime_b = FieldnetRuntime::start(config_b)
        .await
        .expect("Failed to start Runtime B");
    let node_b_id = runtime_b.node_id();

    assert_ne!(node_a_id, node_b_id, "Nodes must have different identities");

    // 3. Verify Discovery and Mutual TCP Connection over real network sockets
    println!("Waiting for discovery and connection between Node A and Node B...");
    let mut connected = false;
    for _ in 0..50 {
        // wait up to 10 seconds
        tokio::time::sleep(Duration::from_millis(200)).await;
        let peers_a = runtime_a.get_connected_peers().await;
        let peers_b = runtime_b.get_connected_peers().await;

        if peers_a.contains(&node_b_id) && peers_b.contains(&node_a_id) {
            connected = true;
            break;
        }
    }
    assert!(
        connected,
        "Node A and Node B failed to discover and mutually connect via real sockets"
    );
    println!("Node A and Node B are mutually authenticated and connected via secure session!");

    // 4. Node A sends authenticated DATA message to Node B
    let payload = "Hello from FIELDNET Node A - Authenticated Message";
    let msg_id = runtime_a
        .send_data_message(node_b_id, payload)
        .await
        .expect("Node A failed to send DATA message");

    // Initial status on sender should be Sent
    let status_before = runtime_a.get_delivery_status(&msg_id).await.unwrap();
    assert!(matches!(status_before, MessageDeliveryStatus::Sent { .. }));

    // 5. Node A waits for ACK from Node B
    println!("Node A waiting for ACK from Node B...");
    let ack_result = runtime_a.wait_for_ack(msg_id, Duration::from_secs(5)).await;
    assert!(
        ack_result.is_ok(),
        "Node A timed out waiting for ACK from Node B"
    );

    let ack_status = ack_result.unwrap();
    match ack_status {
        MessageDeliveryStatus::AckReceived { ack_at, status } => {
            println!(
                "ACK successfully received! Status: {:?}, timestamp: {}",
                status, ack_at
            );
            assert_eq!(status, fieldnet_protocol::AckStatus::Received);
        }
        _ => panic!("Expected AckReceived status"),
    }

    // 6. Verify Node B received, verified, and recorded the message
    let received_on_b = runtime_b.get_received_messages().await;
    assert_eq!(
        received_on_b.len(),
        1,
        "Node B should have exactly 1 received message"
    );
    let rec = &received_on_b[0];
    assert_eq!(rec.message.message_id, msg_id);
    assert_eq!(rec.message.source, node_a_id);
    assert_eq!(rec.message.destination, node_b_id);
    assert_eq!(rec.message.message_type, MessageType::Data);
    assert_eq!(rec.message.payload, payload.as_bytes());
    assert!(!rec.is_duplicate);

    // Verify digital signature on Node B using Node A's public key
    let pubkey_a = runtime_a.identity().public_key_bytes();
    assert!(
        rec.message.verify_signature(&pubkey_a).is_ok(),
        "Node B signature verification failed on received DATA message"
    );

    // 7. Verify Duplicate Detection: Node A resends same message ID
    // We send an identical raw message over connection to trigger duplicate handling
    let duplicate_msg = rec.message.clone();
    runtime_a
        .connection_manager()
        .send_message(&node_b_id, duplicate_msg)
        .await
        .expect("Failed to send duplicate message");

    tokio::time::sleep(Duration::from_millis(500)).await;
    let received_after_dup = runtime_b.get_received_messages().await;
    assert_eq!(
        received_after_dup.len(),
        1,
        "Duplicate message must be detected and NOT added to received messages list"
    );

    // 8. Clean shutdown
    runtime_a.shutdown();
    runtime_b.shutdown();

    let _ = fs::remove_dir_all(&base_dir);
    println!("Test completed successfully!");
}
