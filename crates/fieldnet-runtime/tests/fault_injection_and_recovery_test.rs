use fieldnet_identity::NodeIdentity;
use fieldnet_protocol::AckStatus;
use fieldnet_runtime::{FieldnetRuntime, MessageDeliveryStatus, RuntimeConfig};
use std::fs;
use std::time::Duration;

#[tokio::test]
async fn test_route_failure_and_dtn_retry_recovery() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_route_failure_test");
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");
    let dir_c = base_dir.join("node_c");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();
    fs::create_dir_all(&dir_c).unwrap();

    let test_discovery_port = 4882;

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
        beacon_interval: Duration::from_millis(200),
        allowed_peers: Some(vec![node_b_id]),
        blocked_peers: Some(vec![node_c_id]),
    };

    let config_b = RuntimeConfig {
        data_dir: dir_b.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(200),
        allowed_peers: Some(vec![node_a_id, node_c_id]),
        blocked_peers: None,
    };

    let config_c = RuntimeConfig {
        data_dir: dir_c.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(200),
        allowed_peers: Some(vec![node_b_id]),
        blocked_peers: Some(vec![node_a_id]),
    };

    let runtime_a = FieldnetRuntime::start(config_a.clone()).await.unwrap();
    let runtime_b = FieldnetRuntime::start(config_b.clone()).await.unwrap();
    let mut runtime_c = Some(FieldnetRuntime::start(config_c.clone()).await.unwrap());

    // 1. Wait for initial convergence
    println!("Waiting for A <-> B <-> C convergence...");
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        if runtime_a
            .routing_table()
            .get_next_hop(&node_c_id)
            .await
            .is_some()
        {
            break;
        }
    }
    assert!(runtime_a
        .routing_table()
        .get_next_hop(&node_c_id)
        .await
        .is_some());

    // Send first message A -> C to establish baseline
    let msg1_id = runtime_a
        .send_data_message(node_c_id, "Message 1 before failure")
        .await
        .unwrap();
    let ack1 = runtime_a
        .wait_for_ack(msg1_id, Duration::from_secs(5))
        .await
        .unwrap();
    assert!(matches!(
        ack1,
        MessageDeliveryStatus::AckReceived {
            status: AckStatus::Received,
            ..
        }
    ));
    println!("Message 1 delivered successfully.");

    // 2. DISCONNECT B <-> C by shutting down Node C
    println!("Injecting Route Failure: Shutting down Node C...");
    if let Some(c) = runtime_c.take() {
        c.shutdown();
    }

    // Wait for Node B to detect C's disconnection and remove the route to C
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        if !runtime_b.get_connected_peers().await.contains(&node_c_id) {
            break;
        }
    }
    assert!(!runtime_b.get_connected_peers().await.contains(&node_c_id));

    // Wait for route to C to be removed on B
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if runtime_b
            .routing_table()
            .get_next_hop(&node_c_id)
            .await
            .is_none()
        {
            break;
        }
    }
    println!("Route to C removed from B's routing table.");

    // 3. Send second message A -> C during route failure
    println!("Node A sending Message 2 to unreachable Node C...");
    let msg2_id = runtime_a
        .send_data_message(node_c_id, "Message 2 during partition")
        .await
        .unwrap();

    // Verify runtime remains healthy (no crash)
    assert_eq!(runtime_a.node_id(), node_a_id);
    assert_eq!(runtime_b.node_id(), node_b_id);

    // Verify Message 2 enters persistent DTN queue (either on A or B)
    tokio::time::sleep(Duration::from_millis(500)).await;
    let dtn_count_a = runtime_a.dtn_queue().count().unwrap();
    let dtn_count_b = runtime_b.dtn_queue().count().unwrap();
    println!(
        "DTN queue sizes: Node A = {}, Node B = {}",
        dtn_count_a, dtn_count_b
    );
    assert!(
        dtn_count_a + dtn_count_b > 0,
        "Message must be durably staged in persistent DTN queue during route failure"
    );

    // 4. RESTORE B <-> C by restarting Node C with identical identity and data dir
    println!("Restoring Node C...");
    let restarted_c = FieldnetRuntime::start(config_c).await.unwrap();
    assert_eq!(restarted_c.node_id(), node_c_id);

    // 5. Verify queued message is automatically retried and delivered
    println!("Waiting for DTN automatic retry and delivery of Message 2 at Node C...");
    let mut msg2_delivered = false;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let received = restarted_c.get_received_messages().await;
        if received.iter().any(|m| m.message.message_id == msg2_id) {
            msg2_delivered = true;
            break;
        }
    }
    assert!(
        msg2_delivered,
        "Queued DTN message was not delivered after route restoration"
    );

    // 6. Verify end-to-end ACK arrives at Node A
    let ack2 = runtime_a
        .wait_for_ack(msg2_id, Duration::from_secs(10))
        .await
        .unwrap();
    assert!(matches!(
        ack2,
        MessageDeliveryStatus::AckReceived {
            status: AckStatus::Received,
            ..
        }
    ));
    println!("Route failure & DTN recovery test PASSED!");

    runtime_a.shutdown();
    runtime_b.shutdown();
    restarted_c.shutdown();
}

#[tokio::test]
async fn test_intermediate_node_crash_and_recovery() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_intermediate_crash_test");
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

    let config_a = RuntimeConfig {
        data_dir: dir_a.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(200),
        allowed_peers: Some(vec![node_b_id]),
        blocked_peers: Some(vec![node_c_id]),
    };

    let config_b = RuntimeConfig {
        data_dir: dir_b.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(200),
        allowed_peers: Some(vec![node_a_id, node_c_id]),
        blocked_peers: None,
    };

    let config_c = RuntimeConfig {
        data_dir: dir_c.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(200),
        allowed_peers: Some(vec![node_b_id]),
        blocked_peers: Some(vec![node_a_id]),
    };

    let runtime_a = FieldnetRuntime::start(config_a.clone()).await.unwrap();
    let mut runtime_b = Some(FieldnetRuntime::start(config_b.clone()).await.unwrap());
    let runtime_c = FieldnetRuntime::start(config_c.clone()).await.unwrap();

    // Wait for topology formation
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        if runtime_a
            .routing_table()
            .get_next_hop(&node_c_id)
            .await
            .is_some()
        {
            break;
        }
    }

    // 1. Crash intermediate Relay Node B
    println!("Terminating intermediate relay Node B unexpectedly...");
    if let Some(b) = runtime_b.take() {
        b.shutdown();
    }

    // 2. Verify A does not crash and C does not crash
    assert_eq!(runtime_a.node_id(), node_a_id);
    assert_eq!(runtime_c.node_id(), node_c_id);

    // 3. Verify routes involving B are removed on A and C
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let route_c_at_a = runtime_a.routing_table().get_next_hop(&node_c_id).await;
        let route_a_at_c = runtime_c.routing_table().get_next_hop(&node_a_id).await;
        if route_c_at_a.is_none() && route_a_at_c.is_none() {
            break;
        }
    }
    assert!(runtime_a
        .routing_table()
        .get_next_hop(&node_c_id)
        .await
        .is_none());
    assert!(runtime_c
        .routing_table()
        .get_next_hop(&node_a_id)
        .await
        .is_none());

    // 4. Send message from A while B is down; must be queued durably on A
    let pending_msg_id = runtime_a
        .send_data_message(node_c_id, "Message while B is dead")
        .await
        .unwrap();
    assert!(runtime_a.dtn_queue().contains(&pending_msg_id).unwrap());

    // 5. Restart intermediate Relay Node B
    println!("Restarting intermediate relay Node B...");
    let restarted_b = FieldnetRuntime::start(config_b).await.unwrap();
    assert_eq!(restarted_b.node_id(), node_b_id);

    // 6. Verify routing recovers and queued message is dispatched through recovered B
    println!("Waiting for routing recovery and delivery of queued message...");
    let mut delivered = false;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let received = runtime_c.get_received_messages().await;
        if received
            .iter()
            .any(|m| m.message.message_id == pending_msg_id)
        {
            delivered = true;
            break;
        }
    }
    assert!(
        delivered,
        "Message queued during intermediate crash must be delivered after recovery"
    );

    println!("Intermediate node crash and recovery test PASSED!");

    runtime_a.shutdown();
    restarted_b.shutdown();
    runtime_c.shutdown();
}

#[tokio::test]
async fn test_runtime_restart_with_persisted_dtn_queue() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_restart_dtn_test");
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

    let config_a = RuntimeConfig {
        data_dir: dir_a.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(200),
        allowed_peers: Some(vec![node_b_id]),
        blocked_peers: None,
    };

    let config_b = RuntimeConfig {
        data_dir: dir_b.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(200),
        allowed_peers: Some(vec![node_a_id]),
        blocked_peers: None,
    };

    // 1. Start Node A only (Node B is offline)
    let runtime_a = FieldnetRuntime::start(config_a.clone()).await.unwrap();
    assert_eq!(runtime_a.node_id(), node_a_id);

    // Send message to offline Node B -> enqueues into SQLite DTN
    let msg_id = runtime_a
        .send_data_message(node_b_id, "Persistent message across restarts")
        .await
        .unwrap();
    assert!(runtime_a.dtn_queue().contains(&msg_id).unwrap());
    assert_eq!(runtime_a.dtn_queue().count().unwrap(), 1);

    // 2. Terminate Node A
    println!("Terminating Node A with pending DTN bundle...");
    runtime_a.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // 3. Restart Node A using the EXACT SAME data directory
    println!("Restarting Node A from data dir...");
    let restarted_a = FieldnetRuntime::start(config_a).await.unwrap();

    // Verify Node ID is identical
    assert_eq!(
        restarted_a.node_id(),
        node_a_id,
        "Node ID must remain identical across restarts"
    );

    // Verify pending DTN bundle survived restart in SQLite
    assert_eq!(
        restarted_a.dtn_queue().count().unwrap(),
        1,
        "DTN bundle must survive process restart"
    );
    assert!(restarted_a.dtn_queue().contains(&msg_id).unwrap());

    // 4. Now bring Node B online
    println!("Bringing Node B online...");
    let runtime_b = FieldnetRuntime::start(config_b).await.unwrap();
    assert_eq!(runtime_b.node_id(), node_b_id);

    // 5. Verify Node A automatically flushes bundle to B upon connection establishment
    println!("Waiting for restarted Node A to deliver persisted message to Node B...");
    let mut delivered = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let received = runtime_b.get_received_messages().await;
        if received.iter().any(|m| m.message.message_id == msg_id) {
            delivered = true;
            break;
        }
    }
    assert!(
        delivered,
        "Persisted bundle was not delivered after restart and peer connection"
    );

    // 6. Verify duplicate delivery does NOT occur
    tokio::time::sleep(Duration::from_millis(500)).await;
    let received_all = runtime_b.get_received_messages().await;
    let count_for_id = received_all
        .iter()
        .filter(|m| m.message.message_id == msg_id)
        .count();
    assert_eq!(count_for_id, 1, "Duplicate delivery must not occur");

    println!("Runtime restart with persisted DTN queue test PASSED!");

    restarted_a.shutdown();
    runtime_b.shutdown();
}
