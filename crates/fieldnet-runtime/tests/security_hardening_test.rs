use fieldnet_core::{NodeId, Priority, Timestamp};
use fieldnet_identity::NodeIdentity;
use fieldnet_protocol::{AckStatus, Message, MessageType, RouteAdvertisement, RouteEntry};
use fieldnet_runtime::dtn::{DtnQueue, MAX_DTN_MESSAGE_BYTES};
use fieldnet_runtime::routing::RoutingTable;
use fieldnet_runtime::{FieldnetRuntime, MessageDeliveryStatus, RuntimeConfig};
use std::fs;
use std::time::Duration;

#[tokio::test]
async fn test_duplicate_and_replay_rejection() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_dedupe_test");
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();

    let test_discovery_port = 4885;

    let id_a = NodeIdentity::generate();
    id_a.save_to_file(&dir_a.join("identity.json")).unwrap();
    let id_b = NodeIdentity::generate();
    id_b.save_to_file(&dir_b.join("identity.json")).unwrap();

    let node_a_id = id_a.node_id();
    let node_b_id = id_b.node_id();

    let config_a = RuntimeConfig {
        data_dir: dir_a,
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(200),
        allowed_peers: Some(vec![node_b_id]),
        blocked_peers: None,
    };

    let config_b = RuntimeConfig {
        data_dir: dir_b,
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(200),
        allowed_peers: Some(vec![node_a_id]),
        blocked_peers: None,
    };

    let runtime_a = FieldnetRuntime::start(config_a).await.unwrap();
    let runtime_b = FieldnetRuntime::start(config_b).await.unwrap();

    // Wait for connection
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        if runtime_a.get_connected_peers().await.contains(&node_b_id) {
            break;
        }
    }
    assert!(runtime_a.get_connected_peers().await.contains(&node_b_id));

    // Send original message
    let msg = Message::sign_and_build(
        &id_a,
        node_b_id,
        MessageType::Data,
        b"Original Message".to_vec(),
        16,
        60_000,
        1,
        Priority::Normal,
    )
    .unwrap();
    let original_msg_id = msg.message_id;

    // Send message directly over connection
    runtime_a
        .connection_manager()
        .send_message(&node_b_id, msg.clone())
        .await
        .unwrap();

    // Inject exact duplicate message
    tokio::time::sleep(Duration::from_millis(100)).await;
    runtime_a
        .connection_manager()
        .send_message(&node_b_id, msg.clone())
        .await
        .unwrap();

    // Inject replay attempt: same message ID sent again
    tokio::time::sleep(Duration::from_millis(100)).await;
    runtime_a
        .connection_manager()
        .send_message(&node_b_id, msg)
        .await
        .unwrap();

    // Verify Node B received exactly 1 message delivered to application
    tokio::time::sleep(Duration::from_millis(500)).await;
    let received = runtime_b.get_received_messages().await;
    let count = received
        .iter()
        .filter(|m| m.message.message_id == original_msg_id)
        .count();
    assert_eq!(
        count, 1,
        "Duplicate and replay messages must be rejected; exactly 1 delivery expected"
    );

    runtime_a.shutdown();
    runtime_b.shutdown();
}

#[tokio::test]
async fn test_ttl_and_expiration_enforcement() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_ttl_test");
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();

    let test_discovery_port = 4886;

    let id_a = NodeIdentity::generate();
    id_a.save_to_file(&dir_a.join("identity.json")).unwrap();
    let id_b = NodeIdentity::generate();
    id_b.save_to_file(&dir_b.join("identity.json")).unwrap();

    let node_a_id = id_a.node_id();
    let node_b_id = id_b.node_id();

    let config_a = RuntimeConfig {
        data_dir: dir_a,
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(200),
        allowed_peers: Some(vec![node_b_id]),
        blocked_peers: None,
    };

    let config_b = RuntimeConfig {
        data_dir: dir_b,
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(200),
        allowed_peers: Some(vec![node_a_id]),
        blocked_peers: None,
    };

    let runtime_a = FieldnetRuntime::start(config_a).await.unwrap();
    let runtime_b = FieldnetRuntime::start(config_b).await.unwrap();

    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        if runtime_a.get_connected_peers().await.contains(&node_b_id) {
            break;
        }
    }

    // 1. Test TTL = 0 injection: must be dropped at ingress
    let mut msg_ttl_0 = Message::sign_and_build(
        &id_a,
        node_b_id,
        MessageType::Data,
        b"TTL Zero Payload".to_vec(),
        1,
        60_000,
        10,
        Priority::Normal,
    )
    .unwrap();
    msg_ttl_0.ttl = 0; // forcibly set to 0
    let msg_ttl_0_id = msg_ttl_0.message_id;

    runtime_a
        .connection_manager()
        .send_message(&node_b_id, msg_ttl_0)
        .await
        .unwrap();

    // 2. Test Expired Message: expires_at in the past
    let mut msg_expired = Message::sign_and_build(
        &id_a,
        node_b_id,
        MessageType::Data,
        b"Expired Payload".to_vec(),
        16,
        60_000,
        11,
        Priority::Normal,
    )
    .unwrap();
    msg_expired.expires_at = Timestamp::now().as_millis().saturating_sub(10_000); // 10s in the past
    let msg_expired_id = msg_expired.message_id;

    runtime_a
        .connection_manager()
        .send_message(&node_b_id, msg_expired)
        .await
        .unwrap();

    // Wait and verify neither message is delivered to application
    tokio::time::sleep(Duration::from_millis(500)).await;
    let received = runtime_b.get_received_messages().await;
    assert!(
        !received
            .iter()
            .any(|m| m.message.message_id == msg_ttl_0_id),
        "Message with TTL=0 must never be delivered"
    );
    assert!(
        !received
            .iter()
            .any(|m| m.message.message_id == msg_expired_id),
        "Expired message must never be delivered"
    );

    // 3. Test Routing Table Route Expiration
    let table = RoutingTable::new(node_a_id);
    let expired_target = NodeId::from_bytes([77u8; 32]);
    let advert = RouteAdvertisement::new(
        node_b_id,
        vec![RouteEntry {
            destination: expired_target,
            hops: 1,
            sequence: 1,
        }],
    );
    table.update_from_advertisement(node_b_id, advert).await;
    assert!(table.get_next_hop(&expired_target).await.is_some());

    // Artificially expire the route
    table.expire_route(&expired_target).await;
    // Expired route MUST NOT be selected
    assert!(
        table.get_next_hop(&expired_target).await.is_none(),
        "Expired route must not be selected as next hop"
    );
    let purged = table.purge_expired().await;
    assert_eq!(purged, 1, "Purge must remove expired route");

    runtime_a.shutdown();
    runtime_b.shutdown();
}

#[tokio::test]
async fn test_malformed_input_robustness() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_malformed_test");
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();

    let test_discovery_port = 4887;

    let id_a = NodeIdentity::generate();
    id_a.save_to_file(&dir_a.join("identity.json")).unwrap();
    let id_b = NodeIdentity::generate();
    id_b.save_to_file(&dir_b.join("identity.json")).unwrap();

    let node_a_id = id_a.node_id();
    let node_b_id = id_b.node_id();

    let config_a = RuntimeConfig {
        data_dir: dir_a,
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(200),
        allowed_peers: Some(vec![node_b_id]),
        blocked_peers: None,
    };

    let config_b = RuntimeConfig {
        data_dir: dir_b,
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(200),
        allowed_peers: Some(vec![node_a_id]),
        blocked_peers: None,
    };

    let runtime_a = FieldnetRuntime::start(config_a).await.unwrap();
    let runtime_b = FieldnetRuntime::start(config_b).await.unwrap();

    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        if runtime_a.get_connected_peers().await.contains(&node_b_id) {
            break;
        }
    }

    // 1. Send malformed ROUTE advertisement (random corrupted bytes in payload)
    let bad_route_msg = Message::sign_and_build(
        &id_a,
        node_b_id,
        MessageType::Route,
        b"INVALID_CORRUPTED_JSON_ROUTE_ADVERT_NOT_A_VALID_OBJECT".to_vec(),
        1,
        60_000,
        100,
        Priority::Normal,
    )
    .unwrap();
    runtime_a
        .connection_manager()
        .send_message(&node_b_id, bad_route_msg)
        .await
        .unwrap();

    // 2. Send malformed ACK payload
    let bad_ack_msg = Message::sign_and_build(
        &id_a,
        node_b_id,
        MessageType::Ack,
        b"{ not valid ack json }".to_vec(),
        1,
        60_000,
        101,
        Priority::Normal,
    )
    .unwrap();
    runtime_a
        .connection_manager()
        .send_message(&node_b_id, bad_ack_msg)
        .await
        .unwrap();

    // 3. Send message with corrupted signature
    let mut bad_sig_msg = Message::sign_and_build(
        &id_a,
        node_b_id,
        MessageType::Data,
        b"Tampered Content".to_vec(),
        16,
        60_000,
        102,
        Priority::Normal,
    )
    .unwrap();
    bad_sig_msg.auth_tag[0] ^= 0xFF; // flip signature byte
    runtime_a
        .connection_manager()
        .send_message(&node_b_id, bad_sig_msg)
        .await
        .unwrap();

    // 4. Verify runtime remains 100% operational and did not panic
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(runtime_b.node_id(), node_b_id);
    assert!(runtime_b.get_connected_peers().await.contains(&node_a_id));

    // Send a valid message now to prove Node B is still fully operational
    let valid_id = runtime_a
        .send_data_message(node_b_id, "Valid message after attacks")
        .await
        .unwrap();
    let ack = runtime_a
        .wait_for_ack(valid_id, Duration::from_secs(5))
        .await
        .unwrap();
    assert!(matches!(
        ack,
        MessageDeliveryStatus::AckReceived {
            status: AckStatus::Received,
            ..
        }
    ));

    println!("Malformed input robustness test PASSED: Runtime never panicked on corrupt inputs");

    runtime_a.shutdown();
    runtime_b.shutdown();
}

#[tokio::test]
async fn test_routing_consistency_loop_prevention_and_diamond_topology() {
    let local_id = NodeId::from_bytes([1u8; 32]);
    let table = RoutingTable::new(local_id);

    // Peer Table != Routing Table check:
    // RoutingTable maintains multi-hop distance-vector state distinct from 1-hop physical links
    let neighbor_b = NodeId::from_bytes([2u8; 32]);
    let neighbor_c = NodeId::from_bytes([3u8; 32]);
    let target_d = NodeId::from_bytes([4u8; 32]);

    table.add_direct_neighbor(neighbor_b).await;
    table.add_direct_neighbor(neighbor_c).await;

    // 1. Loop Prevention: advertisements advertising local node are ignored
    let advert_with_self = RouteAdvertisement::new(
        neighbor_b,
        vec![
            RouteEntry {
                destination: local_id,
                hops: 0,
                sequence: 1,
            },
            RouteEntry {
                destination: target_d,
                hops: 2,
                sequence: 1,
            },
        ],
    );
    let changed = table
        .update_from_advertisement(neighbor_b, advert_with_self)
        .await;
    assert!(changed);
    assert!(
        table.get_next_hop(&local_id).await.is_none(),
        "Table must never route to self"
    );

    // 2. Loop Prevention: advertisements exceeding MAX_ROUTE_HOPS (16) are rejected
    let advert_excessive_hops = RouteAdvertisement::new(
        neighbor_b,
        vec![RouteEntry {
            destination: NodeId::from_bytes([99u8; 32]),
            hops: 16, // at limit
            sequence: 1,
        }],
    );
    let changed_excess = table
        .update_from_advertisement(neighbor_b, advert_excessive_hops)
        .await;
    assert!(!changed_excess, "Routes >= MAX_ROUTE_HOPS must be rejected");

    // 3. Diamond topology shortest-path selection:
    // Route to D via B has 2 hops (advertised as 2 -> 3 hops from local).
    // Route to D via C arrives with 1 hop (advertised as 1 -> 2 hops from local).
    let advert_c = RouteAdvertisement::new(
        neighbor_c,
        vec![RouteEntry {
            destination: target_d,
            hops: 1,
            sequence: 1,
        }],
    );
    table.update_from_advertisement(neighbor_c, advert_c).await;

    // Next hop for D must be C because it has lower metric (2 hops vs 3 hops)
    let best_next_hop = table.get_next_hop(&target_d).await.unwrap();
    assert_eq!(best_next_hop, neighbor_c, "Shortest path must be selected");
    let route = table.get_route(&target_d).await.unwrap();
    assert_eq!(route.hops, 2);

    // 4. Failover: When C disconnects, route through C is invalidated, route falls back or clears
    let removed = table.remove_routes_via(&neighbor_c).await;
    assert!(removed.contains(&target_d));
    assert!(table.get_next_hop(&target_d).await.is_none());

    // When B re-advertises D, table seamlessly converges to route via B
    let advert_b = RouteAdvertisement::new(
        neighbor_b,
        vec![RouteEntry {
            destination: target_d,
            hops: 2,
            sequence: 2,
        }],
    );
    table.update_from_advertisement(neighbor_b, advert_b).await;
    assert_eq!(table.get_next_hop(&target_d).await, Some(neighbor_b));

    println!("Routing consistency and loop prevention test PASSED");
}

#[tokio::test]
async fn test_dtn_queue_priority_and_resource_safety() {
    let temp_dir = std::env::temp_dir().join("fieldnet_dtn_hardening_test");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).unwrap();

    let db_path = temp_dir.join("dtn_hardening.db");
    let queue = DtnQueue::open(&db_path).unwrap();

    let id = NodeIdentity::generate();
    let dest = NodeId::from_bytes([88u8; 32]);

    // 1. Priority Ordering Verification: Critical > High > Normal > Low
    let msg_low = Message::sign_and_build(
        &id,
        dest,
        MessageType::Data,
        b"LOW".to_vec(),
        16,
        60_000,
        1,
        Priority::Low,
    )
    .unwrap();
    let msg_norm = Message::sign_and_build(
        &id,
        dest,
        MessageType::Data,
        b"NORM".to_vec(),
        16,
        60_000,
        2,
        Priority::Normal,
    )
    .unwrap();
    let msg_high = Message::sign_and_build(
        &id,
        dest,
        MessageType::Data,
        b"HIGH".to_vec(),
        16,
        60_000,
        3,
        Priority::High,
    )
    .unwrap();
    let msg_crit = Message::sign_and_build(
        &id,
        dest,
        MessageType::Data,
        b"CRIT".to_vec(),
        16,
        60_000,
        4,
        Priority::Critical,
    )
    .unwrap();

    // Enqueue in reverse priority order
    queue.enqueue(&msg_low).unwrap();
    queue.enqueue(&msg_norm).unwrap();
    queue.enqueue(&msg_high).unwrap();
    queue.enqueue(&msg_crit).unwrap();

    let runnable = queue
        .get_runnable(Timestamp::now().as_millis(), 10)
        .unwrap();
    assert_eq!(runnable.len(), 4);
    assert_eq!(
        runnable[0].message_id, msg_crit.message_id,
        "1st must be Critical"
    );
    assert_eq!(
        runnable[1].message_id, msg_high.message_id,
        "2nd must be High"
    );
    assert_eq!(
        runnable[2].message_id, msg_norm.message_id,
        "3rd must be Normal"
    );
    assert_eq!(
        runnable[3].message_id, msg_low.message_id,
        "4th must be Low"
    );

    // 2. Resource Safety: Message Size Limit Protection
    let oversized_payload = vec![0xAAu8; MAX_DTN_MESSAGE_BYTES + 1024];
    let oversized_msg = Message::sign_and_build(
        &id,
        dest,
        MessageType::Data,
        oversized_payload,
        16,
        60_000,
        5,
        Priority::Normal,
    )
    .unwrap();
    let enqueue_err = queue.enqueue(&oversized_msg);
    assert!(
        enqueue_err.is_err(),
        "Oversized message exceeding MAX_DTN_MESSAGE_BYTES must be rejected"
    );

    // 3. Retry Limit & Purging
    let test_id = msg_crit.message_id;
    for _ in 0..10 {
        queue.record_retry_failure(&test_id).unwrap();
    }
    // After 10 retries, bundle exceeds MAX_DTN_RETRIES and is not returned in runnable
    let remaining_runnable = queue
        .get_runnable(Timestamp::now().as_millis(), 10)
        .unwrap();
    assert!(
        !remaining_runnable.iter().any(|b| b.message_id == test_id),
        "Bundle with retries >= MAX_DTN_RETRIES must not be returned"
    );

    println!("DTN queue priority and resource safety test PASSED");
}
