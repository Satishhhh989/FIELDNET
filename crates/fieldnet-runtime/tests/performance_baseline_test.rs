use fieldnet_identity::NodeIdentity;
use fieldnet_protocol::AckStatus;
use fieldnet_runtime::{FieldnetRuntime, MessageDeliveryStatus, RuntimeConfig};
use std::fs;
use std::time::{Duration, Instant};

#[tokio::test]
async fn test_performance_baseline_benchmarks() {
    let _ = tracing_subscriber::fmt().with_env_filter("warn").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_perf_test");
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");
    let dir_c = base_dir.join("node_c");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();
    fs::create_dir_all(&dir_c).unwrap();

    let test_discovery_port = 4888;

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
        data_dir: dir_a,
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(100),
        allowed_peers: Some(vec![node_b_id]),
        blocked_peers: Some(vec![node_c_id]),
    };

    let config_b = RuntimeConfig {
        data_dir: dir_b,
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(100),
        allowed_peers: Some(vec![node_a_id, node_c_id]),
        blocked_peers: None,
    };

    let config_c = RuntimeConfig {
        data_dir: dir_c,
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(100),
        allowed_peers: Some(vec![node_b_id]),
        blocked_peers: Some(vec![node_a_id]),
    };

    println!("\n==================================================");
    println!("FIELDNET PHASE 05 — PERFORMANCE BASELINE BENCHMARKS");
    println!("==================================================");

    // 1. Measure Peer Discovery and Mutual Authentication Time (A <-> B)
    let start_discovery = Instant::now();
    let runtime_a = FieldnetRuntime::start(config_a).await.unwrap();
    let runtime_b = FieldnetRuntime::start(config_b).await.unwrap();

    let mut auth_time = None;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        if runtime_a.get_connected_peers().await.contains(&node_b_id) {
            auth_time = Some(start_discovery.elapsed());
            break;
        }
    }
    let discovery_and_auth_duration = auth_time.expect("Discovery & Auth timed out");
    println!(
        "1. Peer Discovery & Mutual Authentication: {:.2?}",
        discovery_and_auth_duration
    );

    // 2. Measure Direct 1-Hop Round-Trip Delivery Latency (A -> B -> A ACK)
    let mut direct_latencies = Vec::new();
    for i in 0..5 {
        let t0 = Instant::now();
        let payload = format!("Direct ping message #{}", i);
        let msg_id = runtime_a
            .send_data_message(node_b_id, &payload)
            .await
            .unwrap();
        let ack = runtime_a
            .wait_for_ack(msg_id, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(matches!(
            ack,
            MessageDeliveryStatus::AckReceived {
                status: AckStatus::Received,
                ..
            }
        ));
        direct_latencies.push(t0.elapsed());
    }
    let avg_direct: Duration =
        direct_latencies.iter().sum::<Duration>() / direct_latencies.len() as u32;
    println!(
        "2. Direct 1-Hop Round-Trip Latency (avg of 5): {:.2?} (min: {:.2?}, max: {:.2?})",
        avg_direct,
        direct_latencies.iter().min().unwrap(),
        direct_latencies.iter().max().unwrap()
    );

    // 3. Start Node C and measure Multi-Hop Route Convergence Time
    let start_convergence = Instant::now();
    let runtime_c = FieldnetRuntime::start(config_c.clone()).await.unwrap();

    let mut conv_time = None;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if runtime_a
            .routing_table()
            .get_next_hop(&node_c_id)
            .await
            .is_some()
        {
            conv_time = Some(start_convergence.elapsed());
            break;
        }
    }
    let convergence_duration = conv_time.expect("Route convergence timed out");
    println!(
        "3. Multi-Hop Route Convergence Time: {:.2?}",
        convergence_duration
    );

    // 4. Measure Multi-Hop Round-Trip Delivery Latency (A -> B -> C -> B -> A ACK)
    let mut multihop_latencies = Vec::new();
    for i in 0..5 {
        let t0 = Instant::now();
        let payload = format!("Multi-hop ping message #{}", i);
        let msg_id = runtime_a
            .send_data_message(node_c_id, &payload)
            .await
            .unwrap();
        let ack = runtime_a
            .wait_for_ack(msg_id, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(matches!(
            ack,
            MessageDeliveryStatus::AckReceived {
                status: AckStatus::Received,
                ..
            }
        ));
        multihop_latencies.push(t0.elapsed());
    }
    let avg_multihop: Duration =
        multihop_latencies.iter().sum::<Duration>() / multihop_latencies.len() as u32;
    println!(
        "4. Multi-Hop 2-Hop Round-Trip Latency (avg of 5): {:.2?} (min: {:.2?}, max: {:.2?})",
        avg_multihop,
        multihop_latencies.iter().min().unwrap(),
        multihop_latencies.iter().max().unwrap()
    );

    // 5. Measure Queued Message Recovery Time
    // Disconnect C
    runtime_c.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Send while offline
    let offline_msg_id = runtime_a
        .send_data_message(node_c_id, "Offline recovery payload")
        .await
        .unwrap();

    // Reconnect C and measure recovery latency
    let recovery_start = Instant::now();
    let restarted_c = FieldnetRuntime::start(config_c).await.unwrap();

    let mut recovery_time = None;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let received = restarted_c.get_received_messages().await;
        if received
            .iter()
            .any(|m| m.message.message_id == offline_msg_id)
        {
            recovery_time = Some(recovery_start.elapsed());
            break;
        }
    }
    let recovery_duration = recovery_time.expect("DTN recovery timed out");
    println!(
        "5. DTN Queued Message Recovery Time: {:.2?}",
        recovery_duration
    );

    // 6. Message Throughput Baseline (burst sending)
    let burst_count = 20;
    let burst_start = Instant::now();
    let mut msg_ids = Vec::new();
    for i in 0..burst_count {
        let payload = format!("Throughput burst message #{:04}", i);
        let id = runtime_a
            .send_data_message(node_b_id, &payload)
            .await
            .unwrap();
        msg_ids.push(id);
    }

    // Wait for all acks
    for id in msg_ids {
        let _ = runtime_a
            .wait_for_ack(id, Duration::from_secs(5))
            .await
            .unwrap();
    }
    let burst_duration = burst_start.elapsed();
    let msg_per_sec = (burst_count as f64) / burst_duration.as_secs_f64();
    println!(
        "6. Direct Message Throughput: {:.1} messages/sec (burst of {} in {:.2?})",
        msg_per_sec, burst_count, burst_duration
    );
    println!("==================================================\n");

    runtime_a.shutdown();
    runtime_b.shutdown();
    restarted_c.shutdown();
}
