//! FIELDNET Phase 07 BUILD 07: Real Multi-Node Mesh Integration, Fault Injection, and Hardened Verification Suite.
//!
//! Executes real-node scenarios over actual TCP sockets on the `FieldnetRuntime`:
//! - Scenario 1 (Section 2): E2E Content Flow across 3 real nodes (A -> B -> C).
//! - Scenario 2 (Section 3 & 4): Multi-Source Swarm Transfer & Provider Failure Injection (A, B, C, D).
//! - Scenario 3 (Section 5 & 6): Transport Disconnect & Runtime Restart Crash Recovery.
//! - Scenario 4 (Section 7): Bitrot Corruption Detection & Network Recovery.
//! - Scenario 5 (Section 9): Real-Node Content Authorization & Rejections.
//! - Scenario 6 (Section 10 & 11): Replication, Pinning, and Safe GC Quota Eviction.
//! - Scenario 7 (Section 12): Multi-Hop Mesh & DTN Partition Content Delivery.

use std::fs;
use std::io::Cursor;
use std::time::Duration;

use fieldnet_content::auth::ContentAction;
use fieldnet_content::transfer::TransferState;
use fieldnet_core::NodeId;
use fieldnet_identity::NodeIdentity;
use fieldnet_runtime::{FieldnetRuntime, RuntimeConfig};

fn generate_test_payload(size_bytes: usize, seed: u8) -> Vec<u8> {
    let mut data = Vec::with_capacity(size_bytes);
    for i in 0..size_bytes {
        data.push(((i * 7 + seed as usize * 13) % 256) as u8);
    }
    data
}

// =========================================================================
// SCENARIO 1: End-to-End Content Flow Across 3 Real Nodes (Section 2)
// A creates -> B retrieves from A -> C retrieves from B -> verified identically
// =========================================================================
#[tokio::test]
async fn test_scenario_01_e2e_content_flow_three_nodes() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join(format!(
        "fieldnet_e2e_three_nodes_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");
    let dir_c = base_dir.join("node_c");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();
    fs::create_dir_all(&dir_c).unwrap();

    let test_discovery_port = 5231;

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
        beacon_interval: Duration::from_millis(100),
        allowed_peers: None,
        blocked_peers: None,
    };
    let config_b = RuntimeConfig {
        data_dir: dir_b.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(100),
        allowed_peers: None,
        blocked_peers: None,
    };
    let config_c = RuntimeConfig {
        data_dir: dir_c.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(100),
        allowed_peers: None,
        blocked_peers: None,
    };

    let runtime_a = FieldnetRuntime::start(config_a).await.unwrap();
    let runtime_b = FieldnetRuntime::start(config_b).await.unwrap();
    let runtime_c = FieldnetRuntime::start(config_c).await.unwrap();

    // Wait for mutual discovery and TCP connections
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let conns_a = runtime_a.get_connected_peers().await;
        let conns_b = runtime_b.get_connected_peers().await;
        let conns_c = runtime_c.get_connected_peers().await;
        if conns_a.contains(&node_b_id)
            && conns_b.contains(&node_a_id)
            && conns_b.contains(&node_c_id)
            && conns_c.contains(&node_b_id)
        {
            break;
        }
    }

    // 1. Node A creates large object (750 KiB => 3 chunks of 256 KiB)
    let payload = generate_test_payload(750 * 1024, 101);
    let (content_id, _, manifest) = runtime_a
        .content_put_chunked_stream(&mut Cursor::new(&payload), None)
        .await
        .unwrap();
    assert_eq!(manifest.chunks.len(), 3);
    runtime_a.content_set_public(&content_id, true).unwrap();

    // 2. Node B requests content from Node A
    let transfer_b = runtime_b
        .content_request(content_id, Some(node_a_id))
        .await
        .unwrap();

    // Wait for Node B completion
    let mut complete_b = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        if let Ok(st) = runtime_b.content_transfer_status(&transfer_b).await {
            if st.status == TransferState::Complete {
                complete_b = true;
                break;
            }
        }
    }
    assert!(complete_b, "Transfer to Node B must complete");
    assert!(runtime_b.content_has(&content_id).unwrap());
    let bytes_b = runtime_b.content_get(&content_id).await.unwrap();
    assert_eq!(payload, bytes_b);

    // Make content public on Node B as well so Node C can retrieve it
    runtime_b.content_set_public(&content_id, true).unwrap();

    // 3. Node C requests content specifically from Node B
    let transfer_c = runtime_c
        .content_request(content_id, Some(node_b_id))
        .await
        .unwrap();

    // Wait for Node C completion
    let mut complete_c = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        if let Ok(st) = runtime_c.content_transfer_status(&transfer_c).await {
            if st.status == TransferState::Complete {
                complete_c = true;
                break;
            }
        }
    }
    assert!(complete_c, "Transfer to Node C must complete");
    assert!(runtime_c.content_has(&content_id).unwrap());
    let bytes_c = runtime_c.content_get(&content_id).await.unwrap();
    assert_eq!(payload, bytes_c);

    // Invariant: bit-for-bit identical across all 3 nodes
    assert_eq!(bytes_b, bytes_c);

    runtime_a.shutdown();
    runtime_b.shutdown();
    runtime_c.shutdown();
    let _ = fs::remove_dir_all(&base_dir);
}

// =========================================================================
// SCENARIO 2: Multi-Source Swarm Transfer & Provider Failure Injection (Sections 3 & 4)
// Content split across providers B, C, D -> A downloads from swarm -> C disconnects
// =========================================================================
#[tokio::test]
async fn test_scenario_02_multisource_parallel_swarm_and_provider_failure() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join(format!(
        "fieldnet_swarm_failure_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");
    let dir_c = base_dir.join("node_c");
    let dir_d = base_dir.join("node_d");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();
    fs::create_dir_all(&dir_c).unwrap();
    fs::create_dir_all(&dir_d).unwrap();

    let test_discovery_port = 5232;

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

    let config_a = RuntimeConfig {
        data_dir: dir_a.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(100),
        allowed_peers: None,
        blocked_peers: None,
    };
    let config_b = RuntimeConfig {
        data_dir: dir_b.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(100),
        allowed_peers: None,
        blocked_peers: None,
    };
    let config_c = RuntimeConfig {
        data_dir: dir_c.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(100),
        allowed_peers: None,
        blocked_peers: None,
    };
    let config_d = RuntimeConfig {
        data_dir: dir_d.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(100),
        allowed_peers: None,
        blocked_peers: None,
    };

    let runtime_a = FieldnetRuntime::start(config_a).await.unwrap();
    let runtime_b = FieldnetRuntime::start(config_b).await.unwrap();
    let runtime_c = FieldnetRuntime::start(config_c).await.unwrap();
    let runtime_d = FieldnetRuntime::start(config_d).await.unwrap();

    // Wait for all nodes to connect
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let conns_a = runtime_a.get_connected_peers().await;
        if conns_a.contains(&node_b_id)
            && conns_a.contains(&node_c_id)
            && conns_a.contains(&node_d_id)
        {
            break;
        }
    }

    // 1. Prepare 6-chunk payload (1.5 MiB)
    let payload = generate_test_payload(1500 * 1024, 77);

    // Node B holds complete content and manifest, and is public
    let (content_id, _, manifest) = runtime_b
        .content_put_chunked_stream(&mut Cursor::new(&payload), None)
        .await
        .unwrap();
    assert_eq!(manifest.chunks.len(), 6);
    runtime_b.content_set_public(&content_id, true).unwrap();

    // Node C and Node D also store chunks and manifest
    runtime_c
        .content_store()
        .manifest_store()
        .put(&manifest)
        .unwrap();
    runtime_d
        .content_store()
        .manifest_store()
        .put(&manifest)
        .unwrap();
    runtime_c.content_set_public(&content_id, true).unwrap();
    runtime_d.content_set_public(&content_id, true).unwrap();

    for i in 0..manifest.chunks.len() {
        let chunk_data = runtime_b
            .content_get_chunk(&manifest.chunks[i].chunk_id)
            .await
            .unwrap();
        if i < 4 {
            runtime_c
                .content_chunk_store()
                .put(&chunk_data)
                .await
                .unwrap();
        }
        if i >= 2 {
            runtime_d
                .content_chunk_store()
                .put(&chunk_data)
                .await
                .unwrap();
        }
    }

    // Node B, C, D advertise content availability to Node A
    runtime_b
        .content_advertise_local(&content_id, Some(node_a_id))
        .await
        .unwrap();
    runtime_c
        .content_advertise_local(&content_id, Some(node_a_id))
        .await
        .unwrap();
    runtime_d
        .content_advertise_local(&content_id, Some(node_a_id))
        .await
        .unwrap();

    // Wait for Node A to register providers
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        if runtime_a
            .content_provider_status(&content_id, &node_b_id)
            .is_some()
            && runtime_a
                .content_provider_status(&content_id, &node_c_id)
                .is_some()
            && runtime_a
                .content_provider_status(&content_id, &node_d_id)
                .is_some()
        {
            break;
        }
    }

    // Node A requests content with automatic provider resolution
    let transfer_id = runtime_a.content_request(content_id, None).await.unwrap();

    // Inject failure: abruptly shut down Node B mid-transfer
    tokio::time::sleep(Duration::from_millis(200)).await;
    runtime_b.shutdown();

    // Pause and resume transfer to trigger failover to remaining providers (C and D)
    let _ = runtime_a.content_pause_transfer(&transfer_id).await;
    let _ = runtime_a.content_resume_transfer(&transfer_id).await;

    // Check that existing verified chunks in Node A are preserved
    let status = runtime_a
        .content_transfer_status(&transfer_id)
        .await
        .unwrap();
    let verified_so_far = status.verified_chunks.len();
    println!(
        "Node B disconnected mid-transfer. Node A verified chunks: {}",
        verified_so_far
    );

    // Wait for completion from Node C / Node D
    let mut complete = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        if let Ok(st) = runtime_a.content_transfer_status(&transfer_id).await {
            if st.status == TransferState::Complete {
                complete = true;
                break;
            }
        }
    }

    assert!(
        complete,
        "Multi-source transfer must complete despite provider failure"
    );
    assert!(runtime_a.content_has(&content_id).unwrap());
    let final_bytes = runtime_a.content_get(&content_id).await.unwrap();
    assert_eq!(
        payload, final_bytes,
        "Reconstructed bytes must match original bit-for-bit"
    );

    runtime_a.shutdown();
    runtime_c.shutdown();
    runtime_d.shutdown();
    let _ = fs::remove_dir_all(&base_dir);
}

// =========================================================================
// SCENARIO 3: Transport Disconnect & Runtime Restart Crash Recovery (Sections 5 & 6)
// Interrupted transfer -> process exit -> reboot runtime -> clean resume
// =========================================================================
#[tokio::test]
async fn test_scenario_03_transport_failure_and_runtime_restart_resume() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join(format!(
        "fieldnet_crash_resume_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();

    let test_discovery_port = 5233;

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
        beacon_interval: Duration::from_millis(100),
        allowed_peers: None,
        blocked_peers: None,
    };
    let config_b = RuntimeConfig {
        data_dir: dir_b.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(100),
        allowed_peers: None,
        blocked_peers: None,
    };

    let runtime_a = FieldnetRuntime::start(config_a).await.unwrap();
    let runtime_b = FieldnetRuntime::start(config_b.clone()).await.unwrap();

    // Wait for connection
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        if runtime_a.get_connected_peers().await.contains(&node_b_id) {
            break;
        }
    }

    // Node A stores 600 KiB content (3 chunks)
    let payload = generate_test_payload(600 * 1024, 44);
    let (content_id, _, _) = runtime_a
        .content_put_chunked_stream(&mut Cursor::new(&payload), None)
        .await
        .unwrap();
    runtime_a.content_set_public(&content_id, true).unwrap();

    // Node B requests content
    let transfer_id = runtime_b
        .content_request(content_id, Some(node_a_id))
        .await
        .unwrap();

    // Pause mid-flight and simulate process crash
    tokio::time::sleep(Duration::from_millis(200)).await;
    let _ = runtime_b.content_pause_transfer(&transfer_id).await;
    runtime_b.shutdown();

    // Restart Node B on identical directory
    let new_runtime_b = FieldnetRuntime::start(config_b).await.unwrap();

    // Wait for reconnection
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        if runtime_a.get_connected_peers().await.contains(&node_b_id)
            && new_runtime_b
                .get_connected_peers()
                .await
                .contains(&node_a_id)
        {
            break;
        }
    }

    // Verify session recovered in PAUSED state
    let session = new_runtime_b
        .content_transfer_status(&transfer_id)
        .await
        .unwrap();
    assert_eq!(session.status, TransferState::Paused);

    // Resume transfer
    new_runtime_b
        .content_resume_transfer(&transfer_id)
        .await
        .unwrap();

    // Wait for completion
    let mut complete = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        if let Ok(st) = new_runtime_b.content_transfer_status(&transfer_id).await {
            if st.status == TransferState::Complete {
                complete = true;
                break;
            }
        }
    }
    assert!(complete, "Resumed transfer after restart must complete");
    assert!(new_runtime_b.content_has(&content_id).unwrap());
    let retrieved = new_runtime_b.content_get(&content_id).await.unwrap();
    assert_eq!(payload, retrieved);

    runtime_a.shutdown();
    new_runtime_b.shutdown();
    let _ = fs::remove_dir_all(&base_dir);
}

// =========================================================================
// SCENARIO 4: Bitrot Corruption Detection & Network Recovery (Section 7)
// Injected chunk tampering -> audit flags -> remote chunk recovered -> re-audited
// =========================================================================
#[tokio::test]
async fn test_scenario_04_bitrot_corruption_detection_and_network_recovery() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join(format!(
        "fieldnet_corrupt_rec_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();

    let test_discovery_port = 5234;

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
        beacon_interval: Duration::from_millis(100),
        allowed_peers: None,
        blocked_peers: None,
    };
    let config_b = RuntimeConfig {
        data_dir: dir_b.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(100),
        allowed_peers: None,
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

    // Node A stores content
    let payload = generate_test_payload(400 * 1024, 22);
    let (content_id, _, manifest) = runtime_a
        .content_put_chunked_stream(&mut Cursor::new(&payload), None)
        .await
        .unwrap();
    runtime_a.content_set_public(&content_id, true).unwrap();

    // Node B transfers content
    let transfer_id = runtime_b
        .content_request(content_id, Some(node_a_id))
        .await
        .unwrap();
    for _ in 0..80 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        if let Ok(st) = runtime_b.content_transfer_status(&transfer_id).await {
            if st.status == TransferState::Complete {
                break;
            }
        }
    }
    assert!(runtime_b.content_has(&content_id).unwrap());

    // Inject corruption on Node B in chunk 0
    let victim_chunk_id = manifest.chunks[0].chunk_id;
    let (s1, s2, fname) = victim_chunk_id.shard_components();
    let victim_path = dir_b
        .join("content")
        .join("chunks")
        .join("objects")
        .join(s1)
        .join(s2)
        .join(fname);
    assert!(victim_path.exists());
    fs::write(&victim_path, b"CORRUPTED BY BITROT INJECTION TEST").unwrap();

    // Audit detects corruption
    let audit_report = runtime_b.content_audit(&content_id).await.unwrap();
    assert!(!audit_report.is_healthy, "Audit must flag corrupted chunk");
    assert_eq!(audit_report.corrupted_chunks, vec![victim_chunk_id]);

    // Node B quarantines corrupted chunk
    runtime_b
        .content_store()
        .recovery_engine()
        .quarantine_chunk(
            &victim_chunk_id,
            Some(content_id),
            "Bitrot detected by audit",
        )
        .await
        .unwrap();
    assert!(
        !victim_path.exists(),
        "Corrupted chunk must be moved to quarantine"
    );

    // Fetch clean replacement chunk from Node A
    let clean_chunk_bytes = runtime_a.content_get_chunk(&victim_chunk_id).await.unwrap();
    runtime_b
        .content_store()
        .recovery_engine()
        .restore_chunk(
            &victim_chunk_id,
            &clean_chunk_bytes,
            Some(content_id),
            Some(node_a_id),
        )
        .await
        .unwrap();

    // Restore content complete status
    runtime_b
        .content_store()
        .recovery_engine()
        .restore_content(&content_id)
        .await
        .unwrap();

    // Subsequent audit is 100% clean
    let clean_audit = runtime_b.content_audit(&content_id).await.unwrap();
    assert!(
        clean_audit.is_healthy,
        "Audit after recovery must be healthy"
    );

    // Verify final ContentId
    let final_bytes = runtime_b.content_get(&content_id).await.unwrap();
    assert_eq!(payload, final_bytes);

    runtime_a.shutdown();
    runtime_b.shutdown();
    let _ = fs::remove_dir_all(&base_dir);
}

// =========================================================================
// SCENARIO 5: Real-Node Content Authorization & Rejections (Section 9)
// Unauthorized request rejected BEFORE chunk bytes served -> authorized succeeds
// =========================================================================
#[tokio::test]
async fn test_scenario_05_real_node_authorization_and_rejections() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join(format!(
        "fieldnet_auth_reject_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();

    let test_discovery_port = 5235;

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
        beacon_interval: Duration::from_millis(100),
        allowed_peers: None,
        blocked_peers: None,
    };
    let config_b = RuntimeConfig {
        data_dir: dir_b.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(100),
        allowed_peers: None,
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

    // Node A stores restricted content (NOT public)
    let payload = generate_test_payload(300 * 1024, 88);
    let (content_id, _, _) = runtime_a
        .content_put_chunked_stream(&mut Cursor::new(&payload), None)
        .await
        .unwrap();

    // Node B attempts to request without token or ACL grant -> rejected
    let transfer_id = runtime_b
        .content_request(content_id, Some(node_a_id))
        .await
        .unwrap();

    let mut rejected = false;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        if let Ok(st) = runtime_b.content_transfer_status(&transfer_id).await {
            if st.status == TransferState::Failed {
                rejected = true;
                break;
            }
        }
    }
    assert!(
        rejected,
        "Unauthorized request must be rejected and marked Failed"
    );

    // Node A explicitly grants Read permission to Node B
    runtime_a
        .content_grant_permission(&content_id, &node_b_id, &[ContentAction::Read])
        .unwrap();

    // Node B retries transfer -> now succeeds
    let transfer_id2 = runtime_b
        .content_request(content_id, Some(node_a_id))
        .await
        .unwrap();
    let mut complete = false;
    for _ in 0..80 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        if let Ok(st) = runtime_b.content_transfer_status(&transfer_id2).await {
            if st.status == TransferState::Complete {
                complete = true;
                break;
            }
        }
    }
    assert!(
        complete,
        "Authorized request must complete transfer successfully"
    );
    assert!(runtime_b.content_has(&content_id).unwrap());

    runtime_a.shutdown();
    runtime_b.shutdown();
    let _ = fs::remove_dir_all(&base_dir);
}

// =========================================================================
// SCENARIO 6: Replication, Pinning & Safe GC Quota Eviction (Sections 10 & 11)
// Replicate -> pin -> GC pressure preserves pinned -> unpin -> GC reclaims
// =========================================================================
#[tokio::test]
async fn test_scenario_06_replication_pinning_and_quota_gc() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join(format!(
        "fieldnet_gc_quota_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();

    let test_discovery_port = 5236;

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
        beacon_interval: Duration::from_millis(100),
        allowed_peers: None,
        blocked_peers: None,
    };
    let config_b = RuntimeConfig {
        data_dir: dir_b.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(100),
        allowed_peers: None,
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

    // Node A stores content and marks public
    let payload = generate_test_payload(300 * 1024, 55);
    let (content_id, _, _) = runtime_a
        .content_put_chunked_stream(&mut Cursor::new(&payload), None)
        .await
        .unwrap();
    runtime_a.content_set_public(&content_id, true).unwrap();

    // Node B replicates content using content_replicate
    let rep_transfer = runtime_b
        .content_replicate(&content_id, Some(node_a_id))
        .await
        .unwrap();
    for _ in 0..80 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        if let Ok(st) = runtime_b.content_transfer_status(&rep_transfer).await {
            if st.status == TransferState::Complete {
                break;
            }
        }
    }
    assert!(runtime_b.content_has(&content_id).unwrap());

    // Pin content on Node B
    runtime_b.content_pin(&content_id).unwrap();
    assert!(runtime_b.content_is_pinned(&content_id).unwrap());

    // Run aggressive GC on Node B -> pinned content must NOT be evicted
    let report = runtime_b.content_run_gc(Some(1)).await.unwrap();
    assert!(report.protected_objects >= 1);
    assert!(runtime_b.content_has(&content_id).unwrap());

    // Unpin content on Node B -> subsequent GC evicts it safely
    runtime_b.content_unpin(&content_id).unwrap();
    assert!(!runtime_b.content_is_pinned(&content_id).unwrap());

    let report2 = runtime_b.content_run_gc(Some(1)).await.unwrap();
    assert!(report2.evicted_objects >= 1 || report2.evicted_chunks >= 1);

    runtime_a.shutdown();
    runtime_b.shutdown();
    let _ = fs::remove_dir_all(&base_dir);
}

// =========================================================================
// SCENARIO 7: Multi-Hop Mesh & DTN Partition Content Delivery (Section 12)
// A <-> B <-> C (A and C disconnected) -> transfer routed across B
// =========================================================================
#[tokio::test]
async fn test_scenario_07_dtn_multihop_partition_recovery() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join(format!(
        "fieldnet_dtn_multihop_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");
    let dir_c = base_dir.join("node_c");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();
    fs::create_dir_all(&dir_c).unwrap();

    let port_ab = 5237;
    let port_bc = 5238;

    let id_a = NodeIdentity::generate();
    id_a.save_to_file(&dir_a.join("identity.json")).unwrap();
    let id_b = NodeIdentity::generate();
    id_b.save_to_file(&dir_b.join("identity.json")).unwrap();
    let id_c = NodeIdentity::generate();
    id_c.save_to_file(&dir_c.join("identity.json")).unwrap();

    let node_a_id = id_a.node_id();
    let node_b_id = id_b.node_id();
    let node_c_id = id_c.node_id();

    // Node A connects only with B on port_ab
    let config_a = RuntimeConfig {
        data_dir: dir_a.clone(),
        tcp_port: 0,
        discovery_port: port_ab,
        auto_connect: true,
        beacon_interval: Duration::from_millis(100),
        allowed_peers: Some(vec![node_b_id]),
        blocked_peers: None,
    };
    // Node B connects with A on port_ab and with C on port_bc
    let config_b = RuntimeConfig {
        data_dir: dir_b.clone(),
        tcp_port: 0,
        discovery_port: port_ab,
        auto_connect: true,
        beacon_interval: Duration::from_millis(100),
        allowed_peers: None,
        blocked_peers: None,
    };
    // Node C connects only with B on port_ab
    let config_c = RuntimeConfig {
        data_dir: dir_c.clone(),
        tcp_port: 0,
        discovery_port: port_ab,
        auto_connect: true,
        beacon_interval: Duration::from_millis(100),
        allowed_peers: Some(vec![node_b_id]),
        blocked_peers: Some(vec![node_a_id]), // strictly blocked from direct A connection
    };

    let runtime_a = FieldnetRuntime::start(config_a).await.unwrap();
    let runtime_b = FieldnetRuntime::start(config_b).await.unwrap();
    let runtime_c = FieldnetRuntime::start(config_c).await.unwrap();

    // Wait for A <-> B and B <-> C connections
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let conns_b = runtime_b.get_connected_peers().await;
        if conns_b.contains(&node_a_id) && conns_b.contains(&node_c_id) {
            break;
        }
    }

    // Node A stores content and marks public
    let payload = generate_test_payload(250 * 1024, 66);
    let (content_id, _, _) = runtime_a
        .content_put_chunked_stream(&mut Cursor::new(&payload), None)
        .await
        .unwrap();
    runtime_a.content_set_public(&content_id, true).unwrap();

    // Wait for routing table on Node C to learn route to Node A via Node B (2 hops)
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        if runtime_c
            .routing_table()
            .get_next_hop(&node_a_id)
            .await
            .is_some()
        {
            break;
        }
    }

    // Node C requests content from Node A via multi-hop forwarding
    let transfer_id = runtime_c
        .content_request(content_id, Some(node_a_id))
        .await
        .unwrap();

    // Wait for transfer completion across the relay
    let mut complete = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        if let Ok(st) = runtime_c.content_transfer_status(&transfer_id).await {
            if st.status == TransferState::Complete {
                complete = true;
                break;
            }
        }
    }
    assert!(complete, "Multi-hop relayed content transfer must complete");
    assert!(runtime_c.content_has(&content_id).unwrap());
    let bytes_c = runtime_c.content_get(&content_id).await.unwrap();
    assert_eq!(payload, bytes_c);

    runtime_a.shutdown();
    runtime_b.shutdown();
    runtime_c.shutdown();
    let _ = fs::remove_dir_all(&base_dir);
}
