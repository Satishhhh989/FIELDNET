use std::fs;
use std::io::Cursor;
use std::time::Duration;

use fieldnet_content::TransferState;
use fieldnet_core::NodeId;
use fieldnet_identity::NodeIdentity;
use fieldnet_runtime::{FieldnetRuntime, RuntimeConfig};

fn generate_test_payload(size_bytes: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size_bytes);
    for i in 0..size_bytes {
        data.push(((i * 7 + 13) % 256) as u8);
    }
    data
}

#[tokio::test]
async fn test_two_real_nodes_chunked_content_transfer_e2e() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join(format!(
        "fieldnet_transfer_test_e2e_{}",
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

    let test_discovery_port = 5211;

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

    let runtime_a = FieldnetRuntime::start(config_a)
        .await
        .expect("Failed to start Runtime A");
    let runtime_b = FieldnetRuntime::start(config_b)
        .await
        .expect("Failed to start Runtime B");

    // 1. Wait for discovery and mutual TCP connection
    let mut connected = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let conns_a = runtime_a.get_connected_peers().await;
        let conns_b = runtime_b.get_connected_peers().await;
        if conns_a.contains(&node_b_id) && conns_b.contains(&node_a_id) {
            connected = true;
            break;
        }
    }
    assert!(
        connected,
        "Node A and Node B must establish mutual TCP connection"
    );

    // 2. Node A stores a multi-chunk object (600 KiB -> 3 chunks with 256 KiB chunk size)
    let payload = generate_test_payload(600 * 1024);
    let mut cursor = Cursor::new(&payload);
    let (content_id, _put_res, manifest) = runtime_a
        .content_put_chunked_stream(&mut cursor, Some("application/octet-stream".into()))
        .await
        .expect("Node A content_put_chunked_stream should succeed");
    runtime_a.content_set_public(&content_id, true).unwrap();

    println!("Node A put content: {}", content_id);
    assert_eq!(manifest.chunk_count, 3);
    assert_eq!(manifest.total_size, 600 * 1024);
    assert!(runtime_a.content_has(&content_id).unwrap());
    assert!(!runtime_b.content_has(&content_id).unwrap());

    // 3. Node B requests the content from Node A
    println!("Node B requesting content from Node A...");
    let transfer_id = runtime_b
        .content_request(content_id, Some(node_a_id))
        .await
        .expect("content_request should succeed");
    println!("Transfer created: {}", transfer_id);

    // 4. Poll for transfer completion with timeout
    let mut completed = false;
    for i in 0..100 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        if let Ok(status) = runtime_b.content_transfer_status(&transfer_id).await {
            if i % 10 == 0 || status.status == TransferState::Complete {
                println!(
                    "Poll {}: status={:?}, verified={}/{}, error={:?}",
                    i,
                    status.status,
                    status.verified_chunks.len(),
                    status.total_chunks.unwrap_or(0),
                    status.error_message
                );
            }
            if status.status == TransferState::Complete {
                completed = true;
                assert_eq!(status.verified_chunks.len(), 3);
                assert_eq!(status.total_bytes, Some(600 * 1024));
                break;
            }
            if status.status == TransferState::Failed || status.status == TransferState::Corrupted {
                panic!("Transfer failed unexpectedly: {:?}", status.error_message);
            }
        }
    }
    assert!(completed, "Transfer must complete within timeout");

    // 5. Verify Node B now possesses the content in CAS and it matches bit-for-bit
    assert!(runtime_b.content_has(&content_id).unwrap());
    let retrieved = runtime_b
        .content_get(&content_id)
        .await
        .expect("Node B must retrieve content from local store");
    assert_eq!(retrieved, payload);

    // Verify all chunks are present in Node B chunk store
    for chunk_desc in &manifest.chunks {
        assert!(runtime_b.content_has_chunk(&chunk_desc.chunk_id).unwrap());
    }

    let _ = fs::remove_dir_all(&base_dir);
}

#[tokio::test]
async fn test_two_real_nodes_interrupted_transfer_and_resume() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join(format!(
        "fieldnet_transfer_test_resume_{}",
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

    let test_discovery_port = 5212;

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

    let runtime_a = FieldnetRuntime::start(config_a.clone())
        .await
        .expect("Failed to start Runtime A");
    let runtime_b = FieldnetRuntime::start(config_b.clone())
        .await
        .expect("Failed to start Runtime B");

    // 1. Wait for connection
    let mut connected = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        if runtime_a.get_connected_peers().await.contains(&node_b_id)
            && runtime_b.get_connected_peers().await.contains(&node_a_id)
        {
            connected = true;
            break;
        }
    }
    assert!(connected, "Nodes must connect");

    // 2. Node A stores a 1.5 MiB object (6 chunks: 5x 256K + 220K)
    let payload = generate_test_payload(1500 * 1024);
    let mut cursor = Cursor::new(&payload);
    let (content_id, _put_res, manifest) = runtime_a
        .content_put_chunked_stream(&mut cursor, Some("application/octet-stream".into()))
        .await
        .unwrap();
    runtime_a.content_set_public(&content_id, true).unwrap();
    assert_eq!(manifest.chunk_count, 6);

    // 3. Subscribe to chunk events on Node B to pause deterministically after exactly 2 chunks
    let mut event_rx = runtime_b.transfer_manager().subscribe_events();
    let transfer_id = runtime_b
        .content_request(content_id, Some(node_a_id))
        .await
        .unwrap();

    // 4. Intercept chunk verification events and pause immediately when 2 chunks are verified
    let mut verified_count_before = 0;
    while let Ok(event) = event_rx.recv().await {
        if let fieldnet_content::ContentEvent::ChunkTransferVerified { .. } = event {
            verified_count_before += 1;
            if verified_count_before >= 2 {
                runtime_b
                    .content_pause_transfer(&transfer_id)
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(100)).await;
                let status_after = runtime_b
                    .content_transfer_status(&transfer_id)
                    .await
                    .unwrap();
                verified_count_before = status_after.verified_chunks.len();
                break;
            }
        }
    }
    assert!(verified_count_before >= 2 && verified_count_before < 6);
    println!(
        "Transfer paused with {} verified chunks",
        verified_count_before
    );

    // 5. Cleanly shut down Node B to simulate runtime crash / process interruption
    runtime_b.shutdown();
    drop(runtime_b);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Restart Node B from same persistent data directory
    let new_runtime_b = FieldnetRuntime::start(config_b)
        .await
        .expect("Failed to restart Runtime B");

    // Verify transfer session is restored from persistent SQLite storage
    let restored_session = new_runtime_b
        .content_transfer_status(&transfer_id)
        .await
        .expect("Session must survive runtime restart");
    assert_eq!(
        restored_session.verified_chunks.len(),
        verified_count_before,
        "Restored session must retain verified chunks"
    );

    // 6. Wait for reconnection
    let mut reconnected = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        if runtime_a.get_connected_peers().await.contains(&node_b_id)
            && new_runtime_b
                .get_connected_peers()
                .await
                .contains(&node_a_id)
        {
            reconnected = true;
            break;
        }
    }
    assert!(reconnected, "Nodes must reconnect after restart");

    // 7. Resume the transfer on Node B (requests only missing chunks)
    new_runtime_b
        .content_resume_transfer(&transfer_id)
        .await
        .expect("content_resume_transfer should succeed");

    // 8. Poll until transfer completes
    let mut complete = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        if let Ok(status) = new_runtime_b.content_transfer_status(&transfer_id).await {
            if status.status == TransferState::Complete {
                complete = true;
                assert_eq!(status.verified_chunks.len(), 6);
                break;
            }
        }
    }
    assert!(complete, "Resumed transfer must reach Complete state");

    // 9. Verify full reconstruction and cryptographic integrity
    assert!(new_runtime_b.content_has(&content_id).unwrap());
    let retrieved = new_runtime_b.content_get(&content_id).await.unwrap();
    assert_eq!(retrieved, payload);

    let _ = fs::remove_dir_all(&base_dir);
}

#[tokio::test]
async fn test_two_real_nodes_unauthorized_content_request() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join(format!(
        "fieldnet_transfer_test_unauth_{}",
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

    let test_discovery_port = 5213;

    let id_a = NodeIdentity::generate();
    id_a.save_to_file(&dir_a.join("identity.json")).unwrap();
    let id_b = NodeIdentity::generate();
    id_b.save_to_file(&dir_b.join("identity.json")).unwrap();

    let node_a_id = id_a.node_id();
    let node_b_id = id_b.node_id();

    // Node A only allows a dummy node ID, so Node B is unauthorized!
    let dummy_node = NodeId::from_bytes([99u8; 32]);
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

    let runtime_a = FieldnetRuntime::start(config_a)
        .await
        .expect("Failed to start Runtime A");
    let runtime_b = FieldnetRuntime::start(config_b)
        .await
        .expect("Failed to start Runtime B");

    // Configure content access control: only dummy_node is authorized; Node B is not authorized
    runtime_a
        .set_content_authorized_peers(Some(vec![dummy_node]))
        .await;

    // Wait for TCP connection
    let mut connected = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        if runtime_a.get_connected_peers().await.contains(&node_b_id)
            && runtime_b.get_connected_peers().await.contains(&node_a_id)
        {
            connected = true;
            break;
        }
    }
    assert!(connected, "Nodes connect at transport level");

    // Node A stores confidential content
    let payload = b"Confidential tactical briefing payload";
    let (content_id, _, _) = runtime_a
        .content_put_chunked_stream(&mut Cursor::new(payload), None)
        .await
        .unwrap();

    // Node B attempts to request the content
    let transfer_id = runtime_b
        .content_request(content_id, Some(node_a_id))
        .await
        .unwrap();

    // Poll for rejection
    let mut rejected = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        if let Ok(status) = runtime_b.content_transfer_status(&transfer_id).await {
            if status.status == TransferState::Failed {
                rejected = true;
                assert!(status.error_message.is_some());
                break;
            }
        }
    }
    assert!(
        rejected,
        "Unauthorized content request must be rejected and marked Failed"
    );

    let _ = fs::remove_dir_all(&base_dir);
}

#[tokio::test]
async fn test_two_real_nodes_transfer_cancellation() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join(format!(
        "fieldnet_transfer_test_cancel_{}",
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

    let test_discovery_port = 5214;

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

    let runtime_a = FieldnetRuntime::start(config_a)
        .await
        .expect("Failed to start Runtime A");
    let runtime_b = FieldnetRuntime::start(config_b)
        .await
        .expect("Failed to start Runtime B");

    let mut connected = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        if runtime_a.get_connected_peers().await.contains(&node_b_id)
            && runtime_b.get_connected_peers().await.contains(&node_a_id)
        {
            connected = true;
            break;
        }
    }
    assert!(connected, "Nodes must connect");

    let payload = generate_test_payload(400 * 1024);
    let (content_id, _, _) = runtime_a
        .content_put_chunked_stream(&mut Cursor::new(payload), None)
        .await
        .unwrap();

    let transfer_id = runtime_b
        .content_request(content_id, Some(node_a_id))
        .await
        .unwrap();

    // Cancel transfer
    runtime_b
        .content_cancel_transfer(&transfer_id)
        .await
        .expect("content_cancel_transfer must succeed");

    let session = runtime_b
        .content_transfer_status(&transfer_id)
        .await
        .expect("Session must exist");
    assert_eq!(session.status, TransferState::Cancelled);

    let _ = fs::remove_dir_all(&base_dir);
}

#[tokio::test]
async fn test_two_real_nodes_discovery_then_transfer_without_explicit_provider() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join(format!(
        "fieldnet_transfer_test_disc_{}",
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

    let test_discovery_port = 5215;

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

    let runtime_a = FieldnetRuntime::start(config_a)
        .await
        .expect("Failed to start Runtime A");
    let runtime_b = FieldnetRuntime::start(config_b)
        .await
        .expect("Failed to start Runtime B");

    let mut connected = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        if runtime_a.get_connected_peers().await.contains(&node_b_id)
            && runtime_b.get_connected_peers().await.contains(&node_a_id)
        {
            connected = true;
            break;
        }
    }
    assert!(connected, "Nodes must connect");

    // 1. Node A stores content
    let payload = generate_test_payload(300 * 1024);
    let (content_id, _, _) = runtime_a
        .content_put_chunked_stream(&mut Cursor::new(&payload), None)
        .await
        .unwrap();
    runtime_a.content_set_public(&content_id, true).unwrap();

    // 2. Node A advertises to Node B
    runtime_a
        .content_advertise_local(&content_id, Some(node_b_id))
        .await
        .unwrap();

    // 3. Wait for Node B to receive and record the advertisement in its ProviderRegistry
    let mut discovered = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        if runtime_b
            .content_provider_status(&content_id, &node_a_id)
            .is_some()
        {
            discovered = true;
            break;
        }
    }
    assert!(discovered, "Node B must discover Node A as provider");

    // 4. Node B requests content with provider = None (TransferManager automatically resolves provider!)
    let transfer_id = runtime_b
        .content_request(content_id, None)
        .await
        .expect("content_request with automatic resolution must succeed");

    // 5. Wait for transfer completion
    let mut completed = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        if let Ok(status) = runtime_b.content_transfer_status(&transfer_id).await {
            if status.status == TransferState::Complete {
                completed = true;
                break;
            }
        }
    }
    assert!(
        completed,
        "Transfer through automatic provider resolution must complete"
    );

    assert!(runtime_b.content_has(&content_id).unwrap());
    let retrieved = runtime_b.content_get(&content_id).await.unwrap();
    assert_eq!(retrieved, payload);

    let _ = fs::remove_dir_all(&base_dir);
}
