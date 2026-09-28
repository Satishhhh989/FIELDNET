use std::fs;
use std::io::Cursor;
use std::time::Duration;

use fieldnet_content::{auth::ContentAction, transfer::TransferState};
use fieldnet_identity::NodeIdentity;
use fieldnet_runtime::{FieldnetRuntime, RuntimeConfig};

fn generate_test_payload(size_bytes: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size_bytes);
    for i in 0..size_bytes {
        data.push(((i * 11 + 17 + (i / 1024)) % 256) as u8);
    }
    data
}

/// SECTION 24 MANDATORY REAL FAILURE TEST
/// Real multi-node scenario testing:
/// Node A, Node B, Node C over real TCP sockets.
/// 1. Node A owns content.
/// 2. Node B downloads it.
/// 3. Interrupt Node B during transfer.
/// 4. Restart Node B.
/// 5. Resume transfer.
/// 6. Corrupt one local chunk on Node B.
/// 7. Run integrity detection (audit).
/// 8. Recover chunk from Node A.
/// 9. Verify final ContentId.
/// 10. Pin content on Node B.
/// 11. Run GC on Node B -> verify pinned content survives.
/// 12. Unpin -> allow it to become evictable -> verify GC reclaims it.
#[tokio::test]
async fn test_three_nodes_failure_recovery_corruption_and_gc_lifecycle() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join(format!(
        "fieldnet_resilience_section24_{}",
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

    let test_discovery_port = 5219;

    let id_a = NodeIdentity::generate();
    id_a.save_to_file(&dir_a.join("identity.json")).unwrap();
    let id_b = NodeIdentity::generate();
    id_b.save_to_file(&dir_b.join("identity.json")).unwrap();
    let id_c = NodeIdentity::generate();
    id_c.save_to_file(&dir_c.join("identity.json")).unwrap();

    let node_a_id = id_a.node_id();
    let node_b_id = id_b.node_id();
    let _node_c_id = id_c.node_id();

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

    let runtime_a = FieldnetRuntime::start(config_a)
        .await
        .expect("Failed to start Runtime A");
    let runtime_b = FieldnetRuntime::start(config_b.clone())
        .await
        .expect("Failed to start Runtime B");
    let runtime_c = FieldnetRuntime::start(config_c)
        .await
        .expect("Failed to start Runtime C");

    // 1. Wait for three-node mesh discovery and connections
    let mut mesh_connected = false;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let conns_a = runtime_a.get_connected_peers().await;
        let conns_b = runtime_b.get_connected_peers().await;
        let conns_c = runtime_c.get_connected_peers().await;
        if conns_a.contains(&node_b_id)
            && conns_b.contains(&node_a_id)
            && (conns_c.contains(&node_a_id) || conns_c.contains(&node_b_id))
        {
            mesh_connected = true;
            break;
        }
    }
    assert!(mesh_connected, "Mesh nodes A, B, and C must connect");

    // 2. Node A creates & stores multi-chunk content (750 KiB -> 3 chunks)
    let payload = generate_test_payload(750 * 1024);
    let mut cursor = Cursor::new(&payload);
    let (content_id, _put_res, manifest) = runtime_a
        .content_put_chunked_stream(&mut cursor, Some("application/octet-stream".into()))
        .await
        .expect("Node A put_chunked_stream failed");
    assert_eq!(manifest.chunk_count, 3);
    assert_eq!(manifest.total_size, 750 * 1024);

    // Make content public or grant permission to Node B on Node A's authorizer
    runtime_a
        .content_grant_permission(
            &content_id,
            &node_b_id,
            &[ContentAction::Read, ContentAction::Replicate],
        )
        .expect("Node A grant_permission failed");

    // 3. Node B initiates transfer from Node A
    let mut event_rx = runtime_b.transfer_manager().subscribe_events();
    let transfer_id = runtime_b
        .content_request(content_id, Some(node_a_id))
        .await
        .expect("content_request should succeed");

    // 4. Interrupt Node B during transfer after first chunk verified
    let mut chunk_received = false;
    while let Ok(event) = event_rx.recv().await {
        if let fieldnet_content::ContentEvent::ChunkTransferVerified { .. } = event {
            chunk_received = true;
            // Interrupt Node B immediately
            runtime_b
                .content_pause_transfer(&transfer_id)
                .await
                .unwrap();
            break;
        }
    }
    assert!(
        chunk_received,
        "Node B must receive at least 1 chunk before interruption"
    );

    // 5. Cleanly shut down Node B to simulate crash
    runtime_b.shutdown();
    drop(runtime_b);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 6. Restart Node B from persistent data directory
    let new_runtime_b = FieldnetRuntime::start(config_b)
        .await
        .expect("Failed to restart Runtime B");

    // Verify in-flight transfer session was recovered to Paused by CrashRecoveryEngine
    let session_status = new_runtime_b
        .content_transfer_status(&transfer_id)
        .await
        .expect("Transfer session must be recovered across restart");
    assert_eq!(session_status.status, TransferState::Paused);

    // Reconnect Node A and Node B
    let mut reconnected = false;
    for _ in 0..60 {
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
    assert!(
        reconnected,
        "Node A and Node B must reconnect after restart"
    );

    // 7. Resume the transfer on Node B
    new_runtime_b
        .content_resume_transfer(&transfer_id)
        .await
        .expect("content_resume_transfer failed");

    // Poll until complete
    let mut complete = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        if let Ok(st) = new_runtime_b.content_transfer_status(&transfer_id).await {
            if st.status == TransferState::Complete {
                complete = true;
                assert_eq!(st.verified_chunks.len(), 3);
                break;
            }
        }
    }
    assert!(
        complete,
        "Resumed transfer on Node B must complete successfully"
    );

    // Node B possesses complete content bit-for-bit
    assert!(new_runtime_b.content_has(&content_id).unwrap());
    let downloaded_bytes = new_runtime_b.content_get(&content_id).await.unwrap();
    assert_eq!(downloaded_bytes, payload);

    // 8. Corrupt one local chunk on Node B
    let victim_chunk_id = manifest.chunks[0].chunk_id;
    let victim_chunk_rel_path = victim_chunk_id.relative_storage_path();
    let victim_chunk_path = new_runtime_b
        .content_store()
        .chunk_store()
        .objects_dir()
        .join(&victim_chunk_rel_path);
    assert!(victim_chunk_path.exists());

    let mut chunk_raw = fs::read(&victim_chunk_path).unwrap();
    chunk_raw[5] ^= 0xCC;
    fs::write(&victim_chunk_path, &chunk_raw).unwrap();

    // 9. Run integrity detection (Audit) on Node B
    let audit_report = new_runtime_b
        .content_audit(&content_id)
        .await
        .expect("content_audit should succeed");
    assert!(
        !audit_report.is_healthy,
        "Audit must detect corrupted chunk!"
    );
    assert_eq!(audit_report.corrupted_chunks, vec![victim_chunk_id]);

    // 10. Recover corrupted chunk from Node A
    let genuine_chunk_bytes = runtime_a
        .content_store()
        .chunk_store()
        .get(&victim_chunk_id)
        .await
        .expect("Node A must have genuine chunk");

    // Quarantine corrupted chunk and restore valid bytes
    new_runtime_b
        .content_store()
        .recovery_engine()
        .quarantine_chunk(&victim_chunk_id, Some(content_id), "Detected by audit")
        .await
        .unwrap();

    new_runtime_b
        .content_store()
        .recovery_engine()
        .restore_chunk(
            &victim_chunk_id,
            &genuine_chunk_bytes,
            Some(content_id),
            Some(node_a_id),
        )
        .await
        .expect("Valid chunk replacement must succeed");

    new_runtime_b
        .content_store()
        .recovery_engine()
        .restore_content(&content_id)
        .await
        .unwrap();

    // Verify re-audit is completely healthy
    let clean_audit = new_runtime_b.content_audit(&content_id).await.unwrap();
    assert!(
        clean_audit.is_healthy,
        "Audit after recovery must be completely healthy"
    );

    // 11. Verify final ContentId matches bit-for-bit
    let final_bytes = new_runtime_b.content_get(&content_id).await.unwrap();
    assert_eq!(final_bytes, payload);

    // 12. Pin content on Node B
    new_runtime_b.content_pin(&content_id).unwrap();
    assert!(new_runtime_b.content_is_pinned(&content_id).unwrap());

    // 13. Run GC on Node B -> verify pinned content survives
    let gc_rep = new_runtime_b.content_run_gc(None).await.unwrap();
    assert!(gc_rep.protected_objects >= 1);
    assert!(new_runtime_b.content_has(&content_id).unwrap());

    // 14. Unpin content on Node B -> verify it becomes eligible for eviction
    new_runtime_b.content_unpin(&content_id).unwrap();
    assert!(!new_runtime_b.content_is_pinned(&content_id).unwrap());

    let gc_rep_2 = new_runtime_b.content_run_gc(None).await.unwrap();
    assert!(gc_rep_2.evicted_objects >= 1);
    assert!(!new_runtime_b.content_has(&content_id).unwrap());

    // Clean up
    new_runtime_b.shutdown();
    runtime_a.shutdown();
    runtime_c.shutdown();
    let _ = fs::remove_dir_all(&base_dir);
}
