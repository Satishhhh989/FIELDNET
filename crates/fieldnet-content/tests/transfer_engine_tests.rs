use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;

use fieldnet_content::{
    AckStatus, ChunkDescriptor, ChunkId, Chunker, ContentChunk, ContentId, ContentManifest,
    ContentRequest, ContentResponse, FixedSizeChunker, PutOptions, TransferConfig, TransferId,
    TransferManager, TransferResponseStatus, TransferState, CHUNKING_ALGORITHM_FIXED_256K,
};
use fieldnet_core::{NodeId, Priority};

/// Helper to create an initialized ContentStore with test directory.
fn create_test_store() -> (tempfile::TempDir, Arc<fieldnet_content::ContentStore>) {
    let dir = tempdir().unwrap();
    let store = fieldnet_content::ContentStore::open(dir.path()).unwrap();
    (dir, store)
}

/// Helper to create a TransferManager instance.
fn create_transfer_manager(
    node_id: NodeId,
    store: Arc<fieldnet_content::ContentStore>,
    config: TransferConfig,
) -> Arc<TransferManager> {
    TransferManager::open(
        node_id,
        Arc::clone(&store),
        Arc::clone(store.conn()),
        store.event_tx().clone(),
        config,
    )
    .unwrap()
}

#[tokio::test]
async fn test_01_transfer_session_state_machine_transitions() {
    let (_dir, store) = create_test_store();
    let local_node = NodeId::from_bytes([1u8; 32]);
    let provider_node = NodeId::from_bytes([2u8; 32]);
    let manager = create_transfer_manager(local_node, store, TransferConfig::default());

    let content_id = ContentId::from_bytes([42u8; 32]);
    let (tid, target, req) = manager
        .create_transfer(content_id, Some(provider_node), Priority::Normal)
        .await
        .unwrap();

    assert_eq!(target, provider_node);
    assert_eq!(req.transfer_id, tid);
    assert_eq!(req.content_id, content_id);
    assert!(req.requested_chunks.is_empty()); // Initial manifest request

    let session = manager.get_session(&tid).await.unwrap();
    assert_eq!(session.status, TransferState::Requested);
    assert_eq!(session.provider_node_id, provider_node);
}

#[tokio::test]
async fn test_02_manifest_negotiation_and_chunk_scheduling() {
    let (_dir, store) = create_test_store();
    let local_node = NodeId::from_bytes([1u8; 32]);
    let provider_node = NodeId::from_bytes([2u8; 32]);
    let manager = create_transfer_manager(
        local_node,
        store,
        TransferConfig {
            max_in_flight_chunks: 2,
            ..Default::default()
        },
    );

    // Create a dummy manifest with 4 chunks
    let chunks = vec![
        ChunkDescriptor {
            index: 0,
            offset: 0,
            size: 1024,
            chunk_id: ChunkId::from_bytes([10u8; 32]),
        },
        ChunkDescriptor {
            index: 1,
            offset: 1024,
            size: 1024,
            chunk_id: ChunkId::from_bytes([11u8; 32]),
        },
        ChunkDescriptor {
            index: 2,
            offset: 2048,
            size: 1024,
            chunk_id: ChunkId::from_bytes([12u8; 32]),
        },
        ChunkDescriptor {
            index: 3,
            offset: 3072,
            size: 1024,
            chunk_id: ChunkId::from_bytes([13u8; 32]),
        },
    ];
    let content_id = ContentId::from_bytes([99u8; 32]);
    let manifest = ContentManifest::new(
        content_id,
        4096,
        1024,
        chunks,
        CHUNKING_ALGORITHM_FIXED_256K,
        None,
        HashMap::new(),
        None,
    );

    let (tid, _, _) = manager
        .create_transfer(content_id, Some(provider_node), Priority::Normal)
        .await
        .unwrap();

    // Provider accepts and returns manifest
    let resp = ContentResponse {
        transfer_id: tid,
        content_id,
        manifest: Some(manifest),
        status: TransferResponseStatus::Accepted,
        accepted_chunks: vec![],
        rejected_chunks: vec![],
        total_chunks: 4,
        total_bytes: 4096,
        nominal_chunk_size: 1024,
        error_message: None,
    };

    let next_req = manager
        .handle_response(resp)
        .await
        .unwrap()
        .expect("Expected chunk request");
    assert_eq!(next_req.transfer_id, tid);
    assert_eq!(next_req.content_id, content_id);
    // Backpressure window is 2, so chunks [0, 1] should be requested
    assert_eq!(next_req.requested_chunks, vec![0, 1]);

    let session = manager.get_session(&tid).await.unwrap();
    assert_eq!(session.status, TransferState::Transferring);
    assert_eq!(session.in_flight_chunks.len(), 2);
    assert!(session.in_flight_chunks.contains(&0));
    assert!(session.in_flight_chunks.contains(&1));
}

#[tokio::test]
async fn test_03_chunk_streaming_verification_and_ack() {
    let (_dir, store) = create_test_store();
    let local_node = NodeId::from_bytes([1u8; 32]);
    let provider_node = NodeId::from_bytes([2u8; 32]);
    let manager = create_transfer_manager(
        local_node,
        store,
        TransferConfig {
            max_in_flight_chunks: 1,
            ..Default::default()
        },
    );

    let data0 = vec![0xAAu8; 1024];
    let chunk_id0 = ChunkId::from_bytes(*fieldnet_content::hash_bytes(&data0).as_bytes());

    let data1 = vec![0xBBu8; 1024];
    let chunk_id1 = ChunkId::from_bytes(*fieldnet_content::hash_bytes(&data1).as_bytes());

    let mut full_data = Vec::new();
    full_data.extend_from_slice(&data0);
    full_data.extend_from_slice(&data1);
    let expected_content_id = fieldnet_content::hash_bytes(&full_data);

    let chunks = vec![
        ChunkDescriptor {
            index: 0,
            offset: 0,
            size: 1024,
            chunk_id: chunk_id0,
        },
        ChunkDescriptor {
            index: 1,
            offset: 1024,
            size: 1024,
            chunk_id: chunk_id1,
        },
    ];
    let manifest = ContentManifest::new(
        expected_content_id,
        2048,
        1024,
        chunks,
        CHUNKING_ALGORITHM_FIXED_256K,
        None,
        HashMap::new(),
        None,
    );

    let (tid, _, _) = manager
        .create_transfer(expected_content_id, Some(provider_node), Priority::Normal)
        .await
        .unwrap();

    let resp = ContentResponse {
        transfer_id: tid,
        content_id: expected_content_id,
        manifest: Some(manifest),
        status: TransferResponseStatus::Accepted,
        accepted_chunks: vec![],
        rejected_chunks: vec![],
        total_chunks: 2,
        total_bytes: 2048,
        nominal_chunk_size: 1024,
        error_message: None,
    };
    manager.handle_response(resp).await.unwrap();

    // Stream Chunk 0
    let chunk0 = ContentChunk {
        transfer_id: tid,
        content_id: expected_content_id,
        chunk_index: 0,
        chunk_id: chunk_id0,
        data: data0,
    };
    let (ack0, next_req0, complete0) = manager.handle_chunk(chunk0).await.unwrap();
    assert_eq!(ack0.status, AckStatus::Verified);
    assert!(!complete0);
    let next_req0 = next_req0.expect("Expected next chunk request");
    assert_eq!(next_req0.requested_chunks, vec![1]);

    // Stream Chunk 1 (final chunk)
    let chunk1 = ContentChunk {
        transfer_id: tid,
        content_id: expected_content_id,
        chunk_index: 1,
        chunk_id: chunk_id1,
        data: data1,
    };
    let (ack1, next_req1, complete1) = manager.handle_chunk(chunk1).await.unwrap();
    assert_eq!(ack1.status, AckStatus::Verified);
    assert!(complete1);
    assert!(next_req1.is_none());

    let session = manager.get_session(&tid).await.unwrap();
    assert_eq!(session.status, TransferState::Complete);
    assert_eq!(session.verified_chunks.len(), 2);
}

#[tokio::test]
async fn test_04_corrupted_chunk_detection_and_rejection() {
    let (_dir, store) = create_test_store();
    let local_node = NodeId::from_bytes([1u8; 32]);
    let provider_node = NodeId::from_bytes([2u8; 32]);
    let manager = create_transfer_manager(local_node, store, TransferConfig::default());

    let data0 = vec![0xAAu8; 512];
    let chunk_id0 = ChunkId::from_bytes(*fieldnet_content::hash_bytes(&data0).as_bytes());

    let chunks = vec![ChunkDescriptor {
        index: 0,
        offset: 0,
        size: 512,
        chunk_id: chunk_id0,
    }];
    let content_id = ContentId::from_bytes([33u8; 32]);
    let manifest = ContentManifest::new(
        content_id,
        512,
        512,
        chunks,
        CHUNKING_ALGORITHM_FIXED_256K,
        None,
        HashMap::new(),
        None,
    );

    let (tid, _, _) = manager
        .create_transfer(content_id, Some(provider_node), Priority::Normal)
        .await
        .unwrap();

    let resp = ContentResponse {
        transfer_id: tid,
        content_id,
        manifest: Some(manifest),
        status: TransferResponseStatus::Accepted,
        accepted_chunks: vec![],
        rejected_chunks: vec![],
        total_chunks: 1,
        total_bytes: 512,
        nominal_chunk_size: 512,
        error_message: None,
    };
    manager.handle_response(resp).await.unwrap();

    // Send corrupted data with mismatching hash
    let corrupted_data = vec![0xFFu8; 512]; // different from 0xAA
    let corrupted_chunk = ContentChunk {
        transfer_id: tid,
        content_id,
        chunk_index: 0,
        chunk_id: chunk_id0, // claiming to be chunk_id0
        data: corrupted_data,
    };

    let (ack, next_req, complete) = manager.handle_chunk(corrupted_chunk).await.unwrap();
    assert_eq!(ack.status, AckStatus::Failed);
    assert!(next_req.is_none());
    assert!(!complete);

    let session = manager.get_session(&tid).await.unwrap();
    assert!(!session.verified_chunks.contains(&0));
    assert_eq!(session.failed_chunks.get(&0), Some(&1));
}

#[tokio::test]
async fn test_05_transfer_persistence_across_restart() {
    let dir = tempdir().unwrap();
    let local_node = NodeId::from_bytes([1u8; 32]);
    let provider_node = NodeId::from_bytes([2u8; 32]);
    let content_id = ContentId::from_bytes([88u8; 32]);

    let tid = {
        let store = fieldnet_content::ContentStore::open(dir.path()).unwrap();
        let manager = create_transfer_manager(local_node, store, TransferConfig::default());
        let (tid, _, _) = manager
            .create_transfer(content_id, Some(provider_node), Priority::High)
            .await
            .unwrap();

        // Mark progress
        let mut session = manager.get_session(&tid).await.unwrap();
        session.transition_to(TransferState::Negotiating).unwrap();
        session.transition_to(TransferState::Transferring).unwrap();
        session.total_chunks = Some(4);
        session.mark_chunk_verified(0, 1024);
        session.mark_chunk_verified(1, 1024);
        let conn = fieldnet_content::TransferPersistence::new(Arc::new(std::sync::Mutex::new(
            rusqlite::Connection::open(dir.path().join("content.db")).unwrap(),
        )))
        .unwrap();
        conn.save(&session).unwrap();
        tid
    };

    // Simulate process restart
    {
        let store2 = fieldnet_content::ContentStore::open(dir.path()).unwrap();
        let manager2 = create_transfer_manager(local_node, store2, TransferConfig::default());

        let recovered = manager2
            .get_session(&tid)
            .await
            .expect("Session must survive restart");
        assert_eq!(recovered.transfer_id, tid);
        assert_eq!(recovered.content_id, content_id);
        assert_eq!(recovered.verified_chunks.len(), 2);
        assert!(recovered.verified_chunks.contains(&0));
        assert!(recovered.verified_chunks.contains(&1));
        assert_eq!(recovered.verified_bytes, 2048);
    }
}

#[tokio::test]
async fn test_06_resumable_transfer_requests_only_missing_chunks() {
    let (_dir, store) = create_test_store();
    let local_node = NodeId::from_bytes([1u8; 32]);
    let provider_node = NodeId::from_bytes([2u8; 32]);
    let manager = create_transfer_manager(local_node, store, TransferConfig::default());

    let data0 = vec![0x11u8; 1024];
    let cid0 = ChunkId::from_bytes(*fieldnet_content::hash_bytes(&data0).as_bytes());

    let data1 = vec![0x22u8; 1024];
    let cid1 = ChunkId::from_bytes(*fieldnet_content::hash_bytes(&data1).as_bytes());

    let chunks = vec![
        ChunkDescriptor {
            index: 0,
            offset: 0,
            size: 1024,
            chunk_id: cid0,
        },
        ChunkDescriptor {
            index: 1,
            offset: 1024,
            size: 1024,
            chunk_id: cid1,
        },
        ChunkDescriptor {
            index: 2,
            offset: 2048,
            size: 1024,
            chunk_id: ChunkId::from_bytes([3u8; 32]),
        },
        ChunkDescriptor {
            index: 3,
            offset: 3072,
            size: 1024,
            chunk_id: ChunkId::from_bytes([4u8; 32]),
        },
    ];
    let content_id = ContentId::from_bytes([77u8; 32]);
    let manifest = ContentManifest::new(
        content_id,
        4096,
        1024,
        chunks,
        CHUNKING_ALGORITHM_FIXED_256K,
        None,
        HashMap::new(),
        None,
    );

    let (tid, _, _) = manager
        .create_transfer(content_id, Some(provider_node), Priority::Normal)
        .await
        .unwrap();

    let resp = ContentResponse {
        transfer_id: tid,
        content_id,
        manifest: Some(manifest),
        status: TransferResponseStatus::Accepted,
        accepted_chunks: vec![],
        rejected_chunks: vec![],
        total_chunks: 4,
        total_bytes: 4096,
        nominal_chunk_size: 1024,
        error_message: None,
    };
    manager.handle_response(resp).await.unwrap();

    // Verify Chunk 0 and Chunk 1 via real handle_chunk
    let chunk0 = ContentChunk {
        transfer_id: tid,
        content_id,
        chunk_index: 0,
        chunk_id: cid0,
        data: data0,
    };
    manager.handle_chunk(chunk0).await.unwrap();

    let chunk1 = ContentChunk {
        transfer_id: tid,
        content_id,
        chunk_index: 1,
        chunk_id: cid1,
        data: data1,
    };
    manager.handle_chunk(chunk1).await.unwrap();

    // Pause transfer
    manager.pause_transfer(&tid).await.unwrap();
    let session = manager.get_session(&tid).await.unwrap();
    assert_eq!(session.status, TransferState::Paused);
    assert_eq!(session.verified_chunks.len(), 2);

    // Resume transfer: MUST only request missing chunks (2 and 3)
    let resume_req = manager
        .resume_transfer(&tid)
        .await
        .unwrap()
        .expect("Expected resume request");
    assert_eq!(resume_req.transfer_id, tid);
    assert_eq!(resume_req.requested_chunks, vec![2, 3]);

    let session = manager.get_session(&tid).await.unwrap();
    assert_eq!(session.status, TransferState::Transferring);
}

#[tokio::test]
async fn test_07_cancellation_and_release() {
    let (_dir, store) = create_test_store();
    let local_node = NodeId::from_bytes([1u8; 32]);
    let provider_node = NodeId::from_bytes([2u8; 32]);
    let manager = create_transfer_manager(local_node, store, TransferConfig::default());

    let content_id = ContentId::from_bytes([55u8; 32]);
    let (tid, _, _) = manager
        .create_transfer(content_id, Some(provider_node), Priority::Normal)
        .await
        .unwrap();

    manager
        .cancel_transfer(&tid, "User cancelled")
        .await
        .unwrap();
    let session = manager.get_session(&tid).await.unwrap();
    assert_eq!(session.status, TransferState::Cancelled);
    assert_eq!(session.error_message, Some("User cancelled".to_string()));
}

#[tokio::test]
async fn test_08_provider_service_authorization_and_manifest_serving() {
    let (_dir, store) = create_test_store();
    let content_data = b"Provider authorization and streaming test payload content";
    let put_res = store
        .put(content_data, Some(PutOptions::default()))
        .await
        .unwrap();
    let content_id = put_res.content_id();

    // Chunk the content and build manifest
    let chunker = FixedSizeChunker::new(16).unwrap();
    let chunks = chunker.chunk_slice(content_data);
    let mut manifest_chunks = Vec::new();
    for (i, offset, chunk_bytes) in chunks {
        let (cid, _) = store.chunk_store().put(&chunk_bytes).await.unwrap();
        manifest_chunks.push(ChunkDescriptor {
            index: i as u32,
            offset,
            size: chunk_bytes.len() as u32,
            chunk_id: cid,
        });
    }
    let manifest = ContentManifest::new(
        content_id,
        content_data.len() as u64,
        16,
        manifest_chunks,
        CHUNKING_ALGORITHM_FIXED_256K,
        None,
        HashMap::new(),
        None,
    );
    store.manifest_store().put(&manifest).unwrap();

    let provider_node = NodeId::from_bytes([2u8; 32]);
    let requester_node = NodeId::from_bytes([1u8; 32]);
    let manager = create_transfer_manager(provider_node, store, TransferConfig::default());
    let provider_svc = manager.provider_service();

    // 1. Unauthorized request must be rejected fail-closed
    let unauth_req = ContentRequest {
        transfer_id: TransferId::random(),
        content_id,
        manifest_id: None,
        requested_chunks: vec![],
        requester_node_id: requester_node,
        max_window_bytes: 65536,
        priority: 5,
        created_at: 1000,
        expires_at: 2000,
        auth_token: None,
    };
    let unauth_res = provider_svc
        .handle_request(&unauth_req, false)
        .await
        .unwrap();
    match unauth_res {
        fieldnet_content::ProviderResponse::Response(resp) => {
            assert_eq!(resp.status, TransferResponseStatus::Unauthorized);
            assert!(resp.manifest.is_none());
        }
        _ => panic!("Expected unauthorized response"),
    }

    // 2. Authorized manifest request succeeds
    let auth_res = provider_svc
        .handle_request(&unauth_req, true)
        .await
        .unwrap();
    match auth_res {
        fieldnet_content::ProviderResponse::Response(resp) => {
            assert_eq!(resp.status, TransferResponseStatus::Accepted);
            assert!(resp.manifest.is_some());
            assert_eq!(resp.total_chunks, manifest.chunk_count);
        }
        _ => panic!("Expected accepted response"),
    }

    // 3. Authorized chunk request streams individual chunks
    let chunk_req = ContentRequest {
        transfer_id: TransferId::random(),
        content_id,
        manifest_id: Some(manifest.manifest_id),
        requested_chunks: vec![0, 1],
        requester_node_id: requester_node,
        max_window_bytes: 65536,
        priority: 5,
        created_at: 1000,
        expires_at: 2000,
        auth_token: None,
    };
    let chunk_res = provider_svc.handle_request(&chunk_req, true).await.unwrap();
    match chunk_res {
        fieldnet_content::ProviderResponse::Chunks(chunks) => {
            assert_eq!(chunks.len(), 2);
            assert_eq!(chunks[0].chunk_index, 0);
            assert_eq!(chunks[1].chunk_index, 1);
            assert_eq!(chunks[0].data, &content_data[0..16]);
            assert_eq!(chunks[1].data, &content_data[16..32]);
        }
        _ => panic!("Expected chunks stream"),
    }
}

#[tokio::test]
async fn test_09_final_content_reconstruction_and_verification() {
    let (_dir, store) = create_test_store();
    let local_node = NodeId::from_bytes([1u8; 32]);
    let provider_node = NodeId::from_bytes([2u8; 32]);
    let manager =
        create_transfer_manager(local_node, Arc::clone(&store), TransferConfig::default());

    let payload =
        b"Distributed Transfer Engine: final reconstruction and ContentId hash verification";
    let expected_content_id = fieldnet_content::hash_bytes(payload);

    let part1 = &payload[..50];
    let part2 = &payload[50..];
    let cid1 = ChunkId::from_bytes(*fieldnet_content::hash_bytes(part1).as_bytes());
    let cid2 = ChunkId::from_bytes(*fieldnet_content::hash_bytes(part2).as_bytes());

    let chunks = vec![
        ChunkDescriptor {
            index: 0,
            offset: 0,
            size: part1.len() as u32,
            chunk_id: cid1,
        },
        ChunkDescriptor {
            index: 1,
            offset: part1.len() as u64,
            size: part2.len() as u32,
            chunk_id: cid2,
        },
    ];
    let manifest = ContentManifest::new(
        expected_content_id,
        payload.len() as u64,
        part1.len() as u32,
        chunks,
        CHUNKING_ALGORITHM_FIXED_256K,
        None,
        HashMap::new(),
        None,
    );

    let (tid, _, _) = manager
        .create_transfer(expected_content_id, Some(provider_node), Priority::Normal)
        .await
        .unwrap();

    let resp = ContentResponse {
        transfer_id: tid,
        content_id: expected_content_id,
        manifest: Some(manifest),
        status: TransferResponseStatus::Accepted,
        accepted_chunks: vec![],
        rejected_chunks: vec![],
        total_chunks: 2,
        total_bytes: payload.len() as u64,
        nominal_chunk_size: part1.len() as u32,
        error_message: None,
    };
    manager.handle_response(resp).await.unwrap();

    manager
        .handle_chunk(ContentChunk {
            transfer_id: tid,
            content_id: expected_content_id,
            chunk_index: 0,
            chunk_id: cid1,
            data: part1.to_vec(),
        })
        .await
        .unwrap();

    let (_, _, complete) = manager
        .handle_chunk(ContentChunk {
            transfer_id: tid,
            content_id: expected_content_id,
            chunk_index: 1,
            chunk_id: cid2,
            data: part2.to_vec(),
        })
        .await
        .unwrap();

    assert!(complete);

    // Verify session state is Complete
    let session = manager.get_session(&tid).await.unwrap();
    assert_eq!(session.status, TransferState::Complete);

    // Verify content is stored in CAS and can be fetched
    assert!(store.has(&expected_content_id).unwrap());
    let retrieved = store.get(&expected_content_id).await.unwrap();
    assert_eq!(retrieved, payload);
}

#[tokio::test]
async fn test_10_corrupted_final_verification_rejection() {
    let (_dir, store) = create_test_store();
    let local_node = NodeId::from_bytes([1u8; 32]);
    let provider_node = NodeId::from_bytes([2u8; 32]);
    let manager =
        create_transfer_manager(local_node, Arc::clone(&store), TransferConfig::default());

    let part1 = b"Real chunk part 1";
    let part2 = b"Real chunk part 2";
    let cid1 = ChunkId::from_bytes(*fieldnet_content::hash_bytes(part1).as_bytes());
    let cid2 = ChunkId::from_bytes(*fieldnet_content::hash_bytes(part2).as_bytes());

    // Fake claimed content_id that does NOT match the hash of part1 + part2
    let fake_content_id = ContentId::from_bytes([0xEEu8; 32]);

    let chunks = vec![
        ChunkDescriptor {
            index: 0,
            offset: 0,
            size: part1.len() as u32,
            chunk_id: cid1,
        },
        ChunkDescriptor {
            index: 1,
            offset: part1.len() as u64,
            size: part2.len() as u32,
            chunk_id: cid2,
        },
    ];
    let manifest = ContentManifest::new(
        fake_content_id,
        (part1.len() + part2.len()) as u64,
        part1.len() as u32,
        chunks,
        CHUNKING_ALGORITHM_FIXED_256K,
        None,
        HashMap::new(),
        None,
    );

    let (tid, _, _) = manager
        .create_transfer(fake_content_id, Some(provider_node), Priority::Normal)
        .await
        .unwrap();

    let resp = ContentResponse {
        transfer_id: tid,
        content_id: fake_content_id,
        manifest: Some(manifest),
        status: TransferResponseStatus::Accepted,
        accepted_chunks: vec![],
        rejected_chunks: vec![],
        total_chunks: 2,
        total_bytes: (part1.len() + part2.len()) as u64,
        nominal_chunk_size: part1.len() as u32,
        error_message: None,
    };
    manager.handle_response(resp).await.unwrap();

    // Chunk 0 verifies individual hash
    manager
        .handle_chunk(ContentChunk {
            transfer_id: tid,
            content_id: fake_content_id,
            chunk_index: 0,
            chunk_id: cid1,
            data: part1.to_vec(),
        })
        .await
        .unwrap();

    // Chunk 1 verifies individual hash, but final concatenation mismatch with fake_content_id!
    let res = manager
        .handle_chunk(ContentChunk {
            transfer_id: tid,
            content_id: fake_content_id,
            chunk_index: 1,
            chunk_id: cid2,
            data: part2.to_vec(),
        })
        .await;

    // Final integrity failure must be returned
    assert!(res.is_err());
    let session = manager.get_session(&tid).await.unwrap();
    assert_eq!(session.status, TransferState::Corrupted);

    // Corrupted content must NEVER be exposed as available in CAS
    assert!(!store.has(&fake_content_id).unwrap());
}

#[tokio::test]
async fn test_11_timeout_and_expiration_enforcement() {
    let (_dir, store) = create_test_store();
    let local_node = NodeId::from_bytes([1u8; 32]);
    let provider_node = NodeId::from_bytes([2u8; 32]);
    let manager = create_transfer_manager(
        local_node,
        store,
        TransferConfig {
            session_timeout_secs: 1, // 1-second timeout
            idle_timeout_secs: 1,
            max_retries: 2,
            ..Default::default()
        },
    );

    let content_id = ContentId::from_bytes([44u8; 32]);
    let (tid, _, _) = manager
        .create_transfer(content_id, Some(provider_node), Priority::Normal)
        .await
        .unwrap();

    // Wait 1.1 seconds for expiration
    tokio::time::sleep(tokio::time::Duration::from_millis(1100)).await;

    let retries = manager.check_timeouts_and_retries().await.unwrap();
    assert!(retries.is_empty());

    let session = manager.get_session(&tid).await.unwrap();
    assert_eq!(session.status, TransferState::Expired);
}
