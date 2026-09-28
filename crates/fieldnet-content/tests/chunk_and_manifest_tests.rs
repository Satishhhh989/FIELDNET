use sha2::Digest;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use tempfile::TempDir;

use fieldnet_content::{
    compute_merkle_root, hash_bytes, ChunkAvailabilityBitmap, ChunkDescriptor, ChunkId,
    ChunkStorageState, ChunkStore, Chunker, ContentError, ContentEvent, ContentId, ContentManifest,
    ContentReconstructor, ContentStore, FixedSizeChunker, ManifestId, ManifestStore,
    ManifestValidator, PutOptions, PutResult, CHUNKING_ALGORITHM_FIXED_256K, DEFAULT_CHUNK_SIZE,
};
use fieldnet_core::NodeId;

#[test]
fn test_01_chunk_hashing() {
    let chunk_data = b"FIELDNET sovereign content chunk payload verification";
    let expected_hex = hex::encode(sha2::Sha256::digest(chunk_data));

    let chunk_id = ChunkId::from_bytes(hash_bytes(chunk_data).to_bytes());
    assert_eq!(chunk_id.to_hex(), expected_hex);
    assert_eq!(chunk_id.to_string(), expected_hex);

    // Parsing
    let parsed: ChunkId = expected_hex.parse().expect("Parse failed");
    assert_eq!(parsed, chunk_id);

    // Prefix handling
    let with_pref = format!("fn-chk-{}", expected_hex);
    let parsed_pref = ChunkId::from_hex(&with_pref).unwrap();
    assert_eq!(parsed_pref, chunk_id);

    // Sharding paths
    let (s1, s2, fname) = chunk_id.shard_components();
    assert_eq!(s1, &expected_hex[0..2]);
    assert_eq!(s2, &expected_hex[2..4]);
    assert_eq!(fname, expected_hex);
}

#[test]
fn test_02_chunk_id_determinism() {
    let payload = vec![0xABu8; 1024];

    let id1 = ChunkId::from_bytes(hash_bytes(&payload).to_bytes());
    let id2 = ChunkId::from_bytes(hash_bytes(&payload).to_bytes());
    assert_eq!(
        id1, id2,
        "Identical chunk bytes must produce identical ChunkId"
    );

    let diff_payload = vec![0xACu8; 1024];
    let id_diff = ChunkId::from_bytes(hash_bytes(&diff_payload).to_bytes());
    assert_ne!(
        id1, id_diff,
        "Different chunk bytes must produce different ChunkId"
    );
}

#[tokio::test]
async fn test_03_chunk_store_put_get() {
    let temp_dir = TempDir::new().unwrap();
    let conn = Arc::new(std::sync::Mutex::new(
        rusqlite::Connection::open_in_memory().unwrap(),
    ));
    let (event_tx, _) = tokio::sync::broadcast::channel(32);

    let store = ChunkStore::open(temp_dir.path(), conn, event_tx).unwrap();

    let chunk_data = b"Testing ChunkStore put and get operations";
    let (chunk_id, put_res) = store.put(chunk_data).await.expect("put chunk failed");
    assert!(put_res.is_new());

    // Check existence
    assert!(store.has(&chunk_id).unwrap());

    // Retrieve
    let retrieved = store.get(&chunk_id).await.expect("get chunk failed");
    assert_eq!(retrieved, chunk_data);

    // Verified retrieval
    let verified = store
        .get_verified(&chunk_id)
        .await
        .expect("get_verified chunk failed");
    assert_eq!(verified, chunk_data);

    // Stat
    let stat = store.stat(&chunk_id).expect("stat chunk failed");
    assert_eq!(stat.chunk_id, chunk_id);
    assert_eq!(stat.size_bytes, chunk_data.len() as u32);
    assert_eq!(stat.storage_state, ChunkStorageState::Verified);
    assert_eq!(stat.ref_count, 1);
}

#[tokio::test]
async fn test_04_chunk_store_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("content.db");
    let chunks_dir = temp_dir.path().join("chunks");

    let chunk1 = b"Persistent chunk 1 data";
    let chunk2 = b"Persistent chunk 2 data";

    let (id1, id2) = {
        let conn = Arc::new(std::sync::Mutex::new(
            rusqlite::Connection::open(&db_path).unwrap(),
        ));
        let (tx, _) = tokio::sync::broadcast::channel(32);
        let store = ChunkStore::open(&chunks_dir, conn, tx).unwrap();

        let (c1, _) = store.put(chunk1).await.unwrap();
        let (c2, _) = store.put(chunk2).await.unwrap();
        assert_eq!(store.count().unwrap(), 2);
        (c1, c2)
    };

    // Reopen store on same database and disk path
    {
        let conn = Arc::new(std::sync::Mutex::new(
            rusqlite::Connection::open(&db_path).unwrap(),
        ));
        let (tx, _) = tokio::sync::broadcast::channel(32);
        let store = ChunkStore::open(&chunks_dir, conn, tx).unwrap();

        assert!(store.has(&id1).unwrap());
        assert!(store.has(&id2).unwrap());
        assert_eq!(store.get(&id1).await.unwrap(), chunk1);
        assert_eq!(store.get(&id2).await.unwrap(), chunk2);
        assert!(store.verify_chunk(&id1).unwrap());
        assert!(store.verify_chunk(&id2).unwrap());
        assert_eq!(store.count().unwrap(), 2);
    }
}

#[tokio::test]
async fn test_05_chunk_deduplication() {
    let temp_dir = TempDir::new().unwrap();
    let conn = Arc::new(std::sync::Mutex::new(
        rusqlite::Connection::open_in_memory().unwrap(),
    ));
    let (event_tx, _) = tokio::sync::broadcast::channel(32);

    let store = ChunkStore::open(temp_dir.path(), conn, event_tx).unwrap();

    let chunk_data = b"Duplicate chunk content for deduplication test";

    let (id1, res1) = store.put(chunk_data).await.unwrap();
    assert!(res1.is_new());

    let (id2, res2) = store.put(chunk_data).await.unwrap();
    assert!(!res2.is_new());
    assert_eq!(id1, id2);

    // Verify exactly 1 physical chunk stored
    assert_eq!(store.count().unwrap(), 1);
    let stat = store.stat(&id1).unwrap();
    assert_eq!(
        stat.ref_count, 2,
        "Ref count should be 2 after deduplication"
    );
}

#[tokio::test]
async fn test_06_concurrent_identical_chunk_insertion() {
    let temp_dir = TempDir::new().unwrap();
    let conn = Arc::new(std::sync::Mutex::new(
        rusqlite::Connection::open_in_memory().unwrap(),
    ));
    let (event_tx, _) = tokio::sync::broadcast::channel(32);

    let store = ChunkStore::open(temp_dir.path(), conn, event_tx).unwrap();

    let chunk_data = b"Concurrent chunk payload for race testing";

    let mut handles = Vec::new();
    for _ in 0..10 {
        let store_clone = Arc::clone(&store);
        handles.push(tokio::spawn(
            async move { store_clone.put(chunk_data).await },
        ));
    }

    let mut created_count = 0;
    let mut already_exists_count = 0;

    for h in handles {
        let (_id, res) = h.await.unwrap().expect("Concurrent chunk put failed");
        match res {
            PutResult::Created(_) => created_count += 1,
            PutResult::AlreadyExists(_) => already_exists_count += 1,
        }
    }

    assert_eq!(created_count, 1, "Exactly one writer commits the new chunk");
    assert_eq!(
        already_exists_count, 9,
        "All 9 subsequent concurrent writers detect existing chunk"
    );
    assert_eq!(store.count().unwrap(), 1);
}

#[tokio::test]
async fn test_07_chunk_corruption_detection() {
    let temp_dir = TempDir::new().unwrap();
    let conn = Arc::new(std::sync::Mutex::new(
        rusqlite::Connection::open_in_memory().unwrap(),
    ));
    let (event_tx, mut event_rx) = tokio::sync::broadcast::channel(32);

    let store = ChunkStore::open(temp_dir.path(), conn, event_tx).unwrap();

    let chunk_data = b"Pristine chunk data before disk bitrot";
    let (chunk_id, _) = store.put(chunk_data).await.unwrap();

    assert!(store.verify_chunk(&chunk_id).unwrap());

    // Tamper with chunk file on disk
    let obj_path = store.objects_dir().join(chunk_id.relative_storage_path());
    tokio::fs::write(&obj_path, b"Tampered corrupted chunk!")
        .await
        .unwrap();

    // get_verified must detect mismatch
    let err = store
        .get_verified(&chunk_id)
        .await
        .expect_err("Must fail verification");
    assert!(
        matches!(err, ContentError::ChunkIntegrityFailure { expected, .. } if expected == chunk_id)
    );

    // verify_chunk must return false
    assert!(!store.verify_chunk(&chunk_id).unwrap());

    // State in index transitioned to CORRUPTED
    let stat = store.stat(&chunk_id).unwrap();
    assert_eq!(stat.storage_state, ChunkStorageState::Corrupted);

    // Event emitted
    let mut found_rejected = false;
    while let Ok(ev) = event_rx.try_recv() {
        if let ContentEvent::ChunkRejected { chunk_id: id, .. } = ev {
            if id == chunk_id {
                found_rejected = true;
                break;
            }
        }
    }
    assert!(found_rejected, "Expected ChunkRejected event on event bus");
}

#[tokio::test]
async fn test_08_chunk_staging_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let conn = Arc::new(std::sync::Mutex::new(
        rusqlite::Connection::open_in_memory().unwrap(),
    ));
    let (event_tx, _) = tokio::sync::broadcast::channel(32);

    let staging_dir = temp_dir.path().join("staging");
    tokio::fs::create_dir_all(&staging_dir).await.unwrap();

    let orphaned_part = staging_dir.join("999999_1234.part");
    tokio::fs::write(&orphaned_part, b"Interrupted chunk write")
        .await
        .unwrap();

    let _store = ChunkStore::open(temp_dir.path(), conn, event_tx).unwrap();

    // Staging file must be cleaned up on recovery
    assert!(
        !orphaned_part.exists(),
        "Orphaned chunk staging file must be purged"
    );
}

#[test]
fn test_09_manifest_creation() {
    let content_bytes = b"Hello FIELDNET distributed manifest construction test";
    let content_id = hash_bytes(content_bytes);

    let chunk1 = b"Hello FIELDNET distributed ";
    let chunk2 = b"manifest construction test";

    let cid1 = ChunkId::from_bytes(hash_bytes(chunk1).to_bytes());
    let cid2 = ChunkId::from_bytes(hash_bytes(chunk2).to_bytes());

    let chunks = vec![
        ChunkDescriptor {
            index: 0,
            offset: 0,
            size: chunk1.len() as u32,
            chunk_id: cid1,
        },
        ChunkDescriptor {
            index: 1,
            offset: chunk1.len() as u64,
            size: chunk2.len() as u32,
            chunk_id: cid2,
        },
    ];

    let manifest = ContentManifest::new(
        content_id,
        content_bytes.len() as u64,
        chunk1.len() as u32,
        chunks,
        CHUNKING_ALGORITHM_FIXED_256K,
        Some("text/plain".into()),
        HashMap::new(),
        None,
    );

    assert_eq!(manifest.content_id, content_id);
    assert_eq!(manifest.total_size, content_bytes.len() as u64);
    assert_eq!(manifest.chunk_count, 2);
    assert_eq!(manifest.chunks.len(), 2);
    assert_ne!(manifest.merkle_root, [0u8; 32]);
    assert_eq!(manifest.merkle_root, compute_merkle_root(&manifest.chunks));
    assert_eq!(
        manifest.manifest_id,
        ManifestId::from_bytes(manifest.manifest_id.to_bytes())
    );
    let expected_mid = ContentManifest::compute_manifest_id(
        &manifest.content_id,
        manifest.total_size,
        manifest.chunk_size,
        manifest.chunk_count,
        &manifest.merkle_root,
        &manifest.chunking_algorithm,
    );
    assert_eq!(manifest.manifest_id, expected_mid);
    assert_eq!(manifest.chunk_ids(), vec![cid1, cid2]);

    // Validation must pass
    assert!(ManifestValidator::validate(&manifest).is_ok());

    // Direct chunker test (in-memory slice and streaming)
    let chunker = FixedSizeChunker::new(16).unwrap();
    assert_eq!(chunker.chunk_size(), 16);
    assert_eq!(chunker.algorithm_name(), CHUNKING_ALGORITHM_FIXED_256K);
    let slices = chunker.chunk_slice(b"0123456789abcdefextra");
    assert_eq!(slices.len(), 2);
    assert_eq!(slices[0].0, 0);
    assert_eq!(slices[0].2.len(), 16);
    assert_eq!(slices[1].0, 1);
    assert_eq!(slices[1].2.len(), 5);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let mut reader = Cursor::new(b"0123456789abcdefextra");
        let mut gathered = Vec::new();
        let (cid, total, count) = chunker
            .chunk_stream(&mut reader, |idx, offset, data| {
                gathered.push((idx, offset, data));
                async move { Ok(()) }
            })
            .await
            .unwrap();
        assert_eq!(total, 21);
        assert_eq!(count, 2);
        assert_eq!(gathered.len(), 2);
        assert_eq!(cid, hash_bytes(b"0123456789abcdefextra"));
    });
}

#[test]
fn test_10_manifest_persistence() {
    let conn = Arc::new(std::sync::Mutex::new(
        rusqlite::Connection::open_in_memory().unwrap(),
    ));
    let (event_tx, _) = tokio::sync::broadcast::channel(32);

    let store = ManifestStore::open(conn, event_tx).unwrap();

    let content_id = hash_bytes(b"Manifest persistence payload");
    let chunk_id = ChunkId::from_bytes(hash_bytes(b"chunk1").to_bytes());

    let chunks = vec![ChunkDescriptor {
        index: 0,
        offset: 0,
        size: 6,
        chunk_id,
    }];

    let manifest = ContentManifest::new(
        content_id,
        6,
        DEFAULT_CHUNK_SIZE as u32,
        chunks,
        CHUNKING_ALGORITHM_FIXED_256K,
        Some("text/plain".into()),
        HashMap::from([("key".into(), "val".into())]),
        Some(NodeId::from_bytes([0x88; 32])),
    );

    store.put(&manifest).expect("put manifest failed");

    assert!(store.has(&manifest.manifest_id).unwrap());
    assert!(store.has_for_content(&content_id).unwrap());

    let loaded = store
        .get(&manifest.manifest_id)
        .expect("get manifest failed");
    assert_eq!(loaded, manifest);

    let loaded_by_cid = store
        .get_by_content_id(&content_id)
        .expect("get by cid failed");
    assert_eq!(loaded_by_cid, manifest);

    let list = store.list().unwrap();
    assert_eq!(list.len(), 1);
}

#[test]
fn test_11_manifest_validation() {
    let content_id = hash_bytes(b"Test validation");
    let chunk_id = ChunkId::from_bytes(hash_bytes(b"chunk").to_bytes());

    let chunks = vec![ChunkDescriptor {
        index: 0,
        offset: 0,
        size: 5,
        chunk_id,
    }];

    let valid_manifest = ContentManifest::new(
        content_id,
        5,
        DEFAULT_CHUNK_SIZE as u32,
        chunks,
        CHUNKING_ALGORITHM_FIXED_256K,
        None,
        HashMap::new(),
        None,
    );
    assert!(ManifestValidator::validate(&valid_manifest).is_ok());

    // 1. Invalid algorithm
    let mut bad_algo = valid_manifest.clone();
    bad_algo.chunking_algorithm = "UNSUPPORTED_RABIN".into();
    assert!(matches!(
        ManifestValidator::validate(&bad_algo),
        Err(ContentError::UnsupportedChunking(_))
    ));

    // 2. Out of order index
    let mut bad_order = valid_manifest.clone();
    bad_order.chunks[0].index = 1;
    assert!(matches!(
        ManifestValidator::validate(&bad_order),
        Err(ContentError::ManifestInvalid(_))
    ));

    // 3. Size mismatch
    let mut bad_size = valid_manifest.clone();
    bad_size.total_size = 999;
    assert!(matches!(
        ManifestValidator::validate(&bad_size),
        Err(ContentError::ManifestInvalid(_))
    ));

    // 4. Merkle root mismatch
    let mut bad_merkle = valid_manifest.clone();
    bad_merkle.merkle_root = [0xFF; 32];
    assert!(matches!(
        ManifestValidator::validate(&bad_merkle),
        Err(ContentError::ManifestIntegrityFailure(_))
    ));
}

#[test]
fn test_12_partial_content_tracking() {
    let mut bitmap = ChunkAvailabilityBitmap::new(10);
    assert_eq!(bitmap.total_chunks(), 10);
    assert_eq!(bitmap.available_count(), 0);
    assert!(!bitmap.is_complete());

    // Mark 0, 1, 2, 5, 7
    bitmap.mark_available(0);
    bitmap.mark_available(1);
    bitmap.mark_available(2);
    bitmap.mark_available(5);
    bitmap.mark_available(7);

    assert_eq!(bitmap.available_count(), 5);
    assert!(bitmap.is_available(0));
    assert!(bitmap.is_available(1));
    assert!(bitmap.is_available(2));
    assert!(!bitmap.is_available(3));
    assert!(!bitmap.is_available(4));
    assert!(bitmap.is_available(5));
    assert!(!bitmap.is_available(6));
    assert!(bitmap.is_available(7));
    assert!(!bitmap.is_available(8));
    assert!(!bitmap.is_available(9));

    assert_eq!(bitmap.missing_indices(), vec![3, 4, 6, 8, 9]);
    assert_eq!(bitmap.present_indices(), vec![0, 1, 2, 5, 7]);
    assert_eq!(bitmap.to_ranges(), vec![(0, 2), (5, 5), (7, 7)]);

    // Byte serialization roundtrip
    let bytes = bitmap.as_bytes();
    let reconstructed_bitmap = ChunkAvailabilityBitmap::from_bytes(10, bytes).unwrap();
    assert_eq!(bitmap, reconstructed_bitmap);

    // Complete bitmap
    let full = ChunkAvailabilityBitmap::full(10);
    assert!(full.is_complete());
    assert_eq!(full.available_count(), 10);
    assert_eq!(full.to_ranges(), vec![(0, 9)]);
}

#[tokio::test]
async fn test_13_missing_chunk_detection() {
    let temp_dir = TempDir::new().unwrap();
    let conn = Arc::new(std::sync::Mutex::new(
        rusqlite::Connection::open_in_memory().unwrap(),
    ));
    let (event_tx, _) = tokio::sync::broadcast::channel(32);

    let chunk_store = ChunkStore::open(temp_dir.path(), conn, event_tx).unwrap();

    let chunk1 = b"First chunk present in store...";
    let (cid1, _) = chunk_store.put(chunk1).await.unwrap();

    let cid2 = ChunkId::from_bytes([0xEE; 32]); // Missing chunk

    let chunks = vec![
        ChunkDescriptor {
            index: 0,
            offset: 0,
            size: chunk1.len() as u32,
            chunk_id: cid1,
        },
        ChunkDescriptor {
            index: 1,
            offset: chunk1.len() as u64,
            size: 16,
            chunk_id: cid2,
        },
    ];

    let manifest = ContentManifest::new(
        ContentId::from_bytes([0xAA; 32]),
        chunk1.len() as u64 + 16,
        chunk1.len() as u32,
        chunks,
        CHUNKING_ALGORITHM_FIXED_256K,
        None,
        HashMap::new(),
        None,
    );

    let err = ContentReconstructor::reconstruct_to_bytes(&manifest, &chunk_store, None)
        .await
        .expect_err("Reconstruction must fail on missing chunk");

    assert!(matches!(err, ContentError::MissingChunk { index: 1, chunk_id } if chunk_id == cid2));
}

#[tokio::test]
async fn test_14_complete_reconstruction() {
    let temp_dir = TempDir::new().unwrap();
    let conn = Arc::new(std::sync::Mutex::new(
        rusqlite::Connection::open_in_memory().unwrap(),
    ));
    let (event_tx, _) = tokio::sync::broadcast::channel(32);

    let chunk_store = ChunkStore::open(temp_dir.path(), conn, event_tx).unwrap();

    let part1 = b"Part 1: The foundational structure of the network."; // 50 bytes
    let part2 = b"Part 2: Cryptographic peer authentication routing."; // 50 bytes
    let part3 = b"Part 3: Distributed local-first complete."; // 41 bytes

    let mut full_payload = Vec::new();
    full_payload.extend_from_slice(part1);
    full_payload.extend_from_slice(part2);
    full_payload.extend_from_slice(part3);

    let expected_cid = hash_bytes(&full_payload);

    let (cid1, _) = chunk_store.put(part1).await.unwrap();
    let (cid2, _) = chunk_store.put(part2).await.unwrap();
    let (cid3, _) = chunk_store.put(part3).await.unwrap();

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
        ChunkDescriptor {
            index: 2,
            offset: (part1.len() + part2.len()) as u64,
            size: part3.len() as u32,
            chunk_id: cid3,
        },
    ];

    let manifest = ContentManifest::new(
        expected_cid,
        full_payload.len() as u64,
        part1.len() as u32,
        chunks,
        CHUNKING_ALGORITHM_FIXED_256K,
        None,
        HashMap::new(),
        None,
    );

    // Reconstruct to bytes
    let reconstructed = ContentReconstructor::reconstruct_to_bytes(&manifest, &chunk_store, None)
        .await
        .expect("Reconstruction failed");

    assert_eq!(reconstructed, full_payload);
    assert_eq!(hash_bytes(&reconstructed), expected_cid);

    // Reconstruct to stream
    let mut out_stream = Vec::new();
    let bytes_written =
        ContentReconstructor::reconstruct_to_stream(&manifest, &chunk_store, &mut out_stream, None)
            .await
            .expect("Stream reconstruction failed");

    assert_eq!(bytes_written, full_payload.len() as u64);
    assert_eq!(out_stream, full_payload);
}

#[tokio::test]
async fn test_15_reconstruction_order() {
    let temp_dir = TempDir::new().unwrap();
    let conn = Arc::new(std::sync::Mutex::new(
        rusqlite::Connection::open_in_memory().unwrap(),
    ));
    let (event_tx, _) = tokio::sync::broadcast::channel(32);

    let chunk_store = ChunkStore::open(temp_dir.path(), conn, event_tx).unwrap();

    let c0 = b"A";
    let c1 = b"B";
    let c2 = b"C";

    let (id0, _) = chunk_store.put(c0).await.unwrap();
    let (id1, _) = chunk_store.put(c1).await.unwrap();
    let (id2, _) = chunk_store.put(c2).await.unwrap();

    let expected_payload = b"ABC";
    let cid = hash_bytes(expected_payload);

    let chunks = vec![
        ChunkDescriptor {
            index: 0,
            offset: 0,
            size: 1,
            chunk_id: id0,
        },
        ChunkDescriptor {
            index: 1,
            offset: 1,
            size: 1,
            chunk_id: id1,
        },
        ChunkDescriptor {
            index: 2,
            offset: 2,
            size: 1,
            chunk_id: id2,
        },
    ];

    let manifest = ContentManifest::new(
        cid,
        3,
        1,
        chunks,
        CHUNKING_ALGORITHM_FIXED_256K,
        None,
        HashMap::new(),
        None,
    );

    let reconstructed = ContentReconstructor::reconstruct_to_bytes(&manifest, &chunk_store, None)
        .await
        .unwrap();
    assert_eq!(
        reconstructed, expected_payload,
        "Reconstruction must follow sequential index order"
    );
}

#[tokio::test]
async fn test_16_final_content_id_verification() {
    let temp_dir = TempDir::new().unwrap();
    let conn = Arc::new(std::sync::Mutex::new(
        rusqlite::Connection::open_in_memory().unwrap(),
    ));
    let (event_tx, _) = tokio::sync::broadcast::channel(32);

    let chunk_store = ChunkStore::open(temp_dir.path(), conn, event_tx).unwrap();

    let payload = b"Original correct payload";
    let (cid, _) = chunk_store.put(payload).await.unwrap();

    let forged_content_id = hash_bytes(b"Forged fake content ID");

    let chunks = vec![ChunkDescriptor {
        index: 0,
        offset: 0,
        size: payload.len() as u32,
        chunk_id: cid,
    }];

    // Create manifest with mismatched ContentId
    let manifest = ContentManifest::new(
        forged_content_id,
        payload.len() as u64,
        DEFAULT_CHUNK_SIZE as u32,
        chunks,
        CHUNKING_ALGORITHM_FIXED_256K,
        None,
        HashMap::new(),
        None,
    );

    let err = ContentReconstructor::reconstruct_to_bytes(&manifest, &chunk_store, None)
        .await
        .expect_err("Reconstruction must fail on final ContentId mismatch");

    assert!(
        matches!(err, ContentError::IntegrityFailure { expected, actual } if expected == forged_content_id && actual == hash_bytes(payload))
    );
}

#[tokio::test]
async fn test_17_reconstruction_corruption() {
    let temp_dir = TempDir::new().unwrap();
    let conn = Arc::new(std::sync::Mutex::new(
        rusqlite::Connection::open_in_memory().unwrap(),
    ));
    let (event_tx, _) = tokio::sync::broadcast::channel(32);

    let chunk_store = ChunkStore::open(temp_dir.path(), conn, event_tx).unwrap();

    let c0 = b"Chunk zero pristine bytes"; // 25 bytes
    let c1 = b"Chunk one to be corrupted"; // 25 bytes

    let (id0, _) = chunk_store.put(c0).await.unwrap();
    let (id1, _) = chunk_store.put(c1).await.unwrap();

    let mut full = Vec::new();
    full.extend_from_slice(c0);
    full.extend_from_slice(c1);
    let cid = hash_bytes(&full);

    let chunks = vec![
        ChunkDescriptor {
            index: 0,
            offset: 0,
            size: c0.len() as u32,
            chunk_id: id0,
        },
        ChunkDescriptor {
            index: 1,
            offset: c0.len() as u64,
            size: c1.len() as u32,
            chunk_id: id1,
        },
    ];

    let manifest = ContentManifest::new(
        cid,
        full.len() as u64,
        c0.len() as u32,
        chunks,
        CHUNKING_ALGORITHM_FIXED_256K,
        None,
        HashMap::new(),
        None,
    );

    // Corrupt physical file of chunk 1 on disk
    let obj_path = chunk_store.objects_dir().join(id1.relative_storage_path());
    tokio::fs::write(&obj_path, b"Corrupted chunk bytes")
        .await
        .unwrap();

    let err = ContentReconstructor::reconstruct_to_bytes(&manifest, &chunk_store, None)
        .await
        .expect_err("Reconstruction must fail when a constituent chunk is corrupted");

    assert!(matches!(err, ContentError::ChunkIntegrityFailure { expected, .. } if expected == id1));
}

#[tokio::test]
async fn test_18_large_streamed_content() {
    let temp_dir = TempDir::new().unwrap();
    let store = ContentStore::open(temp_dir.path()).unwrap();

    // 2.5 MiB of patterned data (10 chunks of 256 KiB)
    let chunk_size = DEFAULT_CHUNK_SIZE;
    let num_chunks = 10;
    let mut full_data = Vec::with_capacity(chunk_size * num_chunks);
    for i in 0..num_chunks {
        let pattern_byte = ((i * 17) % 256) as u8;
        full_data.resize(full_data.len() + chunk_size, pattern_byte);
    }

    let expected_cid = hash_bytes(&full_data);

    // Stream through put_chunked_stream
    let mut input_cursor = Cursor::new(&full_data);
    let (cid, put_res, manifest) = store
        .put_chunked_stream(
            &mut input_cursor,
            Some(PutOptions {
                mime_type: Some("application/octet-stream".into()),
                creator_node_id: None,
                is_pinned: false,
                custom_metadata: HashMap::new(),
            }),
        )
        .await
        .expect("put_chunked_stream failed");

    assert_eq!(cid, expected_cid);
    assert!(put_res.is_new());
    assert_eq!(manifest.chunk_count, 10);
    assert_eq!(manifest.total_size, full_data.len() as u64);

    // Verify all chunks exist in chunk_store
    for c in &manifest.chunks {
        assert!(store.has_chunk(&c.chunk_id).unwrap());
    }

    // Reconstruct via store
    let reconstructed = store
        .reconstruct_to_bytes(&cid)
        .await
        .expect("reconstruct_to_bytes failed");
    assert_eq!(reconstructed.len(), full_data.len());
    assert_eq!(reconstructed, full_data);
    assert_eq!(hash_bytes(&reconstructed), expected_cid);
}

#[tokio::test]
async fn test_19_runtime_restart() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_path_buf();

    let data = b"Testing Chunk and Manifest persistence across full process reboot";
    let expected_cid = hash_bytes(data);

    let manifest_id = {
        let store_a = ContentStore::open(&base_path).unwrap();
        let mut cursor = Cursor::new(data);
        let (cid, _, manifest) = store_a
            .put_chunked_stream(
                &mut cursor,
                Some(PutOptions {
                    mime_type: Some("text/plain".into()),
                    creator_node_id: Some(NodeId::from_bytes([0x55; 32])),
                    is_pinned: true,
                    custom_metadata: HashMap::from([("tag".into(), "restart-test".into())]),
                }),
            )
            .await
            .unwrap();

        assert_eq!(cid, expected_cid);
        manifest.manifest_id
    }; // store_a dropped here

    // Reopen store_b on same directory
    {
        let store_b = ContentStore::open(&base_path).unwrap();

        // Check manifest survives
        let manifest = store_b
            .get_manifest(&expected_cid)
            .expect("Manifest must survive restart");
        assert_eq!(manifest.manifest_id, manifest_id);
        assert_eq!(manifest.total_size, data.len() as u64);

        // Check chunk availability is complete
        let avail = store_b.chunk_availability(&manifest_id).unwrap();
        assert!(avail.is_complete());
        assert!(store_b.is_manifest_available(&manifest_id).unwrap());

        // Reconstruct content
        let reconstructed = store_b.reconstruct_to_bytes(&expected_cid).await.unwrap();
        assert_eq!(reconstructed, data);

        // Content stat survives
        let stat = store_b.stat(&expected_cid).unwrap();
        assert!(stat.is_pinned);
        assert_eq!(stat.mime_type.as_deref(), Some("text/plain"));
    }
}

#[tokio::test]
async fn test_20_concurrent_reads() {
    let temp_dir = TempDir::new().unwrap();
    let store = ContentStore::open(temp_dir.path()).unwrap();

    let data = b"Concurrent reader chunked content payload - 777";
    let mut cursor = Cursor::new(data);
    let (cid, _, _) = store.put_chunked_stream(&mut cursor, None).await.unwrap();

    let mut handles = Vec::new();
    for _ in 0..16 {
        let store_clone = Arc::clone(&store);
        let cid_clone = cid;
        handles.push(tokio::spawn(async move {
            let bytes = store_clone.reconstruct_to_bytes(&cid_clone).await.unwrap();
            assert_eq!(bytes, data);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test]
async fn test_21_concurrent_writes() {
    let temp_dir = TempDir::new().unwrap();
    let store = ContentStore::open(temp_dir.path()).unwrap();

    let mut handles = Vec::new();
    for i in 0..8 {
        let store_clone = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            let payload = format!("Independent payload stream #{}", i).into_bytes();
            let mut cursor = Cursor::new(&payload);
            let (cid, _, _) = store_clone
                .put_chunked_stream(&mut cursor, None)
                .await
                .unwrap();
            let reconstructed = store_clone.reconstruct_to_bytes(&cid).await.unwrap();
            assert_eq!(reconstructed, payload);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    assert_eq!(store.manifest_store().list().unwrap().len(), 8);
}

#[tokio::test]
async fn test_22_property_reconstruction_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let store = ContentStore::open(temp_dir.path()).unwrap();

    // Property-style test over edge-case payload sizes:
    // 0 bytes, 1 byte, 100 bytes, 256KB-1, 256KB, 256KB+1, 512KB+333
    let test_sizes = vec![
        0,
        1,
        100,
        DEFAULT_CHUNK_SIZE - 1,
        DEFAULT_CHUNK_SIZE,
        DEFAULT_CHUNK_SIZE + 1,
        DEFAULT_CHUNK_SIZE * 2 + 333,
    ];

    for (case_idx, size) in test_sizes.into_iter().enumerate() {
        let mut data = Vec::with_capacity(size);
        for b in 0..size {
            data.push(((b + case_idx) % 256) as u8);
        }

        let expected_cid = hash_bytes(&data);
        let mut cursor = Cursor::new(&data);

        let (cid, _, manifest) = store.put_chunked_stream(&mut cursor, None).await.unwrap();
        assert_eq!(cid, expected_cid, "ContentId must match expected digest");
        assert_eq!(manifest.total_size, size as u64);

        let reconstructed = store.reconstruct_to_bytes(&cid).await.unwrap();
        assert_eq!(
            reconstructed, data,
            "Exact bytes must be preserved for size {}",
            size
        );
        assert_eq!(hash_bytes(&reconstructed), hash_bytes(&data));
        assert_eq!(hash_bytes(&reconstructed), cid);
    }
}
