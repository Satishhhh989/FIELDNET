use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;

use fieldnet_content::{
    hash_bytes, hash_reader, hash_stream, ContentError, ContentEvent, ContentId,
    ContentStorageState, ContentStore, PutOptions, PutResult,
};
use fieldnet_core::NodeId;

#[test]
fn test_01_hash_deterministic_content() {
    // NIST test vector: "hello world"
    let input = b"hello world";
    let expected_hex = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";

    let cid = hash_bytes(input);
    assert_eq!(cid.to_hex(), expected_hex);
    assert_eq!(cid.to_string(), expected_hex);

    // Synchronous reader
    let mut cursor = Cursor::new(input);
    let cid_reader = hash_reader(&mut cursor).expect("hash_reader failed");
    assert_eq!(cid_reader, cid);

    // Parsing
    let parsed: ContentId = expected_hex.parse().expect("parse failed");
    assert_eq!(parsed, cid);
    assert_eq!(parsed.as_bytes(), cid.as_bytes());

    // With prefixes
    let with_prefix = format!("fn-cid-{}", expected_hex);
    let parsed_pref = ContentId::from_hex(&with_prefix).expect("parse prefixed failed");
    assert_eq!(parsed_pref, cid);

    let with_sha_prefix = format!("sha256:{}", expected_hex);
    let parsed_sha = ContentId::from_hex(&with_sha_prefix).expect("parse sha prefix failed");
    assert_eq!(parsed_sha, cid);
}

#[test]
fn test_02_hash_empty_content() {
    let input = b"";
    let expected_empty_hex = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    let cid = hash_bytes(input);
    assert_eq!(cid.to_hex(), expected_empty_hex);

    let mut cursor = Cursor::new(input);
    let cid_reader = hash_reader(&mut cursor).unwrap();
    assert_eq!(cid_reader, cid);
}

#[tokio::test]
async fn test_03_hash_large_streamed_content() {
    // Generate 4 MiB of patterned data (64 chunks of 64 KiB)
    let chunk_size = 64 * 1024;
    let num_chunks = 64;
    let mut full_data = Vec::with_capacity(chunk_size * num_chunks);
    for i in 0..num_chunks {
        let pattern_byte = (i % 256) as u8;
        full_data.resize(full_data.len() + chunk_size, pattern_byte);
    }

    let expected_cid = hash_bytes(&full_data);

    // Stream hash
    let mut async_cursor = Cursor::new(&full_data);
    let streamed_cid = hash_stream(&mut async_cursor)
        .await
        .expect("hash_stream failed");
    assert_eq!(streamed_cid, expected_cid);

    // Reader hash
    let mut sync_cursor = Cursor::new(&full_data);
    let reader_cid = hash_reader(&mut sync_cursor).expect("hash_reader failed");
    assert_eq!(reader_cid, expected_cid);
}

#[tokio::test]
async fn test_04_put_content() {
    let temp_dir = TempDir::new().unwrap();
    let store = ContentStore::open(temp_dir.path()).unwrap();

    let data = b"FIELDNET sovereign distributed content storage test payload";
    let creator = NodeId::from_bytes([0x42; 32]);

    let options = PutOptions {
        mime_type: Some("text/plain".into()),
        creator_node_id: Some(creator),
        is_pinned: false,
        custom_metadata: HashMap::from([("app".into(), "fieldnet-mesh".into())]),
    };

    let result = store.put(data, Some(options)).await.expect("put failed");
    assert!(result.is_new());

    let meta = result.metadata();
    assert_eq!(meta.size_bytes, data.len() as u64);
    assert_eq!(meta.storage_state, ContentStorageState::Complete);
    assert_eq!(meta.mime_type.as_deref(), Some("text/plain"));
    assert_eq!(meta.creator_node_id, Some(creator));
    assert_eq!(
        meta.custom_metadata.get("app").map(|s| s.as_str()),
        Some("fieldnet-mesh")
    );
    assert_eq!(meta.content_id, hash_bytes(data));
}

#[tokio::test]
async fn test_05_get_content() {
    let temp_dir = TempDir::new().unwrap();
    let store = ContentStore::open(temp_dir.path()).unwrap();

    let data = b"Payload for get verification";
    let result = store.put(data, None).await.unwrap();
    let cid = result.content_id();

    // Standard get
    let retrieved = store.get(&cid).await.expect("get failed");
    assert_eq!(retrieved, data);

    // Verified get
    let verified = store.get_verified(&cid).await.expect("get_verified failed");
    assert_eq!(verified, data);
}

#[tokio::test]
async fn test_06_has_content() {
    let temp_dir = TempDir::new().unwrap();
    let store = ContentStore::open(temp_dir.path()).unwrap();

    let data = b"Payload for has() check";
    let cid = hash_bytes(data);

    assert!(!store.has(&cid).unwrap());

    store.put(data, None).await.unwrap();

    assert!(store.has(&cid).unwrap());

    let missing_cid = hash_bytes(b"non-existent content");
    assert!(!store.has(&missing_cid).unwrap());
}

#[tokio::test]
async fn test_07_stat_content() {
    let temp_dir = TempDir::new().unwrap();
    let store = ContentStore::open(temp_dir.path()).unwrap();

    let data = b"Stat test payload";
    let options = PutOptions {
        mime_type: Some("application/octet-stream".into()),
        creator_node_id: None,
        is_pinned: true,
        custom_metadata: HashMap::from([("tag".into(), "v1".into())]),
    };

    let result = store.put(data, Some(options)).await.unwrap();
    let cid = result.content_id();

    let meta = store.stat(&cid).expect("stat failed");
    assert_eq!(meta.content_id, cid);
    assert_eq!(meta.size_bytes, data.len() as u64);
    assert!(meta.is_pinned);
    assert_eq!(meta.mime_type.as_deref(), Some("application/octet-stream"));
    assert_eq!(
        meta.custom_metadata.get("tag").map(|s| s.as_str()),
        Some("v1")
    );
}

#[tokio::test]
async fn test_08_delete_content() {
    let temp_dir = TempDir::new().unwrap();
    let store = ContentStore::open(temp_dir.path()).unwrap();

    let mut event_rx = store.subscribe_events();

    let data = b"Delete me please";
    let res = store.put(data, None).await.unwrap();
    let cid = res.content_id();

    assert!(store.has(&cid).unwrap());

    let deleted = store.delete(&cid).await.expect("delete failed");
    assert!(deleted);

    // Verify removed
    assert!(!store.has(&cid).unwrap());
    assert!(matches!(
        store.get(&cid).await,
        Err(ContentError::ContentNotFound(_))
    ));
    assert!(matches!(
        store.stat(&cid),
        Err(ContentError::ContentNotFound(_))
    ));

    // Verify event received
    let mut found_delete_event = false;
    while let Ok(ev) = event_rx.try_recv() {
        if let ContentEvent::Deleted { id } = ev {
            if id == cid {
                found_delete_event = true;
                break;
            }
        }
    }
    assert!(found_delete_event, "Expected Deleted event on event bus");
}

#[tokio::test]
async fn test_09_duplicate_insertion() {
    let temp_dir = TempDir::new().unwrap();
    let store = ContentStore::open(temp_dir.path()).unwrap();

    let data = b"Identical duplicate payload";

    let res1 = store.put(data, None).await.unwrap();
    assert!(matches!(res1, PutResult::Created(_)));

    let res2 = store.put(data, None).await.unwrap();
    assert!(matches!(res2, PutResult::AlreadyExists(_)));
    assert_eq!(res1.content_id(), res2.content_id());

    // Verify exactly one object is stored
    assert_eq!(store.count().unwrap(), 1);
    assert_eq!(store.total_size_bytes().unwrap(), data.len() as u64);
}

#[tokio::test]
async fn test_10_missing_content() {
    let temp_dir = TempDir::new().unwrap();
    let store = ContentStore::open(temp_dir.path()).unwrap();

    let bogus_cid = hash_bytes(b"never inserted into store");

    assert!(!store.has(&bogus_cid).unwrap());
    assert!(
        matches!(store.get(&bogus_cid).await, Err(ContentError::ContentNotFound(id)) if id == bogus_cid)
    );
    assert!(
        matches!(store.stat(&bogus_cid), Err(ContentError::ContentNotFound(id)) if id == bogus_cid)
    );
    assert!(
        matches!(store.delete(&bogus_cid).await, Err(ContentError::ContentNotFound(id)) if id == bogus_cid)
    );
}

#[test]
fn test_11_invalid_content_id() {
    // Too short (63 chars)
    let short_hex = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde";
    assert!(matches!(
        ContentId::from_hex(short_hex),
        Err(ContentError::InvalidContentId(_))
    ));

    // Too long (65 chars)
    let long_hex = format!("{}aa", short_hex);
    assert!(matches!(
        ContentId::from_hex(&long_hex),
        Err(ContentError::InvalidContentId(_))
    ));

    // Non-hex chars
    let invalid_chars = "zzzz27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";
    assert!(matches!(
        ContentId::from_hex(invalid_chars),
        Err(ContentError::InvalidContentId(_))
    ));
}

#[tokio::test]
async fn test_12_corrupted_object_detection() {
    let temp_dir = TempDir::new().unwrap();
    let store = ContentStore::open(temp_dir.path()).unwrap();

    let mut event_rx = store.subscribe_events();

    let data = b"Integrity check payload before tampering";
    let res = store.put(data, None).await.unwrap();
    let cid = res.content_id();

    // Verify initially passes
    assert!(store.verify_integrity(&cid).unwrap());

    // Now tamper with the physical file on disk
    let obj_path = store.objects_dir().join(cid.relative_storage_path());
    assert!(obj_path.exists());
    tokio::fs::write(&obj_path, b"Tampered corrupted bytes!!!")
        .await
        .unwrap();

    // get_verified must detect integrity failure and return typed IntegrityFailure
    let err = store
        .get_verified(&cid)
        .await
        .expect_err("Must fail integrity check");
    assert!(matches!(err, ContentError::IntegrityFailure { expected, .. } if expected == cid));

    // verify_integrity must now return false
    let valid = store.verify_integrity(&cid).unwrap();
    assert!(!valid);

    // Calling get must return ContentCorrupted
    let get_err = store
        .get(&cid)
        .await
        .expect_err("get must fail on corrupted object");
    assert!(matches!(get_err, ContentError::ContentCorrupted { id, .. } if id == cid));

    // Verify state transitioned to CORRUPTED
    let meta = store.stat(&cid).unwrap();
    assert_eq!(meta.storage_state, ContentStorageState::Corrupted);

    // Verify Corrupted and IntegrityFailed events were emitted
    let mut found_corrupted = false;
    let mut found_integrity_failed = false;
    while let Ok(ev) = event_rx.try_recv() {
        match ev {
            ContentEvent::Corrupted { id, .. } if id == cid => found_corrupted = true,
            ContentEvent::IntegrityFailed { id, .. } if id == cid => found_integrity_failed = true,
            _ => {}
        }
    }
    assert!(found_corrupted, "Expected Corrupted event on event bus");
    assert!(
        found_integrity_failed,
        "Expected IntegrityFailed event on event bus"
    );
}

#[tokio::test]
async fn test_13_process_restart_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_path_buf();

    let data1 = b"Process A stored object 1";
    let data2 = b"Process A stored object 2";

    let cid1 = hash_bytes(data1);
    let cid2 = hash_bytes(data2);

    // 1. Process instance A
    {
        let store_a = ContentStore::open(&base_path).unwrap();
        store_a.put(data1, None).await.unwrap();
        store_a
            .put(
                data2,
                Some(PutOptions {
                    mime_type: Some("text/custom".into()),
                    creator_node_id: None,
                    is_pinned: true,
                    custom_metadata: HashMap::from([("key".into(), "val".into())]),
                }),
            )
            .await
            .unwrap();
        assert_eq!(store_a.count().unwrap(), 2);
    } // store_a dropped here (simulating shutdown)

    // 2. Process instance B opening same path
    {
        let store_b = ContentStore::open(&base_path).unwrap();

        assert!(store_b.has(&cid1).unwrap());
        assert!(store_b.has(&cid2).unwrap());

        let retrieved1 = store_b.get(&cid1).await.unwrap();
        assert_eq!(retrieved1, data1);

        let retrieved2 = store_b.get(&cid2).await.unwrap();
        assert_eq!(retrieved2, data2);

        let meta2 = store_b.stat(&cid2).unwrap();
        assert!(meta2.is_pinned);
        assert_eq!(meta2.mime_type.as_deref(), Some("text/custom"));
        assert_eq!(
            meta2.custom_metadata.get("key").map(|s| s.as_str()),
            Some("val")
        );

        let list = store_b.list().unwrap();
        assert_eq!(list.len(), 2);
    }
}

#[tokio::test]
async fn test_14_crash_safe_staging_behavior() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_path_buf();

    let incomplete_data = b"Incomplete chunk written right before a hard crash";
    let incomplete_cid = hash_bytes(incomplete_data);

    // 1. Simulate an interrupted write: create orphaned .part file in staging/
    let staging_dir = base_path.join("staging");
    tokio::fs::create_dir_all(&staging_dir).await.unwrap();

    let orphaned_part = staging_dir.join("123456789_9999.part");
    let mut file = tokio::fs::File::create(&orphaned_part).await.unwrap();
    file.write_all(incomplete_data).await.unwrap();
    file.flush().await.unwrap();
    drop(file);

    // 2. Open store
    let store = ContentStore::open(&base_path).unwrap();

    // Incomplete content must NOT appear as available or complete!
    assert!(!store.has(&incomplete_cid).unwrap());
    assert!(matches!(
        store.get(&incomplete_cid).await,
        Err(ContentError::ContentNotFound(_))
    ));
    assert!(matches!(
        store.stat(&incomplete_cid),
        Err(ContentError::ContentNotFound(_))
    ));

    // The orphaned staging file should have been cleaned up on recovery
    assert!(
        !orphaned_part.exists(),
        "Orphaned staging file must be cleaned up on recovery"
    );
}

#[tokio::test]
async fn test_15_metadata_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_path_buf();

    let data = b"Metadata persistence payload";
    let creator = NodeId::from_bytes([0x77; 32]);
    let mut custom = HashMap::new();
    custom.insert("author".to_string(), "satish".to_string());
    custom.insert("classification".to_string(), "top-secret".to_string());

    let cid = {
        let store = ContentStore::open(&base_path).unwrap();
        let res = store
            .put(
                data,
                Some(PutOptions {
                    mime_type: Some("application/fieldnet-doc".into()),
                    creator_node_id: Some(creator),
                    is_pinned: true,
                    custom_metadata: custom.clone(),
                }),
            )
            .await
            .unwrap();
        res.content_id()
    };

    // Reopen store
    let store_reopened = ContentStore::open(&base_path).unwrap();
    let meta = store_reopened.stat(&cid).expect("stat after reopen failed");

    assert_eq!(meta.content_id, cid);
    assert_eq!(meta.mime_type.as_deref(), Some("application/fieldnet-doc"));
    assert_eq!(meta.creator_node_id, Some(creator));
    assert!(meta.is_pinned);
    assert_eq!(meta.custom_metadata, custom);
    assert_eq!(meta.storage_state, ContentStorageState::Complete);
}

#[tokio::test]
async fn test_16_concurrent_reads() {
    let temp_dir = TempDir::new().unwrap();
    let store = ContentStore::open(temp_dir.path()).unwrap();

    let data = b"Concurrent reader test data payload - 42";
    let res = store.put(data, None).await.unwrap();
    let cid = res.content_id();

    let mut handles = Vec::new();
    for _ in 0..20 {
        let store_clone = Arc::clone(&store);
        let cid_clone = cid;
        handles.push(tokio::spawn(async move {
            let bytes = store_clone.get(&cid_clone).await.unwrap();
            assert_eq!(bytes, data);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test]
async fn test_17_concurrent_writes_of_identical_content() {
    let temp_dir = TempDir::new().unwrap();
    let store = ContentStore::open(temp_dir.path()).unwrap();

    let data = b"Identical payload written by multiple concurrent workers";

    let mut handles = Vec::new();
    for _ in 0..10 {
        let store_clone = Arc::clone(&store);
        handles.push(tokio::spawn(
            async move { store_clone.put(data, None).await },
        ));
    }

    let mut created_count = 0;
    let mut already_exists_count = 0;

    for h in handles {
        let result = h.await.unwrap().expect("Concurrent write put failed");
        match result {
            PutResult::Created(_) => created_count += 1,
            PutResult::AlreadyExists(_) => already_exists_count += 1,
        }
    }

    // At least one writer must have created it, the others saw AlreadyExists
    assert_eq!(
        created_count, 1,
        "Exactly one writer commits the new object"
    );
    assert_eq!(
        already_exists_count, 9,
        "All subsequent concurrent writers get AlreadyExists"
    );
    assert_eq!(store.count().unwrap(), 1);
}

#[tokio::test]
async fn test_18_concurrent_delete_read_and_pin_protection() {
    let temp_dir = TempDir::new().unwrap();
    let store = ContentStore::open(temp_dir.path()).unwrap();

    let data = b"Safe deletion and pin protection test";
    let res = store.put(data, None).await.unwrap();
    let cid = res.content_id();

    // 1. Active reference protection
    let lock = store
        .acquire_reference(&cid)
        .await
        .expect("Failed to acquire reference");
    let del_err = store
        .delete(&cid)
        .await
        .expect_err("Delete must fail while reference is active");
    assert!(matches!(del_err, ContentError::ActiveReference(id) if id == cid));

    // Drop lock
    drop(lock);
    // Allow background drop task to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // 2. Pin protection
    store.pin(&cid).expect("pin failed");
    assert!(store.stat(&cid).unwrap().is_pinned);

    let pin_del_err = store
        .delete(&cid)
        .await
        .expect_err("Delete must fail on pinned content");
    assert!(matches!(pin_del_err, ContentError::PinnedContent(id) if id == cid));

    // Unpin and delete
    store.unpin(&cid).expect("unpin failed");
    assert!(!store.stat(&cid).unwrap().is_pinned);

    let deleted = store
        .delete(&cid)
        .await
        .expect("Delete should succeed once unpinned");
    assert!(deleted);
    assert!(!store.has(&cid).unwrap());
}
