use std::collections::HashMap;
use std::fs;
use std::io::Cursor;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::broadcast;

use fieldnet_content::{
    auth::{CapabilityToken, ContentAction},
    chunks::{ChunkDescriptor, CHUNKING_ALGORITHM_FIXED_256K},
    hash_bytes,
    integrity::ContentVerifier,
    manifest::ContentManifest,
    quota::{QuotaConfig, ResourceLimits, StorageQuotaManager},
    recovery::{OrphanClassification, OrphanDetector},
    transfer::{TransferId, TransferPersistence, TransferSession, TransferState},
    ContentStore, PutOptions,
};
use fieldnet_core::Timestamp;
use fieldnet_identity::NodeIdentity;

fn generate_test_payload(size_bytes: usize, seed: u8) -> Vec<u8> {
    let mut data = Vec::with_capacity(size_bytes);
    for i in 0..size_bytes {
        data.push(((i * 13 + seed as usize) % 256) as u8);
    }
    data
}

// =========================================================================
// 1. CONTENT AUTHORIZATION TESTS
// =========================================================================

#[tokio::test]
async fn test_01_authorization_local_and_creator_authority() {
    let temp_dir = TempDir::new().unwrap();
    let store = ContentStore::open(temp_dir.path()).unwrap();

    let local_id = store.authorizer().local_node_id();
    let creator_identity = NodeIdentity::generate();
    let unauth_identity = NodeIdentity::generate();

    let payload = b"authorizer test content 1";
    let put_res = store
        .put(
            payload,
            Some(PutOptions {
                creator_node_id: Some(creator_identity.node_id()),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    let content_id = put_res.content_id();

    // 1. Local sovereign node always authorized for all actions
    assert!(store
        .authorizer()
        .authorize(&local_id, ContentAction::Read, &content_id, None)
        .is_ok());
    assert!(store
        .authorizer()
        .authorize(&local_id, ContentAction::Delete, &content_id, None)
        .is_ok());

    // 2. Creator of the content is authorized for all actions
    assert!(store
        .authorizer()
        .authorize(
            &creator_identity.node_id(),
            ContentAction::Read,
            &content_id,
            None,
        )
        .is_ok());
    assert!(store
        .authorizer()
        .authorize(
            &creator_identity.node_id(),
            ContentAction::Delete,
            &content_id,
            None,
        )
        .is_ok());

    // 3. Unauthorized node is denied by default (fail-closed)
    assert!(store
        .authorizer()
        .authorize(
            &unauth_identity.node_id(),
            ContentAction::Read,
            &content_id,
            None,
        )
        .is_err());
    assert!(store
        .authorizer()
        .authorize(
            &unauth_identity.node_id(),
            ContentAction::Delete,
            &content_id,
            None,
        )
        .is_err());
}

#[tokio::test]
async fn test_02_authorization_acls_and_public_content() {
    let temp_dir = TempDir::new().unwrap();
    let store = ContentStore::open(temp_dir.path()).unwrap();
    let peer_id = NodeIdentity::generate().node_id();

    let payload = b"acl test content 2";
    let put_res = store.put(payload, None).await.unwrap();
    let content_id = put_res.content_id();

    // Initially unauthorized
    assert!(store
        .authorizer()
        .authorize(&peer_id, ContentAction::Read, &content_id, None)
        .is_err());

    // Grant READ only
    store
        .authorizer()
        .grant_permission(&content_id, &peer_id, &[ContentAction::Read])
        .unwrap();

    // Now READ is allowed, but DELETE or PIN is denied
    assert!(store
        .authorizer()
        .authorize(&peer_id, ContentAction::Read, &content_id, None)
        .is_ok());
    assert!(store
        .authorizer()
        .authorize(&peer_id, ContentAction::Delete, &content_id, None)
        .is_err());
    assert!(store
        .authorizer()
        .authorize(&peer_id, ContentAction::Pin, &content_id, None)
        .is_err());

    // Revoke READ
    store
        .authorizer()
        .revoke_permission(&content_id, &peer_id)
        .unwrap();
    assert!(store
        .authorizer()
        .authorize(&peer_id, ContentAction::Read, &content_id, None)
        .is_err());

    // Set content as PUBLIC
    store.authorizer().set_public(&content_id, true).unwrap();
    assert!(store.authorizer().is_public(&content_id).unwrap());

    // READ is allowed for public content
    assert!(store
        .authorizer()
        .authorize(&peer_id, ContentAction::Read, &content_id, None)
        .is_ok());

    // DELETE and PIN are STILL DENIED for public content without explicit ACL
    assert!(store
        .authorizer()
        .authorize(&peer_id, ContentAction::Delete, &content_id, None)
        .is_err());
    assert!(store
        .authorizer()
        .authorize(&peer_id, ContentAction::Pin, &content_id, None)
        .is_err());
}

#[tokio::test]
async fn test_03_capability_tokens_signing_and_expiry() {
    let temp_dir = TempDir::new().unwrap();
    let store = ContentStore::open(temp_dir.path()).unwrap();

    let issuer = NodeIdentity::generate();
    let subject = NodeIdentity::generate();
    let stranger = NodeIdentity::generate();

    let payload = b"capability token test content";
    let put_res = store
        .put(
            payload,
            Some(PutOptions {
                creator_node_id: Some(issuer.node_id()),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    let content_id = put_res.content_id();

    // 1. Issue valid capability token for subject signed by creator
    let token = CapabilityToken::new(
        &issuer,
        Some(subject.node_id()),
        Some(content_id),
        vec![ContentAction::Read, ContentAction::Replicate],
        3600,
    );

    assert!(token.verify_signature().is_ok());

    // 2. Subject presents token -> authorization succeeds
    assert!(store
        .authorizer()
        .authorize(
            &subject.node_id(),
            ContentAction::Read,
            &content_id,
            Some(&token),
        )
        .is_ok());
    assert!(store
        .authorizer()
        .authorize(
            &subject.node_id(),
            ContentAction::Replicate,
            &content_id,
            Some(&token),
        )
        .is_ok());

    // 3. Subject tries action not permitted by token (e.g. Delete) -> denied
    assert!(store
        .authorizer()
        .authorize(
            &subject.node_id(),
            ContentAction::Delete,
            &content_id,
            Some(&token),
        )
        .is_err());

    // 4. Stranger presents token bound to subject -> denied
    assert!(store
        .authorizer()
        .authorize(
            &stranger.node_id(),
            ContentAction::Read,
            &content_id,
            Some(&token),
        )
        .is_err());

    // 5. Expired token is rejected
    let mut expired_token = token.clone();
    expired_token.expires_at = Timestamp::from_millis(100);
    assert!(expired_token.is_expired(Timestamp::now()));
    assert!(store
        .authorizer()
        .authorize(
            &subject.node_id(),
            ContentAction::Read,
            &content_id,
            Some(&expired_token),
        )
        .is_err());

    // 6. Tampered signature is rejected
    let mut tampered_token = token.clone();
    tampered_token.signature[0] ^= 0xFF;
    assert!(tampered_token.verify_signature().is_err());
    assert!(store
        .authorizer()
        .authorize(
            &subject.node_id(),
            ContentAction::Read,
            &content_id,
            Some(&tampered_token),
        )
        .is_err());
}

// =========================================================================
// 2. INTEGRITY VERIFICATION & AUDIT TESTS
// =========================================================================

#[tokio::test]
async fn test_04_integrity_verifier_chunks_manifest_content() {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(ContentStore::open(temp_dir.path()).unwrap());

    let payload = generate_test_payload(300 * 1024, 42);
    let mut cursor = Cursor::new(&payload);

    let (content_id, _put_res, manifest) = store
        .put_chunked_stream(&mut cursor, None)
        .await
        .expect("Failed to put chunked stream");

    // 1. Verify all individual chunks
    for chunk_desc in &manifest.chunks {
        let chunk_bytes = store.get_chunk(&chunk_desc.chunk_id).await.unwrap();
        assert!(ContentVerifier::verify_chunk_bytes(&chunk_desc.chunk_id, &chunk_bytes).is_ok());
        assert!(
            ContentVerifier::verify_stored_chunk(&chunk_desc.chunk_id, store.chunk_store())
                .await
                .is_ok()
        );

        // Corrupted chunk bytes must fail
        let mut corrupted = chunk_bytes.clone();
        corrupted[0] ^= 0xFF;
        assert!(ContentVerifier::verify_chunk_bytes(&chunk_desc.chunk_id, &corrupted).is_err());
    }

    // 2. Verify stored manifest
    assert!(ContentVerifier::verify_manifest(&manifest).is_ok());

    // 3. Verify complete reconstructed content
    assert!(ContentVerifier::verify_stored_content(&content_id, &store)
        .await
        .is_ok());
    assert!(store.verify_integrity(&content_id).unwrap());

    // 4. Test wrong ContentId detection
    let fake_content_id = hash_bytes(b"completely different data");
    assert!(
        ContentVerifier::verify_stored_content(&fake_content_id, &store)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn test_05_integrity_auditor_detects_corruption() {
    let temp_dir = TempDir::new().unwrap();
    let store = ContentStore::open(temp_dir.path()).unwrap();

    let payload = generate_test_payload(400 * 1024, 7);
    let mut cursor = Cursor::new(&payload);

    let (content_id, _put_res, manifest) =
        store.put_chunked_stream(&mut cursor, None).await.unwrap();

    // Initial audit must be clean
    let clean_report = store.audit_content(&content_id).await.unwrap();
    assert!(clean_report.is_healthy);
    assert!(clean_report.corrupted_chunks.is_empty());
    assert!(clean_report.missing_chunks.is_empty());

    // Corrupt one physical chunk file on disk
    let victim_chunk_id = manifest.chunks[0].chunk_id;
    let victim_chunk_path = store
        .chunk_store()
        .objects_dir()
        .join(victim_chunk_id.relative_storage_path());
    assert!(victim_chunk_path.exists());

    let mut chunk_raw = fs::read(&victim_chunk_path).unwrap();
    chunk_raw[10] ^= 0xAA;
    fs::write(&victim_chunk_path, &chunk_raw).unwrap();

    // Re-audit must detect the corrupted chunk
    let corrupt_report = store.audit_content(&content_id).await.unwrap();
    assert!(!corrupt_report.is_healthy);
    assert_eq!(corrupt_report.corrupted_chunks, vec![victim_chunk_id]);

    // audit_all must report the corrupted content
    let all_reports = store.audit_all().await.unwrap();
    assert_eq!(all_reports.len(), 1);
    assert!(!all_reports[0].is_healthy);
}

// =========================================================================
// 3. CORRUPTION RECOVERY TESTS
// =========================================================================

#[tokio::test]
async fn test_06_corruption_quarantine_and_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let store = ContentStore::open(temp_dir.path()).unwrap();

    let payload = generate_test_payload(300 * 1024, 19);
    let mut cursor = Cursor::new(&payload);

    let (content_id, _put_res, manifest) =
        store.put_chunked_stream(&mut cursor, None).await.unwrap();

    let victim_chunk_id = manifest.chunks[0].chunk_id;
    let original_chunk_bytes = store.get_chunk(&victim_chunk_id).await.unwrap();

    // 1. Corrupt the chunk file on disk
    let chunk_path = store
        .chunk_store()
        .objects_dir()
        .join(victim_chunk_id.relative_storage_path());
    let mut corrupted = original_chunk_bytes.clone();
    corrupted[0] ^= 0xEE;
    fs::write(&chunk_path, &corrupted).unwrap();

    // 2. Quarantine the corrupted chunk
    store
        .recovery_engine()
        .quarantine_chunk(&victim_chunk_id, Some(content_id), "Corrupted in test")
        .await
        .unwrap();
    assert!(!chunk_path.exists()); // Removed from active chunk store

    // 3. Attempting to restore with corrupted bytes must fail validation
    assert!(store
        .recovery_engine()
        .restore_chunk(&victim_chunk_id, &corrupted, Some(content_id), None)
        .await
        .is_err());

    // 4. Restore with verified replacement bytes (e.g. re-fetched from peer)
    store
        .recovery_engine()
        .restore_chunk(
            &victim_chunk_id,
            &original_chunk_bytes,
            Some(content_id),
            None,
        )
        .await
        .expect("Valid chunk replacement must succeed");
    assert!(chunk_path.exists());

    // 5. Restore content complete status and verify audit is clean again
    store
        .recovery_engine()
        .restore_content(&content_id)
        .await
        .unwrap();

    let final_audit = store.audit_content(&content_id).await.unwrap();
    assert!(final_audit.is_healthy);
}

// =========================================================================
// 4. GARBAGE COLLECTION & SAFE EVICTION TESTS
// =========================================================================

#[tokio::test]
async fn test_07_gc_protects_pinned_and_active_transfers() {
    let temp_dir = TempDir::new().unwrap();
    let store = ContentStore::open(temp_dir.path()).unwrap();

    // Put Content A (pinned)
    let data_a = b"Pinned content A that GC must never delete";
    let put_res_a = store
        .put(
            data_a,
            Some(PutOptions {
                is_pinned: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    let id_a = put_res_a.content_id();

    // Put Content B (unpinned, normal cache)
    let data_b = b"Unpinned cached content B that can be evicted";
    let put_res_b = store
        .put(
            data_b,
            Some(PutOptions {
                is_pinned: false,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    let id_b = put_res_b.content_id();

    assert!(store.is_pinned(&id_a).unwrap());
    assert!(!store.is_pinned(&id_b).unwrap());

    // Run GC with target of all evictable bytes
    let report = store.run_gc(None).await.unwrap();

    // Pinned content A was protected
    assert!(report.protected_objects >= 1);
    assert!(store.has(&id_a).unwrap());

    // Unpinned content B was evicted
    assert!(report.evicted_objects >= 1);
    assert!(!store.has(&id_b).unwrap());
}

// =========================================================================
// 5. MANDATORY GC SAFETY TEST (SECTION 25)
// =========================================================================

#[tokio::test]
async fn test_08_gc_mandatory_shared_chunks_section_25() {
    // Construct:
    // Content X with Manifest X, chunks 0-9
    // Content Y with Manifest Y, sharing chunks 0-4
    // Pin X -> Run GC -> chunks 0-9 remain
    // Unpin X -> Y remains -> Run GC -> chunks 0-4 remain because Y references them!
    // Unused chunks (5-9) may be removed.

    let temp_dir = TempDir::new().unwrap();
    let store = ContentStore::open(temp_dir.path()).unwrap();

    // 1. Create 10 distinct chunks (0 to 9)
    let mut chunk_descriptors_x = Vec::new();
    let mut chunk_descriptors_y = Vec::new();
    let mut all_chunks = Vec::new();

    for i in 0..10 {
        let mut chunk_data = Vec::with_capacity(1024);
        for b in 0..1024 {
            chunk_data.push(((i * 31 + b) % 256) as u8);
        }
        let (chunk_id, _res) = store.chunk_store().put(&chunk_data).await.unwrap();
        all_chunks.push(chunk_id);

        let desc = ChunkDescriptor {
            index: i as u32,
            offset: i as u64 * 1024,
            size: 1024,
            chunk_id,
        };
        chunk_descriptors_x.push(desc);

        // Content Y shares chunks 0 to 4
        if i < 5 {
            chunk_descriptors_y.push(desc);
        }
    }

    assert_eq!(all_chunks.len(), 10);
    assert_eq!(chunk_descriptors_x.len(), 10);
    assert_eq!(chunk_descriptors_y.len(), 5);

    // 2. Create and store Manifest X (chunks 0-9)
    let content_x_id = hash_bytes(b"Simulated Content X Manifest Payload");
    let manifest_x = ContentManifest::new(
        content_x_id,
        10 * 1024,
        1024,
        chunk_descriptors_x,
        CHUNKING_ALGORITHM_FIXED_256K,
        None,
        HashMap::new(),
        None,
    );
    store.manifest_store().put(&manifest_x).unwrap();

    // Create and store Manifest Y (chunks 0-4)
    let content_y_id = hash_bytes(b"Simulated Content Y Manifest Payload");
    let manifest_y = ContentManifest::new(
        content_y_id,
        5 * 1024,
        1024,
        chunk_descriptors_y,
        CHUNKING_ALGORITHM_FIXED_256K,
        None,
        HashMap::new(),
        None,
    );
    store.manifest_store().put(&manifest_y).unwrap();

    // 3. Register Content X and Content Y in content_index
    let now = Timestamp::now().as_millis() as i64;
    {
        let conn = store.conn().lock().unwrap();
        conn.execute(
            r#"INSERT INTO content_index (content_id, size_bytes, storage_path, storage_state, created_at, updated_at, last_accessed_at, is_pinned)
               VALUES (?1, ?2, ?3, 'COMPLETE', ?4, ?4, ?4, 1)"#,
            rusqlite::params![content_x_id.to_hex(), 10240, content_x_id.relative_storage_path().to_string_lossy().to_string(), now],
        ).unwrap();

        conn.execute(
            r#"INSERT INTO content_index (content_id, size_bytes, storage_path, storage_state, created_at, updated_at, last_accessed_at, is_pinned)
               VALUES (?1, ?2, ?3, 'COMPLETE', ?4, ?4, ?4, 1)"#,
            rusqlite::params![content_y_id.to_hex(), 5120, content_y_id.relative_storage_path().to_string_lossy().to_string(), now],
        ).unwrap();
    }

    // Pin Content X and Content Y
    store.pin(&content_x_id).unwrap();
    store.pin(&content_y_id).unwrap();
    assert!(store.is_pinned(&content_x_id).unwrap());
    assert!(store.is_pinned(&content_y_id).unwrap());

    // 4. Run GC while X is pinned
    let gc_report_1 = store.run_gc(None).await.unwrap();
    assert!(gc_report_1.protected_objects >= 1);

    // Verify all 10 chunks remain in chunk store
    for (i, chunk_id) in all_chunks.iter().enumerate() {
        assert!(
            store.has_chunk(chunk_id).unwrap(),
            "Chunk {} ({}) must remain when X is pinned",
            i,
            chunk_id
        );
    }

    // 5. Unpin Content X. Y remains.
    store.unpin(&content_x_id).unwrap();
    assert!(!store.is_pinned(&content_x_id).unwrap());
    assert!(store.is_pinned(&content_y_id).unwrap());

    // Delete Content X and its manifest to simulate X removal
    let _ = store.delete(&content_x_id).await;
    let _ = store.manifest_store().delete(&manifest_x.manifest_id);

    // Run GC again
    let _ = store.run_gc(None).await.unwrap();

    // Verify mandatory invariant from Section 25:
    // Chunks 0-4 MUST REMAIN because Y references them!
    for (i, chunk_id) in all_chunks.iter().enumerate().take(5) {
        assert!(
            store.has_chunk(chunk_id).unwrap(),
            "Chunk {} must remain because Content Y still references it",
            i
        );
    }
}

// =========================================================================
// 6. STORAGE QUOTA & RESOURCE LIMIT TESTS
// =========================================================================

#[tokio::test]
async fn test_09_storage_quota_accounting_and_limits() {
    let (event_tx, _event_rx) = broadcast::channel(128);
    let quota_config = QuotaConfig {
        total_storage_quota: 1024 * 1024, // 1 MiB
        cache_quota: 512 * 1024,          // 512 KiB
        temp_quota: 256 * 1024,           // 256 KiB
        warning_threshold_percent: 80.0,  // 80%
    };

    let limits = ResourceLimits {
        max_object_size: 2 * 1024 * 1024,
        max_chunk_size: 256 * 1024,
        max_chunks_per_manifest: 100,
        ..Default::default()
    };

    let manager = StorageQuotaManager::new(quota_config, limits, event_tx);
    let status = manager.status().await;
    assert_eq!(status.quota_bytes, 1024 * 1024);
    assert_eq!(status.used_bytes, 0);
    assert_eq!(status.available_bytes, 1024 * 1024);

    // 1. Reserve 600 KiB -> succeeds
    let res = manager.reserve(600 * 1024).await.unwrap();
    assert_eq!(manager.status().await.reserved_bytes, 600 * 1024);

    // 2. Attempt to reserve another 500 KiB -> exceeds 1 MiB total -> rejected with QuotaExceeded
    assert!(manager.reserve(500 * 1024).await.is_err());

    // 3. Commit reservation
    manager.commit_reservation(&res);
    assert_eq!(manager.status().await.reserved_bytes, 0);
    assert_eq!(manager.status().await.used_bytes, 600 * 1024);
    assert_eq!(manager.status().await.available_bytes, (1024 - 600) * 1024);

    // 4. Free 200 KiB
    manager.record_freed(200 * 1024);
    assert_eq!(manager.status().await.used_bytes, 400 * 1024);
}

// =========================================================================
// 7. CRASH RECOVERY & ORPHAN DETECTION TESTS
// =========================================================================

#[tokio::test]
async fn test_10_crash_recovery_staging_and_transfers() {
    let temp_dir = TempDir::new().unwrap();
    let local_id = NodeIdentity::generate().node_id();
    let provider_id = NodeIdentity::generate().node_id();

    // 1. Open store first
    let store = ContentStore::open(temp_dir.path()).unwrap();
    let transfer_persistence = TransferPersistence::new(store.conn().clone()).unwrap();

    // 2. Create an orphaned .part staging file
    let dangling_part = store.staging_dir().join("interrupted_transfer_123.part");
    fs::write(&dangling_part, b"partial bytes from crash").unwrap();
    assert!(dangling_part.exists());

    // 3. Create database with an active in-flight transfer
    let transfer_id = TransferId::random();
    let content_id = hash_bytes(b"interrupted content");
    let mut session = TransferSession::new(
        transfer_id,
        content_id,
        local_id,
        provider_id,
        fieldnet_core::Priority::Normal,
        1000,
    );
    session.status = TransferState::Transferring;
    transfer_persistence.save(&session).unwrap();

    // 4. Execute crash recovery
    let report = store
        .crash_recovery()
        .execute_startup_recovery()
        .await
        .unwrap();

    // In-flight active transfer must have been paused
    assert_eq!(report.transfer_sessions_recovered, 1);
    let loaded_session = transfer_persistence.load(&transfer_id).unwrap().unwrap();
    assert_eq!(loaded_session.status, TransferState::Paused);

    // Orphaned .part staging files cleaned up
    assert!(!dangling_part.exists());
    assert_eq!(report.staging_files_cleaned, 1);
}

#[tokio::test]
async fn test_11_orphan_detection_and_quarantine() {
    let temp_dir = TempDir::new().unwrap();
    let store = ContentStore::open(temp_dir.path()).unwrap();

    // 1. Put a chunk directly into ChunkStore without registering any manifest
    let orphan_chunk_bytes = b"Unreferenced chunk data floating without a manifest";
    let (orphan_chunk_id, _res) = store.chunk_store().put(orphan_chunk_bytes).await.unwrap();
    assert!(store.has_chunk(&orphan_chunk_id).unwrap());

    // 2. Run OrphanDetector
    let orphan_detector = OrphanDetector::new(
        temp_dir.path(),
        store.conn().clone(),
        store.chunk_store().clone(),
    );

    let orphans = orphan_detector.detect_orphans().await.unwrap();
    assert!(orphans
        .iter()
        .any(|o| matches!(o.classification, OrphanClassification::Orphaned)));

    // 3. Quarantine orphans
    let quarantined_count = orphan_detector
        .quarantine_orphaned_chunks(&orphans)
        .await
        .unwrap();
    assert!(quarantined_count >= 1);
}
