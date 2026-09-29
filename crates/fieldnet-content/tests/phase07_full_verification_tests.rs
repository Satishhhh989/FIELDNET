//! FIELDNET Phase 07 BUILD 07: Full Subsystem Integration, Property, Fuzz, and Benchmark Verification Suite.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use fieldnet_content::auth::{CapabilityToken, ContentAction};
use fieldnet_content::chunks::chunker::{Chunker, FixedSizeChunker};
use fieldnet_content::chunks::id::ChunkId;
use fieldnet_content::error::ContentError;
use fieldnet_content::hash_bytes;
use fieldnet_content::identity::ContentId;
use fieldnet_content::integrity::verifier::ContentVerifier;
use fieldnet_content::manifest::id::ManifestId;
use fieldnet_content::manifest::model::ContentManifest;
use fieldnet_content::manifest::validator::ManifestValidator;
use fieldnet_content::providers::advertisement::ContentAdvertise;
use fieldnet_content::providers::record::TrustTier;
use fieldnet_content::providers::registry::{ProviderRegistry, ProviderRegistryConfig};
use fieldnet_content::quota::limits::QuotaConfig;
use fieldnet_content::store::{ContentStore, PutOptions};
use fieldnet_core::{NodeId, Timestamp};
use fieldnet_identity::NodeIdentity;
use tempfile::tempdir;

fn generate_test_payload(size_bytes: usize, seed: u8) -> Vec<u8> {
    let mut data = Vec::with_capacity(size_bytes);
    for i in 0..size_bytes {
        data.push(((i * 7 + seed as usize * 13) % 256) as u8);
    }
    data
}

// =========================================================================
// TEST 01: Malicious Provider Tampered Chunk Rejection (Section 8)
// =========================================================================
#[tokio::test]
async fn test_01_malicious_provider_tampered_chunk_rejection() {
    let tmp = tempdir().unwrap();
    let store = Arc::new(ContentStore::open(tmp.path()).unwrap());

    let payload = generate_test_payload(128 * 1024, 1);
    let chunker = FixedSizeChunker::default();
    let slices = chunker.chunk_slice(&payload);
    assert_eq!(slices.len(), 1);

    let original_chunk_id = ChunkId::from_bytes(hash_bytes(&payload).to_bytes());

    // Honest chunk bytes pass verification
    assert!(ContentVerifier::verify_chunk_bytes(&original_chunk_id, &payload).is_ok());

    // Malicious provider advertises original_chunk_id, but provides tampered bytes
    let mut tampered_bytes = payload.clone();
    tampered_bytes[42] ^= 0xFF; // Bit flip

    // Verify ContentVerifier rejects tampered bytes
    let verify_res = ContentVerifier::verify_chunk_bytes(&original_chunk_id, &tampered_bytes);
    assert!(
        verify_res.is_err(),
        "Tampered bytes must fail cryptographic ChunkId verification"
    );
    match verify_res.unwrap_err() {
        ContentError::ChunkIntegrityFailure { expected, .. } => {
            assert_eq!(expected, original_chunk_id);
        }
        other => panic!("Expected ChunkIntegrityFailure, got: {:?}", other),
    }

    // Honest chunk is stored and verified cleanly
    let honest_res = store.chunk_store().put(&payload).await;
    assert!(honest_res.is_ok());
    assert!(store.chunk_store().has(&original_chunk_id).unwrap());
}

// =========================================================================
// TEST 02: Fine-Grained Authorization & Rejections (Section 9)
// =========================================================================
#[tokio::test]
async fn test_02_fine_grained_authorization_and_rejections() {
    let tmp = tempdir().unwrap();
    let store = ContentStore::open(tmp.path()).unwrap();
    let auth = store.authorizer();

    let owner_id = NodeIdentity::generate();
    let authorized_peer = NodeIdentity::generate();
    let unauthorized_peer = NodeIdentity::generate();

    let put_res = store
        .put(
            b"fine grained auth content",
            Some(PutOptions {
                creator_node_id: Some(owner_id.node_id()),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    let dummy_cid = put_res.content_id();

    // Unauthorized peer without token is rejected
    assert!(auth
        .authorize(
            &unauthorized_peer.node_id(),
            ContentAction::Read,
            &dummy_cid,
            None
        )
        .is_err());
    assert!(auth
        .authorize(
            &unauthorized_peer.node_id(),
            ContentAction::Delete,
            &dummy_cid,
            None
        )
        .is_err());
    assert!(auth
        .authorize(
            &unauthorized_peer.node_id(),
            ContentAction::Pin,
            &dummy_cid,
            None
        )
        .is_err());
    assert!(auth
        .authorize(
            &unauthorized_peer.node_id(),
            ContentAction::Replicate,
            &dummy_cid,
            None
        )
        .is_err());

    // Explicit ACL grant for Read and Replicate
    auth.grant_permission(
        &dummy_cid,
        &authorized_peer.node_id(),
        &[ContentAction::Read],
    )
    .unwrap();
    auth.grant_permission(
        &dummy_cid,
        &authorized_peer.node_id(),
        &[ContentAction::Replicate],
    )
    .unwrap();

    // Authorized peer can Read and Replicate
    assert!(auth
        .authorize(
            &authorized_peer.node_id(),
            ContentAction::Read,
            &dummy_cid,
            None
        )
        .is_ok());
    assert!(auth
        .authorize(
            &authorized_peer.node_id(),
            ContentAction::Replicate,
            &dummy_cid,
            None
        )
        .is_ok());

    // But authorized peer cannot Delete or Pin
    assert!(auth
        .authorize(
            &authorized_peer.node_id(),
            ContentAction::Delete,
            &dummy_cid,
            None
        )
        .is_err());
    assert!(auth
        .authorize(
            &authorized_peer.node_id(),
            ContentAction::Pin,
            &dummy_cid,
            None
        )
        .is_err());

    // Capability Token: Delegated Pinning signed by creator
    let token = CapabilityToken::new(
        &owner_id,
        Some(authorized_peer.node_id()),
        Some(dummy_cid),
        vec![ContentAction::Pin],
        3600,
    );
    assert!(auth
        .authorize(
            &authorized_peer.node_id(),
            ContentAction::Pin,
            &dummy_cid,
            Some(&token)
        )
        .is_ok());

    // Forged token (signed by unauthorized attacker)
    let attacker = NodeIdentity::generate();
    let forged_token = CapabilityToken::new(
        &attacker,
        Some(unauthorized_peer.node_id()),
        Some(dummy_cid),
        vec![ContentAction::Delete],
        3600,
    );
    assert!(auth
        .authorize(
            &unauthorized_peer.node_id(),
            ContentAction::Delete,
            &dummy_cid,
            Some(&forged_token)
        )
        .is_err());
}

// =========================================================================
// TEST 03: Shared Chunk GC & Multi-Manifest Reference Safety (Section 10)
// =========================================================================
#[tokio::test]
async fn test_03_shared_chunk_gc_and_reference_safety() {
    let tmp = tempdir().unwrap();
    let store = Arc::new(ContentStore::open(tmp.path()).unwrap());

    // Create shared chunk (256 KiB)
    let shared_block = generate_test_payload(256 * 1024, 7);

    // Content X = shared_block + unique_x
    let mut payload_x = shared_block.clone();
    payload_x.extend_from_slice(&generate_test_payload(256 * 1024, 11));

    // Content Y = shared_block + unique_y
    let mut payload_y = shared_block.clone();
    payload_y.extend_from_slice(&generate_test_payload(256 * 1024, 19));

    let (cid_x, _, manifest_x) = store
        .put_chunked_stream(&mut payload_x.as_slice(), None)
        .await
        .unwrap();
    let (cid_y, _, manifest_y) = store
        .put_chunked_stream(&mut payload_y.as_slice(), None)
        .await
        .unwrap();

    assert_ne!(cid_x, cid_y);

    let shared_chunk_id = manifest_x.chunks[0].chunk_id;
    assert_eq!(manifest_y.chunks[0].chunk_id, shared_chunk_id);

    // Pin Content X
    store.pin(&cid_x).unwrap();
    assert!(store.is_pinned(&cid_x).unwrap());
    assert!(!store.is_pinned(&cid_y).unwrap());

    // Run GC targeting all evictable bytes
    let report = store.run_gc(None).await.unwrap();
    assert!(
        report.protected_objects >= 1,
        "Pinned Content X must be protected"
    );

    // Invariant checks:
    // 1. Pinned Content X manifest is intact
    assert!(store.manifest_store().has(&manifest_x.manifest_id).unwrap());
    // 2. Shared chunk 0 MUST still exist because retained manifest X references it
    assert!(
        store.chunk_store().has(&shared_chunk_id).unwrap(),
        "Shared chunk MUST NOT be deleted while referenced by Content X!"
    );
    // 3. Unique chunk of Content X must still exist
    assert!(store
        .chunk_store()
        .has(&manifest_x.chunks[1].chunk_id)
        .unwrap());

    // 4. Content X can be reconstructed perfectly
    let reconstructed_x = store.reconstruct_to_bytes(&cid_x).await.unwrap();
    assert_eq!(reconstructed_x, payload_x);
}

// =========================================================================
// TEST 04: Storage Quota Enforcement & Reservations (Section 11)
// =========================================================================
#[tokio::test]
async fn test_04_storage_quota_enforcement_and_reservations() {
    let tmp = tempdir().unwrap();
    let store = Arc::new(ContentStore::open(tmp.path()).unwrap());

    // Configure small quota: 600 KiB max
    store
        .quota_manager()
        .update_config(QuotaConfig {
            total_storage_quota: 600 * 1024,
            cache_quota: 300 * 1024,
            temp_quota: 200 * 1024,
            warning_threshold_percent: 80.0,
        })
        .await;

    let quota_manager = store.quota_manager();

    // Try reserving 500 KiB -> succeeds
    let res1 = quota_manager.reserve(500 * 1024).await;
    assert!(res1.is_ok());
    let token1 = res1.unwrap();

    // Try reserving another 200 KiB -> exceeds 600 KiB hard quota -> returns QuotaExceeded
    let res2 = quota_manager.reserve(200 * 1024).await;
    assert!(res2.is_err());
    match res2.unwrap_err() {
        ContentError::QuotaExceeded {
            requested_bytes,
            quota_bytes,
            ..
        } => {
            assert_eq!(requested_bytes, 200 * 1024);
            assert_eq!(quota_bytes, 600 * 1024);
        }
        other => panic!("Expected QuotaExceeded error, got: {:?}", other),
    }

    // Release reservation 1
    quota_manager.release_reservation(&token1);

    // Now 200 KiB reservation succeeds
    let res3 = quota_manager.reserve(200 * 1024).await;
    assert!(res3.is_ok());
}

// =========================================================================
// TEST 05: Event Bus Sequencing & Invariants (Section 13)
// =========================================================================
#[tokio::test]
async fn test_05_event_bus_sequencing_and_invariants() {
    let tmp = tempdir().unwrap();
    let store = Arc::new(ContentStore::open(tmp.path()).unwrap());

    let mut event_rx = store.subscribe_events();

    let payload = generate_test_payload(300 * 1024, 42);
    let put_res = store
        .put(&payload, Some(PutOptions::default()))
        .await
        .unwrap();
    let cid = put_res.metadata().content_id;

    // Verify events emitted in order: Created -> Available
    let ev1 = event_rx.recv().await.unwrap();
    assert_eq!(ev1.event_type(), "content.created");
    let ev2 = event_rx.recv().await.unwrap();
    assert_eq!(ev2.event_type(), "content.available");

    // Pin event
    store.pin(&cid).unwrap();
    let ev3 = event_rx.recv().await.unwrap();
    assert_eq!(ev3.event_type(), "content.pinned");

    // Unpin event
    store.unpin(&cid).unwrap();
    let ev4 = event_rx.recv().await.unwrap();
    assert_eq!(ev4.event_type(), "content.unpinned");

    // Delete event
    store.delete(&cid).await.unwrap();
    let ev5 = event_rx.recv().await.unwrap();
    assert_eq!(ev5.event_type(), "content.deleted");
}

// =========================================================================
// TEST 06: Resource Exhaustion & Capacity Bounds (Section 15)
// =========================================================================
#[test]
fn test_06_resource_exhaustion_and_capacity_bounds() {
    let (tx, _) = tokio::sync::broadcast::channel(100);
    let config = ProviderRegistryConfig {
        max_providers_per_content: 2,
        max_tracked_contents: 3,
        max_advertisements_per_peer: 2,
        max_total_providers: 10,
        default_ttl_seconds: 300,
    };
    let registry = ProviderRegistry::new(config, tx);

    let content_id = ContentId::from_bytes([13u8; 32]);
    let id1 = NodeIdentity::generate();
    let id2 = NodeIdentity::generate();
    let id3 = NodeIdentity::generate();

    for (seq, id) in [(1, &id1), (2, &id2)] {
        let mut ad = ContentAdvertise::new_full(
            content_id,
            None,
            id.node_id(),
            id.public_key_bytes(),
            Some(10),
            300,
            seq,
        );
        ad.sign(id);
        assert!(registry
            .handle_advertisement(&ad, TrustTier::Authorized)
            .is_ok());
    }

    // 3rd provider for same content exceeds max_providers_per_content = 2
    let mut ad3 = ContentAdvertise::new_full(
        content_id,
        None,
        id3.node_id(),
        id3.public_key_bytes(),
        Some(10),
        300,
        3,
    );
    ad3.sign(&id3);
    let res = registry.handle_advertisement(&ad3, TrustTier::Authorized);
    assert!(
        res.is_err(),
        "Registry must reject exceeding provider limit per content"
    );
    match res.unwrap_err() {
        ContentError::RegistryCapacityExceeded(_) => {}
        other => panic!("Expected RegistryCapacityExceeded, got {:?}", other),
    }
}

// =========================================================================
// TEST 07: Property Testing Roundtrip for Arbitrary Sizes (Section 16)
// =========================================================================
#[tokio::test]
async fn test_07_property_testing_roundtrip_all_sizes() {
    let test_sizes = vec![
        0usize,           // Empty content
        1usize,           // Single byte
        1024,             // 1 KiB
        256 * 1024 - 1,   // Exactly 1 byte below chunk boundary
        256 * 1024,       // Exactly on chunk boundary
        256 * 1024 + 1,   // Exactly 1 byte above chunk boundary
        650 * 1024,       // Multi-chunk (3 chunks)
        1024 * 1024 + 42, // > 1 MiB
    ];

    let tmp = tempdir().unwrap();
    let store = Arc::new(ContentStore::open(tmp.path()).unwrap());

    for (idx, size) in test_sizes.into_iter().enumerate() {
        let payload = generate_test_payload(size, idx as u8);

        // Put chunked
        let (cid, _, _) = store
            .put_chunked_stream(&mut payload.as_slice(), None)
            .await
            .unwrap_or_else(|_| panic!("Failed to store chunked content of size {}", size));

        // Reconstruct
        let reconstructed = store
            .reconstruct_to_bytes(&cid)
            .await
            .unwrap_or_else(|_| panic!("Failed to reconstruct content of size {}", size));

        // Invariant: original bytes == reconstructed bytes
        assert_eq!(
            payload, reconstructed,
            "Reconstructed bytes must match original for size {}",
            size
        );

        // Invariant: original ContentId == reconstructed ContentId
        let expected_cid = ContentId::from_bytes(hash_bytes(&reconstructed).to_bytes());
        assert_eq!(
            cid, expected_cid,
            "Reconstructed ContentId must match expected ContentId for size {}",
            size
        );
    }
}

// =========================================================================
// TEST 08: Fuzz Testing Parsers & Validators (Section 17)
// =========================================================================
#[test]
fn test_08_fuzz_testing_parsers_and_validators() {
    let garbage_strings = vec![
        "",
        "   ",
        "invalid",
        "fn-cid-",
        "fn-cid-12345",
        "fn-cid-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
        "fn-cid-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef00", // too long
        "fn-chk-",
        "fn-chk-short",
        "sha256:nothex",
        "\0\0\0\0\0\0\0",
        "🦀🚀✨🔥",
        "../../etc/passwd",
        "'; DROP TABLE content; --",
    ];

    for s in garbage_strings {
        // ContentId parsing must never panic on invalid inputs
        let res_cid = ContentId::from_str(s);
        assert!(
            res_cid.is_err(),
            "Invalid string '{}' must fail ContentId parsing",
            s
        );

        // ChunkId parsing must never panic on invalid inputs
        let res_chk = ChunkId::from_str(s);
        assert!(
            res_chk.is_err(),
            "Invalid string '{}' must fail ChunkId parsing",
            s
        );
    }

    // Fuzz Manifest validation with corrupt manifests
    let valid_cid = ContentId::from_bytes([0x55; 32]);
    let valid_mid = ManifestId::from_bytes([0x66; 32]);

    let mut invalid_manifest = ContentManifest {
        manifest_id: valid_mid,
        content_id: valid_cid,
        schema_version: 99, // unsupported schema
        total_size: 100,
        chunk_size: 256 * 1024,
        chunk_count: 1,
        chunks: vec![],
        chunking_algorithm: "fixed-256k".to_string(),
        merkle_root: [0u8; 32],
        mime_type: None,
        metadata: Default::default(),
        created_at: Timestamp::now(),
        creator_node_id: Some(NodeId::from_bytes([0u8; 32])),
    };

    assert!(ManifestValidator::validate(&invalid_manifest).is_err());

    // Chunk count mismatch
    invalid_manifest.schema_version = 1;
    invalid_manifest.chunk_count = 5;
    invalid_manifest.chunks = vec![]; // empty chunks despite chunk_count = 5
    assert!(ManifestValidator::validate(&invalid_manifest).is_err());
}

// =========================================================================
// TEST 09: Performance Baseline Metrics (Section 19)
// =========================================================================
#[tokio::test]
async fn test_09_performance_baseline_metrics() {
    let tmp = tempdir().unwrap();
    let store = Arc::new(ContentStore::open(tmp.path()).unwrap());

    let payload_size = 2 * 1024 * 1024; // 2 MiB
    let payload = generate_test_payload(payload_size, 99);

    // 1. Chunking Throughput
    let chunker = FixedSizeChunker::default();
    let t0 = Instant::now();
    let slices = chunker.chunk_slice(&payload);
    let chunk_dur = t0.elapsed();
    let chunk_mb_per_sec = (payload_size as f64 / 1_000_000.0) / chunk_dur.as_secs_f64();

    println!(
        "\n[BENCHMARK] Chunking: {:.2} MB in {:?} => {:.2} MB/s ({} chunks)",
        payload_size as f64 / 1_000_000.0,
        chunk_dur,
        chunk_mb_per_sec,
        slices.len()
    );
    assert!(
        chunk_mb_per_sec > 10.0,
        "Chunking throughput should exceed 10 MB/s"
    );

    // 2. Ingestion Throughput
    let t1 = Instant::now();
    let (cid, _, _) = store
        .put_chunked_stream(&mut payload.as_slice(), None)
        .await
        .unwrap();
    let ingest_dur = t1.elapsed();
    let ingest_mb_per_sec = (payload_size as f64 / 1_000_000.0) / ingest_dur.as_secs_f64();

    println!(
        "[BENCHMARK] Ingestion: {:.2} MB in {:?} => {:.2} MB/s",
        payload_size as f64 / 1_000_000.0,
        ingest_dur,
        ingest_mb_per_sec
    );
    let min_ingest_mb = if cfg!(debug_assertions) { 2.0 } else { 5.0 };
    assert!(
        ingest_mb_per_sec > min_ingest_mb,
        "Ingestion throughput should exceed {} MB/s",
        min_ingest_mb
    );

    // 3. Reconstruction Throughput
    let t2 = Instant::now();
    let reconstructed = store.reconstruct_to_bytes(&cid).await.unwrap();
    let recon_dur = t2.elapsed();
    let recon_mb_per_sec = (payload_size as f64 / 1_000_000.0) / recon_dur.as_secs_f64();

    println!(
        "[BENCHMARK] Reconstruction: {:.2} MB in {:?} => {:.2} MB/s",
        payload_size as f64 / 1_000_000.0,
        recon_dur,
        recon_mb_per_sec
    );
    assert_eq!(reconstructed.len(), payload_size);
    assert!(
        recon_mb_per_sec > 5.0,
        "Reconstruction throughput should exceed 5 MB/s"
    );

    // 4. GC Execution Duration
    let t3 = Instant::now();
    let _ = store.run_gc(None).await.unwrap();
    let gc_dur = t3.elapsed();
    println!(
        "[BENCHMARK] Garbage Collection Cycle Duration: {:?}",
        gc_dur
    );
}
