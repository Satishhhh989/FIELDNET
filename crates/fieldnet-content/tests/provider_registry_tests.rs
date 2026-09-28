use std::sync::{Arc, Mutex};
use tempfile::tempdir;

use fieldnet_content::{
    ChunkAvailabilityBitmap, ContentAdvertise, ContentError, ContentId, ContentMessage,
    ContentQuery, ContentResolution, ContentResolver, ContentStore, ProviderRegistry,
    ProviderRegistryConfig, ProviderState, TrustTier,
};
use fieldnet_core::{NodeId, Timestamp};
use fieldnet_identity::NodeIdentity;

fn test_registry() -> ProviderRegistry {
    let (tx, _) = tokio::sync::broadcast::channel(100);
    ProviderRegistry::new(ProviderRegistryConfig::default(), tx)
}

fn test_registry_with_config(config: ProviderRegistryConfig) -> ProviderRegistry {
    let (tx, _) = tokio::sync::broadcast::channel(100);
    ProviderRegistry::new(config, tx)
}

fn test_persisted_registry(conn: Arc<Mutex<rusqlite::Connection>>) -> ProviderRegistry {
    let (tx, _) = tokio::sync::broadcast::channel(100);
    ProviderRegistry::with_persistence(ProviderRegistryConfig::default(), conn, tx).unwrap()
}

fn make_bitmap(indices: &[u32], total: u32) -> ChunkAvailabilityBitmap {
    let mut bitmap = ChunkAvailabilityBitmap::new(total);
    for &i in indices {
        bitmap.mark_available(i);
    }
    bitmap
}

#[test]
fn test_01_provider_registration() {
    let registry = test_registry();
    let identity = NodeIdentity::generate();
    let content_id = ContentId::from_bytes([1u8; 32]);

    let mut ad = ContentAdvertise::new_full(
        content_id,
        None,
        identity.node_id(),
        identity.public_key_bytes(),
        Some(1024),
        300,
        1,
    );
    ad.sign(&identity);

    let res = registry.handle_advertisement(&ad, TrustTier::Authorized);
    assert!(res.is_ok());

    let record = registry
        .get(&content_id, &identity.node_id())
        .expect("Provider should be found");
    assert_eq!(record.content_id, content_id);
    assert_eq!(record.provider_node_id, identity.node_id());
    assert!(record.has_full_content);
    assert_eq!(record.trust_tier, TrustTier::Authorized);
    assert_eq!(record.availability_state, ProviderState::Active);
}

#[test]
fn test_02_provider_update() {
    let registry = test_registry();
    let identity = NodeIdentity::generate();
    let content_id = ContentId::from_bytes([2u8; 32]);

    // Initial partial advertisement (chunks 0..3)
    let bitmap1 = make_bitmap(&[0, 1, 2], 10);
    let mut ad1 = ContentAdvertise::new(
        None,
        content_id,
        None,
        identity.node_id(),
        identity.public_key_bytes(),
        false,
        Some(bitmap1),
        Some(10),
        Some(500),
        300,
        1,
    );
    ad1.sign(&identity);
    registry
        .handle_advertisement(&ad1, TrustTier::Authenticated)
        .unwrap();

    let rec1 = registry.get(&content_id, &identity.node_id()).unwrap();
    assert!(!rec1.has_full_content);
    assert_eq!(rec1.sequence, 1);

    // Update with newer sequence and full content
    let mut ad2 = ContentAdvertise::new_full(
        content_id,
        None,
        identity.node_id(),
        identity.public_key_bytes(),
        Some(1000),
        300,
        2,
    );
    ad2.sign(&identity);
    registry
        .handle_advertisement(&ad2, TrustTier::Authorized)
        .unwrap();

    let rec2 = registry.get(&content_id, &identity.node_id()).unwrap();
    assert!(rec2.has_full_content);
    assert_eq!(rec2.sequence, 2);
    assert_eq!(rec2.trust_tier, TrustTier::Authorized);
}

#[test]
fn test_03_multiple_providers_for_content() {
    let registry = test_registry();
    let id_a = NodeIdentity::generate();
    let id_b = NodeIdentity::generate();
    let id_c = NodeIdentity::generate();
    let content_id = ContentId::from_bytes([3u8; 32]);

    for (seq, id) in [(1, &id_a), (2, &id_b), (3, &id_c)] {
        let mut ad = ContentAdvertise::new_full(
            content_id,
            None,
            id.node_id(),
            id.public_key_bytes(),
            Some(2048),
            300,
            seq,
        );
        ad.sign(id);
        registry
            .handle_advertisement(&ad, TrustTier::Authenticated)
            .unwrap();
    }

    let providers = registry.find_providers(&content_id);
    assert_eq!(providers.len(), 3);

    let provider_ids: Vec<NodeId> = providers.iter().map(|p| p.provider_node_id).collect();
    assert!(provider_ids.contains(&id_a.node_id()));
    assert!(provider_ids.contains(&id_b.node_id()));
    assert!(provider_ids.contains(&id_c.node_id()));
}

#[test]
fn test_04_provider_expiration_and_cleanup() {
    let registry = test_registry();
    let id = NodeIdentity::generate();
    let content_id = ContentId::from_bytes([4u8; 32]);

    // Advertisement with 1-second TTL
    let mut ad = ContentAdvertise::new_full(
        content_id,
        None,
        id.node_id(),
        id.public_key_bytes(),
        Some(1024),
        1,
        1,
    );
    ad.sign(&id);
    registry
        .handle_advertisement(&ad, TrustTier::Authorized)
        .unwrap();

    assert_eq!(registry.find_providers(&content_id).len(), 1);

    // Fast forward beyond expiration
    let future_time = Timestamp::from_millis(Timestamp::now().as_millis() + 2_000);
    let expired_count = registry.expire(future_time);
    assert_eq!(expired_count, 1);

    // Expired providers are excluded from find_providers
    assert_eq!(registry.find_providers(&content_id).len(), 0);

    // Expired records are purged from registry and return None on get
    assert!(registry.get(&content_id, &id.node_id()).is_none());
}

#[test]
fn test_05_provider_invalidation_and_banning() {
    let registry = test_registry();
    let id = NodeIdentity::generate();
    let content_id = ContentId::from_bytes([5u8; 32]);

    let mut ad = ContentAdvertise::new_full(
        content_id,
        None,
        id.node_id(),
        id.public_key_bytes(),
        Some(1024),
        300,
        1,
    );
    ad.sign(&id);
    registry
        .handle_advertisement(&ad, TrustTier::Authorized)
        .unwrap();
    assert_eq!(registry.find_providers(&content_id).len(), 1);

    // Invalidate provider
    let invalidated =
        registry.invalidate(&content_id, &id.node_id(), "Falsified hash verification");
    assert!(invalidated);

    let rec = registry.get(&content_id, &id.node_id()).unwrap();
    assert_eq!(rec.availability_state, ProviderState::Banned);

    // Banned provider must not appear in candidate queries
    assert_eq!(registry.find_providers(&content_id).len(), 0);
}

#[test]
fn test_06_stale_provider_handling() {
    let registry = test_registry();
    let id = NodeIdentity::generate();
    let content_id = ContentId::from_bytes([6u8; 32]);

    let mut ad = ContentAdvertise::new_full(
        content_id,
        None,
        id.node_id(),
        id.public_key_bytes(),
        Some(1024),
        300,
        1,
    );
    ad.sign(&id);
    registry
        .handle_advertisement(&ad, TrustTier::Authorized)
        .unwrap();

    // Mark as stale when peer disconnects
    registry.handle_peer_expired(&id.node_id());

    let rec = registry.get(&content_id, &id.node_id()).unwrap();
    assert_eq!(rec.availability_state, ProviderState::Stale);

    // Stale provider is not usable in active provider lookups
    assert_eq!(registry.find_providers(&content_id).len(), 0);

    // When peer reappears, state transitions back to Active
    registry.handle_peer_discovered(&id.node_id());
    let rec_reappear = registry.get(&content_id, &id.node_id()).unwrap();
    assert_eq!(rec_reappear.availability_state, ProviderState::Active);
    assert_eq!(registry.find_providers(&content_id).len(), 1);
}

#[test]
fn test_07_chunk_availability_bitmap_filtering() {
    let registry = test_registry();
    let id_a = NodeIdentity::generate();
    let id_b = NodeIdentity::generate();
    let content_id = ContentId::from_bytes([7u8; 32]);

    // Provider A has chunks 0..40
    let bitmap_a = make_bitmap(&(0..40).collect::<Vec<_>>(), 100);
    let mut ad_a = ContentAdvertise::new(
        None,
        content_id,
        None,
        id_a.node_id(),
        id_a.public_key_bytes(),
        false,
        Some(bitmap_a),
        Some(100),
        Some(100_000),
        300,
        1,
    );
    ad_a.sign(&id_a);
    registry
        .handle_advertisement(&ad_a, TrustTier::Authenticated)
        .unwrap();

    // Provider B has chunks 40..80
    let bitmap_b = make_bitmap(&(40..80).collect::<Vec<_>>(), 100);
    let mut ad_b = ContentAdvertise::new(
        None,
        content_id,
        None,
        id_b.node_id(),
        id_b.public_key_bytes(),
        false,
        Some(bitmap_b),
        Some(100),
        Some(100_000),
        300,
        1,
    );
    ad_b.sign(&id_b);
    registry
        .handle_advertisement(&ad_b, TrustTier::Authenticated)
        .unwrap();

    // Query for chunk 10 -> only A should match
    let providers_for_10 = registry.find_providers_for_chunks(&content_id, &[10]);
    assert_eq!(providers_for_10.len(), 1);
    assert_eq!(providers_for_10[0].provider_node_id, id_a.node_id());

    // Query for chunk 50 -> only B should match
    let providers_for_50 = registry.find_providers_for_chunks(&content_id, &[50]);
    assert_eq!(providers_for_50.len(), 1);
    assert_eq!(providers_for_50[0].provider_node_id, id_b.node_id());

    // Query for chunks [10, 50] -> both match
    let providers_both = registry.find_providers_for_chunks(&content_id, &[10, 50]);
    assert_eq!(providers_both.len(), 2);

    // Query for chunk 95 -> neither matches
    let providers_missing = registry.find_providers_for_chunks(&content_id, &[95]);
    assert_eq!(providers_missing.len(), 0);
}

#[test]
fn test_08_content_advertisement_signing_and_verification() {
    let identity = NodeIdentity::generate();
    let content_id = ContentId::from_bytes([8u8; 32]);

    let mut ad = ContentAdvertise::new_full(
        content_id,
        None,
        identity.node_id(),
        identity.public_key_bytes(),
        Some(5000),
        120,
        1,
    );

    // Before signing, signature is all zeroes -> verify fails
    assert!(ad.verify().is_err());

    // Sign with valid identity -> verify succeeds
    ad.sign(&identity);
    assert!(ad.verify().is_ok());

    // Tampering with content_size invalidates signature
    ad.content_size = Some(9999);
    assert!(ad.verify().is_err());
}

#[test]
fn test_09_content_query_signing_and_verification() {
    let identity = NodeIdentity::generate();
    let content_id = ContentId::from_bytes([9u8; 32]);

    let mut query = ContentQuery::create(
        content_id,
        None,
        Some((0, 10)),
        identity.node_id(),
        identity.public_key_bytes(),
        60,
    );

    // Unsigned query verification fails
    assert!(query.verify().is_err());

    // Sign query
    query.sign(&identity);
    assert!(query.verify().is_ok());
    assert!(query.validate(Timestamp::now()).is_ok());

    // Expired query validation fails
    let future = Timestamp::from_millis(Timestamp::now().as_millis() + 100_000);
    assert!(query.validate(future).is_err());
}

#[test]
fn test_10_unknown_unauthenticated_unauthorized_trust_tiers() {
    let registry = test_registry();
    let id_unknown = NodeIdentity::generate();
    let id_discovered = NodeIdentity::generate();
    let id_auth = NodeIdentity::generate();
    let content_id = ContentId::from_bytes([10u8; 32]);

    // Unknown provider ad
    let mut ad_u = ContentAdvertise::new_full(
        content_id,
        None,
        id_unknown.node_id(),
        id_unknown.public_key_bytes(),
        Some(10),
        300,
        1,
    );
    ad_u.sign(&id_unknown);
    registry
        .handle_advertisement(&ad_u, TrustTier::Unknown)
        .unwrap();

    // Discovered provider ad
    let mut ad_d = ContentAdvertise::new_full(
        content_id,
        None,
        id_discovered.node_id(),
        id_discovered.public_key_bytes(),
        Some(10),
        300,
        1,
    );
    ad_d.sign(&id_discovered);
    registry
        .handle_advertisement(&ad_d, TrustTier::Discovered)
        .unwrap();

    // Authorized provider ad
    let mut ad_a = ContentAdvertise::new_full(
        content_id,
        None,
        id_auth.node_id(),
        id_auth.public_key_bytes(),
        Some(10),
        300,
        1,
    );
    ad_a.sign(&id_auth);
    registry
        .handle_advertisement(&ad_a, TrustTier::Authorized)
        .unwrap();

    let rec_u = registry.get(&content_id, &id_unknown.node_id()).unwrap();
    assert_eq!(rec_u.trust_tier, TrustTier::Unknown);

    let rec_d = registry.get(&content_id, &id_discovered.node_id()).unwrap();
    assert_eq!(rec_d.trust_tier, TrustTier::Discovered);

    let rec_a = registry.get(&content_id, &id_auth.node_id()).unwrap();
    assert_eq!(rec_a.trust_tier, TrustTier::Authorized);
}

#[test]
fn test_11_malformed_advertisement_rejection() {
    let registry = test_registry();
    let identity = NodeIdentity::generate();
    let other_identity = NodeIdentity::generate();
    let content_id = ContentId::from_bytes([11u8; 32]);

    // Spoofed provider node ID (identity does not match NodeId)
    let mut ad = ContentAdvertise::new_full(
        content_id,
        None,
        identity.node_id(),
        other_identity.public_key_bytes(), // Mismatched public key
        Some(100),
        300,
        1,
    );
    ad.sign(&other_identity);
    // Setting provider_node_id manually to forged ID
    ad.provider_node_id = identity.node_id();

    let res = registry.handle_advertisement(&ad, TrustTier::Authorized);
    assert!(res.is_err());
    match res.unwrap_err() {
        ContentError::InvalidAdvertisement(_) => {}
        other => panic!("Expected InvalidAdvertisement, got {:?}", other),
    }
}

#[test]
fn test_12_oversized_advertisement_rejection() {
    // Attempting to deserialize payload larger than MAX_CONTENT_MESSAGE_SIZE
    let oversized_payload = vec![0x42u8; 70 * 1024]; // 70 KiB
    let res = ContentMessage::from_bytes(&oversized_payload);
    assert!(res.is_err());
}

#[test]
fn test_13_registry_resource_limits_and_capacity_enforcement() {
    let config = ProviderRegistryConfig {
        max_providers_per_content: 2,
        max_tracked_contents: 3,
        max_advertisements_per_peer: 2,
        max_total_providers: 10,
        default_ttl_seconds: 300,
    };
    let registry = test_registry_with_config(config);
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
        registry
            .handle_advertisement(&ad, TrustTier::Authorized)
            .unwrap();
    }

    // 3rd provider for same content should fail capacity limit
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
    assert!(res.is_err());
    match res.unwrap_err() {
        ContentError::RegistryCapacityExceeded(_) => {}
        other => panic!("Expected RegistryCapacityExceeded, got {:?}", other),
    }
}

#[test]
fn test_14_peer_disappearance_and_reappearance() {
    let registry = test_registry();
    let id = NodeIdentity::generate();
    let content_id = ContentId::from_bytes([14u8; 32]);

    let mut ad = ContentAdvertise::new_full(
        content_id,
        None,
        id.node_id(),
        id.public_key_bytes(),
        Some(500),
        300,
        1,
    );
    ad.sign(&id);
    registry
        .handle_advertisement(&ad, TrustTier::Authorized)
        .unwrap();

    // Disappear
    registry.handle_peer_expired(&id.node_id());
    assert_eq!(
        registry
            .get(&content_id, &id.node_id())
            .unwrap()
            .availability_state,
        ProviderState::Stale
    );

    // Reappear
    registry.handle_peer_discovered(&id.node_id());
    assert_eq!(
        registry
            .get(&content_id, &id.node_id())
            .unwrap()
            .availability_state,
        ProviderState::Active
    );
}

#[test]
fn test_15_runtime_restart_sqlite_cache_persistence() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("test_content.db");

    let id = NodeIdentity::generate();
    let content_id = ContentId::from_bytes([15u8; 32]);

    // Instance 1: register and persist to SQLite
    {
        let conn = Arc::new(Mutex::new(rusqlite::Connection::open(&db_path).unwrap()));
        let registry = test_persisted_registry(conn);

        let mut ad = ContentAdvertise::new_full(
            content_id,
            None,
            id.node_id(),
            id.public_key_bytes(),
            Some(777),
            300,
            1,
        );
        ad.sign(&id);
        registry
            .handle_advertisement(&ad, TrustTier::Authorized)
            .unwrap();
        assert_eq!(registry.find_providers(&content_id).len(), 1);
    }

    // Instance 2: reopen SQLite DB and verify cached provider recovered
    {
        let conn2 = Arc::new(Mutex::new(rusqlite::Connection::open(&db_path).unwrap()));
        let registry2 = test_persisted_registry(conn2);

        // Cached provider is present in registry in Stale state pending re-validation
        let cached = registry2
            .get(&content_id, &id.node_id())
            .expect("Cached record recovered");
        assert_eq!(cached.provider_node_id, id.node_id());
        assert_eq!(cached.content_size, Some(777));
        assert_eq!(cached.trust_tier, TrustTier::Authorized);
        assert_eq!(cached.availability_state, ProviderState::Stale);

        // When peer is re-discovered/connected, provider becomes Active and visible
        registry2.handle_peer_discovered(&id.node_id());
        let providers = registry2.find_providers(&content_id);
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].provider_node_id, id.node_id());
        assert_eq!(providers[0].availability_state, ProviderState::Active);
    }
}

#[tokio::test]
async fn test_16_content_resolver_local_vs_remote_vs_not_found() {
    let tmp = tempdir().unwrap();
    let content_store = Arc::new(ContentStore::open(tmp.path()).unwrap());
    let provider_registry = Arc::clone(content_store.provider_registry());
    let resolver = ContentResolver::new(Arc::clone(&content_store), Arc::clone(&provider_registry));

    // 1. Content not found anywhere
    let missing_id = ContentId::from_bytes([99u8; 32]);
    let res = resolver.resolve(&missing_id).unwrap();
    assert_eq!(res, ContentResolution::NotFound(missing_id));

    // 2. Content available locally
    let test_data = b"Hello, local content resolution!";
    let put_res = content_store.put(&test_data[..], None).await.unwrap();
    let local_id = put_res.content_id();
    let res_local = resolver.resolve(&local_id).unwrap();
    match res_local {
        ContentResolution::Local {
            content_id,
            metadata,
            ..
        } => {
            assert_eq!(content_id, local_id);
            assert_eq!(metadata.size_bytes, test_data.len() as u64);
        }
        other => panic!("Expected Local resolution, got {:?}", other),
    }

    // 3. Content available only remotely from provider
    let remote_id = ContentId::from_bytes([88u8; 32]);
    let id_remote = NodeIdentity::generate();
    let mut ad = ContentAdvertise::new_full(
        remote_id,
        None,
        id_remote.node_id(),
        id_remote.public_key_bytes(),
        Some(1234),
        300,
        1,
    );
    ad.sign(&id_remote);
    provider_registry
        .handle_advertisement(&ad, TrustTier::Authorized)
        .unwrap();

    let res_remote = resolver.resolve(&remote_id).unwrap();
    match res_remote {
        ContentResolution::Remote {
            content_id,
            providers,
        } => {
            assert_eq!(content_id, remote_id);
            assert_eq!(providers.len(), 1);
            assert_eq!(providers[0].provider_node_id, id_remote.node_id());
        }
        other => panic!("Expected Remote resolution, got {:?}", other),
    }
}

#[test]
fn test_17_content_message_framing_and_size_bounds() {
    let identity = NodeIdentity::generate();
    let content_id = ContentId::from_bytes([17u8; 32]);

    let mut query = ContentQuery::create(
        content_id,
        None,
        None,
        identity.node_id(),
        identity.public_key_bytes(),
        60,
    );
    query.sign(&identity);

    let msg = ContentMessage::Query(query.clone());
    let bytes = msg.to_bytes().unwrap();
    assert!(bytes.len() < 1024);

    let decoded = ContentMessage::from_bytes(&bytes).unwrap();
    match decoded {
        ContentMessage::Query(q) => {
            assert_eq!(q.content_id, content_id);
            assert_eq!(q.requester, identity.node_id());
        }
        _ => panic!("Expected Query message"),
    }
}
