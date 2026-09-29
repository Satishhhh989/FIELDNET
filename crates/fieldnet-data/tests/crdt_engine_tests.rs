use fieldnet_core::{NodeId, Timestamp};
use fieldnet_data::crdt::{
    ConflictClassification, Crdt, CrdtMap, ElementTag, LwwRegister, MvRegister, OrSet, OrSetDelta,
    PnCounter, PnCounterDelta,
};
use fieldnet_data::{
    DataError, DocumentId, DocumentStore, HybridLogicalClock, Operation, OperationId, OperationType,
};
use fieldnet_identity::NodeIdentity;
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

fn random_node_id() -> NodeId {
    NodeIdentity::generate().node_id()
}

// ============================================================================
// TEST 1: Counter increment
// ============================================================================
#[test]
fn test_01_counter_increment() {
    let actor = random_node_id();
    let mut counter = PnCounter::new();
    assert_eq!(counter.read(), 0);

    counter.increment(actor, 5);
    assert_eq!(counter.read(), 5);

    counter.increment(actor, 10);
    assert_eq!(counter.read(), 15);
}

// ============================================================================
// TEST 2: Counter decrement
// ============================================================================
#[test]
fn test_02_counter_decrement() {
    let actor = random_node_id();
    let mut counter = PnCounter::new();

    counter.increment(actor, 20);
    counter.decrement(actor, 7);
    assert_eq!(counter.read(), 13);

    // Negative counter values are supported
    counter.decrement(actor, 20);
    assert_eq!(counter.read(), -7);
}

// ============================================================================
// TEST 3: Concurrent counter increments converge
// ============================================================================
#[test]
fn test_03_concurrent_counter_increments_converge() {
    let actor_a = random_node_id();
    let actor_b = random_node_id();

    // Node A offline: +5
    let mut counter_a = PnCounter::new();
    counter_a.increment(actor_a, 5);

    // Node B offline: +3
    let mut counter_b = PnCounter::new();
    counter_b.increment(actor_b, 3);

    // Merge A into B
    counter_b.merge(&counter_a).unwrap();
    assert_eq!(counter_b.read(), 8);

    // Merge B into A
    counter_a.merge(&counter_b).unwrap();
    assert_eq!(counter_a.read(), 8);

    // Both replicas converge to identical state and state hash
    assert_eq!(counter_a, counter_b);
    assert_eq!(counter_a.state_hash(), counter_b.state_hash());
}

// ============================================================================
// TEST 4: Set add
// ============================================================================
#[test]
fn test_04_set_add() {
    let actor = random_node_id();
    let hlc = HybridLogicalClock::now();
    let mut set: OrSet<String> = OrSet::new();

    let tag1 = ElementTag::new(actor, hlc, 1);
    let tag2 = ElementTag::new(actor, hlc, 2);

    set.add("apple".to_string(), tag1);
    set.add("banana".to_string(), tag2);

    assert!(set.contains(&"apple".to_string()));
    assert!(set.contains(&"banana".to_string()));
    assert!(!set.contains(&"cherry".to_string()));
    assert_eq!(
        set.values(),
        vec!["apple".to_string(), "banana".to_string()]
    );
}

// ============================================================================
// TEST 5: Set remove
// ============================================================================
#[test]
fn test_05_set_remove() {
    let actor = random_node_id();
    let hlc = HybridLogicalClock::now();
    let mut set: OrSet<String> = OrSet::new();

    let tag = ElementTag::new(actor, hlc, 1);
    set.add("apple".to_string(), tag);
    assert!(set.contains(&"apple".to_string()));

    let remove_delta = set.generate_remove(&"apple".to_string());
    set.apply_delta(&remove_delta).unwrap();

    assert!(!set.contains(&"apple".to_string()));
    assert!(set.is_empty());
}

// ============================================================================
// TEST 6: Concurrent set operations converge according to defined semantics (Add-Wins)
// ============================================================================
#[test]
fn test_06_concurrent_set_operations_converge_add_wins() {
    let actor_a = random_node_id();
    let actor_b = random_node_id();
    let hlc_init = HybridLogicalClock::new(100, 0);

    // Initial state shared by both replicas
    let initial_tag = ElementTag::new(actor_a, hlc_init, 1);
    let mut set_a: OrSet<String> = OrSet::new();
    set_a.add("resource_alpha".to_string(), initial_tag);

    let mut set_b = set_a.clone();

    // Node A offline removes "resource_alpha" (observes initial_tag)
    let remove_delta = set_a.generate_remove(&"resource_alpha".to_string());
    set_a.apply_delta(&remove_delta).unwrap();
    assert!(!set_a.contains(&"resource_alpha".to_string()));

    // Node B offline adds "resource_alpha" concurrently (generates new unobserved tag)
    let hlc_b = HybridLogicalClock::new(200, 0);
    let add_delta = set_b.generate_add("resource_alpha".to_string(), actor_b, hlc_b, 2);
    set_b.apply_delta(&add_delta).unwrap();

    // Merge in both directions
    let mut merged_a = set_a.clone();
    merged_a.merge(&set_b).unwrap();

    let mut merged_b = set_b.clone();
    merged_b.merge(&set_a).unwrap();

    // Add-Wins invariant: concurrent add wins over remove of previously observed tags
    assert!(merged_a.contains(&"resource_alpha".to_string()));
    assert!(merged_b.contains(&"resource_alpha".to_string()));

    // Identical convergence
    assert_eq!(merged_a, merged_b);
    assert_eq!(merged_a.state_hash(), merged_b.state_hash());
}

// ============================================================================
// TEST 7: Register causal update
// ============================================================================
#[test]
fn test_07_register_causal_update() {
    let actor_a = random_node_id();
    let actor_b = random_node_id();

    let hlc_1 = HybridLogicalClock::new(100, 0);
    let hlc_2 = HybridLogicalClock::new(200, 0); // Causally later

    let mut reg_a = LwwRegister::new("offline".to_string(), hlc_1, actor_a);
    let reg_b = LwwRegister::new("online".to_string(), hlc_2, actor_b);

    // Merge: reg_b causally dominates reg_a
    reg_a.merge(&reg_b).unwrap();
    assert_eq!(reg_a.read(), "online");
    assert_eq!(reg_a.hlc, hlc_2);
    assert_eq!(reg_a.author, actor_b);
}

// ============================================================================
// TEST 8: Register concurrent update
// ============================================================================
#[test]
fn test_08_register_concurrent_update() {
    let actor_a = random_node_id();
    let actor_b = random_node_id();

    // Same HLC timestamp (concurrent modification)
    let hlc_same = HybridLogicalClock::new(500, 0);

    let reg_a = LwwRegister::new("status_a".to_string(), hlc_same, actor_a);
    let reg_b = LwwRegister::new("status_b".to_string(), hlc_same, actor_b);

    let mut replica_1 = reg_a.clone();
    replica_1.merge(&reg_b).unwrap();

    let mut replica_2 = reg_b.clone();
    replica_2.merge(&reg_a).unwrap();

    // Both must converge to the exact same winner based on deterministic tie-break
    assert_eq!(replica_1.read(), replica_2.read());
    assert_eq!(replica_1.author, replica_2.author);
    assert_eq!(replica_1.state_hash(), replica_2.state_hash());

    let expected_winner = if actor_a.as_bytes() > actor_b.as_bytes() {
        "status_a"
    } else {
        "status_b"
    };
    assert_eq!(replica_1.read(), expected_winner);

    // Multi-Value Register preserves both concurrent values
    let mut mv_a = MvRegister::new("draft_a".to_string(), hlc_same, actor_a);
    let mv_b = MvRegister::new("draft_b".to_string(), hlc_same, actor_b);
    mv_a.merge_branches(&mv_b);
    assert!(mv_a.is_conflicted());
    assert_eq!(mv_a.read_branches().len(), 2);
}

// ============================================================================
// TEST 9: Map independent-field updates merge
// ============================================================================
#[test]
fn test_09_map_independent_field_updates_merge() {
    let actor_a = random_node_id();
    let actor_b = random_node_id();

    let mut map_a = CrdtMap::new();
    let mut map_b = CrdtMap::new();

    // Node A changes "name" -> "Alice"
    map_a.set(
        "name",
        serde_json::json!("Alice"),
        HybridLogicalClock::new(100, 0),
        actor_a,
    );

    // Node B changes "score" -> 42
    map_b.set(
        "score",
        serde_json::json!(42),
        HybridLogicalClock::new(100, 0),
        actor_b,
    );

    // Merge in both directions
    let mut merged_a = map_a.clone();
    merged_a.merge(&map_b).unwrap();

    let mut merged_b = map_b.clone();
    merged_b.merge(&map_a).unwrap();

    // Both fields must survive!
    assert_eq!(merged_a.get("name").unwrap(), &serde_json::json!("Alice"));
    assert_eq!(merged_a.get("score").unwrap(), &serde_json::json!(42));

    assert_eq!(merged_a, merged_b);
    assert_eq!(merged_a.state_hash(), merged_b.state_hash());
}

// ============================================================================
// TEST 10: Duplicate operation is idempotent
// ============================================================================
#[test]
fn test_10_duplicate_operation_is_idempotent() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_idempotent.db");
    let identity = Arc::new(NodeIdentity::generate());
    let store = DocumentStore::with_identity(&db_path, identity.clone()).unwrap();

    let mut counter = PnCounter::new();
    counter.increment(identity.node_id(), 10);
    let (doc, op) = store.create_crdt_sync(&counter).unwrap();

    // First application of op via remote API returns NoConflict (already applied upon creation)
    let res1 = store.apply_remote_operation_sync(&op).unwrap();
    assert_eq!(res1, ConflictClassification::NoConflict);

    // Read counter, verify value is 10
    let current_counter: PnCounter = store.get_crdt_sync(&doc.document_id).unwrap();
    assert_eq!(current_counter.read(), 10);

    // Second application (duplicate) has no second effect
    let res2 = store.apply_remote_operation_sync(&op).unwrap();
    assert_eq!(res2, ConflictClassification::NoConflict);

    let counter_after: PnCounter = store.get_crdt_sync(&doc.document_id).unwrap();
    assert_eq!(counter_after.read(), 10);
    assert_eq!(current_counter.state_hash(), counter_after.state_hash());
}

// ============================================================================
// TEST 11: Out-of-order operation is rejected/deferred
// ============================================================================
#[test]
fn test_11_out_of_order_operation_is_rejected_deferred() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_out_of_order.db");
    let identity_author = Arc::new(NodeIdentity::generate());
    let identity_receiver = Arc::new(NodeIdentity::generate());

    let author_store =
        DocumentStore::with_identity(dir.path().join("author.db"), identity_author.clone())
            .unwrap();
    let receiver_store = DocumentStore::with_identity(&db_path, identity_receiver).unwrap();

    // Author creates document (Op 1)
    let counter = PnCounter::new();
    let (doc, op1) = author_store.create_crdt_sync(&counter).unwrap();

    // Author mutates document (Op 2, depending on Op 1)
    let delta = PnCounterDelta::Increment {
        actor: identity_author.node_id(),
        amount: 25,
    };
    let (_, op2) = author_store
        .apply_crdt_delta_sync::<PnCounter>(&doc.document_id, &delta)
        .unwrap();
    assert!(op2.causal_dependencies.contains(&op1.op_id));

    // Receiver receives Op 2 BEFORE Op 1
    let err = receiver_store
        .apply_remote_operation_sync(&op2)
        .unwrap_err();
    match err {
        DataError::MissingDependency(missing_id) => {
            assert_eq!(missing_id, op1.op_id);
        }
        other => panic!("Expected MissingDependency error, got {:?}", other),
    }

    // Verify Op 2 is buffered in unapplied_operations
    let unapplied = receiver_store
        .list_unapplied_operations_sync(&doc.document_id)
        .unwrap();
    assert_eq!(unapplied.len(), 1);
    assert_eq!(unapplied[0].op_id, op2.op_id);

    // Receiver now receives Op 1
    let classification = receiver_store.apply_remote_operation_sync(&op1).unwrap();
    assert!(
        classification == ConflictClassification::CausalUpdate
            || classification == ConflictClassification::NoConflict
    );

    // Now retry unapplied operations: Op 2 should successfully apply!
    let retried = receiver_store
        .retry_unapplied_operations_sync(&doc.document_id)
        .unwrap();
    assert_eq!(retried, vec![op2.op_id]);

    // Materialized state reflects both operations: counter is 25!
    let final_counter: PnCounter = receiver_store.get_crdt_sync(&doc.document_id).unwrap();
    assert_eq!(final_counter.read(), 25);
}

// ============================================================================
// TEST 12: Delete/tombstone prevents stale resurrection
// ============================================================================
#[test]
fn test_12_delete_tombstone_prevents_stale_resurrection() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_tombstone.db");
    let identity = Arc::new(NodeIdentity::generate());
    let store = DocumentStore::with_identity(&db_path, identity.clone()).unwrap();

    let counter = PnCounter::new();
    let (doc, create_op) = store.create_crdt_sync(&counter).unwrap();

    // Node deletes the document
    let (deleted_doc, _delete_op) = store.delete_with_op_sync(&doc.document_id).unwrap();
    assert!(deleted_doc.deleted);

    // Verify it is recorded in tombstones
    let is_tombstoned = store.is_tombstoned_sync(&doc.document_id).unwrap();
    assert!(is_tombstoned);

    // Old replica attempts to replay the stale CREATE or UPDATE operation
    let stale_op = Operation {
        op_id: Operation::compute_id(
            &doc.document_id,
            &identity.node_id(),
            999,
            OperationType::CrdtDelta,
            &b"stale_state"[..],
            &HybridLogicalClock::new(50, 0),
            &[create_op.op_id],
            1,
        ),
        document_id: doc.document_id,
        actor_node_id: identity.node_id(),
        sequence: 999,
        op_type: OperationType::CrdtDelta,
        payload: b"stale_state".to_vec(),
        hlc: HybridLogicalClock::new(50, 0),
        causal_dependencies: vec![create_op.op_id],
        signature: [0u8; 64],
        created_at: Timestamp::now(),
        schema_version: 1,
        status: fieldnet_data::OpStatus::Durable,
    };

    // Stale mutation is rejected because document is tombstoned
    let res = store.apply_remote_operation_sync(&stale_op);
    assert!(matches!(res, Err(DataError::DocumentDeleted(_))));

    // Document remains deleted
    let fetched = store.get_active_sync(&doc.document_id);
    assert!(matches!(fetched, Err(DataError::DocumentDeleted(_))));
}

// ============================================================================
// TEST 13: Equivalent states produce identical hashes
// ============================================================================
#[test]
fn test_13_equivalent_states_produce_identical_hashes() {
    let actor_a = random_node_id();
    let actor_b = random_node_id();

    // Order 1: add A then B, increment A then B
    let mut map1 = CrdtMap::new();
    map1.set(
        "key1",
        serde_json::json!("val1"),
        HybridLogicalClock::new(100, 0),
        actor_a,
    );
    map1.set(
        "key2",
        serde_json::json!("val2"),
        HybridLogicalClock::new(100, 0),
        actor_b,
    );

    // Order 2: add B then A
    let mut map2 = CrdtMap::new();
    map2.set(
        "key2",
        serde_json::json!("val2"),
        HybridLogicalClock::new(100, 0),
        actor_b,
    );
    map2.set(
        "key1",
        serde_json::json!("val1"),
        HybridLogicalClock::new(100, 0),
        actor_a,
    );

    assert_eq!(map1.state_hash(), map2.state_hash());

    // Counter order independence
    let mut cnt1 = PnCounter::new();
    cnt1.increment(actor_a, 10);
    cnt1.increment(actor_b, 20);

    let mut cnt2 = PnCounter::new();
    cnt2.increment(actor_b, 20);
    cnt2.increment(actor_a, 10);

    assert_eq!(cnt1.state_hash(), cnt2.state_hash());
}

// ============================================================================
// TEST 14: Serialization/deserialization preserves logical state
// ============================================================================
#[test]
fn test_14_serialization_deserialization_preserves_logical_state() {
    let actor = random_node_id();
    let mut map = CrdtMap::new();
    map.set(
        "title",
        serde_json::json!("FIELDNET Node"),
        HybridLogicalClock::now(),
        actor,
    );
    map.set(
        "priority",
        serde_json::json!(1),
        HybridLogicalClock::now(),
        actor,
    );

    let serialized = map.serialize_state().unwrap();
    let deserialized = CrdtMap::deserialize_state(&serialized).unwrap();

    assert_eq!(map, deserialized);
    assert_eq!(map.state_hash(), deserialized.state_hash());
    assert_eq!(
        deserialized.get("title").unwrap(),
        &serde_json::json!("FIELDNET Node")
    );
}

// ============================================================================
// TEST 15: Restart preserves CRDT state
// ============================================================================
#[test]
fn test_15_restart_preserves_crdt_state() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_restart_crdt.db");
    let identity = Arc::new(NodeIdentity::generate());
    let doc_id: DocumentId;
    let expected_hash: [u8; 32];

    {
        let store = DocumentStore::with_identity(&db_path, identity.clone()).unwrap();
        let mut map = CrdtMap::new();
        map.set(
            "mission",
            serde_json::json!("recon"),
            HybridLogicalClock::now(),
            identity.node_id(),
        );
        let (doc, _) = store.create_crdt_sync(&map).unwrap();
        doc_id = doc.document_id;
        expected_hash = map.state_hash();
    }

    // Reopen from disk with fresh DocumentStore
    {
        let store2 = DocumentStore::with_identity(&db_path, identity).unwrap();
        let loaded_map: CrdtMap = store2.get_crdt_sync(&doc_id).unwrap();
        assert_eq!(
            loaded_map.get("mission").unwrap(),
            &serde_json::json!("recon")
        );
        assert_eq!(loaded_map.state_hash(), expected_hash);
    }
}

// ============================================================================
// TEST 16: Transaction failure leaves state unchanged
// ============================================================================
#[test]
fn test_16_transaction_failure_leaves_state_unchanged() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_tx_failure.db");
    let identity = Arc::new(NodeIdentity::generate());
    let store = DocumentStore::with_identity(&db_path, identity.clone()).unwrap();

    let mut counter = PnCounter::new();
    counter.increment(identity.node_id(), 50);
    let (doc, _) = store.create_crdt_sync(&counter).unwrap();

    // Attempt invalid transaction (wrong CRDT delta type)
    let invalid_delta = OrSetDelta::Add {
        item: "string_item".to_string(),
        tag: ElementTag::new(identity.node_id(), HybridLogicalClock::now(), 1),
    };

    let result = store.apply_crdt_delta_sync::<OrSet<String>>(&doc.document_id, &invalid_delta);
    assert!(result.is_err());

    // Counter remains exactly 50; no partial mutation occurred
    let current_counter: PnCounter = store.get_crdt_sync(&doc.document_id).unwrap();
    assert_eq!(current_counter.read(), 50);
}

// ============================================================================
// TEST 17: Invalid CRDT operation is rejected
// ============================================================================
#[test]
fn test_17_invalid_crdt_operation_is_rejected() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_invalid_op.db");
    let identity = Arc::new(NodeIdentity::generate());
    let store = DocumentStore::with_identity(&db_path, identity).unwrap();

    let op = Operation {
        op_id: OperationId::from_bytes([99u8; 32]), // Purposely wrong hash
        document_id: DocumentId::generate(),
        actor_node_id: random_node_id(),
        sequence: 1,
        op_type: OperationType::Create,
        payload: b"corrupt".to_vec(),
        hlc: HybridLogicalClock::now(),
        causal_dependencies: vec![],
        signature: [0u8; 64],
        created_at: Timestamp::now(),
        schema_version: 1,
        status: fieldnet_data::OpStatus::Durable,
    };

    let res = store.apply_remote_operation_sync(&op);
    assert!(matches!(res, Err(DataError::InvalidOperation(_))));
}

// ============================================================================
// TEST 18: Wrong CRDT type operation is rejected
// ============================================================================
#[test]
fn test_18_wrong_crdt_type_operation_is_rejected() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_wrong_type.db");
    let identity = Arc::new(NodeIdentity::generate());
    let store = DocumentStore::with_identity(&db_path, identity.clone()).unwrap();

    let counter = PnCounter::new();
    let (doc, _) = store.create_crdt_sync(&counter).unwrap();

    // Attempting to read counter as a CrdtMap must fail with TypeMismatch
    let res = store.get_crdt_sync::<CrdtMap>(&doc.document_id);
    assert!(matches!(res, Err(DataError::TypeMismatch { .. })));
}

// ============================================================================
// CONVERGENCE PROPERTY TESTING: Permutations of delivery order converge
// ============================================================================
#[test]
fn test_convergence_property_permutations_converge() {
    let actor_a = random_node_id();
    let actor_b = random_node_id();
    let actor_c = random_node_id();

    // Define 4 operations
    let ops = vec![
        PnCounterDelta::Increment {
            actor: actor_a,
            amount: 10,
        },
        PnCounterDelta::Increment {
            actor: actor_b,
            amount: 20,
        },
        PnCounterDelta::Decrement {
            actor: actor_c,
            amount: 5,
        },
        PnCounterDelta::Increment {
            actor: actor_a,
            amount: 15,
        },
    ];

    // Permutation 1: in order [0, 1, 2, 3]
    let mut state1 = PnCounter::new();
    for op in &ops {
        state1.apply_delta(op).unwrap();
    }

    // Permutation 2: in reverse order [3, 2, 1, 0]
    let mut state2 = PnCounter::new();
    for op in ops.iter().rev() {
        state2.apply_delta(op).unwrap();
    }

    // Permutation 3: mixed order [2, 0, 3, 1]
    let mut state3 = PnCounter::new();
    state3.apply_delta(&ops[2]).unwrap();
    state3.apply_delta(&ops[0]).unwrap();
    state3.apply_delta(&ops[3]).unwrap();
    state3.apply_delta(&ops[1]).unwrap();

    // Invariant: all permutations converge to identical value and state hash!
    assert_eq!(state1.read(), 40);
    assert_eq!(state2.read(), 40);
    assert_eq!(state3.read(), 40);

    assert_eq!(state1.state_hash(), state2.state_hash());
    assert_eq!(state2.state_hash(), state3.state_hash());
}

// ============================================================================
// THREE-NODE LOCAL SIMULATION
// ============================================================================
#[test]
fn test_three_node_local_simulation() {
    let dir = tempdir().unwrap();

    let identity_a = Arc::new(NodeIdentity::generate());
    let identity_b = Arc::new(NodeIdentity::generate());
    let identity_c = Arc::new(NodeIdentity::generate());

    let store_a =
        DocumentStore::with_identity(dir.path().join("node_a.db"), identity_a.clone()).unwrap();
    let store_b =
        DocumentStore::with_identity(dir.path().join("node_b.db"), identity_b.clone()).unwrap();
    let store_c =
        DocumentStore::with_identity(dir.path().join("node_c.db"), identity_c.clone()).unwrap();

    // 1. Node A initializes a shared counter document
    let initial_counter = PnCounter::new();
    let (doc_a, op_create) = store_a.create_crdt_sync(&initial_counter).unwrap();

    // 2. Node B and C receive create operation
    store_b.apply_remote_operation_sync(&op_create).unwrap();
    store_c.apply_remote_operation_sync(&op_create).unwrap();

    // 3. Node A, B, C mutate concurrently without central authority
    // Node A: +10
    let delta_a = PnCounterDelta::Increment {
        actor: identity_a.node_id(),
        amount: 10,
    };
    let (_, op_a) = store_a
        .apply_crdt_delta_sync::<PnCounter>(&doc_a.document_id, &delta_a)
        .unwrap();

    // Node B: +25
    let delta_b = PnCounterDelta::Increment {
        actor: identity_b.node_id(),
        amount: 25,
    };
    let (_, op_b) = store_b
        .apply_crdt_delta_sync::<PnCounter>(&doc_a.document_id, &delta_b)
        .unwrap();

    // Node C: -5
    let delta_c = PnCounterDelta::Decrement {
        actor: identity_c.node_id(),
        amount: 5,
    };
    let (_, op_c) = store_c
        .apply_crdt_delta_sync::<PnCounter>(&doc_a.document_id, &delta_c)
        .unwrap();

    // 4. Mesh exchange: operations exchanged between nodes
    // Node A receives B and C
    store_a.apply_remote_operation_sync(&op_b).unwrap();
    store_a.apply_remote_operation_sync(&op_c).unwrap();

    // Node B receives A and C
    store_b.apply_remote_operation_sync(&op_a).unwrap();
    store_b.apply_remote_operation_sync(&op_c).unwrap();

    // Node C receives A and B
    store_c.apply_remote_operation_sync(&op_a).unwrap();
    store_c.apply_remote_operation_sync(&op_b).unwrap();

    // 5. Verify all three replicas converge to exact same value and state hash!
    let counter_a: PnCounter = store_a.get_crdt_sync(&doc_a.document_id).unwrap();
    let counter_b: PnCounter = store_b.get_crdt_sync(&doc_a.document_id).unwrap();
    let counter_c: PnCounter = store_c.get_crdt_sync(&doc_a.document_id).unwrap();

    // Expected value: 0 + 10 + 25 - 5 = 30
    assert_eq!(counter_a.read(), 30);
    assert_eq!(counter_b.read(), 30);
    assert_eq!(counter_c.read(), 30);

    assert_eq!(counter_a.state_hash(), counter_b.state_hash());
    assert_eq!(counter_b.state_hash(), counter_c.state_hash());
}

// ============================================================================
// PERFORMANCE BASELINE
// ============================================================================
#[test]
fn test_crdt_performance_baseline() {
    let actor = random_node_id();

    // Large Set: 1,000 additions
    let start_set = Instant::now();
    let mut set: OrSet<u64> = OrSet::new();
    let hlc = HybridLogicalClock::now();
    for i in 0..1000 {
        let tag = ElementTag::new(actor, hlc, i);
        set.add(i, tag);
    }
    let set_duration = start_set.elapsed();
    assert_eq!(set.len(), 1000);

    // Large Map: 1,000 fields
    let start_map = Instant::now();
    let mut map = CrdtMap::new();
    for i in 0..1000 {
        map.set(format!("key_{}", i), serde_json::json!(i), hlc, actor);
    }
    let map_duration = start_map.elapsed();
    assert_eq!(map.to_map().len(), 1000);

    // State Hashing
    let start_hash = Instant::now();
    let _hash = map.state_hash();
    let hash_duration = start_hash.elapsed();

    println!(
        "CRDT Perf: 1k OrSet adds in {:?}, 1k CrdtMap sets in {:?}, 1k Map hash in {:?}",
        set_duration, map_duration, hash_duration
    );
}
