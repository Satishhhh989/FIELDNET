use fieldnet_core::{NodeId, Timestamp};
use fieldnet_data::causality::{HybridLogicalClock, StateVector};
use fieldnet_data::crdt::{
    Crdt, ElementTag, LwwRegister, OrSet, OrSetDelta, PnCounter, PnCounterDelta,
};
use fieldnet_data::snapshot::DocumentSnapshot;
use fieldnet_data::{DataError, DocumentId, DocumentStore, Operation, OperationType};
use fieldnet_identity::NodeIdentity;
use std::time::Instant;
use tempfile::tempdir;

fn random_node_id() -> NodeId {
    NodeIdentity::generate().node_id()
}

// ============================================================================
// TEST 1: Create snapshot
// ============================================================================
#[test]
fn test_01_create_snapshot() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_snap_01.db");
    let node_id = random_node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    let mut counter = PnCounter::new();
    counter.increment(node_id, 10);
    let (doc, _op1) = store.create_crdt_sync(&counter).unwrap();

    // Perform two more increments
    let delta1 = PnCounterDelta::Increment {
        actor: node_id,
        amount: 15,
    };
    store
        .apply_crdt_delta_sync::<PnCounter>(&doc.document_id, &delta1)
        .unwrap();
    let delta2 = PnCounterDelta::Increment {
        actor: node_id,
        amount: 25,
    };
    store
        .apply_crdt_delta_sync::<PnCounter>(&doc.document_id, &delta2)
        .unwrap();

    // Create snapshot
    let snapshot = store.create_snapshot_sync(&doc.document_id).unwrap();

    assert_eq!(snapshot.document_id, doc.document_id);
    assert_eq!(snapshot.snapshot_version, 1);
    assert_eq!(snapshot.compacted_up_to_seq, 3);
    assert_eq!(snapshot.crdt_type, "pn_counter");
    assert!(snapshot.state_vector.get(&node_id) > 0);
    assert_eq!(snapshot.state_vector.get(&node_id), 3);
    assert!(snapshot.validate_integrity().is_ok());

    // Verify stored in DB
    let retrieved = store
        .get_latest_snapshot_sync(&doc.document_id)
        .unwrap()
        .unwrap();
    assert_eq!(retrieved.snapshot_id, snapshot.snapshot_id);
    assert_eq!(retrieved.state_hash, snapshot.state_hash);
}

// ============================================================================
// TEST 2: Snapshot survives restart
// ============================================================================
#[test]
fn test_02_snapshot_survives_restart() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_snap_02.db");
    let node_id = random_node_id();

    let doc_id;
    let expected_hash;
    let expected_id;
    {
        let store = DocumentStore::open(&db_path, node_id).unwrap();
        let mut counter = PnCounter::new();
        counter.increment(node_id, 42);
        let (doc, _) = store.create_crdt_sync(&counter).unwrap();
        doc_id = doc.document_id;

        let snapshot = store.create_snapshot_sync(&doc_id).unwrap();
        expected_hash = snapshot.state_hash;
        expected_id = snapshot.snapshot_id;
    }

    // Reopen database
    {
        let reopened = DocumentStore::open(&db_path, node_id).unwrap();
        let snapshot = reopened.get_latest_snapshot_sync(&doc_id).unwrap().unwrap();

        assert_eq!(snapshot.snapshot_id, expected_id);
        assert_eq!(snapshot.state_hash, expected_hash);
        assert_eq!(snapshot.snapshot_version, 1);
        assert!(snapshot.validate_integrity().is_ok());

        let counter: PnCounter = PnCounter::deserialize_state(&snapshot.state).unwrap();
        assert_eq!(counter.read(), 42);
    }
}

// ============================================================================
// TEST 3: Restore snapshot
// ============================================================================
#[test]
fn test_03_restore_snapshot() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_snap_03.db");
    let node_id = random_node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    let mut counter = PnCounter::new();
    counter.increment(node_id, 100);
    let (doc, _) = store.create_crdt_sync(&counter).unwrap();
    let snapshot = store.create_snapshot_sync(&doc.document_id).unwrap();

    // Restore snapshot directly
    let restored = store.restore_snapshot_sync(&snapshot.snapshot_id).unwrap();
    assert_eq!(restored.document_id, doc.document_id);
    assert_eq!(restored.version, 1);
    assert!(!restored.deleted);

    let counter_restored: PnCounter = PnCounter::deserialize_state(&restored.state).unwrap();
    assert_eq!(counter_restored.read(), 100);

    // Verify DocumentStore::get returns the restored state
    let active_doc = store.get_sync(&doc.document_id).unwrap().unwrap();
    assert_eq!(active_doc.state, restored.state);
}

// ============================================================================
// TEST 4: Snapshot + later operations reconstruct current state
// ============================================================================
#[test]
fn test_04_snapshot_plus_later_operations_reconstruct_current_state() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_snap_04.db");
    let node_id = random_node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    // 1. Initial creation (seq 1): counter = 10
    let mut counter = PnCounter::new();
    counter.increment(node_id, 10);
    let (doc, _) = store.create_crdt_sync(&counter).unwrap();

    // 2. Increments (seq 2, 3): counter = 10 + 20 + 30 = 60
    let d1 = PnCounterDelta::Increment {
        actor: node_id,
        amount: 20,
    };
    store
        .apply_crdt_delta_sync::<PnCounter>(&doc.document_id, &d1)
        .unwrap();
    let d2 = PnCounterDelta::Increment {
        actor: node_id,
        amount: 30,
    };
    store
        .apply_crdt_delta_sync::<PnCounter>(&doc.document_id, &d2)
        .unwrap();

    // 3. Take snapshot at seq 3
    let snapshot = store.create_snapshot_sync(&doc.document_id).unwrap();
    assert_eq!(snapshot.compacted_up_to_seq, 3);

    // 4. Newer operations (seq 4, 5): counter = 60 + 40 - 15 = 85
    let d3 = PnCounterDelta::Increment {
        actor: node_id,
        amount: 40,
    };
    store
        .apply_crdt_delta_sync::<PnCounter>(&doc.document_id, &d3)
        .unwrap();
    let d4 = PnCounterDelta::Decrement {
        actor: node_id,
        amount: 15,
    };
    store
        .apply_crdt_delta_sync::<PnCounter>(&doc.document_id, &d4)
        .unwrap();

    let full_history_doc = store.get_sync(&doc.document_id).unwrap().unwrap();
    let full_counter: PnCounter = PnCounter::deserialize_state(&full_history_doc.state).unwrap();
    assert_eq!(full_counter.read(), 85);

    // 5. Restore snapshot (which should replay seq 4 and 5)
    let restored = store.restore_snapshot_sync(&snapshot.snapshot_id).unwrap();
    let restored_counter: PnCounter = PnCounter::deserialize_state(&restored.state).unwrap();

    // Invariant: Snapshot + newer operations == full history state!
    assert_eq!(restored_counter.read(), full_counter.read());
    assert_eq!(restored.state, full_history_doc.state);
}

// ============================================================================
// TEST 5: Snapshot state hash is deterministic
// ============================================================================
#[test]
fn test_05_snapshot_state_hash_is_deterministic() {
    let doc_id = DocumentId::generate();
    let mut sv = StateVector::new();
    let node_id = random_node_id();
    sv.set(node_id, 10);
    let hlc = HybridLogicalClock::new(1000, 1);
    let payload = vec![1, 2, 3, 4, 5, 6, 7, 8];

    let hash1 =
        DocumentSnapshot::compute_state_hash(&doc_id, 1, 10, &sv, &hlc, "pn_counter", &payload, 1);

    let hash2 =
        DocumentSnapshot::compute_state_hash(&doc_id, 1, 10, &sv, &hlc, "pn_counter", &payload, 1);

    assert_eq!(hash1, hash2, "Identical inputs must yield identical hash");

    // Altering 1 byte in payload changes hash
    let mut altered_payload = payload.clone();
    altered_payload[0] ^= 0xFF;
    let hash_altered_payload = DocumentSnapshot::compute_state_hash(
        &doc_id,
        1,
        10,
        &sv,
        &hlc,
        "pn_counter",
        &altered_payload,
        1,
    );
    assert_ne!(hash1, hash_altered_payload);

    // Altering sequence changes hash
    let hash_altered_seq =
        DocumentSnapshot::compute_state_hash(&doc_id, 1, 11, &sv, &hlc, "pn_counter", &payload, 1);
    assert_ne!(hash1, hash_altered_seq);

    // Altering HLC changes hash
    let hash_altered_hlc = DocumentSnapshot::compute_state_hash(
        &doc_id,
        1,
        10,
        &sv,
        &HybridLogicalClock::new(1001, 1),
        "pn_counter",
        &payload,
        1,
    );
    assert_ne!(hash1, hash_altered_hlc);
}

// ============================================================================
// TEST 6: Corrupted snapshot is rejected
// ============================================================================
#[test]
fn test_06_corrupted_snapshot_is_rejected() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_snap_06.db");
    let node_id = random_node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    let mut counter = PnCounter::new();
    counter.increment(node_id, 50);
    let (doc, _) = store.create_crdt_sync(&counter).unwrap();

    let snapshot = store.create_snapshot_sync(&doc.document_id).unwrap();

    // Corrupt snapshot payload in storage
    store
        .corrupt_snapshot_payload_for_testing(&snapshot.snapshot_id)
        .unwrap();

    // get_latest_snapshot must reject corrupted snapshot
    let err = store
        .get_latest_snapshot_sync(&doc.document_id)
        .unwrap_err();
    match err {
        DataError::CorruptedSnapshot {
            snapshot_id,
            reason,
        } => {
            assert_eq!(snapshot_id, snapshot.snapshot_id.to_string());
            assert!(reason.contains("hash mismatch"));
        }
        other => panic!("Expected CorruptedSnapshot, got: {:?}", other),
    }

    // restore_snapshot must also reject corrupted snapshot
    let restore_err = store
        .restore_snapshot_sync(&snapshot.snapshot_id)
        .unwrap_err();
    match restore_err {
        DataError::CorruptedSnapshot { snapshot_id, .. } => {
            assert_eq!(snapshot_id, snapshot.snapshot_id.to_string());
        }
        other => panic!("Expected CorruptedSnapshot, got: {:?}", other),
    }
}

// ============================================================================
// TEST 7: Latest snapshot corruption falls back safely where possible
// ============================================================================
#[test]
fn test_07_latest_snapshot_corruption_falls_back_safely() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_snap_07.db");
    let node_id = random_node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    // 1. Initial: counter = 10 (seq 1)
    let mut counter = PnCounter::new();
    counter.increment(node_id, 10);
    let (doc, _) = store.create_crdt_sync(&counter).unwrap();

    // Snapshot gen 1 (retained)
    let snap_gen1 = store.create_snapshot_sync(&doc.document_id).unwrap();
    assert_eq!(snap_gen1.snapshot_version, 1);

    // 2. Increment to 30 (seq 2)
    let d1 = PnCounterDelta::Increment {
        actor: node_id,
        amount: 20,
    };
    store
        .apply_crdt_delta_sync::<PnCounter>(&doc.document_id, &d1)
        .unwrap();

    // Snapshot gen 2
    let snap_gen2 = store.create_snapshot_sync(&doc.document_id).unwrap();
    assert_eq!(snap_gen2.snapshot_version, 2);

    // 3. Increment to 60 (seq 3)
    let d2 = PnCounterDelta::Increment {
        actor: node_id,
        amount: 30,
    };
    store
        .apply_crdt_delta_sync::<PnCounter>(&doc.document_id, &d2)
        .unwrap();

    // 4. Corrupt newest snapshot gen 2
    store
        .corrupt_snapshot_payload_for_testing(&snap_gen2.snapshot_id)
        .unwrap();

    // 5. Call recover_document_sync: It should detect gen 2 corruption,
    // fall back to gen 1, and replay seq 2 and seq 3 forward!
    let recovered_doc = store.recover_document_sync(&doc.document_id).unwrap();
    let recovered_counter: PnCounter = PnCounter::deserialize_state(&recovered_doc.state).unwrap();
    assert_eq!(recovered_counter.read(), 60);
}

// ============================================================================
// TEST 8: Compaction removes only eligible operations
// ============================================================================
#[test]
fn test_08_compaction_removes_only_eligible_operations() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_snap_08.db");
    let node_id = random_node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    let mut counter = PnCounter::new();
    counter.increment(node_id, 1);
    let (doc, _) = store.create_crdt_sync(&counter).unwrap();

    for i in 2..=5 {
        let delta = PnCounterDelta::Increment {
            actor: node_id,
            amount: i,
        };
        store
            .apply_crdt_delta_sync::<PnCounter>(&doc.document_id, &delta)
            .unwrap();
    }

    // Verify 5 operations exist
    let ops_before = store
        .list_operations_for_document_sync(&doc.document_id, None, None)
        .unwrap();
    assert_eq!(ops_before.len(), 5);

    // Snapshot covers seq 1..5
    let snap = store.create_snapshot_sync(&doc.document_id).unwrap();
    assert_eq!(snap.compacted_up_to_seq, 5);

    // Compact
    let record = store.compact_sync(&doc.document_id).unwrap();
    assert_eq!(record.ops_removed, 5);
    assert_eq!(record.compacted_up_to_seq, 5);
    assert!(record.bytes_reclaimed > 0);

    // Operations 1..5 must be removed
    let ops_after = store
        .list_operations_for_document_sync(&doc.document_id, None, None)
        .unwrap();
    assert_eq!(ops_after.len(), 0);

    // Document state remains fully intact
    let active_doc = store.get_sync(&doc.document_id).unwrap().unwrap();
    let cur_counter: PnCounter = PnCounter::deserialize_state(&active_doc.state).unwrap();
    assert_eq!(cur_counter.read(), 1 + 2 + 3 + 4 + 5);
}

// ============================================================================
// TEST 9: Unstable operations remain
// ============================================================================
#[test]
fn test_09_unstable_operations_remain() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_snap_09.db");
    let node_id = random_node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    let mut counter = PnCounter::new();
    counter.increment(node_id, 10);
    let (doc, _) = store.create_crdt_sync(&counter).unwrap();

    let d1 = PnCounterDelta::Increment {
        actor: node_id,
        amount: 10,
    };
    store
        .apply_crdt_delta_sync::<PnCounter>(&doc.document_id, &d1)
        .unwrap();
    let d2 = PnCounterDelta::Increment {
        actor: node_id,
        amount: 10,
    };
    store
        .apply_crdt_delta_sync::<PnCounter>(&doc.document_id, &d2)
        .unwrap();

    // Snapshot covers seq 1..3
    let snap = store.create_snapshot_sync(&doc.document_id).unwrap();
    assert_eq!(snap.compacted_up_to_seq, 3);

    // Add operations 4 and 5 (unstable / not covered by snapshot)
    let d3 = PnCounterDelta::Increment {
        actor: node_id,
        amount: 10,
    };
    store
        .apply_crdt_delta_sync::<PnCounter>(&doc.document_id, &d3)
        .unwrap();
    let d4 = PnCounterDelta::Increment {
        actor: node_id,
        amount: 10,
    };
    store
        .apply_crdt_delta_sync::<PnCounter>(&doc.document_id, &d4)
        .unwrap();

    // Compact up to snapshot sequence 3
    let record = store.compact_sync(&doc.document_id).unwrap();
    assert_eq!(record.ops_removed, 3);
    assert_eq!(record.compacted_up_to_seq, 3);

    // Operations 4 and 5 must remain intact!
    let ops_remaining = store
        .list_operations_for_document_sync(&doc.document_id, None, None)
        .unwrap();
    assert_eq!(ops_remaining.len(), 2);
    assert_eq!(ops_remaining[0].sequence, 4);
    assert_eq!(ops_remaining[1].sequence, 5);
}

// ============================================================================
// TEST 10: Tombstones remain when required (no resurrection)
// ============================================================================
#[test]
fn test_10_tombstones_remain_when_required() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_snap_10.db");
    let node_id = random_node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    let (doc, _) = store
        .create_with_op_sync("test_note", b"Tactical Report Alpha".to_vec())
        .unwrap();
    let doc_id = doc.document_id;

    // Delete document (creates delete operation & tombstone)
    store.delete_with_op_sync(&doc_id).unwrap();

    // Snapshot records deleted state
    let snap = store.create_snapshot_sync(&doc_id).unwrap();
    assert_eq!(snap.compacted_up_to_seq, 2);

    // Compact operation log
    store.compact_sync(&doc_id).unwrap();

    // Document is still reported as deleted
    let doc_after = store.get_sync(&doc_id).unwrap().unwrap();
    assert!(doc_after.deleted);
    assert!(store.get_active_sync(&doc_id).is_err());

    // A stale operation from an offline replica attempting to update must NOT resurrect it
    let payload = b"Stale payload resurrection attempt".to_vec();
    let actor_node_id = random_node_id();
    let hlc = HybridLogicalClock::new(100, 1);
    let op_type = OperationType::Update;
    let sequence = 1;
    let schema_version = 1;
    let causal_dependencies = vec![];
    let op_id = Operation::compute_id(
        &doc_id,
        &actor_node_id,
        sequence,
        op_type,
        &payload,
        &hlc,
        &causal_dependencies,
        schema_version,
    );

    let stale_op = Operation {
        op_id,
        document_id: doc_id,
        actor_node_id,
        sequence,
        op_type,
        payload,
        hlc,
        causal_dependencies,
        signature: [0u8; 64],
        created_at: Timestamp::now(),
        schema_version,
        status: fieldnet_data::OpStatus::Durable,
    };

    let err = store.apply_remote_operation_sync(&stale_op).unwrap_err();
    assert!(matches!(err, DataError::DocumentDeleted(_)));

    // Document must remain deleted!
    let final_check = store.get_sync(&doc_id).unwrap().unwrap();
    assert!(
        final_check.deleted,
        "Document must not resurrect from stale operation"
    );
}

// ============================================================================
// TEST 11: Offline peer scenario prevents unsafe compaction
// ============================================================================
#[test]
fn test_11_offline_peer_scenario_prevents_unsafe_compaction() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_snap_11.db");
    let node_id = random_node_id();
    let peer_b = random_node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    let mut counter = PnCounter::new();
    counter.increment(node_id, 1);
    let (doc, _) = store.create_crdt_sync(&counter).unwrap();

    for i in 2..=6 {
        let delta = PnCounterDelta::Increment {
            actor: node_id,
            amount: i,
        };
        store
            .apply_crdt_delta_sync::<PnCounter>(&doc.document_id, &delta)
            .unwrap();
    }

    // Node A creates snapshot covering seq 1..6
    store.create_snapshot_sync(&doc.document_id).unwrap();

    // Record that Peer B has only synchronized up to sequence 3
    store
        .record_peer_sync_watermark_sync(
            &peer_b,
            &doc.document_id,
            3,
            HybridLogicalClock::new(500, 1),
            "CONVERGED",
        )
        .unwrap();

    // Check compaction safety
    let report = store
        .check_compaction_safety_sync(&doc.document_id)
        .unwrap();
    // Safety check clamps max_safe_sequence to min peer synced sequence (3)
    assert_eq!(report.max_safe_sequence, 3);
    assert_eq!(report.eligible_operations_count, 3);
    assert!(report.is_safe);

    // Compact: operations 1..3 are pruned, operations 4..6 are RETAINED for Peer B!
    let record = store.compact_sync(&doc.document_id).unwrap();
    assert_eq!(record.ops_removed, 3);
    assert_eq!(record.compacted_up_to_seq, 3);

    let remaining_ops = store
        .list_operations_for_document_sync(&doc.document_id, None, None)
        .unwrap();
    assert_eq!(remaining_ops.len(), 3);
    assert_eq!(remaining_ops[0].sequence, 4);
    assert_eq!(remaining_ops[1].sequence, 5);
    assert_eq!(remaining_ops[2].sequence, 6);
}

// ============================================================================
// TEST 12: Compaction does not change logical state
// ============================================================================
#[test]
fn test_12_compaction_does_not_change_logical_state() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_snap_12.db");
    let node_id = random_node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    let tag1 = ElementTag::new(node_id, HybridLogicalClock::now(), 1);
    let tag2 = ElementTag::new(node_id, HybridLogicalClock::now(), 2);
    let tag3 = ElementTag::new(node_id, HybridLogicalClock::now(), 3);

    let mut set: OrSet<String> = OrSet::new();
    set.add("alpha".to_string(), tag1);
    set.add("bravo".to_string(), tag2);
    let (doc, _) = store.create_crdt_sync(&set).unwrap();

    let d1 = OrSetDelta::Add {
        item: "charlie".to_string(),
        tag: tag3,
    };
    store
        .apply_crdt_delta_sync::<OrSet<String>>(&doc.document_id, &d1)
        .unwrap();
    let d2 = OrSetDelta::Remove {
        item: "alpha".to_string(),
        observed_tags: vec![tag1],
    };
    store
        .apply_crdt_delta_sync::<OrSet<String>>(&doc.document_id, &d2)
        .unwrap();

    let doc_before = store.get_sync(&doc.document_id).unwrap().unwrap();
    let set_before: OrSet<String> = OrSet::deserialize_state(&doc_before.state).unwrap();
    assert_eq!(
        set_before.read(),
        vec!["bravo".to_string(), "charlie".to_string()]
    );

    // Snapshot & compact
    store.create_snapshot_sync(&doc.document_id).unwrap();
    store.compact_sync(&doc.document_id).unwrap();

    let doc_after = store.get_sync(&doc.document_id).unwrap().unwrap();
    let set_after: OrSet<String> = OrSet::deserialize_state(&doc_after.state).unwrap();

    // Logical state invariant: 100% identical!
    assert_eq!(doc_before.state, doc_after.state);
    assert_eq!(set_before.read(), set_after.read());
    assert_eq!(doc_before.version, doc_after.version);
}

// ============================================================================
// TEST 13: Restart after compaction
// ============================================================================
#[test]
fn test_13_restart_after_compaction() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_snap_13.db");
    let node_id = random_node_id();

    let doc_id;
    {
        let store = DocumentStore::open(&db_path, node_id).unwrap();
        let mut counter = PnCounter::new();
        counter.increment(node_id, 77);
        let (doc, _) = store.create_crdt_sync(&counter).unwrap();
        doc_id = doc.document_id;

        store.create_snapshot_sync(&doc_id).unwrap();
        store.compact_sync(&doc_id).unwrap();
    }

    // Reopen database after compaction
    {
        let reopened = DocumentStore::open(&db_path, node_id).unwrap();
        let doc = reopened.get_sync(&doc_id).unwrap().unwrap();
        let counter: PnCounter = PnCounter::deserialize_state(&doc.state).unwrap();
        assert_eq!(counter.read(), 77);

        // Advance further after restart
        let delta = PnCounterDelta::Increment {
            actor: node_id,
            amount: 23,
        };
        reopened
            .apply_crdt_delta_sync::<PnCounter>(&doc_id, &delta)
            .unwrap();

        let updated = reopened.get_sync(&doc_id).unwrap().unwrap();
        let updated_counter: PnCounter = PnCounter::deserialize_state(&updated.state).unwrap();
        assert_eq!(updated_counter.read(), 100);
    }
}

// ============================================================================
// TEST 14: Crash / Rollback during snapshot creation
// ============================================================================
#[test]
fn test_14_crash_during_snapshot_creation() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_snap_14.db");
    let node_id = random_node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    let (doc, _) = store
        .create_with_op_sync("test_note", b"Pre-crash state".to_vec())
        .unwrap();

    // Verify rolling back a transaction leaves snapshot uncommitted
    let res: Result<(), DataError> = store.database().with_transaction(|tx| {
        tx.execute(
            "INSERT INTO document_snapshots (
                snapshot_id, document_id, snapshot_version, compacted_up_to_seq,
                state_vector_bytes, hlc_physical, hlc_logical, crdt_type,
                snapshot_payload, state_hash, schema_version, created_at
            ) VALUES (?1, ?2, 1, 1, X'00', 100, 0, 'test_note', X'DEAD', X'BEEF', 1, 100)",
            rusqlite::params![[1u8; 32].as_slice(), doc.document_id.as_bytes().as_slice()],
        )?;
        // Simulate crash by returning Error
        Err(DataError::StorageFailure("Simulated crash".into()))
    });
    assert!(res.is_err());

    // Verify no snapshot was committed
    let snaps = store.list_snapshots_sync(&doc.document_id).unwrap();
    assert_eq!(snaps.len(), 0);

    // Document state remains completely intact
    let loaded = store.get_sync(&doc.document_id).unwrap().unwrap();
    assert_eq!(loaded.state, b"Pre-crash state");
}

// ============================================================================
// TEST 15: Crash / Rollback during compaction
// ============================================================================
#[test]
fn test_15_crash_during_compaction() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_snap_15.db");
    let node_id = random_node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    let (doc, _) = store
        .create_with_op_sync("test_note", b"Compaction crash test".to_vec())
        .unwrap();
    store.create_snapshot_sync(&doc.document_id).unwrap();

    // Attempt simulated abort during compaction transaction
    let res: Result<(), DataError> = store.database().with_transaction(|tx| {
        tx.execute(
            "DELETE FROM operations WHERE document_id = ?1",
            rusqlite::params![doc.document_id.as_bytes().as_slice()],
        )?;
        Err(DataError::StorageFailure(
            "Simulated compaction crash".into(),
        ))
    });
    assert!(res.is_err());

    // Operations remain intact because transaction was safely rolled back
    let ops = store
        .list_operations_for_document_sync(&doc.document_id, None, None)
        .unwrap();
    assert_eq!(ops.len(), 1);
}

// ============================================================================
// TEST 16: Multiple documents compact independently
// ============================================================================
#[test]
fn test_16_multiple_documents_compact_independently() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_snap_16.db");
    let node_id = random_node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    // Document A: 3 ops
    let mut counter_a = PnCounter::new();
    counter_a.increment(node_id, 10);
    let (doc_a, _) = store.create_crdt_sync(&counter_a).unwrap();
    let da1 = PnCounterDelta::Increment {
        actor: node_id,
        amount: 10,
    };
    store
        .apply_crdt_delta_sync::<PnCounter>(&doc_a.document_id, &da1)
        .unwrap();
    let da2 = PnCounterDelta::Increment {
        actor: node_id,
        amount: 10,
    };
    store
        .apply_crdt_delta_sync::<PnCounter>(&doc_a.document_id, &da2)
        .unwrap();

    // Document B: 3 ops
    let mut counter_b = PnCounter::new();
    counter_b.increment(node_id, 50);
    let (doc_b, _) = store.create_crdt_sync(&counter_b).unwrap();
    let db1 = PnCounterDelta::Increment {
        actor: node_id,
        amount: 50,
    };
    store
        .apply_crdt_delta_sync::<PnCounter>(&doc_b.document_id, &db1)
        .unwrap();
    let db2 = PnCounterDelta::Increment {
        actor: node_id,
        amount: 50,
    };
    store
        .apply_crdt_delta_sync::<PnCounter>(&doc_b.document_id, &db2)
        .unwrap();

    // Snapshot & compact ONLY Document A
    store.create_snapshot_sync(&doc_a.document_id).unwrap();
    store.compact_sync(&doc_a.document_id).unwrap();

    // Document A ops are pruned
    let ops_a = store
        .list_operations_for_document_sync(&doc_a.document_id, None, None)
        .unwrap();
    assert_eq!(ops_a.len(), 0);

    // Document B ops must be COMPLETELY UNTOUCHED!
    let ops_b = store
        .list_operations_for_document_sync(&doc_b.document_id, None, None)
        .unwrap();
    assert_eq!(ops_b.len(), 3);
}

// ============================================================================
// TEST 17: Large operation history recovery from snapshot
// ============================================================================
#[test]
fn test_17_large_operation_history_recovery() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_snap_17.db");
    let node_id = random_node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    let mut counter = PnCounter::new();
    counter.increment(node_id, 1);
    let (doc, _) = store.create_crdt_sync(&counter).unwrap();

    // 100 increments
    for _ in 0..99 {
        let delta = PnCounterDelta::Increment {
            actor: node_id,
            amount: 1,
        };
        store
            .apply_crdt_delta_sync::<PnCounter>(&doc.document_id, &delta)
            .unwrap();
    }

    // Snapshot at seq 100
    store.create_snapshot_sync(&doc.document_id).unwrap();
    // Compact operations 1..100
    store.compact_sync(&doc.document_id).unwrap();

    // 5 more increments (seq 101..105)
    for _ in 0..5 {
        let delta = PnCounterDelta::Increment {
            actor: node_id,
            amount: 1,
        };
        store
            .apply_crdt_delta_sync::<PnCounter>(&doc.document_id, &delta)
            .unwrap();
    }

    // Recover document: loads snapshot (covering seq 100) and replays only 5 ops
    let recovered = store.recover_document_sync(&doc.document_id).unwrap();
    let cur_counter: PnCounter = PnCounter::deserialize_state(&recovered.state).unwrap();
    assert_eq!(cur_counter.read(), 105);
}

// ============================================================================
// TEST 18: Three-node causal stability behavior
// ============================================================================
#[test]
fn test_18_three_node_causal_stability_behavior() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_snap_18.db");
    let node_a = random_node_id();
    let peer_b = random_node_id();
    let peer_c = random_node_id();
    let store = DocumentStore::open(&db_path, node_a).unwrap();

    // Node A creates doc with 10 operations
    let mut counter = PnCounter::new();
    counter.increment(node_a, 1);
    let (doc, _) = store.create_crdt_sync(&counter).unwrap();

    for i in 2..=10 {
        let delta = PnCounterDelta::Increment {
            actor: node_a,
            amount: i,
        };
        store
            .apply_crdt_delta_sync::<PnCounter>(&doc.document_id, &delta)
            .unwrap();
    }

    // Snapshot at seq 10
    store.create_snapshot_sync(&doc.document_id).unwrap();

    // Peer B has synchronized up to seq 8
    store
        .record_peer_sync_watermark_sync(
            &peer_b,
            &doc.document_id,
            8,
            HybridLogicalClock::new(800, 1),
            "CONVERGED",
        )
        .unwrap();

    // Peer C has synchronized up to seq 5
    store
        .record_peer_sync_watermark_sync(
            &peer_c,
            &doc.document_id,
            5,
            HybridLogicalClock::new(500, 1),
            "CONVERGED",
        )
        .unwrap();

    // Causal stability boundary: min(8, 5) = 5
    let report = store
        .check_compaction_safety_sync(&doc.document_id)
        .unwrap();
    assert_eq!(report.max_safe_sequence, 5);
    assert_eq!(report.eligible_operations_count, 5);
    assert!(report.is_safe);

    // Compact
    let record = store.compact_sync(&doc.document_id).unwrap();
    assert_eq!(record.ops_removed, 5);
    assert_eq!(record.compacted_up_to_seq, 5);

    // Operations 1..5 removed, but operations 6..10 remain available for Peer C and Peer B
    let remaining_ops = store
        .list_operations_for_document_sync(&doc.document_id, None, None)
        .unwrap();
    assert_eq!(remaining_ops.len(), 5);
    assert_eq!(remaining_ops[0].sequence, 6);
    assert_eq!(remaining_ops[4].sequence, 10);
}

// ============================================================================
// PROPERTY TESTS: Causal Compaction Invariant
// ============================================================================
#[test]
fn test_property_state_before_and_after_compaction_identical() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_snap_prop.db");
    let node_id = random_node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    // Test on multiple CRDT types:
    // 1. PN-Counter
    let mut counter = PnCounter::new();
    counter.increment(node_id, 123);
    counter.decrement(node_id, 45);
    let (c_doc, _) = store.create_crdt_sync(&counter).unwrap();
    let before_c = store.get_sync(&c_doc.document_id).unwrap().unwrap();
    store.create_snapshot_sync(&c_doc.document_id).unwrap();
    store.compact_sync(&c_doc.document_id).unwrap();
    let after_c = store.get_sync(&c_doc.document_id).unwrap().unwrap();
    assert_eq!(before_c.state, after_c.state);

    // 2. OR-Set
    let tag1 = ElementTag::new(node_id, HybridLogicalClock::now(), 101);
    let tag2 = ElementTag::new(node_id, HybridLogicalClock::now(), 102);
    let mut set: OrSet<String> = OrSet::new();
    set.add("foo".to_string(), tag1);
    set.add("bar".to_string(), tag2);
    let (s_doc, _) = store.create_crdt_sync(&set).unwrap();
    let before_s = store.get_sync(&s_doc.document_id).unwrap().unwrap();
    store.create_snapshot_sync(&s_doc.document_id).unwrap();
    store.compact_sync(&s_doc.document_id).unwrap();
    let after_s = store.get_sync(&s_doc.document_id).unwrap().unwrap();
    assert_eq!(before_s.state, after_s.state);

    // 3. LWW-Register
    let reg = LwwRegister::new(
        serde_json::json!({"status": "ACTIVE", "power": 9000}),
        HybridLogicalClock::now(),
        node_id,
    );
    let (r_doc, _) = store.create_crdt_sync(&reg).unwrap();
    let before_r = store.get_sync(&r_doc.document_id).unwrap().unwrap();
    store.create_snapshot_sync(&r_doc.document_id).unwrap();
    store.compact_sync(&r_doc.document_id).unwrap();
    let after_r = store.get_sync(&r_doc.document_id).unwrap().unwrap();
    assert_eq!(before_r.state, after_r.state);
}

// ============================================================================
// PERFORMANCE BASELINE
// ============================================================================
#[test]
fn test_performance_baseline_snapshots_and_compaction() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_snap_perf.db");
    let node_id = random_node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    let mut counter = PnCounter::new();
    counter.increment(node_id, 1);
    let (doc, _) = store.create_crdt_sync(&counter).unwrap();

    let op_count = 50usize;
    for _ in 0..op_count {
        let delta = PnCounterDelta::Increment {
            actor: node_id,
            amount: 1,
        };
        store
            .apply_crdt_delta_sync::<PnCounter>(&doc.document_id, &delta)
            .unwrap();
    }

    // 1. Benchmark Snapshot Creation
    let t0 = Instant::now();
    let snap = store.create_snapshot_sync(&doc.document_id).unwrap();
    let snap_duration = t0.elapsed();
    let snap_size = snap.state.len();

    // 2. Benchmark Compaction
    let t1 = Instant::now();
    let comp_record = store.compact_sync(&doc.document_id).unwrap();
    let comp_duration = t1.elapsed();

    // 3. Benchmark Snapshot Restore & Forward Replay (with 5 newer ops)
    for _ in 0..5 {
        let delta = PnCounterDelta::Increment {
            actor: node_id,
            amount: 1,
        };
        store
            .apply_crdt_delta_sync::<PnCounter>(&doc.document_id, &delta)
            .unwrap();
    }

    let t2 = Instant::now();
    let restored = store.restore_snapshot_sync(&snap.snapshot_id).unwrap();
    let restore_duration = t2.elapsed();

    let final_counter: PnCounter = PnCounter::deserialize_state(&restored.state).unwrap();
    assert_eq!(final_counter.read(), (op_count + 1 + 5) as i64);

    println!(
        "\n--- FIELDNET BUILD-05 SNAPSHOT & COMPACTION PERFORMANCE BASELINE ---\n\
         Operations Generated:        {}\n\
         Snapshot Creation Duration:  {:?}\n\
         Snapshot State Payload Size: {} bytes\n\
         Compaction Duration:         {:?}\n\
         Operations Reclaimed:        {}\n\
         Storage Bytes Reclaimed:     {} bytes\n\
         Restore + Replay Duration:   {:?}\n\
         ------------------------------------------------------------------\n",
        op_count + 1,
        snap_duration,
        snap_size,
        comp_duration,
        comp_record.ops_removed,
        comp_record.bytes_reclaimed,
        restore_duration
    );
}
