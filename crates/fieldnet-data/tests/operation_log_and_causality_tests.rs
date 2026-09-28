use fieldnet_core::NodeId;
use fieldnet_data::{
    CausalRelation, DataError, DocumentId, DocumentStore, HybridLogicalClock, OpStatus, Operation,
    OperationType, StateVector,
};
use fieldnet_identity::NodeIdentity;
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

/// TEST 1: Create document produces CREATE operation.
#[test]
fn test_01_create_document_produces_create_operation() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_op_create.db");
    let identity = Arc::new(NodeIdentity::generate());
    let store = DocumentStore::with_identity(&db_path, identity.clone()).unwrap();

    let (doc, op) = store
        .create_with_op_sync("intel", b"Recon Data Point".to_vec())
        .unwrap();

    assert_eq!(op.document_id, doc.document_id);
    assert_eq!(op.actor_node_id, identity.node_id());
    assert_eq!(op.sequence, 1);
    assert_eq!(op.op_type, OperationType::Create);
    assert_eq!(op.payload, b"Recon Data Point".to_vec());
    assert_eq!(op.status, OpStatus::Durable);
    assert!(op.causal_dependencies.is_empty());
    assert!(op.hlc.physical > 0);

    // Cryptographic signature check
    op.verify_signature_with_identity(&identity)
        .expect("Signature on CREATE operation must be valid");

    // Verify it is stored and indexed in operations table
    let fetched_op = store
        .get_operation_sync(&op.op_id)
        .unwrap()
        .expect("Operation must be retrieved from database");
    assert_eq!(fetched_op.op_id, op.op_id);
    assert_eq!(fetched_op.sequence, 1);
}

/// TEST 2: Update produces UPDATE operation.
#[test]
fn test_02_update_produces_update_operation() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_op_update.db");
    let identity = Arc::new(NodeIdentity::generate());
    let store = DocumentStore::with_identity(&db_path, identity.clone()).unwrap();

    let (doc, create_op) = store
        .create_with_op_sync("sitrep", b"Status Green".to_vec())
        .unwrap();

    let (updated_doc, update_op) = store
        .update_with_op_sync(&doc.document_id, b"Status Amber".to_vec())
        .unwrap();

    assert_eq!(updated_doc.version, 2);
    assert_eq!(update_op.document_id, doc.document_id);
    assert_eq!(update_op.sequence, 2);
    assert_eq!(update_op.op_type, OperationType::Update);
    assert_eq!(update_op.payload, b"Status Amber".to_vec());
    assert_eq!(update_op.causal_dependencies, vec![create_op.op_id]);
    assert!(update_op.hlc >= create_op.hlc);

    update_op
        .verify_signature_with_identity(&identity)
        .expect("Signature on UPDATE operation must be valid");
}

/// TEST 3: Delete produces DELETE operation.
#[test]
fn test_03_delete_produces_delete_operation() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_op_delete.db");
    let identity = Arc::new(NodeIdentity::generate());
    let store = DocumentStore::with_identity(&db_path, identity.clone()).unwrap();

    let (doc, _) = store
        .create_with_op_sync("temp", b"Transient".to_vec())
        .unwrap();
    let (deleted_doc, delete_op) = store.delete_with_op_sync(&doc.document_id).unwrap();

    assert!(deleted_doc.deleted);
    assert_eq!(deleted_doc.version, 2);
    assert_eq!(delete_op.document_id, doc.document_id);
    assert_eq!(delete_op.sequence, 2);
    assert_eq!(delete_op.op_type, OperationType::Delete);
    assert_eq!(delete_op.status, OpStatus::Durable);

    delete_op
        .verify_signature_with_identity(&identity)
        .expect("Signature on DELETE operation must be valid");
}

/// TEST 4: Document mutation and operation commit atomically.
#[test]
fn test_04_document_mutation_and_operation_commit_atomically() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_atomic_success.db");
    let identity = Arc::new(NodeIdentity::generate());
    let store = DocumentStore::with_identity(&db_path, identity).unwrap();

    let (doc, op) = store
        .create_with_op_sync("note", b"Payload".to_vec())
        .unwrap();

    let doc_in_db = store.get_active_sync(&doc.document_id).unwrap();
    let op_in_db = store.get_operation_sync(&op.op_id).unwrap().unwrap();
    let applied = store
        .is_operation_applied_sync(&doc.document_id, &op.op_id)
        .unwrap();
    let causal = store
        .get_causal_state_sync(&doc.document_id)
        .unwrap()
        .unwrap();

    assert_eq!(doc_in_db.document_id, doc.document_id);
    assert_eq!(op_in_db.op_id, op.op_id);
    assert!(applied, "Operation must be marked applied");
    assert_eq!(causal.state_vector.get(&doc.owner_node_id), 1);
}

/// TEST 5: Failed transaction leaves neither partially committed.
#[test]
fn test_05_failed_transaction_leaves_neither_partially_committed() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_atomic_failure.db");
    let identity = Arc::new(NodeIdentity::generate());
    let store = DocumentStore::with_identity(&db_path, identity.clone()).unwrap();

    let fake_doc_id = DocumentId::generate();

    // Force a failure inside with_transaction
    let res: Result<(), DataError> = store.database().with_transaction(|tx| {
        // Step 1: insert operation
        let hlc = HybridLogicalClock::now();
        let op = Operation::new_local(
            fake_doc_id,
            &identity,
            999,
            OperationType::Create,
            b"Aborted".to_vec(),
            hlc,
            vec![],
        );

        tx.execute(
            "INSERT INTO operations (
                op_id, document_id, actor_node_id, sequence, op_type, delta_bytes,
                hlc_physical, hlc_logical, dependencies_bytes, signature, created_at, status
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            rusqlite::params![
                op.op_id.as_bytes().as_slice(),
                op.document_id.as_bytes().as_slice(),
                op.actor_node_id.as_bytes().as_slice(),
                op.sequence as i64,
                op.op_type.as_u8() as i64,
                op.payload,
                op.hlc.physical as i64,
                op.hlc.logical as i64,
                vec![0u8; 4],
                op.signature.as_slice(),
                op.created_at.as_millis() as i64,
                op.status.as_u8() as i64,
            ],
        )?;

        // Deliberate error to force rollback
        Err(DataError::TransactionFailure("Forced abort".to_string()))
    });

    assert!(res.is_err());

    // Verify neither document nor operation nor applied tracker exists
    let doc_check = store.get_sync(&fake_doc_id).unwrap();
    assert!(doc_check.is_none());

    let count_ops = store
        .list_operations_for_document_sync(&fake_doc_id, None, None)
        .unwrap();
    assert_eq!(count_ops.len(), 0);
}

/// TEST 6: Operation IDs are unique.
#[test]
fn test_06_operation_ids_are_unique() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_op_unique.db");
    let identity = Arc::new(NodeIdentity::generate());
    let store = DocumentStore::with_identity(&db_path, identity).unwrap();

    let mut op_ids = std::collections::HashSet::new();
    for i in 0..50 {
        let (_, op) = store
            .create_with_op_sync("doc", format!("Data {}", i).into_bytes())
            .unwrap();
        assert!(op_ids.insert(op.op_id), "Operation ID collision detected!");
    }
    assert_eq!(op_ids.len(), 50);
}

/// TEST 7: Operation retrieval works.
#[test]
fn test_07_operation_retrieval_works() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_op_retrieval.db");
    let identity = Arc::new(NodeIdentity::generate());
    let store = DocumentStore::with_identity(&db_path, identity).unwrap();

    let (doc, op1) = store.create_with_op_sync("log", b"First".to_vec()).unwrap();
    let (_, op2) = store
        .update_with_op_sync(&doc.document_id, b"Second".to_vec())
        .unwrap();
    let (_, op3) = store
        .update_with_op_sync(&doc.document_id, b"Third".to_vec())
        .unwrap();

    // 1. By ID
    let fetched1 = store.get_operation_sync(&op1.op_id).unwrap().unwrap();
    assert_eq!(fetched1.op_id, op1.op_id);
    assert_eq!(fetched1.sequence, 1);

    // 2. By document
    let doc_ops = store
        .list_operations_for_document_sync(&doc.document_id, None, None)
        .unwrap();
    assert_eq!(doc_ops.len(), 3);
    assert_eq!(doc_ops[0].op_id, op1.op_id);
    assert_eq!(doc_ops[1].op_id, op2.op_id);
    assert_eq!(doc_ops[2].op_id, op3.op_id);

    // 3. Pending list
    let pending = store.list_pending_operations_sync(None).unwrap();
    assert_eq!(pending.len(), 3);
}

/// TEST 8: Operation survives restart.
#[test]
fn test_08_operation_survives_restart() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_op_restart.db");
    let identity = Arc::new(NodeIdentity::generate());
    let op_id;
    let doc_id;

    // Phase 1: create and close
    {
        let store = DocumentStore::with_identity(&db_path, identity.clone()).unwrap();
        let (doc, op) = store
            .create_with_op_sync("persisted", b"Persistent Op".to_vec())
            .unwrap();
        op_id = op.op_id;
        doc_id = doc.document_id;
    }

    // Phase 2: reopen and verify
    {
        let reopened = DocumentStore::with_identity(&db_path, identity).unwrap();
        let op = reopened
            .get_operation_sync(&op_id)
            .unwrap()
            .expect("Operation must survive restart");
        assert_eq!(op.op_id, op_id);
        assert_eq!(op.document_id, doc_id);
        assert_eq!(op.payload, b"Persistent Op".to_vec());
        assert_eq!(op.sequence, 1);
    }
}

/// TEST 9: Causal state survives restart.
#[test]
fn test_09_causal_state_survives_restart() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_causal_restart.db");
    let identity = Arc::new(NodeIdentity::generate());
    let doc_id;

    {
        let store = DocumentStore::with_identity(&db_path, identity.clone()).unwrap();
        let (doc, _) = store
            .create_with_op_sync("asset", b"State 1".to_vec())
            .unwrap();
        doc_id = doc.document_id;
        store
            .update_with_op_sync(&doc_id, b"State 2".to_vec())
            .unwrap();
        store
            .update_with_op_sync(&doc_id, b"State 3".to_vec())
            .unwrap();
    }

    {
        let reopened = DocumentStore::with_identity(&db_path, identity.clone()).unwrap();
        let causal = reopened
            .get_causal_state_sync(&doc_id)
            .unwrap()
            .expect("Causal state must survive restart");
        assert_eq!(causal.state_vector.get(&identity.node_id()), 3);
        assert_eq!(causal.local_sequence, 3);
    }
}

/// TEST 10: Local sequence remains valid after restart.
#[test]
fn test_10_local_sequence_remains_valid_after_restart() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_seq_restart.db");
    let identity = Arc::new(NodeIdentity::generate());
    let doc_id;

    {
        let store = DocumentStore::with_identity(&db_path, identity.clone()).unwrap();
        let (doc, _) = store
            .create_with_op_sync("item", b"Initial".to_vec())
            .unwrap();
        doc_id = doc.document_id;
        store.update_with_op_sync(&doc_id, b"Up1".to_vec()).unwrap();
        store.update_with_op_sync(&doc_id, b"Up2".to_vec()).unwrap();
        assert_eq!(store.get_local_sequence_sync().unwrap(), 3);
    }

    // Reopen and create next operation
    {
        let reopened = DocumentStore::with_identity(&db_path, identity).unwrap();
        assert_eq!(reopened.get_local_sequence_sync().unwrap(), 3);

        let (_, op4) = reopened
            .update_with_op_sync(&doc_id, b"Up3".to_vec())
            .unwrap();
        assert_eq!(
            op4.sequence, 4,
            "Sequence must continue monotonically from 4"
        );
        assert_eq!(reopened.get_local_sequence_sync().unwrap(), 4);
    }
}

/// TEST 11: Repeated operation ID is rejected/deduplicated.
#[test]
fn test_11_repeated_operation_id_is_rejected_deduplicated() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_op_dedupe.db");
    let identity = Arc::new(NodeIdentity::generate());
    let store = DocumentStore::with_identity(&db_path, identity).unwrap();

    let (doc, op) = store
        .create_with_op_sync("data", b"Original".to_vec())
        .unwrap();

    // Check applied
    assert!(store
        .is_operation_applied_sync(&doc.document_id, &op.op_id)
        .unwrap());

    // Repeated record_applied_operation returns false (already applied)
    let re_recorded = store
        .record_applied_operation_sync(&doc.document_id, &op.op_id)
        .unwrap();
    assert!(!re_recorded, "Duplicate operation record must return false");

    // Inserting identical operation row into DB must trigger UNIQUE constraint violation
    let insert_duplicate_res: Result<(), DataError> = store.database().with_transaction(|tx| {
        tx.execute(
            "INSERT INTO operations (
                op_id, document_id, actor_node_id, sequence, op_type, delta_bytes,
                hlc_physical, hlc_logical, dependencies_bytes, signature, created_at, status
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            rusqlite::params![
                op.op_id.as_bytes().as_slice(),
                op.document_id.as_bytes().as_slice(),
                op.actor_node_id.as_bytes().as_slice(),
                op.sequence as i64,
                op.op_type.as_u8() as i64,
                op.payload,
                op.hlc.physical as i64,
                op.hlc.logical as i64,
                vec![0u8; 4],
                op.signature.as_slice(),
                op.created_at.as_millis() as i64,
                op.status.as_u8() as i64,
            ],
        )?;
        Ok(())
    });

    assert!(
        insert_duplicate_res.is_err(),
        "Duplicate op_id must be rejected by PRIMARY KEY"
    );
}

/// TEST 12: DELETE operation remains durable.
#[test]
fn test_12_delete_operation_remains_durable() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_delete_durable.db");
    let identity = Arc::new(NodeIdentity::generate());
    let store = DocumentStore::with_identity(&db_path, identity).unwrap();

    let (doc, _) = store
        .create_with_op_sync("ephemeral", b"To Delete".to_vec())
        .unwrap();
    let (_, del_op) = store.delete_with_op_sync(&doc.document_id).unwrap();

    let ops = store
        .list_operations_for_document_sync(&doc.document_id, None, None)
        .unwrap();
    assert_eq!(ops.len(), 2);
    assert_eq!(ops[1].op_id, del_op.op_id);
    assert_eq!(ops[1].op_type, OperationType::Delete);
}

/// TEST 13: Multiple documents maintain independent operation histories.
#[test]
fn test_13_multiple_documents_maintain_independent_operation_histories() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_multi_doc_ops.db");
    let identity = Arc::new(NodeIdentity::generate());
    let store = DocumentStore::with_identity(&db_path, identity).unwrap();

    let (doc1, _) = store.create_with_op_sync("doc1", b"D1".to_vec()).unwrap();
    let (doc2, _) = store.create_with_op_sync("doc2", b"D2".to_vec()).unwrap();

    store
        .update_with_op_sync(&doc1.document_id, b"D1-Up".to_vec())
        .unwrap();

    let ops1 = store
        .list_operations_for_document_sync(&doc1.document_id, None, None)
        .unwrap();
    let ops2 = store
        .list_operations_for_document_sync(&doc2.document_id, None, None)
        .unwrap();

    assert_eq!(ops1.len(), 2);
    assert_eq!(ops2.len(), 1);
}

/// TEST 14: Causally ordered operations compare correctly.
#[test]
fn test_14_causally_ordered_operations_compare_correctly() {
    let node_a = NodeId::random_for_test(1);
    let node_b = NodeId::random_for_test(2);

    let mut sv_a = StateVector::new();
    sv_a.set(node_a, 1);

    let mut sv_b = StateVector::new();
    sv_b.set(node_a, 1);
    sv_b.set(node_b, 1);

    assert_eq!(sv_a.relation(&sv_b), CausalRelation::HappensBefore);
    assert_eq!(sv_b.relation(&sv_a), CausalRelation::HappensAfter);
    assert!(sv_b.dominates(&sv_a));
    assert!(sv_a.dominated_by(&sv_b));
}

/// TEST 15: Concurrent causal states compare correctly.
#[test]
fn test_15_concurrent_causal_states_compare_correctly() {
    let node_a = NodeId::random_for_test(1);
    let node_b = NodeId::random_for_test(2);

    // Node A branched to seq 2, while Node B branched to seq 1 on B
    let mut sv_a = StateVector::new();
    sv_a.set(node_a, 2);
    sv_a.set(node_b, 0);

    let mut sv_b = StateVector::new();
    sv_b.set(node_a, 1);
    sv_b.set(node_b, 1);

    assert_eq!(sv_a.relation(&sv_b), CausalRelation::Concurrent);
    assert_eq!(sv_b.relation(&sv_a), CausalRelation::Concurrent);
    assert!(sv_a.is_concurrent(&sv_b));
    assert!(sv_b.is_concurrent(&sv_a));
}

/// TEST 16: Operation log can return operations incrementally.
#[test]
fn test_16_operation_log_can_return_operations_incrementally() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_incremental.db");
    let identity = Arc::new(NodeIdentity::generate());
    let store = DocumentStore::with_identity(&db_path, identity.clone()).unwrap();

    let (doc, _) = store
        .create_with_op_sync("stream", b"Rev1".to_vec())
        .unwrap();
    store
        .update_with_op_sync(&doc.document_id, b"Rev2".to_vec())
        .unwrap();
    store
        .update_with_op_sync(&doc.document_id, b"Rev3".to_vec())
        .unwrap();
    store
        .update_with_op_sync(&doc.document_id, b"Rev4".to_vec())
        .unwrap();

    // Client knows up to sequence 2
    let mut remote_known = StateVector::new();
    remote_known.set(identity.node_id(), 2);

    let missing_ops = store
        .list_operations_since_sync(&doc.document_id, &remote_known, None)
        .unwrap();

    assert_eq!(missing_ops.len(), 2);
    assert_eq!(missing_ops[0].sequence, 3);
    assert_eq!(missing_ops[1].sequence, 4);
}

// ============================================================================
// PROPERTY TESTS (SECTION 22)
// ============================================================================

#[test]
fn property_test_causal_identity_reflexive() {
    let node1 = NodeId::random_for_test(10);
    let node2 = NodeId::random_for_test(20);

    let mut sv = StateVector::new();
    sv.set(node1, 5);
    sv.set(node2, 12);

    assert_eq!(sv.relation(&sv), CausalRelation::Equal);
    assert!(sv.equals(&sv));
}

#[test]
fn property_test_causal_asymmetry() {
    let node1 = NodeId::random_for_test(11);
    let node2 = NodeId::random_for_test(22);

    let mut sv_a = StateVector::new();
    sv_a.set(node1, 3);

    let mut sv_b = StateVector::new();
    sv_b.set(node1, 3);
    sv_b.set(node2, 1);

    assert_eq!(sv_a.relation(&sv_b), CausalRelation::HappensBefore);
    assert_ne!(sv_b.relation(&sv_a), CausalRelation::HappensBefore);
    assert_eq!(sv_b.relation(&sv_a), CausalRelation::HappensAfter);
}

// ============================================================================
// PERFORMANCE BASELINE (SECTION 24)
// ============================================================================

#[test]
fn test_operation_log_performance_baseline() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("perf_ops.db");
    let identity = Arc::new(NodeIdentity::generate());
    let store = DocumentStore::with_identity(&db_path, identity).unwrap();

    let iterations = 200;
    let payload = vec![0x33u8; 1024]; // 1 KB payload

    // 1. Create with Operation
    let start_create = Instant::now();
    let mut doc_ids = Vec::with_capacity(iterations);
    let mut op_ids = Vec::with_capacity(iterations);

    for _ in 0..iterations {
        let (doc, op) = store.create_with_op_sync("perf", payload.clone()).unwrap();
        doc_ids.push(doc.document_id);
        op_ids.push(op.op_id);
    }
    let create_elapsed = start_create.elapsed();
    let create_ops_sec = (iterations as f64) / create_elapsed.as_secs_f64();
    println!(
        "BUILD-02 PERF: {} Creates with Ops in {:?} ({:.1} ops/sec, {:.3} ms/op)",
        iterations,
        create_elapsed,
        create_ops_sec,
        (create_elapsed.as_secs_f64() * 1000.0) / (iterations as f64)
    );

    // 2. Op Lookup
    let start_lookup = Instant::now();
    for op_id in &op_ids {
        let op = store.get_operation_sync(op_id).unwrap().unwrap();
        assert_eq!(op.op_id, *op_id);
    }
    let lookup_elapsed = start_lookup.elapsed();
    let lookup_ops_sec = (iterations as f64) / lookup_elapsed.as_secs_f64();
    println!(
        "BUILD-02 PERF: {} Op Lookups in {:?} ({:.1} ops/sec, {:.3} ms/op)",
        iterations,
        lookup_elapsed,
        lookup_ops_sec,
        (lookup_elapsed.as_secs_f64() * 1000.0) / (iterations as f64)
    );

    // 3. Batch Retrieval
    let start_batch = Instant::now();
    let all_pending = store
        .list_pending_operations_sync(Some(iterations))
        .unwrap();
    let batch_elapsed = start_batch.elapsed();
    assert_eq!(all_pending.len(), iterations);
    println!(
        "BUILD-02 PERF: Batch retrieved {} ops in {:?}",
        iterations, batch_elapsed
    );

    assert!(create_ops_sec > 10.0);
    assert!(lookup_ops_sec > 50.0);
}

// Helper extension for deterministic test NodeIds
trait NodeIdTestExt {
    fn random_for_test(val: u8) -> NodeId;
}

impl NodeIdTestExt for NodeId {
    fn random_for_test(val: u8) -> NodeId {
        let mut b = [0u8; 32];
        b[0] = val;
        NodeId::from_bytes(b)
    }
}
