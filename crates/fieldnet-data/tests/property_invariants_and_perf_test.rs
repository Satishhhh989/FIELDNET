use fieldnet_core::NodeId;
use fieldnet_data::{DataError, DocumentId, DocumentStore};
use rand::RngCore;
use std::time::Instant;
use tempfile::tempdir;

fn random_node_id() -> NodeId {
    let mut b = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut b);
    NodeId::from_bytes(b)
}

/// INVARIANT 1: Document ID remains stable across updates, reads, and queries.
#[test]
fn invariant_01_document_id_remains_stable() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("inv1.db");
    let node_id = random_node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    let doc = store.create_sync("record", b"Initial".to_vec()).unwrap();
    let original_id = doc.document_id;

    for i in 1..=5 {
        let updated = store
            .update_sync(&original_id, format!("Mutation {}", i).into_bytes())
            .unwrap();
        assert_eq!(
            updated.document_id, original_id,
            "DocumentId must remain unchanged across revisions"
        );
        assert_eq!(updated.version, (i + 1) as u64);
    }

    let fetched = store.get_active_sync(&original_id).unwrap();
    assert_eq!(fetched.document_id, original_id);
}

/// INVARIANT 2: A committed document survives restart.
#[test]
fn invariant_02_committed_document_survives_restart() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("inv2.db");
    let node_id = random_node_id();

    let doc_id;
    let expected_payload = b"Durable Invariant Payload".to_vec();

    {
        let store = DocumentStore::open(&db_path, node_id).unwrap();
        let doc = store
            .create_sync("durable", expected_payload.clone())
            .unwrap();
        doc_id = doc.document_id;
    }

    // Reopen from disk
    {
        let reopened = DocumentStore::open(&db_path, node_id).unwrap();
        let doc = reopened.get_active_sync(&doc_id).unwrap();
        assert_eq!(doc.document_id, doc_id);
        assert_eq!(doc.state, expected_payload);
        assert_eq!(doc.version, 1);
    }
}

/// INVARIANT 3: Failed transaction does not partially modify state.
#[test]
fn invariant_03_failed_transaction_does_not_partially_modify_state() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("inv3.db");
    let node_id = random_node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    let doc = store
        .create_sync("atomic", b"Original State".to_vec())
        .unwrap();
    let doc_id = doc.document_id;

    // Simulate multi-step atomic transaction where second step aborts
    let res: Result<(), DataError> = store.database().with_transaction(|tx| {
        tx.execute(
            "UPDATE documents SET payload_bytes = ?1, version = 999 WHERE document_id = ?2",
            rusqlite::params![b"Corrupted State".to_vec(), doc_id.as_bytes().as_slice()],
        )?;
        Err(DataError::TransactionFailure("Simulated crash".to_string()))
    });
    assert!(res.is_err());

    // Verify original state is completely untouched
    let doc_after = store.get_active_sync(&doc_id).unwrap();
    assert_eq!(doc_after.state, b"Original State".to_vec());
    assert_eq!(doc_after.version, 1);
}

/// INVARIANT 4: Deletion does not unexpectedly resurrect on restart.
#[test]
fn invariant_04_deletion_does_not_resurrect_on_restart() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("inv4.db");
    let node_id = random_node_id();

    let doc_id;
    {
        let store = DocumentStore::open(&db_path, node_id).unwrap();
        let doc = store.create_sync("item", b"Doomed".to_vec()).unwrap();
        doc_id = doc.document_id;
        store.delete_sync(&doc_id).unwrap();
    }

    // Restart and verify document remains deleted
    {
        let reopened = DocumentStore::open(&db_path, node_id).unwrap();
        let res = reopened.get_active_sync(&doc_id);
        assert!(
            matches!(res, Err(DataError::DocumentDeleted(_))),
            "Document must remain deleted after restart"
        );

        let tombstone = reopened.get_sync(&doc_id).unwrap().unwrap();
        assert!(tombstone.deleted);
    }
}

/// INVARIANT 5: Database initialization is idempotent.
#[test]
fn invariant_05_database_initialization_is_idempotent() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("inv5.db");
    let node_id = random_node_id();

    // Repeated initializations must succeed without altering schema or state
    for _ in 0..10 {
        let store = DocumentStore::open(&db_path, node_id).unwrap();
        assert_eq!(store.owner_node_id(), node_id);
    }
}

/// INVARIANT 6: Existing data is never silently overwritten by startup.
#[test]
fn invariant_06_existing_data_never_silently_overwritten_by_startup() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("inv6.db");
    let node_id = random_node_id();

    let count = 25;
    {
        let store = DocumentStore::open(&db_path, node_id).unwrap();
        for i in 0..count {
            store
                .create_sync("entry", format!("Data {}", i).into_bytes())
                .unwrap();
        }
    }

    // Startup with new instance on same path
    {
        let reopened = DocumentStore::open(&db_path, node_id).unwrap();
        let current_count = reopened.count_sync(Some("entry"), false).unwrap();
        assert_eq!(
            current_count, count,
            "Startup must strictly preserve all existing rows"
        );
    }
}

/// INVARIANT 7: Invalid input cannot corrupt the database.
#[test]
fn invariant_07_invalid_input_cannot_corrupt_database() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("inv7.db");
    let node_id = random_node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    let valid_doc = store
        .create_sync("valid", b"Valid Payload".to_vec())
        .unwrap();

    // Attempt invalid creations
    let _ = store.create_sync("", b"No Type".to_vec());
    let _ = store.create_sync("invalid/type", b"Bad Chars".to_vec());
    let _ = store.create_sync("valid", vec![0u8; 5 * 1024 * 1024]); // 5 MB > 4 MB limit

    // Attempt invalid updates
    let non_existent_id = DocumentId::generate();
    let _ = store.update_sync(&non_existent_id, b"Ghost Update".to_vec());

    // Verify valid document and database health remains intact
    let checked = store.get_active_sync(&valid_doc.document_id).unwrap();
    assert_eq!(checked.state, b"Valid Payload".to_vec());
    assert_eq!(store.count_sync(None, true).unwrap(), 1);
}

/// SECTION 20: Performance baseline benchmark.
/// Measures throughput and latency for create, read, update, delete operations.
#[test]
fn test_performance_baseline_crud() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("perf.db");
    let node_id = random_node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    let iterations = 200;
    let payload = vec![0x42u8; 1024]; // 1 KB payload

    // 1. Create Benchmark
    let start_create = Instant::now();
    let mut ids = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let doc = store.create_sync("benchmark", payload.clone()).unwrap();
        ids.push(doc.document_id);
    }
    let create_elapsed = start_create.elapsed();
    let create_ops_sec = (iterations as f64) / create_elapsed.as_secs_f64();
    println!(
        "PERF BASELINE: {} Creates in {:?} ({:.1} ops/sec, {:.3} ms/op)",
        iterations,
        create_elapsed,
        create_ops_sec,
        (create_elapsed.as_secs_f64() * 1000.0) / (iterations as f64)
    );

    // 2. Read Benchmark
    let start_read = Instant::now();
    for id in &ids {
        let doc = store.get_active_sync(id).unwrap();
        assert_eq!(doc.state.len(), 1024);
    }
    let read_elapsed = start_read.elapsed();
    let read_ops_sec = (iterations as f64) / read_elapsed.as_secs_f64();
    println!(
        "PERF BASELINE: {} Reads in {:?} ({:.1} ops/sec, {:.3} ms/op)",
        iterations,
        read_elapsed,
        read_ops_sec,
        (read_elapsed.as_secs_f64() * 1000.0) / (iterations as f64)
    );

    // 3. Update Benchmark
    let updated_payload = vec![0x99u8; 1024];
    let start_update = Instant::now();
    for id in &ids {
        let doc = store.update_sync(id, updated_payload.clone()).unwrap();
        assert_eq!(doc.version, 2);
    }
    let update_elapsed = start_update.elapsed();
    let update_ops_sec = (iterations as f64) / update_elapsed.as_secs_f64();
    println!(
        "PERF BASELINE: {} Updates in {:?} ({:.1} ops/sec, {:.3} ms/op)",
        iterations,
        update_elapsed,
        update_ops_sec,
        (update_elapsed.as_secs_f64() * 1000.0) / (iterations as f64)
    );

    // 4. Delete Benchmark
    let start_delete = Instant::now();
    for id in &ids {
        let doc = store.delete_sync(id).unwrap();
        assert!(doc.deleted);
    }
    let delete_elapsed = start_delete.elapsed();
    let delete_ops_sec = (iterations as f64) / delete_elapsed.as_secs_f64();
    println!(
        "PERF BASELINE: {} Deletes in {:?} ({:.1} ops/sec, {:.3} ms/op)",
        iterations,
        delete_elapsed,
        delete_ops_sec,
        (delete_elapsed.as_secs_f64() * 1000.0) / (iterations as f64)
    );

    assert!(
        create_ops_sec > 10.0,
        "Create throughput must exceed 10 ops/sec"
    );
    assert!(
        read_ops_sec > 50.0,
        "Read throughput must exceed 50 ops/sec"
    );
}
