use fieldnet_core::NodeId;
use fieldnet_data::{
    DataError, Document, DocumentId, DocumentQuery, DocumentStore, MigrationRunner,
    MAX_DOCUMENT_SIZE, MAX_DOCUMENT_TYPE_LEN,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tempfile::tempdir;

use rand::RngCore;

fn random_node_id() -> NodeId {
    let mut b = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut b);
    NodeId::from_bytes(b)
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
struct TestNote {
    title: String,
    body: String,
    tags: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct TacticalMarker {
    callsign: String,
    latitude: f64,
    longitude: f64,
}

/// TEST 1: Initialize empty database.
#[test]
fn test_01_initialize_empty_database() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_empty.db");
    let node_id = random_node_id();

    let store = DocumentStore::open(&db_path, node_id).expect("Should initialize empty database");
    assert_eq!(store.owner_node_id(), node_id);

    let count = store.count_sync(None, true).unwrap();
    assert_eq!(count, 0, "Empty database must contain 0 documents");
}

/// TEST 2: Create document.
#[test]
fn test_02_create_document() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_create.db");
    let node_id = random_node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    let note = TestNote {
        title: "Field Observations".to_string(),
        body: "Waypoint Alpha secured".to_string(),
        tags: vec!["recon".to_string(), "sitrep".to_string()],
    };
    let payload = serde_json::to_vec(&note).unwrap();

    let doc = store.create_sync("note", payload.clone()).unwrap();

    assert_eq!(doc.document_type, "note");
    assert_eq!(doc.owner_node_id, node_id);
    assert_eq!(doc.version, 1);
    assert!(!doc.deleted);
    assert_eq!(doc.state, payload);
    assert!(doc.created_at.as_millis() > 0);
    assert_eq!(doc.created_at, doc.updated_at);
}

/// TEST 3: Read document.
#[test]
fn test_03_read_document() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_read.db");
    let node_id = random_node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    let payload = b"Document Content Here".to_vec();
    let created = store.create_sync("text", payload.clone()).unwrap();

    let read_opt = store.get_sync(&created.document_id).unwrap();
    assert!(read_opt.is_some());
    let read_doc = read_opt.unwrap();
    assert_eq!(read_doc.document_id, created.document_id);
    assert_eq!(read_doc.state, payload);
    assert_eq!(read_doc.version, 1);

    let active = store.get_active_sync(&created.document_id).unwrap();
    assert_eq!(active.document_id, created.document_id);
}

/// TEST 4: Update document.
#[test]
fn test_04_update_document() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_update.db");
    let node_id = random_node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    let initial = store.create_sync("note", b"Version 1".to_vec()).unwrap();
    assert_eq!(initial.version, 1);

    // Sleep briefly to ensure updated_at timestamp advances
    std::thread::sleep(std::time::Duration::from_millis(10));

    let updated = store
        .update_sync(&initial.document_id, b"Version 2 Updated".to_vec())
        .unwrap();

    assert_eq!(updated.document_id, initial.document_id);
    assert_eq!(updated.version, 2);
    assert_eq!(updated.state, b"Version 2 Updated".to_vec());
    assert!(updated.updated_at.as_millis() >= initial.updated_at.as_millis());

    // Verify persisted state matches
    let fetched = store.get_active_sync(&initial.document_id).unwrap();
    assert_eq!(fetched.version, 2);
    assert_eq!(fetched.state, b"Version 2 Updated".to_vec());
}

/// TEST 5: Delete document (logical tombstone).
#[test]
fn test_05_delete_document() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_delete.db");
    let node_id = random_node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    let doc = store
        .create_sync("asset", b"Target Asset".to_vec())
        .unwrap();
    let id = doc.document_id;

    let deleted = store.delete_sync(&id).unwrap();
    assert!(deleted.deleted);
    assert_eq!(deleted.version, 2);

    // get_active should fail with DocumentDeleted
    let active_res = store.get_active_sync(&id);
    match active_res {
        Err(DataError::DocumentDeleted(deleted_id)) => assert_eq!(deleted_id, id),
        other => panic!("Expected DocumentDeleted error, got {:?}", other),
    }

    // get_sync should still return the document with deleted=true
    let raw = store
        .get_sync(&id)
        .unwrap()
        .expect("Document must exist as tombstone");
    assert!(raw.deleted);
    assert_eq!(raw.state, b"Target Asset".to_vec());

    // Idempotent delete on already deleted document
    let second_delete = store.delete_sync(&id).unwrap();
    assert!(second_delete.deleted);
}

/// TEST 6: List documents.
#[test]
fn test_06_list_documents() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_list.db");
    let node_id = random_node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    for i in 0..10 {
        store
            .create_sync("telemetry", format!("Reading {}", i).into_bytes())
            .unwrap();
    }

    let doc_del = store.create_sync("telemetry", b"Temp".to_vec()).unwrap();
    store.delete_sync(&doc_del.document_id).unwrap();

    // Query active only
    let active_list = store
        .list_sync(
            DocumentQuery::new()
                .with_type("telemetry")
                .include_deleted(false),
        )
        .unwrap();
    assert_eq!(active_list.len(), 10);

    // Query including deleted
    let all_list = store
        .list_sync(
            DocumentQuery::new()
                .with_type("telemetry")
                .include_deleted(true),
        )
        .unwrap();
    assert_eq!(all_list.len(), 11);

    // Pagination limit & offset
    let paged = store
        .list_sync(
            DocumentQuery::new()
                .with_type("telemetry")
                .include_deleted(false)
                .with_limit(4)
                .with_offset(2),
        )
        .unwrap();
    assert_eq!(paged.len(), 4);
}

/// TEST 7: Missing document.
#[test]
fn test_07_missing_document() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_missing.db");
    let node_id = random_node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    let missing_id = DocumentId::generate();
    let res = store.get_sync(&missing_id).unwrap();
    assert!(res.is_none(), "Non-existent document must return None");

    let active_res = store.get_active_sync(&missing_id);
    match active_res {
        Err(DataError::DocumentNotFound(id)) => assert_eq!(id, missing_id),
        other => panic!("Expected DocumentNotFound error, got {:?}", other),
    }

    let update_res = store.update_sync(&missing_id, b"New".to_vec());
    assert!(matches!(update_res, Err(DataError::DocumentNotFound(_))));

    let delete_res = store.delete_sync(&missing_id);
    assert!(matches!(delete_res, Err(DataError::DocumentNotFound(_))));
}

/// TEST 8: Invalid document input.
#[test]
fn test_08_invalid_document_input() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_invalid.db");
    let node_id = random_node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    // Empty document type
    let empty_type_res = store.create_sync("", b"Data".to_vec());
    assert!(matches!(
        empty_type_res,
        Err(DataError::InvalidDocumentType(_))
    ));

    // Whitespace only type
    let ws_res = store.create_sync("   ", b"Data".to_vec());
    assert!(matches!(ws_res, Err(DataError::InvalidDocumentType(_))));

    // Type name too long
    let long_type = "a".repeat(MAX_DOCUMENT_TYPE_LEN + 1);
    let long_type_res = store.create_sync(&long_type, b"Data".to_vec());
    assert!(matches!(
        long_type_res,
        Err(DataError::InvalidDocumentType(_))
    ));

    // Invalid characters in document type
    let invalid_chars = store.create_sync("bad type/with#chars!", b"Data".to_vec());
    assert!(matches!(
        invalid_chars,
        Err(DataError::InvalidDocumentType(_))
    ));

    // Payload exceeding MAX_DOCUMENT_SIZE (4 MB)
    let huge_payload = vec![0u8; MAX_DOCUMENT_SIZE + 1];
    let large_res = store.create_sync("large", huge_payload);
    assert!(matches!(large_res, Err(DataError::DocumentTooLarge { .. })));
}

/// TEST 9: Transaction rollback.
#[test]
fn test_09_transaction_rollback() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_rollback.db");
    let node_id = random_node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    let initial_count = store.count_sync(None, true).unwrap();
    assert_eq!(initial_count, 0);

    // Deliberately fail inside with_transaction
    let res: Result<(), DataError> = store.database().with_transaction(|tx| {
        let doc = Document::new("test", node_id, b"Should be rolled back".to_vec())?;
        tx.execute(
            "INSERT INTO documents (
                document_id, document_type, schema_version, owner_node_id,
                version, created_at, updated_at, is_deleted, payload_bytes
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            rusqlite::params![
                doc.document_id.as_bytes().as_slice(),
                doc.document_type,
                doc.schema_version,
                doc.owner_node_id.as_bytes().as_slice(),
                doc.version as i64,
                doc.created_at.as_millis() as i64,
                doc.updated_at.as_millis() as i64,
                0,
                doc.state,
            ],
        )?;

        // Return error to trigger abort/rollback
        Err(DataError::StorageFailure("Intentional failure".to_string()))
    });

    assert!(res.is_err());

    // Verify table remains empty
    let count_after = store.count_sync(None, true).unwrap();
    assert_eq!(
        count_after, 0,
        "Rollback must leave database in pristine state"
    );
}

/// TEST 10: Database restart persistence.
#[test]
fn test_10_database_restart_persistence() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_restart.db");
    let node_id = random_node_id();

    let doc_id;
    let tombstone_id;

    // Phase 1: Open, write documents, close
    {
        let store = DocumentStore::open(&db_path, node_id).unwrap();

        let doc1 = store
            .create_sync("note", b"Survives Restart".to_vec())
            .unwrap();
        doc_id = doc1.document_id;

        let doc2 = store
            .create_sync("note", b"Deleted Before Restart".to_vec())
            .unwrap();
        tombstone_id = doc2.document_id;
        store.delete_sync(&tombstone_id).unwrap();

        store
            .update_sync(&doc_id, b"Survives Restart - Updated".to_vec())
            .unwrap();
    }

    // Phase 2: Restart database from same path
    {
        let reopened = DocumentStore::open(&db_path, node_id).unwrap();

        let doc1 = reopened.get_active_sync(&doc_id).unwrap();
        assert_eq!(doc1.version, 2);
        assert_eq!(doc1.state, b"Survives Restart - Updated".to_vec());

        let doc2_raw = reopened.get_sync(&tombstone_id).unwrap().unwrap();
        assert!(
            doc2_raw.deleted,
            "Deleted state must be preserved across restart"
        );
        assert_eq!(doc2_raw.version, 2);
    }
}

/// TEST 11: Multiple documents.
#[test]
fn test_11_multiple_documents() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_multi_docs.db");
    let node_id = random_node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    let count = 50;
    let mut ids = Vec::new();

    for i in 0..count {
        let doc = store
            .create_sync("item", format!("Payload #{}", i).into_bytes())
            .unwrap();
        ids.push(doc.document_id);
    }

    assert_eq!(store.count_sync(Some("item"), false).unwrap(), count);

    for (i, id) in ids.iter().enumerate() {
        let doc = store.get_active_sync(id).unwrap();
        assert_eq!(doc.state, format!("Payload #{}", i).into_bytes());
    }
}

/// TEST 12: Multiple document types.
#[test]
fn test_12_multiple_document_types() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_multi_types.db");
    let node_id = random_node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    let note = TestNote {
        title: "Recon Note".to_string(),
        body: "North perimeter clear".to_string(),
        tags: vec!["recon".to_string()],
    };
    let marker = TacticalMarker {
        callsign: "Viper-1".to_string(),
        latitude: 34.0522,
        longitude: -118.2437,
    };

    let note_doc = store
        .create_sync("note", serde_json::to_vec(&note).unwrap())
        .unwrap();
    let marker_doc = store
        .create_sync("marker", serde_json::to_vec(&marker).unwrap())
        .unwrap();
    store.create_sync("report", b"Report 1".to_vec()).unwrap();
    store.create_sync("config", b"Config 1".to_vec()).unwrap();

    let fetched_note: TestNote = note_doc.state_json().unwrap();
    assert_eq!(fetched_note, note);

    let fetched_marker: TacticalMarker = marker_doc.state_json().unwrap();
    assert_eq!(fetched_marker, marker);

    assert_eq!(store.count_sync(Some("note"), false).unwrap(), 1);
    assert_eq!(store.count_sync(Some("marker"), false).unwrap(), 1);
    assert_eq!(store.count_sync(Some("report"), false).unwrap(), 1);
    assert_eq!(store.count_sync(Some("config"), false).unwrap(), 1);
    assert_eq!(store.count_sync(None, false).unwrap(), 4);
}

/// TEST 13: Concurrent reads/writes.
#[tokio::test]
async fn test_13_concurrent_reads_writes() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_concurrency.db");
    let node_id = random_node_id();
    let store = Arc::new(DocumentStore::open(&db_path, node_id).unwrap());

    let mut handles = Vec::new();

    // Spawn 10 concurrent writer tasks
    for i in 0..10 {
        let store_clone = Arc::clone(&store);
        let handle = tokio::spawn(async move {
            let doc = store_clone
                .create("concurrent", format!("Data from task {}", i).into_bytes())
                .await
                .unwrap();

            // Perform read
            let fetched = store_clone.get_active(doc.document_id).await.unwrap();
            assert_eq!(fetched.document_id, doc.document_id);

            // Perform update
            let updated = store_clone
                .update(
                    doc.document_id,
                    format!("Updated by task {}", i).into_bytes(),
                )
                .await
                .unwrap();
            assert_eq!(updated.version, 2);

            doc.document_id
        });
        handles.push(handle);
    }

    let mut created_ids = Vec::new();
    for h in handles {
        let id = h.await.unwrap();
        created_ids.push(id);
    }

    assert_eq!(created_ids.len(), 10);
    assert_eq!(
        store
            .count(Some("concurrent".to_string()), false)
            .await
            .unwrap(),
        10
    );
}

/// TEST 14: Migration execution.
#[test]
fn test_14_migration_execution() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_migrations.db");
    let mut conn = rusqlite::Connection::open(&db_path).unwrap();

    let applied = MigrationRunner::apply_all(&mut conn).unwrap();
    assert_eq!(
        applied, 5,
        "Migrations should apply all registered migrations"
    );

    let versions = MigrationRunner::get_applied_versions(&conn).unwrap();
    assert_eq!(versions, vec![1, 2, 3, 4, 5]);

    // Re-running apply_all should apply 0 additional migrations
    let applied_second = MigrationRunner::apply_all(&mut conn).unwrap();
    assert_eq!(applied_second, 0);
}

/// TEST 15: Repeated initialization of an existing database.
#[test]
fn test_15_repeated_initialization_of_existing_database() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_reinit.db");
    let node_id = random_node_id();

    let doc_id;
    {
        let store = DocumentStore::open(&db_path, node_id).unwrap();
        let doc = store
            .create_sync("critical", b"Must Not Be Wiped".to_vec())
            .unwrap();
        doc_id = doc.document_id;
    }

    // Repeatedly re-initialize 5 times
    for _ in 0..5 {
        let store = DocumentStore::open(&db_path, node_id).unwrap();
        let doc = store.get_active_sync(&doc_id).unwrap();
        assert_eq!(doc.state, b"Must Not Be Wiped".to_vec());
    }
}
