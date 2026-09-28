use fieldnet_data::{DocumentQuery, DocumentStore};
use fieldnet_runtime::{FieldnetRuntime, RuntimeConfig};
use std::fs;
use std::sync::Arc;

#[tokio::test]
async fn test_runtime_document_store_lifecycle() {
    let data_dir = std::env::temp_dir().join(format!(
        "fieldnet_doc_test_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = fs::remove_dir_all(&data_dir);
    fs::create_dir_all(&data_dir).unwrap();

    let doc_id;
    let expected_payload = b"Field Report: Grid 4 Sector Bravo Normal".to_vec();

    // Phase 1: Boot runtime, create document, update document, shutdown
    {
        let config = RuntimeConfig {
            data_dir: data_dir.clone(),
            tcp_port: 0,
            discovery_port: 0,
            auto_connect: false,
            ..Default::default()
        };

        let runtime = FieldnetRuntime::start(config)
            .await
            .expect("Runtime should start successfully");

        let store = runtime.document_store();
        assert_eq!(store.owner_node_id(), runtime.node_id());

        let doc = store
            .create("sitrep", expected_payload.clone())
            .await
            .expect("Document creation should succeed");
        doc_id = doc.document_id;
        assert_eq!(doc.version, 1);

        let updated = store
            .update(doc_id, b"Field Report: Grid 4 Sector Bravo Active".to_vec())
            .await
            .expect("Document update should succeed");
        assert_eq!(updated.version, 2);

        runtime.shutdown();
    }

    // Verify documents.db file exists on disk
    let doc_db_file = data_dir.join("documents.db");
    assert!(doc_db_file.exists(), "documents.db must exist on disk");

    // Phase 2: Restart runtime on the exact same data directory
    {
        let config = RuntimeConfig {
            data_dir: data_dir.clone(),
            tcp_port: 0,
            discovery_port: 0,
            auto_connect: false,
            ..Default::default()
        };

        let reopened_runtime = FieldnetRuntime::start(config)
            .await
            .expect("Runtime restart should succeed");

        let store = reopened_runtime.document_store();
        let doc = store
            .get_active(doc_id)
            .await
            .expect("Persisted document should be found");

        assert_eq!(doc.document_id, doc_id);
        assert_eq!(doc.version, 2);
        assert_eq!(
            doc.state,
            b"Field Report: Grid 4 Sector Bravo Active".to_vec()
        );

        reopened_runtime.shutdown();
    }
}

#[tokio::test]
async fn test_document_store_operates_without_networking() {
    let data_dir = std::env::temp_dir().join(format!(
        "fieldnet_standalone_doc_test_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = fs::remove_dir_all(&data_dir);
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("standalone_documents.db");
    let sovereign_node_id = fieldnet_core::NodeId::from_bytes([0x77u8; 32]);

    // Local Data Store operates completely standalone with zero networking dependencies
    let store = Arc::new(
        DocumentStore::open(&db_path, sovereign_node_id)
            .expect("Standalone DocumentStore must open cleanly without networking"),
    );

    // Create 10 local documents
    for i in 0..10 {
        store
            .create(
                "sensor_reading",
                format!("Sensor #{} = 24.5 C", i).into_bytes(),
            )
            .await
            .unwrap();
    }

    let docs = store
        .list(DocumentQuery::new().with_type("sensor_reading"))
        .await
        .unwrap();
    assert_eq!(docs.len(), 10);
    assert_eq!(store.owner_node_id(), sovereign_node_id);
}
