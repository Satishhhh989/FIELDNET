use fieldnet_content::{hash_bytes, PutResult};
use fieldnet_runtime::{FieldnetRuntime, RuntimeConfig};
use std::fs;

#[tokio::test]
async fn test_runtime_content_store_facade_and_persistence() {
    let data_dir = std::env::temp_dir().join(format!(
        "fieldnet_content_runtime_test_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = fs::remove_dir_all(&data_dir);
    fs::create_dir_all(&data_dir).unwrap();

    let payload1 = b"Sovereign encrypted geospatial map chunk 01";
    let payload2 = b"Tactical mission telemetry recording v1";

    let cid1 = hash_bytes(payload1);
    let cid2 = hash_bytes(payload2);

    // Phase 1: Boot runtime, put content via application facade, verify retrieval and stat
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

        // Verify has returns false before insertion
        assert!(!runtime.content_has(&cid1).unwrap());

        // Put content via facade
        let (id1, res1) = runtime
            .content_put(payload1, Some("application/geo+json".into()))
            .await
            .expect("content_put should succeed");
        assert_eq!(id1, cid1);
        assert!(matches!(res1, PutResult::Created(_)));

        // Put second content
        let (id2, res2) = runtime
            .content_put(payload2, Some("text/plain".into()))
            .await
            .expect("content_put 2 should succeed");
        assert_eq!(id2, cid2);
        assert!(matches!(res2, PutResult::Created(_)));

        // Duplicate insertion via facade
        let (_dup_id, dup_res) = runtime
            .content_put(payload1, Some("application/geo+json".into()))
            .await
            .expect("duplicate content_put should succeed");
        assert!(matches!(dup_res, PutResult::AlreadyExists(_)));

        // Verify content_has
        assert!(runtime.content_has(&cid1).unwrap());
        assert!(runtime.content_has(&cid2).unwrap());

        // Verify content_get
        let read1 = runtime.content_get(&cid1).await.unwrap();
        assert_eq!(read1, payload1);

        // Verify content_stat
        let stat1 = runtime.content_stat(&cid1).unwrap();
        assert_eq!(stat1.content_id, cid1);
        assert_eq!(stat1.size_bytes, payload1.len() as u64);
        assert_eq!(stat1.mime_type.as_deref(), Some("application/geo+json"));
        assert_eq!(stat1.creator_node_id, Some(runtime.node_id()));

        // Verify content_list
        let list = runtime.content_list().unwrap();
        assert_eq!(list.len(), 2);

        runtime.shutdown();
    }

    // Verify content.db and objects directories exist on disk
    let content_db_file = data_dir.join("content").join("content.db");
    assert!(content_db_file.exists(), "content.db must exist on disk");

    let obj1_path = data_dir
        .join("content")
        .join("objects")
        .join(cid1.relative_storage_path());
    assert!(
        obj1_path.exists(),
        "sharded object file 1 must exist on disk"
    );

    // Phase 2: Restart runtime on exact same data directory
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

        // Verify content survives restart
        assert!(reopened_runtime.content_has(&cid1).unwrap());
        assert!(reopened_runtime.content_has(&cid2).unwrap());

        let retrieved1 = reopened_runtime.content_get(&cid1).await.unwrap();
        assert_eq!(retrieved1, payload1);

        let retrieved2 = reopened_runtime.content_get(&cid2).await.unwrap();
        assert_eq!(retrieved2, payload2);

        // Delete content 1
        let deleted = reopened_runtime
            .content_delete(&cid1)
            .await
            .expect("content_delete should succeed");
        assert!(deleted);
        assert!(!reopened_runtime.content_has(&cid1).unwrap());

        // Content 2 still intact
        assert!(reopened_runtime.content_has(&cid2).unwrap());

        reopened_runtime.shutdown();
    }

    let _ = fs::remove_dir_all(&data_dir);
}
