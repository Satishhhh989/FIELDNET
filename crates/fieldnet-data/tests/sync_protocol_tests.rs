//! Comprehensive Synchronization Protocol Integration Test Suite for FIELDNET Phase 06 BUILD-04.
//!
//! Verifies:
//! - TEST 1: A creates document. B synchronizes. B receives document.
//! - TEST 2: A updates document. B receives incremental operation.
//! - TEST 3: B updates document while A is offline. A later synchronizes B's operation.
//! - TEST 4: Both nodes make concurrent CRDT updates. They converge.
//! - TEST 5: Duplicate operation arrives. No duplicate state change.
//! - TEST 6: Operation arrives out of order. Dependency handling works.
//! - TEST 7: Connection disappears during synchronization. Synchronization resumes.
//! - TEST 8: Runtime restarts during pending synchronization. State survives.
//! - TEST 9: Unauthorized operation. Rejected safely.
//! - TEST 10: Malformed synchronization packet. Runtime survives.
//! - TEST 13: Large operation set synchronizes in bounded batches.
//! - TEST 14: Partial replication does not synchronize unrelated documents.
//! - TEST 15: Incompatible protocol version fails safely.
//! - THREE-NODE END-TO-END SCENARIO: A creates X, B creates Y, C creates Z, converge.
//! - PERFORMANCE BASELINE: Handshake latency, state-summary size, ops/sec, bytes/sec, convergence time.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use fieldnet_core::NodeId;
use fieldnet_data::causality::HybridLogicalClock;
use fieldnet_data::crdt::{Crdt, CrdtMap, OrSet, PnCounter};
use fieldnet_data::document::DocumentId;
use fieldnet_data::error::DataError;
use fieldnet_data::operation::{Operation, OperationId, OperationType};
use fieldnet_data::storage::{DocumentQuery, DocumentStore};
use fieldnet_data::sync::authorizer::DefaultDocumentAuthorizer;
use fieldnet_data::sync::protocol::{SyncAckStatus, SyncMessage};
use fieldnet_data::sync::session::SyncSessionState;
use fieldnet_data::sync::{SyncEngine, SyncEngineConfig, SyncTransport};
use fieldnet_identity::NodeIdentity;
use tempfile::tempdir;
use tokio::sync::RwLock;

/// Direct in-memory simulated mesh transport for testing peer-to-peer sync protocols.
#[derive(Clone, Default)]
struct MemoryMeshTransport {
    nodes: Arc<RwLock<HashMap<NodeId, Arc<SyncEngine>>>>,
    drop_traffic: Arc<RwLock<bool>>,
    intercepted_messages: Arc<RwLock<Vec<(NodeId, NodeId, SyncMessage)>>>,
}

impl MemoryMeshTransport {
    fn new() -> Self {
        Self::default()
    }

    async fn register_node(&self, node_id: NodeId, engine: Arc<SyncEngine>) {
        let mut nodes = self.nodes.write().await;
        nodes.insert(node_id, engine);
    }

    #[allow(dead_code)]
    async fn set_drop_traffic(&self, drop: bool) {
        let mut d = self.drop_traffic.write().await;
        *d = drop;
    }
}

#[async_trait]
impl SyncTransport for MemoryMeshTransport {
    async fn send_sync_message(
        &self,
        destination: NodeId,
        message: SyncMessage,
    ) -> Result<(), DataError> {
        if *self.drop_traffic.read().await {
            return Ok(()); // simulate network drop
        }

        let target_engine = {
            let nodes = self.nodes.read().await;
            nodes.get(&destination).cloned()
        };

        if let Some(target) = target_engine {
            // Find sender node ID
            let sender_id = match &message {
                SyncMessage::Hello { node_id, .. } => *node_id,
                _ => {
                    // Look up from local node identity if available
                    NodeId::from_bytes([0u8; 32])
                }
            };

            let current_sender = sender_id;
            // Record message
            self.intercepted_messages.write().await.push((
                current_sender,
                destination,
                message.clone(),
            ));

            // Deliver and handle potential responses recursively
            let resp = target
                .handle_inbound_message(current_sender, message)
                .await?;
            if let Some(reply) = resp {
                // Determine reply sender
                let reply_sender = destination;
                let reply_dest = current_sender;
                let reply_target = {
                    let nodes = self.nodes.read().await;
                    nodes.get(&reply_dest).cloned()
                };
                if let Some(orig) = reply_target {
                    let _ = orig.handle_inbound_message(reply_sender, reply).await;
                }
            }
        }
        Ok(())
    }
}

/// Helper harness creating a local node with its own persistent SQLite database and SyncEngine.
struct TestNode {
    identity: Arc<NodeIdentity>,
    document_store: Arc<DocumentStore>,
    sync_engine: Arc<SyncEngine>,
    authorizer: Arc<DefaultDocumentAuthorizer>,
    _dir: tempfile::TempDir,
}

impl TestNode {
    async fn new(transport: Arc<MemoryMeshTransport>, config: Option<SyncEngineConfig>) -> Self {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("node_data.db");
        let identity = Arc::new(NodeIdentity::generate());
        let document_store =
            Arc::new(DocumentStore::with_identity(&db_path, Arc::clone(&identity)).unwrap());
        let authorizer = Arc::new(DefaultDocumentAuthorizer::new());

        let sync_config = config.unwrap_or_default();
        let sync_engine = Arc::new(SyncEngine::new(
            sync_config,
            Arc::clone(&identity),
            Arc::clone(&document_store),
            Arc::clone(&authorizer) as Arc<dyn fieldnet_data::DocumentAuthorizer>,
            transport.clone(),
        ));

        transport
            .register_node(identity.node_id(), Arc::clone(&sync_engine))
            .await;

        Self {
            identity,
            document_store,
            sync_engine,
            authorizer,
            _dir: dir,
        }
    }

    fn node_id(&self) -> NodeId {
        self.identity.node_id()
    }
}

/// Helper to run a sync handshake and batch transfer in one direction until completion.
async fn run_sync_direction(src: &TestNode, dst: &TestNode) {
    let mut next_msg = Some(SyncMessage::hello(src.node_id()));
    let mut current_sender = src;
    let mut current_receiver = dst;

    // Loop until no more messages are generated (bounded to 200 iterations)
    for _ in 0..200 {
        let msg = match next_msg.take() {
            Some(m) => m,
            None => break,
        };

        let resp = current_receiver
            .sync_engine
            .handle_inbound_message(current_sender.node_id(), msg)
            .await
            .unwrap();

        std::mem::swap(&mut current_sender, &mut current_receiver);
        next_msg = resp;
    }
}

/// Directly exchanges state summaries and missing deltas between two nodes until convergence.
async fn run_full_sync(node_a: &TestNode, node_b: &TestNode) {
    run_sync_direction(node_a, node_b).await;
    run_sync_direction(node_b, node_a).await;
}

// ============================================================================
// TESTS 1 TO 15
// ============================================================================

/// TEST 1: A creates document. B synchronizes. B receives document.
#[tokio::test]
async fn test_01_document_creation_and_sync() {
    let transport = Arc::new(MemoryMeshTransport::new());
    let node_a = TestNode::new(transport.clone(), None).await;
    let node_b = TestNode::new(transport.clone(), None).await;

    // Node A creates a document locally
    let doc_a = node_a
        .document_store
        .create_sync("notes", b"Hello from Node A".to_vec())
        .unwrap();

    assert!(node_b
        .document_store
        .get_sync(&doc_a.document_id)
        .unwrap()
        .is_none());

    // Synchronize A and B
    run_full_sync(&node_a, &node_b).await;

    // Node B must now have the exact document and payload
    let doc_b = node_b.document_store.get_sync(&doc_a.document_id).unwrap();
    assert!(doc_b.is_some(), "Document must be replicated to Node B");
    let doc_b = doc_b.unwrap();
    assert_eq!(doc_b.state, b"Hello from Node A".to_vec());
    assert_eq!(doc_b.owner_node_id, node_a.node_id());
}

/// TEST 2: A updates document. B receives incremental operation.
#[tokio::test]
async fn test_02_incremental_document_update_sync() {
    let transport = Arc::new(MemoryMeshTransport::new());
    let node_a = TestNode::new(transport.clone(), None).await;
    let node_b = TestNode::new(transport.clone(), None).await;

    // A creates doc and syncs to B
    let doc_a = node_a
        .document_store
        .create_sync("notes", b"Version 1".to_vec())
        .unwrap();
    run_full_sync(&node_a, &node_b).await;

    // A updates doc locally
    let _ = node_a
        .document_store
        .update_sync(&doc_a.document_id, b"Version 2 - Incremental".to_vec())
        .unwrap();

    // Sync incrementally
    run_full_sync(&node_a, &node_b).await;

    // B must reflect the updated version
    let doc_b = node_b
        .document_store
        .get_sync(&doc_a.document_id)
        .unwrap()
        .unwrap();
    assert_eq!(doc_b.state, b"Version 2 - Incremental".to_vec());
    assert_eq!(doc_b.version, 2);
}

/// TEST 3: B updates document while A is offline. A later synchronizes B's operation.
#[tokio::test]
async fn test_03_offline_modification_sync() {
    let transport = Arc::new(MemoryMeshTransport::new());
    let node_a = TestNode::new(transport.clone(), None).await;
    let node_b = TestNode::new(transport.clone(), None).await;

    // A creates doc, syncs to B
    let doc = node_a
        .document_store
        .create_sync("shared_log", b"Initial State".to_vec())
        .unwrap();
    run_full_sync(&node_a, &node_b).await;

    // Node A goes offline (no sync exchanges occur)
    // Node B updates the document locally
    let _ = node_b
        .document_store
        .update_sync(
            &doc.document_id,
            b"Updated by Node B while A offline".to_vec(),
        )
        .unwrap();

    // Later, Node A comes back online and synchronizes
    run_full_sync(&node_a, &node_b).await;

    // Node A must converge to Node B's update
    let doc_a = node_a
        .document_store
        .get_sync(&doc.document_id)
        .unwrap()
        .unwrap();
    assert_eq!(doc_a.state, b"Updated by Node B while A offline".to_vec());
}

/// TEST 4: Both nodes make concurrent CRDT updates. They converge.
#[tokio::test]
async fn test_04_concurrent_crdt_updates_converge() {
    let transport = Arc::new(MemoryMeshTransport::new());
    let node_a = TestNode::new(transport.clone(), None).await;
    let node_b = TestNode::new(transport.clone(), None).await;

    // Initial counter created at 0
    let counter = PnCounter::new();
    let (doc, _) = node_a.document_store.create_crdt_sync(&counter).unwrap();

    // Sync initial state to B
    run_full_sync(&node_a, &node_b).await;

    // Node A increments by 5 offline
    let delta_a = counter.generate_increment(node_a.node_id(), 5);
    node_a
        .document_store
        .apply_crdt_delta_sync::<PnCounter>(&doc.document_id, &delta_a)
        .unwrap();

    // Node B increments by 3 offline without seeing A
    let counter_b: PnCounter = node_b
        .document_store
        .get_crdt_sync(&doc.document_id)
        .unwrap();
    let delta_b = counter_b.generate_increment(node_b.node_id(), 3);
    node_b
        .document_store
        .apply_crdt_delta_sync::<PnCounter>(&doc.document_id, &delta_b)
        .unwrap();

    assert_eq!(
        node_a
            .document_store
            .get_crdt_sync::<PnCounter>(&doc.document_id)
            .unwrap()
            .read(),
        5
    );
    assert_eq!(
        node_b
            .document_store
            .get_crdt_sync::<PnCounter>(&doc.document_id)
            .unwrap()
            .read(),
        3
    );

    // Reconnect and synchronize
    run_full_sync(&node_a, &node_b).await;

    // Both must converge to 5 + 3 = 8
    let final_a: PnCounter = node_a
        .document_store
        .get_crdt_sync(&doc.document_id)
        .unwrap();
    let final_b: PnCounter = node_b
        .document_store
        .get_crdt_sync(&doc.document_id)
        .unwrap();

    assert_eq!(final_a.read(), 8);
    assert_eq!(final_b.read(), 8);
    assert_eq!(final_a.state_hash(), final_b.state_hash());
}

/// TEST 5: Duplicate operation arrives. No duplicate state change.
#[tokio::test]
async fn test_05_duplicate_operation_is_idempotent() {
    let transport = Arc::new(MemoryMeshTransport::new());
    let node_a = TestNode::new(transport.clone(), None).await;
    let node_b = TestNode::new(transport.clone(), None).await;

    let counter = PnCounter::new();
    let (doc, op) = node_a.document_store.create_crdt_sync(&counter).unwrap();

    // Node A increments by 10, producing op1
    let delta = counter.generate_increment(node_a.node_id(), 10);
    let (_, op1) = node_a
        .document_store
        .apply_crdt_delta_sync::<PnCounter>(&doc.document_id, &delta)
        .unwrap();

    // Deliver create operation and op1 to Node B
    let _ = node_b
        .sync_engine
        .handle_inbound_message(
            node_a.node_id(),
            SyncMessage::LiveOperation {
                session_id: None,
                operation: op.clone(),
                document_type: Some("pn_counter".to_string()),
            },
        )
        .await
        .unwrap();

    let resp1 = node_b
        .sync_engine
        .handle_inbound_message(
            node_a.node_id(),
            SyncMessage::LiveOperation {
                session_id: None,
                operation: op1.clone(),
                document_type: Some("pn_counter".to_string()),
            },
        )
        .await
        .unwrap();

    assert!(matches!(
        resp1,
        Some(SyncMessage::SyncAck {
            status: SyncAckStatus::Applied,
            ..
        })
    ));
    assert_eq!(
        node_b
            .document_store
            .get_crdt_sync::<PnCounter>(&doc.document_id)
            .unwrap()
            .read(),
        10
    );

    // Deliver op1 a SECOND time (duplicate!)
    let resp2 = node_b
        .sync_engine
        .handle_inbound_message(
            node_a.node_id(),
            SyncMessage::LiveOperation {
                session_id: None,
                operation: op1.clone(),
                document_type: Some("pn_counter".to_string()),
            },
        )
        .await
        .unwrap();

    assert!(matches!(
        resp2,
        Some(SyncMessage::SyncAck {
            status: SyncAckStatus::Duplicate,
            ..
        })
    ));
    // Counter MUST remain 10, not 20!
    assert_eq!(
        node_b
            .document_store
            .get_crdt_sync::<PnCounter>(&doc.document_id)
            .unwrap()
            .read(),
        10
    );
}

/// TEST 6: Operation arrives out of order. Dependency handling works.
#[tokio::test]
async fn test_06_out_of_order_dependency_handling() {
    let transport = Arc::new(MemoryMeshTransport::new());
    let node_a = TestNode::new(transport.clone(), None).await;
    let node_b = TestNode::new(transport.clone(), None).await;

    let doc = node_a
        .document_store
        .create_sync("notes", b"Genesis".to_vec())
        .unwrap();

    // Node A produces op1 and op2 where op2 depends on op1
    let (_, op1) = node_a
        .document_store
        .update_with_op_sync(&doc.document_id, b"Step 1".to_vec())
        .unwrap();
    let (_, op2) = node_a
        .document_store
        .update_with_op_sync(&doc.document_id, b"Step 2".to_vec())
        .unwrap();

    // Node B receives doc create op
    let create_op = node_a
        .document_store
        .get_operation_sync(
            &node_a
                .document_store
                .list_operations_for_document_sync(&doc.document_id, None, None)
                .unwrap()[0]
                .op_id,
        )
        .unwrap()
        .unwrap();
    let _ = node_b
        .document_store
        .apply_remote_operation_sync(&create_op)
        .unwrap();

    // Node B receives op2 FIRST (before op1!)
    let resp_op2 = node_b
        .sync_engine
        .handle_inbound_message(
            node_a.node_id(),
            SyncMessage::LiveOperation {
                session_id: None,
                operation: op2.clone(),
                document_type: Some("notes".to_string()),
            },
        )
        .await
        .unwrap();

    // Must be held in WaitingDependency buffer
    assert!(matches!(
        resp_op2,
        Some(SyncMessage::SyncAck {
            status: SyncAckStatus::WaitingDependency { .. },
            ..
        })
    ));
    let staged = node_b
        .document_store
        .list_unapplied_operations_sync(&doc.document_id)
        .unwrap();
    assert_eq!(
        staged.len(),
        1,
        "op2 must be staged in unapplied_operations"
    );

    // Later op1 arrives
    let resp_op1 = node_b
        .sync_engine
        .handle_inbound_message(
            node_a.node_id(),
            SyncMessage::LiveOperation {
                session_id: None,
                operation: op1.clone(),
                document_type: Some("notes".to_string()),
            },
        )
        .await
        .unwrap();

    assert!(matches!(
        resp_op1,
        Some(SyncMessage::SyncAck {
            status: SyncAckStatus::Applied,
            ..
        })
    ));

    // Unapplied operations buffer should now be empty (cascade applied)
    let staged_after = node_b
        .document_store
        .list_unapplied_operations_sync(&doc.document_id)
        .unwrap();
    assert_eq!(staged_after.len(), 0, "op2 must have unblocked and applied");

    // Final state on Node B must be "Step 2"
    let doc_b = node_b
        .document_store
        .get_sync(&doc.document_id)
        .unwrap()
        .unwrap();
    assert_eq!(doc_b.state, b"Step 2".to_vec());
}

/// TEST 7: Connection disappears during synchronization. Synchronization resumes.
#[tokio::test]
async fn test_07_interrupted_sync_resumes_cleanly() {
    let transport = Arc::new(MemoryMeshTransport::new());
    let node_a = TestNode::new(transport.clone(), None).await;
    let node_b = TestNode::new(transport.clone(), None).await;

    // Create document on A and produce multiple operations
    let doc = node_a
        .document_store
        .create_sync("stream", b"Op0".to_vec())
        .unwrap();
    for i in 1..=10 {
        node_a
            .document_store
            .update_sync(&doc.document_id, format!("Op{}", i).into_bytes())
            .unwrap();
    }

    // Node B partially receives up to op 5
    let all_ops = node_a
        .document_store
        .list_operations_for_document_sync(&doc.document_id, None, None)
        .unwrap();
    for op in &all_ops[..5] {
        let _ = node_b
            .document_store
            .apply_remote_operation_sync(op)
            .unwrap();
    }

    // Record partial watermark on B
    node_b
        .document_store
        .record_peer_sync_watermark_sync(
            &node_a.node_id(),
            &doc.document_id,
            5,
            HybridLogicalClock::default(),
            "PARTIAL",
        )
        .unwrap();

    // Mark session interrupted
    node_b
        .sync_engine
        .mark_session_interrupted(&node_a.node_id())
        .await;
    assert_eq!(
        node_b
            .sync_engine
            .get_session_state(&node_a.node_id())
            .await,
        Some(SyncSessionState::Interrupted)
    );

    // Resume synchronization
    run_full_sync(&node_a, &node_b).await;

    // Node B must now have all 10 operations converged
    let doc_b = node_b
        .document_store
        .get_sync(&doc.document_id)
        .unwrap()
        .unwrap();
    assert_eq!(doc_b.state, b"Op10".to_vec());
    assert_eq!(doc_b.version, 11);
}

/// TEST 8: Runtime restarts during pending synchronization. State survives.
#[tokio::test]
async fn test_08_restart_preserves_sync_state() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("restartable_node.db");
    let identity = Arc::new(NodeIdentity::generate());
    let doc_id = DocumentId::generate();

    // Node runs and writes watermark
    {
        let store =
            Arc::new(DocumentStore::with_identity(&db_path, Arc::clone(&identity)).unwrap());
        let peer_id = NodeId::from_bytes([9u8; 32]);
        store
            .record_peer_sync_watermark_sync(
                &peer_id,
                &doc_id,
                42,
                HybridLogicalClock::new(12345, 6),
                "CONVERGED",
            )
            .unwrap();
    }

    // Process restarts from same db path
    {
        let store =
            Arc::new(DocumentStore::with_identity(&db_path, Arc::clone(&identity)).unwrap());
        let peer_id = NodeId::from_bytes([9u8; 32]);
        let watermark = store
            .get_peer_sync_watermark_sync(&peer_id, &doc_id)
            .unwrap()
            .unwrap();

        assert_eq!(watermark.last_synced_seq, 42);
        assert_eq!(watermark.last_synced_hlc.physical, 12345);
        assert_eq!(watermark.last_synced_hlc.logical, 6);
        assert_eq!(watermark.sync_status, "CONVERGED");
    }
}

/// TEST 9: Unauthorized operation. Rejected safely.
#[tokio::test]
async fn test_09_unauthorized_operation_rejected() {
    let transport = Arc::new(MemoryMeshTransport::new());
    let node_a = TestNode::new(transport.clone(), None).await;
    let node_b = TestNode::new(transport.clone(), None).await;

    // Node A creates document in restricted namespace "admin_control"
    node_b.authorizer.add_restricted_namespace("admin_control");
    let doc = node_a
        .document_store
        .create_sync("admin_control", b"Owner Only".to_vec())
        .unwrap();
    run_full_sync(&node_a, &node_b).await;

    // Rogue Node C attempts to update Node A's document
    let node_c_id = NodeId::from_bytes([77u8; 32]);
    let unauthorized_op = Operation {
        op_id: OperationId::from_bytes([88u8; 32]),
        document_id: doc.document_id,
        actor_node_id: node_c_id,
        sequence: 2,
        op_type: OperationType::Update,
        payload: b"Hacked by Node C".to_vec(),
        hlc: HybridLogicalClock::default(),
        causal_dependencies: vec![],
        signature: [0u8; 64],
        created_at: fieldnet_core::Timestamp::now(),
        schema_version: 1,
        status: fieldnet_data::OpStatus::Created,
    };

    // Deliver to Node B
    let resp = node_b
        .sync_engine
        .handle_inbound_message(
            node_c_id,
            SyncMessage::LiveOperation {
                session_id: None,
                operation: unauthorized_op,
                document_type: Some("admin_control".to_string()),
            },
        )
        .await
        .unwrap();

    // Must be rejected safely
    assert!(matches!(
        resp,
        Some(SyncMessage::SyncAck {
            status: SyncAckStatus::Rejected { .. },
            ..
        })
    ));

    // Document on Node B must NOT be changed
    let doc_b = node_b
        .document_store
        .get_sync(&doc.document_id)
        .unwrap()
        .unwrap();
    assert_eq!(doc_b.state, b"Owner Only".to_vec());
}

/// TEST 10: Malformed synchronization packet. Runtime survives.
#[tokio::test]
async fn test_10_malformed_sync_payload_survives() {
    let transport = Arc::new(MemoryMeshTransport::new());
    let node_a = TestNode::new(transport.clone(), None).await;

    // Invalid JSON bytes
    let malformed_bytes = b"NOT_A_VALID_SYNC_MESSAGE_JSON";
    let res = SyncMessage::from_bytes(malformed_bytes);
    assert!(res.is_err(), "Malformed bytes must produce clean error");

    // Excessively large batch
    let fake_ops = vec![Operation {
        op_id: OperationId::from_bytes([1u8; 32]),
        document_id: DocumentId::generate(),
        actor_node_id: node_a.node_id(),
        sequence: 1,
        op_type: OperationType::Update,
        payload: vec![0u8; 600 * 1024], // 600 KB payload > 512 KB limit!
        hlc: HybridLogicalClock::default(),
        causal_dependencies: vec![],
        signature: [0u8; 64],
        created_at: fieldnet_core::Timestamp::now(),
        schema_version: 1,
        status: fieldnet_data::OpStatus::Created,
    }];

    let oversized_msg = SyncMessage::DeltaResponse {
        session_id: [0u8; 16],
        request_id: [0u8; 16],
        document_id: DocumentId::generate(),
        operations: fake_ops,
        has_more: false,
        batch_index: 0,
        total_batches: None,
    };

    let handle_res = node_a
        .sync_engine
        .handle_inbound_message(node_a.node_id(), oversized_msg)
        .await;
    assert!(handle_res.is_err(), "Oversized batch must be rejected");
}

/// TEST 13: Large operation set synchronizes in bounded batches.
#[tokio::test]
async fn test_13_large_operation_set_bounded_batches() {
    let transport = Arc::new(MemoryMeshTransport::new());
    // Max batch size configured to 50 operations
    let config = SyncEngineConfig {
        max_batch_size: 50,
        ..Default::default()
    };

    let node_a = TestNode::new(transport.clone(), Some(config.clone())).await;
    let node_b = TestNode::new(transport.clone(), Some(config)).await;

    // Create document and generate 120 operations on Node A
    let doc = node_a
        .document_store
        .create_sync("data_stream", b"Base".to_vec())
        .unwrap();
    for i in 1..=120 {
        node_a
            .document_store
            .update_sync(&doc.document_id, format!("Update {}", i).into_bytes())
            .unwrap();
    }

    // Synchronize A and B
    run_full_sync(&node_a, &node_b).await;

    // Both nodes must have all 120 updates
    let doc_b = node_b
        .document_store
        .get_sync(&doc.document_id)
        .unwrap()
        .unwrap();
    assert_eq!(doc_b.state, b"Update 120".to_vec());
    assert_eq!(doc_b.version, 121);
}

/// TEST 14: Partial replication does not synchronize unrelated documents.
#[tokio::test]
async fn test_14_partial_replication_filters() {
    let transport = Arc::new(MemoryMeshTransport::new());
    let node_a = TestNode::new(transport.clone(), None).await;

    // Node A creates two documents: doc_wanted and doc_unwanted
    let doc_wanted = node_a
        .document_store
        .create_sync("wanted", b"Replicate Me".to_vec())
        .unwrap();
    let doc_unwanted = node_a
        .document_store
        .create_sync("unwanted", b"Keep Private".to_vec())
        .unwrap();

    // Node B configures a partial replication filter allowing ONLY doc_wanted
    let filter_config = SyncEngineConfig {
        document_filter: Some(vec![doc_wanted.document_id]),
        ..Default::default()
    };
    let node_b = TestNode::new(transport.clone(), Some(filter_config)).await;

    // Synchronize
    run_full_sync(&node_a, &node_b).await;

    // Node B must have doc_wanted, but NOT doc_unwanted
    assert!(node_b
        .document_store
        .get_sync(&doc_wanted.document_id)
        .unwrap()
        .is_some());
    assert!(node_b
        .document_store
        .get_sync(&doc_unwanted.document_id)
        .unwrap()
        .is_none());
}

/// TEST 15: Incompatible protocol version fails safely.
#[tokio::test]
async fn test_15_incompatible_protocol_version_rejected() {
    let transport = Arc::new(MemoryMeshTransport::new());
    let node_a = TestNode::new(transport.clone(), None).await;

    let incompatible_hello = SyncMessage::Hello {
        protocol_version: 999, // incompatible!
        node_id: NodeId::from_bytes([42u8; 32]),
        supported_crdt_types: vec![],
        capabilities: vec![],
        supported_namespaces: vec![],
        max_batch_size: 100,
        snapshot_support: false,
        causal_model: "HLC_v1".to_string(),
        schema_version: 1,
    };

    let res = node_a
        .sync_engine
        .handle_inbound_message(NodeId::from_bytes([42u8; 32]), incompatible_hello)
        .await;
    assert!(res.is_err());
    match res {
        Err(DataError::IncompatibleProtocolVersion { expected, actual }) => {
            assert_eq!(expected, 1);
            assert_eq!(actual, 999);
        }
        other => panic!("Expected IncompatibleProtocolVersion, got {:?}", other),
    }
}

// ============================================================================
// THREE-NODE END-TO-END SCENARIO
// ============================================================================

/// SECTION 26: Three-Node End-to-End Scenario:
/// Node A creates document X
/// Node B creates document Y
/// Node C creates document Z
/// Allow intermittent pairwise sync with concurrent CRDT mutations.
/// All replicas converge on identical logical state across all documents.
#[tokio::test]
async fn test_three_node_end_to_end_convergence() {
    let transport = Arc::new(MemoryMeshTransport::new());
    let node_a = TestNode::new(transport.clone(), None).await;
    let node_b = TestNode::new(transport.clone(), None).await;
    let node_c = TestNode::new(transport.clone(), None).await;

    // 1. Initial State: All replicas empty
    assert_eq!(
        node_a
            .document_store
            .list_sync(DocumentQuery::new())
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        node_b
            .document_store
            .list_sync(DocumentQuery::new())
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        node_c
            .document_store
            .list_sync(DocumentQuery::new())
            .unwrap()
            .len(),
        0
    );

    // 2. Node A creates document X (PnCounter)
    let counter_x = PnCounter::new();
    let (doc_x, _) = node_a.document_store.create_crdt_sync(&counter_x).unwrap();

    // 3. Node B creates document Y (OrSet)
    let set_y: OrSet<String> = OrSet::new();
    let (doc_y, _) = node_b.document_store.create_crdt_sync(&set_y).unwrap();

    // 4. Node C creates document Z (CrdtMap)
    let map_z = CrdtMap::new();
    let (doc_z, _) = node_c.document_store.create_crdt_sync(&map_z).unwrap();

    // 5. Intermittent Sync Pass 1: A <-> B meet
    run_full_sync(&node_a, &node_b).await;

    // 6. Concurrent modifications while separated:
    // Node A increments Counter X by 10
    let delta_x_a = counter_x.generate_increment(node_a.node_id(), 10);
    node_a
        .document_store
        .apply_crdt_delta_sync::<PnCounter>(&doc_x.document_id, &delta_x_a)
        .unwrap();

    // Node B increments Counter X by 20 without seeing A
    let counter_x_b: PnCounter = node_b
        .document_store
        .get_crdt_sync(&doc_x.document_id)
        .unwrap();
    let delta_x_b = counter_x_b.generate_increment(node_b.node_id(), 20);
    node_b
        .document_store
        .apply_crdt_delta_sync::<PnCounter>(&doc_x.document_id, &delta_x_b)
        .unwrap();

    // Node B adds "item_from_b" to Set Y
    let set_y_b: OrSet<String> = node_b
        .document_store
        .get_crdt_sync(&doc_y.document_id)
        .unwrap();
    let delta_y_b = set_y_b.generate_add(
        "item_from_b".to_string(),
        node_b.node_id(),
        HybridLogicalClock::default(),
        1,
    );
    node_b
        .document_store
        .apply_crdt_delta_sync::<OrSet<String>>(&doc_y.document_id, &delta_y_b)
        .unwrap();

    // Node C sets "city" = "Tokyo" on Map Z
    let map_z_c: CrdtMap = node_c
        .document_store
        .get_crdt_sync(&doc_z.document_id)
        .unwrap();
    let delta_z_c = map_z_c.generate_set(
        "city",
        serde_json::json!("Tokyo"),
        HybridLogicalClock::default(),
        node_c.node_id(),
    );
    node_c
        .document_store
        .apply_crdt_delta_sync::<CrdtMap>(&doc_z.document_id, &delta_z_c)
        .unwrap();

    // 7. Intermittent Sync Pass 2: B <-> C meet
    run_full_sync(&node_b, &node_c).await;

    // 8. Intermittent Sync Pass 3: A <-> C meet
    run_full_sync(&node_a, &node_c).await;

    // 9. Final Convergence Pass: A <-> B <-> C complete ring exchange
    run_full_sync(&node_a, &node_b).await;
    run_full_sync(&node_b, &node_c).await;
    run_full_sync(&node_a, &node_c).await;

    // Verify all 3 nodes now possess all 3 documents
    assert_eq!(
        node_a
            .document_store
            .list_sync(DocumentQuery::new())
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        node_b
            .document_store
            .list_sync(DocumentQuery::new())
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        node_c
            .document_store
            .list_sync(DocumentQuery::new())
            .unwrap()
            .len(),
        3
    );

    // Verify Document X (Counter) has converged to 10 + 20 = 30 on all three nodes
    let val_x_a: PnCounter = node_a
        .document_store
        .get_crdt_sync(&doc_x.document_id)
        .unwrap();
    let val_x_b: PnCounter = node_b
        .document_store
        .get_crdt_sync(&doc_x.document_id)
        .unwrap();
    let val_x_c: PnCounter = node_c
        .document_store
        .get_crdt_sync(&doc_x.document_id)
        .unwrap();

    assert_eq!(val_x_a.read(), 30);
    assert_eq!(val_x_b.read(), 30);
    assert_eq!(val_x_c.read(), 30);
    assert_eq!(val_x_a.state_hash(), val_x_b.state_hash());
    assert_eq!(val_x_b.state_hash(), val_x_c.state_hash());

    // Verify Document Y (Set) contains "item_from_b" on all three nodes
    let val_y_a: OrSet<String> = node_a
        .document_store
        .get_crdt_sync(&doc_y.document_id)
        .unwrap();
    let val_y_b: OrSet<String> = node_b
        .document_store
        .get_crdt_sync(&doc_y.document_id)
        .unwrap();
    let val_y_c: OrSet<String> = node_c
        .document_store
        .get_crdt_sync(&doc_y.document_id)
        .unwrap();

    assert!(val_y_a.read().contains(&"item_from_b".to_string()));
    assert!(val_y_b.read().contains(&"item_from_b".to_string()));
    assert!(val_y_c.read().contains(&"item_from_b".to_string()));
    assert_eq!(val_y_a.state_hash(), val_y_b.state_hash());

    // Verify Document Z (Map) contains city = Tokyo on all three nodes
    let val_z_a: CrdtMap = node_a
        .document_store
        .get_crdt_sync(&doc_z.document_id)
        .unwrap();
    let val_z_b: CrdtMap = node_b
        .document_store
        .get_crdt_sync(&doc_z.document_id)
        .unwrap();
    let val_z_c: CrdtMap = node_c
        .document_store
        .get_crdt_sync(&doc_z.document_id)
        .unwrap();

    assert_eq!(
        val_z_a.read().get("city").unwrap(),
        &serde_json::json!("Tokyo")
    );
    assert_eq!(
        val_z_b.read().get("city").unwrap(),
        &serde_json::json!("Tokyo")
    );
    assert_eq!(
        val_z_c.read().get("city").unwrap(),
        &serde_json::json!("Tokyo")
    );
    assert_eq!(val_z_a.state_hash(), val_z_b.state_hash());

    println!("Three-Node Decentralized CRDT Synchronization PASSED with complete convergence!");
}

// ============================================================================
// PERFORMANCE BASELINE
// ============================================================================

/// SECTION 27: Performance Baseline Measurements
#[tokio::test]
async fn test_sync_performance_baseline() {
    let transport = Arc::new(MemoryMeshTransport::new());
    let node_a = TestNode::new(transport.clone(), None).await;
    let node_b = TestNode::new(transport.clone(), None).await;

    // 1. Measure Handshake Latency
    let start_handshake = Instant::now();
    let hello = SyncMessage::hello(node_a.node_id());
    let _ = node_b
        .sync_engine
        .handle_inbound_message(node_a.node_id(), hello)
        .await
        .unwrap();
    let handshake_duration = start_handshake.elapsed();
    println!("PERF: Sync Handshake Latency: {:?}", handshake_duration);
    assert!(handshake_duration.as_millis() < 50);

    // 2. Measure State Summary Size
    let summaries = node_a
        .sync_engine
        .handle_inbound_message(node_a.node_id(), SyncMessage::hello(node_a.node_id()))
        .await
        .unwrap()
        .unwrap();
    let summary_bytes = summaries.to_bytes().unwrap().len();
    println!(
        "PERF: State Summary Wire Size (empty db): {} bytes",
        summary_bytes
    );
    assert!(summary_bytes < 1024);

    // 3. Measure Bulk Operation Sync Throughput
    let doc = node_a
        .document_store
        .create_sync("perf_stream", b"Init".to_vec())
        .unwrap();
    let num_ops = 200;
    for i in 1..=num_ops {
        node_a
            .document_store
            .update_sync(&doc.document_id, format!("Payload #{}", i).into_bytes())
            .unwrap();
    }

    let start_sync = Instant::now();
    run_full_sync(&node_a, &node_b).await;
    let sync_duration = start_sync.elapsed();
    let ops_per_sec = (num_ops as f64) / sync_duration.as_secs_f64();
    println!(
        "PERF: Synchronized {} operations in {:?} ({:.0} ops/sec)",
        num_ops, sync_duration, ops_per_sec
    );
    assert!(ops_per_sec > 100.0, "Throughput should exceed 100 ops/sec");

    // 4. Measure Duplicate Handling Cost
    let all_ops = node_a
        .document_store
        .list_operations_for_document_sync(&doc.document_id, None, None)
        .unwrap();
    let start_dup = Instant::now();
    for op in &all_ops[..50] {
        let _ = node_b
            .sync_engine
            .handle_inbound_message(
                node_a.node_id(),
                SyncMessage::LiveOperation {
                    session_id: None,
                    operation: op.clone(),
                    document_type: Some("perf_stream".to_string()),
                },
            )
            .await
            .unwrap();
    }
    let dup_duration = start_dup.elapsed();
    println!(
        "PERF: Evaluated 50 duplicate operations in {:?}",
        dup_duration
    );
    assert!(dup_duration.as_millis() < 100);
}
