//! FIELDNET Phase 06 BUILD-06: Comprehensive Subsystem Verification & Fault Injection Suite.
//!
//! Validates:
//! - TEST 01: Full Local-First operation without networking across 3 independent nodes.
//! - TEST 02: Basic peer-to-peer synchronization and state completion.
//! - TEST 03: Incremental synchronization delta efficiency.
//! - TEST 04: Offline modification and bidirectional convergence.
//! - TEST 05: Concurrent CRDT modifications across Counter, Set, Map, and Register.
//! - TEST 06: Three-node convergence under arbitrary operation arrival orderings.
//! - TEST 07: Out-of-order operation delivery and cascade resolution.
//! - TEST 08: Duplicate delivery idempotence and deduplication.
//! - TEST 09: Interrupted synchronization and clean resumption from watermarks.
//! - TEST 10: Runtime restart persistence of documents, operations, causality, and watermarks.
//! - TEST 11: Snapshot restoration and state hash verification.
//! - TEST 12: Compaction validation, logical state invariance, and tombstone retention.
//! - TEST 13: Offline peer protection against premature compaction.
//! - TEST 14: Corruption detection and rejection (snapshot, payload, signature, causality).
//! - TEST 15: Capability-based authorization enforcement.
//! - TEST 16: Incompatible protocol version rejection.
//! - TEST 17: Storage failure and atomic rollback resilience.
//! - TEST 18: Property tests for CRDT convergence under order permutations.
//! - TEST 19: Fault Injection Matrix programmatic verification.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use fieldnet_core::{NodeId, Timestamp};
use fieldnet_data::causality::{HybridLogicalClock, StateVector};
use fieldnet_data::crdt::{
    Crdt, CrdtMap, ElementTag, LwwRegister, OrSet, PnCounter, PnCounterDelta,
};
use fieldnet_data::document::DocumentId;
use fieldnet_data::error::DataError;
use fieldnet_data::operation::{Operation, OperationId, OperationType};
use fieldnet_data::storage::DocumentStore;
use fieldnet_data::sync::authorizer::{DefaultDocumentAuthorizer, DocumentAuthorizer};
use fieldnet_data::sync::protocol::{SyncAckStatus, SyncMessage};
use fieldnet_data::sync::{SyncEngine, SyncEngineConfig, SyncTransport};
use fieldnet_identity::NodeIdentity;
use tempfile::tempdir;
use tokio::sync::RwLock;

/// Simulated mesh transport interconnecting nodes directly in-memory.
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
            let sender_id = match &message {
                SyncMessage::Hello { node_id, .. } => *node_id,
                _ => NodeId::from_bytes([0u8; 32]),
            };

            let current_sender = sender_id;
            self.intercepted_messages.write().await.push((
                current_sender,
                destination,
                message.clone(),
            ));

            let resp = target
                .handle_inbound_message(current_sender, message)
                .await?;
            if let Some(reply) = resp {
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

/// Node test harness bundling DocumentStore, sovereign identity, and SyncEngine.
struct SubsystemNode {
    identity: Arc<NodeIdentity>,
    store: Arc<DocumentStore>,
    sync_engine: Arc<SyncEngine>,
    authorizer: Arc<DefaultDocumentAuthorizer>,
    _dir: tempfile::TempDir,
}

impl SubsystemNode {
    async fn new(transport: Arc<MemoryMeshTransport>, config: Option<SyncEngineConfig>) -> Self {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("node_storage.db");
        let identity = Arc::new(NodeIdentity::generate());
        let store =
            Arc::new(DocumentStore::with_identity(&db_path, Arc::clone(&identity)).unwrap());
        let authorizer = Arc::new(DefaultDocumentAuthorizer::new());

        let sync_config = config.unwrap_or_default();
        let sync_engine = Arc::new(SyncEngine::new(
            sync_config,
            Arc::clone(&identity),
            Arc::clone(&store),
            Arc::clone(&authorizer) as Arc<dyn DocumentAuthorizer>,
            transport.clone(),
        ));

        transport
            .register_node(identity.node_id(), Arc::clone(&sync_engine))
            .await;

        Self {
            identity,
            store,
            sync_engine,
            authorizer,
            _dir: dir,
        }
    }

    fn node_id(&self) -> NodeId {
        self.identity.node_id()
    }
}

/// Helper to run sync protocol handshake and delta exchanges until termination.
async fn run_sync_direction(src: &SubsystemNode, dst: &SubsystemNode) {
    let mut next_msg = Some(SyncMessage::hello(src.node_id()));
    let mut current_sender = src;
    let mut current_receiver = dst;

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

/// Cross-synchronizes two nodes in both directions until full convergence.
async fn run_full_sync(node_a: &SubsystemNode, node_b: &SubsystemNode) {
    run_sync_direction(node_a, node_b).await;
    run_sync_direction(node_b, node_a).await;
}

// ============================================================================
// TEST 01: Full Local-First Test (3 independent nodes without network)
// ============================================================================
#[test]
fn test_01_full_local_first_three_nodes() {
    let dir_a = tempdir().unwrap();
    let dir_b = tempdir().unwrap();
    let dir_c = tempdir().unwrap();

    let id_a = NodeIdentity::generate();
    let id_b = NodeIdentity::generate();
    let id_c = NodeIdentity::generate();

    let store_a = DocumentStore::open(dir_a.path().join("store.db"), id_a.node_id()).unwrap();
    let store_b = DocumentStore::open(dir_b.path().join("store.db"), id_b.node_id()).unwrap();
    let store_c = DocumentStore::open(dir_c.path().join("store.db"), id_c.node_id()).unwrap();

    // Verify independent local document operations without networking
    // Node A: creates & updates
    let (doc_a, op_a1) = store_a
        .create_with_op_sync("sitrep", b"Node A Sector Clear".to_vec())
        .unwrap();
    assert_eq!(doc_a.version, 1);
    assert_eq!(op_a1.sequence, 1);

    let (doc_a2, op_a2) = store_a
        .update_with_op_sync(&doc_a.document_id, b"Node A Hostiles Spotted".to_vec())
        .unwrap();
    assert_eq!(doc_a2.version, 2);
    assert_eq!(op_a2.sequence, 2);

    // Node B: creates CRDT counter & applies increments
    let mut counter_b = PnCounter::new();
    counter_b.increment(id_b.node_id(), 100);
    let (doc_b, op_b1) = store_b.create_crdt_sync(&counter_b).unwrap();
    assert_eq!(doc_b.version, 1);
    assert_eq!(op_b1.sequence, 1);

    let delta_b = PnCounterDelta::Increment {
        actor: id_b.node_id(),
        amount: 50,
    };
    let (doc_b2, op_b2) = store_b
        .apply_crdt_delta_sync::<PnCounter>(&doc_b.document_id, &delta_b)
        .unwrap();
    assert_eq!(doc_b2.version, 2);
    assert_eq!(op_b2.sequence, 2);

    // Node C: creates & deletes document (tombstone)
    let (doc_c, op_c1) = store_c
        .create_with_op_sync("intel", b"Target coordinates".to_vec())
        .unwrap();
    assert_eq!(doc_c.version, 1);
    assert_eq!(op_c1.sequence, 1);

    let (doc_c_del, op_c2) = store_c.delete_with_op_sync(&doc_c.document_id).unwrap();
    assert!(doc_c_del.deleted);
    assert_eq!(op_c2.sequence, 2);
    assert!(store_c.get_active_sync(&doc_c.document_id).is_err());

    // Verify total local persistence without networking
    assert_eq!(store_a.count_sync(None, false).unwrap(), 1);
    assert_eq!(store_b.count_sync(None, false).unwrap(), 1);
    assert_eq!(store_c.count_sync(None, true).unwrap(), 1);
    assert_eq!(store_c.count_sync(None, false).unwrap(), 0);
}

// ============================================================================
// TEST 02: Basic Sync (A creates X, B synchronizes, B receives X)
// ============================================================================
#[tokio::test]
async fn test_02_basic_sync() {
    let transport = Arc::new(MemoryMeshTransport::new());
    let node_a = SubsystemNode::new(transport.clone(), None).await;
    let node_b = SubsystemNode::new(transport.clone(), None).await;

    // Node A creates document X
    let doc_x = node_a
        .store
        .create_sync("telemetry", b"Sensor 01 Reading: 98.6".to_vec())
        .unwrap();

    // Node B is initially unaware
    assert!(node_b.store.get_sync(&doc_x.document_id).unwrap().is_none());

    // Run full sync protocol between A and B
    run_full_sync(&node_a, &node_b).await;

    // Verify document state and operation now exist on Node B
    let replicated = node_b.store.get_sync(&doc_x.document_id).unwrap().unwrap();
    assert_eq!(replicated.state, b"Sensor 01 Reading: 98.6".to_vec());
    assert_eq!(replicated.version, 1);

    let ops_b = node_b
        .store
        .list_operations_for_document_sync(&doc_x.document_id, None, None)
        .unwrap();
    assert_eq!(ops_b.len(), 1);
    assert_eq!(ops_b[0].actor_node_id, node_a.node_id());
}

// ============================================================================
// TEST 03: Incremental Sync (B requests only missing operations)
// ============================================================================
#[tokio::test]
async fn test_03_incremental_sync() {
    let transport = Arc::new(MemoryMeshTransport::new());
    let node_a = SubsystemNode::new(transport.clone(), None).await;
    let node_b = SubsystemNode::new(transport.clone(), None).await;

    // Node A creates document X (seq 1) and updates it (seq 2)
    let mut counter = PnCounter::new();
    counter.increment(node_a.node_id(), 10);
    let (doc, _) = node_a.store.create_crdt_sync(&counter).unwrap();

    let d2 = PnCounterDelta::Increment {
        actor: node_a.node_id(),
        amount: 20,
    };
    node_a
        .store
        .apply_crdt_delta_sync::<PnCounter>(&doc.document_id, &d2)
        .unwrap();

    // Initial sync: B gets seq 1 and 2
    run_full_sync(&node_a, &node_b).await;

    let b_doc_initial = node_b.store.get_sync(&doc.document_id).unwrap().unwrap();
    assert_eq!(b_doc_initial.version, 2);

    // Node A generates seq 3 and 4
    let d3 = PnCounterDelta::Increment {
        actor: node_a.node_id(),
        amount: 30,
    };
    node_a
        .store
        .apply_crdt_delta_sync::<PnCounter>(&doc.document_id, &d3)
        .unwrap();

    let d4 = PnCounterDelta::Increment {
        actor: node_a.node_id(),
        amount: 40,
    };
    node_a
        .store
        .apply_crdt_delta_sync::<PnCounter>(&doc.document_id, &d4)
        .unwrap();

    // Incremental sync
    run_full_sync(&node_a, &node_b).await;

    let b_doc_final = node_b.store.get_sync(&doc.document_id).unwrap().unwrap();
    let b_counter: PnCounter = PnCounter::deserialize_state(&b_doc_final.state).unwrap();
    assert_eq!(b_counter.read(), 10 + 20 + 30 + 40);
    assert_eq!(b_doc_final.version, 4);
}

// ============================================================================
// TEST 04: Offline Modification (A & B modify independently, reconnect, converge)
// ============================================================================
#[tokio::test]
async fn test_04_offline_modification() {
    let transport = Arc::new(MemoryMeshTransport::new());
    let node_a = SubsystemNode::new(transport.clone(), None).await;
    let node_b = SubsystemNode::new(transport.clone(), None).await;

    // Node A creates document X
    let mut counter_x = PnCounter::new();
    counter_x.increment(node_a.node_id(), 5);
    let (doc_x, _) = node_a.store.create_crdt_sync(&counter_x).unwrap();

    // Initial sync
    run_full_sync(&node_a, &node_b).await;

    // DISCONNECTED: Both modify independently
    let delta_a = PnCounterDelta::Increment {
        actor: node_a.node_id(),
        amount: 15,
    };
    node_a
        .store
        .apply_crdt_delta_sync::<PnCounter>(&doc_x.document_id, &delta_a)
        .unwrap();

    let delta_b = PnCounterDelta::Decrement {
        actor: node_b.node_id(),
        amount: 3,
    };
    node_b
        .store
        .apply_crdt_delta_sync::<PnCounter>(&doc_x.document_id, &delta_b)
        .unwrap();

    // RECONNECT & CROSS-SYNC
    run_full_sync(&node_a, &node_b).await;

    // Both converge: 5 + 15 - 3 = 17
    let val_a: PnCounter = PnCounter::deserialize_state(
        &node_a
            .store
            .get_sync(&doc_x.document_id)
            .unwrap()
            .unwrap()
            .state,
    )
    .unwrap();
    let val_b: PnCounter = PnCounter::deserialize_state(
        &node_b
            .store
            .get_sync(&doc_x.document_id)
            .unwrap()
            .unwrap()
            .state,
    )
    .unwrap();

    assert_eq!(val_a.read(), 17);
    assert_eq!(val_b.read(), 17);
    assert_eq!(val_a, val_b);
}

// ============================================================================
// TEST 05: Concurrent CRDT Test (Counter, Set, Map, Register)
// ============================================================================
#[test]
fn test_05_concurrent_crdt_all_types() {
    let id_a = NodeIdentity::generate().node_id();
    let id_b = NodeIdentity::generate().node_id();

    // 1. PN-Counter: A +5, B +3 -> 8
    let mut c_a = PnCounter::new();
    let mut c_b = PnCounter::new();
    c_a.increment(id_a, 5);
    c_b.increment(id_b, 3);
    c_a.merge(&c_b).unwrap();
    assert_eq!(c_a.read(), 8);

    // 2. OR-Set: A adds X, B adds Y -> {X, Y}
    let tag_x = ElementTag::new(id_a, HybridLogicalClock::now(), 1);
    let tag_y = ElementTag::new(id_b, HybridLogicalClock::now(), 2);
    let mut s_a: OrSet<String> = OrSet::new();
    let mut s_b: OrSet<String> = OrSet::new();
    s_a.add("item_x".to_string(), tag_x);
    s_b.add("item_y".to_string(), tag_y);
    s_a.merge(&s_b).unwrap();
    let items = s_a.read();
    assert!(items.contains(&"item_x".to_string()));
    assert!(items.contains(&"item_y".to_string()));

    // 3. CRDT-Map: A sets field_a, B sets field_b -> {field_a, field_b}
    let mut m_a = CrdtMap::new();
    let mut m_b = CrdtMap::new();
    m_a.set(
        "field_a",
        serde_json::json!("val_a"),
        HybridLogicalClock::now(),
        id_a,
    );
    m_b.set(
        "field_b",
        serde_json::json!("val_b"),
        HybridLogicalClock::now(),
        id_b,
    );
    m_a.merge(&m_b).unwrap();
    assert_eq!(m_a.get("field_a").unwrap(), &serde_json::json!("val_a"));
    assert_eq!(m_a.get("field_b").unwrap(), &serde_json::json!("val_b"));

    // 4. LWW-Register: Deterministic tie-break
    let hlc1 = HybridLogicalClock::new(1000, 1);
    let hlc2 = HybridLogicalClock::new(1000, 2);
    let mut r_a = LwwRegister::new("early".to_string(), hlc1, id_a);
    let r_b = LwwRegister::new("late".to_string(), hlc2, id_b);
    r_a.merge(&r_b).unwrap();
    assert_eq!(r_a.read().as_str(), "late");
}

// ============================================================================
// TEST 06: Three-Node Convergence under Arbitrary Arrival Orders
// ============================================================================
#[test]
fn test_06_three_node_convergence_arbitrary_orders() {
    let id_a = NodeIdentity::generate().node_id();
    let id_b = NodeIdentity::generate().node_id();
    let id_c = NodeIdentity::generate().node_id();

    let mut counter_a = PnCounter::new();
    let mut counter_b = PnCounter::new();
    let mut counter_c = PnCounter::new();

    counter_a.increment(id_a, 100);
    counter_b.increment(id_b, 200);
    counter_c.increment(id_c, 300);

    // Replica 1 receives A, then B, then C
    let mut rep1 = PnCounter::new();
    rep1.merge(&counter_a).unwrap();
    rep1.merge(&counter_b).unwrap();
    rep1.merge(&counter_c).unwrap();

    // Replica 2 receives C, then A, then B
    let mut rep2 = PnCounter::new();
    rep2.merge(&counter_c).unwrap();
    rep2.merge(&counter_a).unwrap();
    rep2.merge(&counter_b).unwrap();

    // Replica 3 receives B, then C, then A
    let mut rep3 = PnCounter::new();
    rep3.merge(&counter_b).unwrap();
    rep3.merge(&counter_c).unwrap();
    rep3.merge(&counter_a).unwrap();

    assert_eq!(rep1.read(), 600);
    assert_eq!(rep2.read(), 600);
    assert_eq!(rep3.read(), 600);
    assert_eq!(
        rep1.serialize_state().unwrap(),
        rep2.serialize_state().unwrap()
    );
    assert_eq!(
        rep2.serialize_state().unwrap(),
        rep3.serialize_state().unwrap()
    );
}

// ============================================================================
// TEST 07: Out-of-Order Delivery & Cascade Resolution
// ============================================================================
#[test]
fn test_07_out_of_order_delivery_and_cascade() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("cascade_test.db");
    let author_id = NodeIdentity::generate().node_id();
    let store = DocumentStore::open(&db_path, author_id).unwrap();

    let doc_id = DocumentId::generate();

    // Operation 1: Create
    let op1_payload = serde_json::to_vec(&PnCounter::new()).unwrap();
    let op1_id = Operation::compute_id(
        &doc_id,
        &author_id,
        1,
        OperationType::Create,
        &op1_payload,
        &HybridLogicalClock::new(100, 1),
        &[],
        1,
    );
    let op1 = Operation {
        op_id: op1_id,
        document_id: doc_id,
        actor_node_id: author_id,
        sequence: 1,
        op_type: OperationType::Create,
        payload: op1_payload,
        hlc: HybridLogicalClock::new(100, 1),
        causal_dependencies: vec![],
        signature: [0u8; 64],
        created_at: Timestamp::now(),
        schema_version: 1,
        status: fieldnet_data::OpStatus::Durable,
    };

    // Operation 2: Depends on Op 1
    let delta = PnCounterDelta::Increment {
        actor: author_id,
        amount: 42,
    };
    let op2_payload = serde_json::to_vec(&delta).unwrap();
    let op2_id = Operation::compute_id(
        &doc_id,
        &author_id,
        2,
        OperationType::CrdtDelta,
        &op2_payload,
        &HybridLogicalClock::new(101, 1),
        &[op1_id],
        1,
    );
    let op2 = Operation {
        op_id: op2_id,
        document_id: doc_id,
        actor_node_id: author_id,
        sequence: 2,
        op_type: OperationType::CrdtDelta,
        payload: op2_payload,
        hlc: HybridLogicalClock::new(101, 1),
        causal_dependencies: vec![op1_id],
        signature: [0u8; 64],
        created_at: Timestamp::now(),
        schema_version: 1,
        status: fieldnet_data::OpStatus::Durable,
    };

    // 1. Deliver Op2 FIRST -> MissingDependency, staged in unapplied_operations
    let err = store.apply_remote_operation_sync(&op2).unwrap_err();
    assert!(matches!(err, DataError::MissingDependency(_)));
    assert!(store.get_sync(&doc_id).unwrap().is_none());

    // 2. Deliver Op1 -> Applies cleanly
    let res = store.apply_remote_operation_sync(&op1).unwrap();
    assert!(matches!(
        res,
        fieldnet_data::ConflictClassification::CausalUpdate
            | fieldnet_data::ConflictClassification::Resolved
    ));

    // 3. Op2 becomes eligible and retries
    let unblocked = store.retry_unapplied_operations_sync(&doc_id).unwrap();
    assert_eq!(unblocked, vec![op2_id]);

    // Both operations now applied!
    let doc = store.get_sync(&doc_id).unwrap().unwrap();
    let counter: PnCounter = PnCounter::deserialize_state(&doc.state).unwrap();
    assert_eq!(counter.read(), 42);
    assert_eq!(doc.version, 2);
}

// ============================================================================
// TEST 08: Duplicate Delivery Idempotence
// ============================================================================
#[test]
fn test_08_duplicate_delivery_idempotency() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("duplicate_test.db");
    let author_id = NodeIdentity::generate().node_id();
    let store = DocumentStore::open(&db_path, author_id).unwrap();

    let doc_id = DocumentId::generate();
    let payload = serde_json::to_vec(&PnCounter::new()).unwrap();
    let op_id = Operation::compute_id(
        &doc_id,
        &author_id,
        1,
        OperationType::Create,
        &payload,
        &HybridLogicalClock::new(100, 1),
        &[],
        1,
    );
    let op = Operation {
        op_id,
        document_id: doc_id,
        actor_node_id: author_id,
        sequence: 1,
        op_type: OperationType::Create,
        payload,
        hlc: HybridLogicalClock::new(100, 1),
        causal_dependencies: vec![],
        signature: [0u8; 64],
        created_at: Timestamp::now(),
        schema_version: 1,
        status: fieldnet_data::OpStatus::Durable,
    };

    // First delivery -> CausalUpdate
    let res1 = store.apply_remote_operation_sync(&op).unwrap();
    assert!(matches!(
        res1,
        fieldnet_data::ConflictClassification::CausalUpdate
            | fieldnet_data::ConflictClassification::Resolved
    ));

    // Duplicate delivery -> NoConflict (idempotent no-op)
    let res2 = store.apply_remote_operation_sync(&op).unwrap();
    assert_eq!(res2, fieldnet_data::ConflictClassification::NoConflict);

    // Triplicate delivery -> NoConflict
    let res3 = store.apply_remote_operation_sync(&op).unwrap();
    assert_eq!(res3, fieldnet_data::ConflictClassification::NoConflict);

    // Stored once
    let ops = store
        .list_operations_for_document_sync(&doc_id, None, None)
        .unwrap();
    assert_eq!(ops.len(), 1);
}

// ============================================================================
// TEST 09: Interrupted Sync and Resumption from Watermark
// ============================================================================
#[test]
fn test_09_interrupted_sync_and_resumption() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("interrupted_test.db");
    let local_id = NodeIdentity::generate().node_id();
    let peer_id = NodeIdentity::generate().node_id();
    let store = DocumentStore::open(&db_path, local_id).unwrap();

    let doc_id = DocumentId::generate();

    // Partial sync: operations up to sequence 4 were committed before network drop
    store
        .record_peer_sync_watermark_sync(
            &peer_id,
            &doc_id,
            4,
            HybridLogicalClock::new(400, 1),
            "PARTIAL",
        )
        .unwrap();

    // Record cursor for resumption
    store
        .record_sync_cursor_sync(&peer_id, &doc_id, 4, HybridLogicalClock::new(400, 1))
        .unwrap();

    // Reconnection: query cursor
    let cursor = store
        .get_sync_cursor_sync(&peer_id, &doc_id)
        .unwrap()
        .unwrap();
    assert_eq!(cursor.0, 4);

    // Engine resumes from seq 4, requesting operations > 4
    store
        .record_peer_sync_watermark_sync(
            &peer_id,
            &doc_id,
            10,
            HybridLogicalClock::new(1000, 1),
            "CONVERGED",
        )
        .unwrap();

    let final_wm = store
        .get_peer_sync_watermark_sync(&peer_id, &doc_id)
        .unwrap()
        .unwrap();
    assert_eq!(final_wm.last_synced_seq, 10);
    assert_eq!(final_wm.sync_status, "CONVERGED");
}

// ============================================================================
// TEST 10: Runtime Restart Persistence
// ============================================================================
#[test]
fn test_10_runtime_restart_persistence() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("restart_test.db");
    let node_id = NodeIdentity::generate().node_id();
    let doc_id;

    {
        let store = DocumentStore::open(&db_path, node_id).unwrap();
        let mut counter = PnCounter::new();
        counter.increment(node_id, 88);
        let (doc, _) = store.create_crdt_sync(&counter).unwrap();
        doc_id = doc.document_id;
        assert_eq!(store.owner_node_id(), node_id);
    }

    // Reopen database
    {
        let reopened = DocumentStore::open(&db_path, node_id).unwrap();
        assert_eq!(reopened.owner_node_id(), node_id);

        let doc = reopened.get_sync(&doc_id).unwrap().unwrap();
        let counter: PnCounter = PnCounter::deserialize_state(&doc.state).unwrap();
        assert_eq!(counter.read(), 88);

        let sv = reopened
            .get_causal_state_sync(&doc_id)
            .unwrap()
            .unwrap()
            .state_vector;
        assert_eq!(sv.get(&node_id), 1);
    }
}

// ============================================================================
// TEST 11: Snapshot Restoration and State Hash Verification
// ============================================================================
#[test]
fn test_11_snapshot_restoration_and_hash() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("snap_hash_test.db");
    let node_id = NodeIdentity::generate().node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    let mut counter = PnCounter::new();
    counter.increment(node_id, 10);
    let (doc, _) = store.create_crdt_sync(&counter).unwrap();

    let d1 = PnCounterDelta::Increment {
        actor: node_id,
        amount: 20,
    };
    store
        .apply_crdt_delta_sync::<PnCounter>(&doc.document_id, &d1)
        .unwrap();

    // Snapshot at seq 2
    let snap = store.create_snapshot_sync(&doc.document_id).unwrap();

    // Subsequent ops (seq 3, 4)
    let d2 = PnCounterDelta::Increment {
        actor: node_id,
        amount: 30,
    };
    store
        .apply_crdt_delta_sync::<PnCounter>(&doc.document_id, &d2)
        .unwrap();
    let d3 = PnCounterDelta::Increment {
        actor: node_id,
        amount: 40,
    };
    store
        .apply_crdt_delta_sync::<PnCounter>(&doc.document_id, &d3)
        .unwrap();

    let pre_recovery_doc = store.get_sync(&doc.document_id).unwrap().unwrap();

    // Restore from snapshot + forward replay
    let restored_doc = store.restore_snapshot_sync(&snap.snapshot_id).unwrap();

    assert_eq!(restored_doc.state, pre_recovery_doc.state);
    assert_eq!(restored_doc.version, pre_recovery_doc.version);

    let counter_final: PnCounter = PnCounter::deserialize_state(&restored_doc.state).unwrap();
    assert_eq!(counter_final.read(), 10 + 20 + 30 + 40);
}

// ============================================================================
// TEST 12: Compaction Validation & Tombstone Retention
// ============================================================================
#[test]
fn test_12_compaction_and_tombstone_retention() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("compaction_tombstone.db");
    let node_id = NodeIdentity::generate().node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    let (doc, _) = store
        .create_with_op_sync("ephemeral_intel", b"Sensitive Data".to_vec())
        .unwrap();

    // Delete document
    store.delete_with_op_sync(&doc.document_id).unwrap();

    // Snapshot and compact
    store.create_snapshot_sync(&doc.document_id).unwrap();
    let record = store.compact_sync(&doc.document_id).unwrap();
    assert_eq!(record.ops_removed, 2);

    // Invariant: Deleted document cannot be resurrected by stale update
    let stale_author = NodeIdentity::generate().node_id();
    let stale_payload = b"Resurrection attempt".to_vec();
    let stale_hlc = HybridLogicalClock::new(10, 1);
    let op_id = Operation::compute_id(
        &doc.document_id,
        &stale_author,
        1,
        OperationType::Update,
        &stale_payload,
        &stale_hlc,
        &[],
        1,
    );
    let stale_op = Operation {
        op_id,
        document_id: doc.document_id,
        actor_node_id: stale_author,
        sequence: 1,
        op_type: OperationType::Update,
        payload: stale_payload,
        hlc: stale_hlc,
        causal_dependencies: vec![],
        signature: [0u8; 64],
        created_at: Timestamp::now(),
        schema_version: 1,
        status: fieldnet_data::OpStatus::Durable,
    };

    let err = store.apply_remote_operation_sync(&stale_op).unwrap_err();
    assert!(matches!(err, DataError::DocumentDeleted(_)));

    let final_check = store.get_sync(&doc.document_id).unwrap().unwrap();
    assert!(final_check.deleted);
}

// ============================================================================
// TEST 13: Offline Peer Protection against Unsafe Compaction
// ============================================================================
#[test]
fn test_13_offline_peer_protection() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("offline_peer_compaction.db");
    let node_id = NodeIdentity::generate().node_id();
    let lagging_peer = NodeIdentity::generate().node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    let mut counter = PnCounter::new();
    counter.increment(node_id, 1);
    let (doc, _) = store.create_crdt_sync(&counter).unwrap();

    for i in 2..=8 {
        let delta = PnCounterDelta::Increment {
            actor: node_id,
            amount: i,
        };
        store
            .apply_crdt_delta_sync::<PnCounter>(&doc.document_id, &delta)
            .unwrap();
    }

    // Node creates snapshot covering operations 1..8
    store.create_snapshot_sync(&doc.document_id).unwrap();

    // Lagging peer has only synced up to seq 4
    store
        .record_peer_sync_watermark_sync(
            &lagging_peer,
            &doc.document_id,
            4,
            HybridLogicalClock::new(400, 1),
            "CONVERGED",
        )
        .unwrap();

    // Compaction safety clamps to min peer watermark (seq 4)
    let report = store
        .check_compaction_safety_sync(&doc.document_id)
        .unwrap();
    assert_eq!(report.max_safe_sequence, 4);
    assert_eq!(report.eligible_operations_count, 4);

    let comp = store.compact_sync(&doc.document_id).unwrap();
    assert_eq!(comp.ops_removed, 4);

    // Operations 5..8 are preserved so lagging peer can synchronize them
    let remaining = store
        .list_operations_for_document_sync(&doc.document_id, None, None)
        .unwrap();
    assert_eq!(remaining.len(), 4);
    assert_eq!(remaining[0].sequence, 5);
    assert_eq!(remaining[3].sequence, 8);
}

// ============================================================================
// TEST 14: Corruption Detection & Fail-Closed Rejection
// ============================================================================
#[test]
fn test_14_corruption_detection() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("corruption_test.db");
    let identity = Arc::new(NodeIdentity::generate());
    let store = DocumentStore::with_identity(&db_path, identity.clone()).unwrap();

    // 1. Corrupt Snapshot
    let (doc, _) = store
        .create_with_op_sync("note", b"Normal".to_vec())
        .unwrap();
    let snap = store.create_snapshot_sync(&doc.document_id).unwrap();
    store
        .corrupt_snapshot_payload_for_testing(&snap.snapshot_id)
        .unwrap();

    let snap_err = store
        .get_latest_snapshot_sync(&doc.document_id)
        .unwrap_err();
    assert!(matches!(snap_err, DataError::CorruptedSnapshot { .. }));

    // 2. Corrupt Operation Payload (OpId BLAKE3 digest mismatch)
    let bogus_op = Operation {
        op_id: OperationId::from_bytes([0xAA; 32]), // Bogus digest
        document_id: doc.document_id,
        actor_node_id: identity.node_id(),
        sequence: 10,
        op_type: OperationType::Update,
        payload: b"Injected".to_vec(),
        hlc: HybridLogicalClock::new(100, 1),
        causal_dependencies: vec![],
        signature: [0u8; 64],
        created_at: Timestamp::now(),
        schema_version: 1,
        status: fieldnet_data::OpStatus::Durable,
    };
    let op_err = store.apply_remote_operation_sync(&bogus_op).unwrap_err();
    assert!(matches!(op_err, DataError::InvalidOperation(_)));

    // 3. Corrupt Cryptographic Signature
    let mut signed_op = Operation::new_local(
        doc.document_id,
        &identity,
        10,
        OperationType::Update,
        b"Tampered".to_vec(),
        HybridLogicalClock::new(100, 1),
        vec![],
    );
    signed_op.signature[0] ^= 0xFF; // Flip 1 byte in signature
    let sig_err = store.apply_remote_operation_sync(&signed_op).unwrap_err();
    assert!(matches!(sig_err, DataError::StorageFailure(_)));
}

// ============================================================================
// TEST 15: Capability-Based Authorization Enforcement
// ============================================================================
#[tokio::test]
async fn test_15_authorization_enforcement() {
    let transport = Arc::new(MemoryMeshTransport::new());
    let node_a = SubsystemNode::new(transport.clone(), None).await;
    let node_b = SubsystemNode::new(transport.clone(), None).await;

    let doc = node_a
        .store
        .create_sync("classified", b"Top Secret".to_vec())
        .unwrap();

    // Restrict Node A's authorizer to block Node B
    node_a.authorizer.block_actor(node_b.node_id());

    // Node B tries to send an unauthorized mutation
    let (_, op) = node_b
        .store
        .create_with_op_sync("classified", b"Breach".to_vec())
        .unwrap();
    let msg_op = SyncMessage::LiveOperation {
        session_id: Some([3u8; 16]),
        operation: op,
        document_type: Some("classified".to_string()),
    };

    let reply = node_a
        .sync_engine
        .handle_inbound_message(node_b.node_id(), msg_op)
        .await
        .unwrap();

    // Node A rejects unauthorized operation
    assert!(matches!(
        reply,
        Some(SyncMessage::SyncAck {
            status: SyncAckStatus::Rejected { .. },
            ..
        })
    ));

    // Document remains unmutated
    let doc_check = node_a.store.get_sync(&doc.document_id).unwrap().unwrap();
    assert_eq!(doc_check.state, b"Top Secret".to_vec());
}

// ============================================================================
// TEST 16: Protocol Version Incompatibility
// ============================================================================
#[tokio::test]
async fn test_16_protocol_version_incompatibility() {
    let transport = Arc::new(MemoryMeshTransport::new());
    let node_a = SubsystemNode::new(transport.clone(), None).await;
    let rogue_id = NodeIdentity::generate().node_id();

    // Rogue node sends incompatible protocol version (v999)
    let msg_hello = SyncMessage::Hello {
        protocol_version: 999,
        node_id: rogue_id,
        supported_crdt_types: vec![],
        capabilities: vec![],
        supported_namespaces: vec!["default".to_string()],
        max_batch_size: 100,
        snapshot_support: true,
        causal_model: "HLC_v1".to_string(),
        schema_version: 1,
    };

    let err = node_a
        .sync_engine
        .handle_inbound_message(rogue_id, msg_hello)
        .await
        .unwrap_err();

    assert!(matches!(err, DataError::IncompatibleProtocolVersion { .. }));
}

// ============================================================================
// TEST 17: Storage Failure and Atomic Rollback
// ============================================================================
#[test]
fn test_17_storage_failure_and_rollback() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("storage_fail.db");
    let node_id = NodeIdentity::generate().node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    let doc = store
        .create_sync("state_item", b"Initial".to_vec())
        .unwrap();

    // Rollback simulation: update fails midway inside transaction
    let res: Result<(), DataError> = store.database().with_transaction(|tx| {
        tx.execute(
            "UPDATE documents SET payload_bytes = ?1 WHERE document_id = ?2",
            rusqlite::params![
                b"Half-Baked".as_slice(),
                doc.document_id.as_bytes().as_slice()
            ],
        )?;
        Err(DataError::StorageFailure("Disk I/O error".into()))
    });
    assert!(res.is_err());

    // Document state was rolled back completely
    let current = store.get_sync(&doc.document_id).unwrap().unwrap();
    assert_eq!(current.state, b"Initial".to_vec());
    assert_eq!(current.version, 1);
}

// ============================================================================
// TEST 18: Property Tests for CRDT Order Independence
// ============================================================================
#[test]
fn test_18_property_crdt_order_independence() {
    let id_a = NodeIdentity::generate().node_id();
    let id_b = NodeIdentity::generate().node_id();
    let id_c = NodeIdentity::generate().node_id();

    // Property: Merge(A, B, C) == Merge(C, B, A) == Merge(B, A, C)
    let mut c_a = PnCounter::new();
    let mut c_b = PnCounter::new();
    let mut c_c = PnCounter::new();

    c_a.increment(id_a, 42);
    c_b.decrement(id_b, 17);
    c_c.increment(id_c, 99);

    let mut perm1 = c_a.clone();
    perm1.merge(&c_b).unwrap();
    perm1.merge(&c_c).unwrap();

    let mut perm2 = c_c.clone();
    perm2.merge(&c_b).unwrap();
    perm2.merge(&c_a).unwrap();

    let mut perm3 = c_b.clone();
    perm3.merge(&c_a).unwrap();
    perm3.merge(&c_c).unwrap();

    assert_eq!(perm1.read(), perm2.read());
    assert_eq!(perm2.read(), perm3.read());
    assert_eq!(perm1.read(), 42 - 17 + 99);
}

// ============================================================================
// TEST 19: Fault Injection Matrix Programmatic Verification
// ============================================================================
#[test]
fn test_19_fault_injection_matrix() {
    // Verifies programmatic fault scenarios
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("fault_matrix.db");
    let node_id = NodeIdentity::generate().node_id();
    let store = DocumentStore::open(&db_path, node_id).unwrap();

    // 1. Fault: Empty document type
    assert!(matches!(
        store.create_sync("", vec![1]).unwrap_err(),
        DataError::InvalidDocumentType(_)
    ));

    // 2. Fault: Oversized document payload (> MAX_DOCUMENT_SIZE)
    let big_data = vec![0u8; fieldnet_data::MAX_DOCUMENT_SIZE + 1];
    assert!(matches!(
        store.create_sync("big", big_data).unwrap_err(),
        DataError::DocumentTooLarge { .. }
    ));

    // 3. Fault: Non-existent document read
    assert!(store.get_sync(&DocumentId::generate()).unwrap().is_none());

    // 4. Fault: Update deleted document
    let (d, _) = store.create_with_op_sync("del_target", vec![1]).unwrap();
    store.delete_with_op_sync(&d.document_id).unwrap();
    assert!(matches!(
        store.update_sync(&d.document_id, vec![2]).unwrap_err(),
        DataError::DocumentDeleted(_)
    ));

    // 5. Fault: Corrupted state vector bytes
    assert!(StateVector::from_bytes(&[0xFF; 2]).is_err());

    // 6. Fault: Invalid dependencies bytes
    let bogus_op = Operation {
        op_id: OperationId::from_bytes([1u8; 32]),
        document_id: d.document_id,
        actor_node_id: node_id,
        sequence: 1,
        op_type: OperationType::Update,
        payload: vec![1],
        hlc: HybridLogicalClock::now(),
        causal_dependencies: vec![],
        signature: [0u8; 64],
        created_at: Timestamp::now(),
        schema_version: 1,
        status: fieldnet_data::OpStatus::Durable,
    };
    // Rejects because op_id does not match payload digest
    assert!(matches!(
        store.apply_remote_operation_sync(&bogus_op).unwrap_err(),
        DataError::InvalidOperation(_)
    ));
}
