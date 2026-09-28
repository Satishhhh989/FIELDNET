# FIELDNET — Phase 06 / Architecture Document 03
**Title:** Synchronization Protocol, Storage Schema & Implementation Contracts  
**Document ID:** `PHASE-06-03-SYNC-PROTOCOL-IMPLEMENTATION-CONTRACTS`  
**Status:** ARCHITECTURE COMPLETE — AWAITING IMPLEMENTATION  
**Authoritative References:** `FIELDNET_MASTER_SPEC.md`, `docs/architecture/01-system-model.md` through `05-protocol-routing-dtn.md`, `docs/architecture/PHASE-06-01-LOCAL-FIRST-DATA.md`, `docs/architecture/PHASE-06-02-CRDT-CONFLICT-CONVERGENCE.md`, `docs/implementation/BUILD-01.md` through `BUILD-03.md`.

---

## 1. Final Phase 06 End-to-End Data Flow

The synchronization lifecycle moves through well-defined architectural boundaries between local storage, the synchronization engine, and the underlying Phase 05 network:

```
                                  ORIGINATING NODE (A)
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ 1. Application Layer:      doc_store.update(doc_id, delta)                             │
├────────────────────────────────────────────────────────────────────────────────────────┤
│ 2. Local Data API:         Validates schema, checks local capabilities                 │
├────────────────────────────────────────────────────────────────────────────────────────┤
│ 3. Atomic SQLite Tx:       - Appends ChangeOperation to change_log                     │
│                            - Evaluates local CRDT join -> updates documents table      │
│                            - Advances local StateVector & HLC in replica_state         │
├────────────────────────────────────────────────────────────────────────────────────────┤
│ 4. Sync Engine:            Detects new un-synced operations via dirty triggers         │
├────────────────────────────────────────────────────────────────────────────────────────┤
│ 5. Sync Protocol Framing:  Wraps ops in SyncMessage::DeltaResponse or LiveOperation    │
├────────────────────────────────────────────────────────────────────────────────────────┤
│ 6. Phase 05 Message Stack: - Encapsulates in Message { type: MessageType::Sync, .. }   │
│                            - Signs envelope with NodeIdentity Ed25519 key              │
│                            - Looks up next-hop in RoutingTable                         │
│                            - Direct TCP send OR stages in SQLite DtnQueue              │
└───────────────────────────────────────────┬────────────────────────────────────────────┘
                                            │ Authenticated Mesh Link / DTN Relay
                                            ▼
                                   RECIPIENT NODE (B)
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ 7. Phase 05 Transport:     - Receives ChaCha20 AEAD frame / Drains from DtnQueue       │
│                            - Verifies envelope signature & freshness                   │
│                            - Dispatches payload to SyncEngine                          │
├────────────────────────────────────────────────────────────────────────────────────────┤
│ 8. Validation Pipeline:    - Parses SyncMessage                                        │
│                            - Verifies author Ed25519 signature on each ChangeOperation │
│                            - Enforces document capability authorization (fail-closed)  │
│                            - Validates HLC freshness & causal dependencies             │
├────────────────────────────────────────────────────────────────────────────────────────┤
│ 9. CRDT & Conflict Engine: - Evaluates VersionVector & DAG ancestry                    │
│                            - Computes mathematical join (Commutative/Associative/Idem) │
│                            - Resolves LWW / stages unmergeable conflicts               │
├────────────────────────────────────────────────────────────────────────────────────────┤
│ 10. Atomic SQLite Commit:  - Writes remote operation to change_log                     │
│                            - Updates materialized view in documents table              │
│                            - Updates peer_sync_state watermark                         │
├────────────────────────────────────────────────────────────────────────────────────────┤
│ 11. Protocol Acknowledgment:Generates signed SyncMessage::SyncAck back to originator    │
├────────────────────────────────────────────────────────────────────────────────────────┤
│ 12. Local Event Emission:  Emits DocumentUpdated event to local application subscribers │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Complete SQLite Schema

The local storage subsystem is implemented as an embedded SQLite database operating in Write-Ahead Logging (`WAL`) mode with `synchronous = NORMAL`.

```sql
-- ============================================================================
-- 1. MATERIALIZED DOCUMENTS TABLE
-- Stores the latest converged local projection for fast read/query access.
-- ============================================================================
CREATE TABLE IF NOT EXISTS documents (
    document_id         BLOB PRIMARY KEY,   -- 32-byte BLAKE3 identifier
    namespace           TEXT NOT NULL,      -- Semantic namespace (e.g., "org.fieldnet.notes")
    schema_version      INTEGER NOT NULL,   -- Schema version for document payload
    owner_node_id       BLOB NOT NULL,      -- Sovereign NodeId of original author
    version             INTEGER NOT NULL,   -- Global logical version (monotonic)
    local_revision      INTEGER NOT NULL,   -- Node-local update sequence
    created_at          INTEGER NOT NULL,   -- Epoch milliseconds (UTC)
    updated_at          INTEGER NOT NULL,   -- Epoch milliseconds (UTC)
    is_deleted          INTEGER NOT NULL DEFAULT 0, -- 0 = active, 1 = tombstone
    crdt_type           INTEGER NOT NULL,   -- 1=LWW, 2=PN_COUNTER, 3=OR_SET, 4=MAP, 5=RGA
    payload_bytes       BLOB NOT NULL,      -- Serialized materialized state
    state_vector_bytes  BLOB NOT NULL       -- Serialized causal StateVector
);

CREATE INDEX IF NOT EXISTS idx_docs_ns ON documents(namespace);
CREATE INDEX IF NOT EXISTS idx_docs_updated ON documents(updated_at);
CREATE INDEX IF NOT EXISTS idx_docs_owner ON documents(owner_node_id);

-- ============================================================================
-- 2. APPEND-ONLY CHANGE LOG TABLE
-- Stores immutable, cryptographically signed operation deltas.
-- ============================================================================
CREATE TABLE IF NOT EXISTS operations (
    op_id               BLOB PRIMARY KEY,   -- 32-byte BLAKE3 hash of operation
    document_id         BLOB NOT NULL,      -- Target document identifier
    actor_node_id       BLOB NOT NULL,      -- Sovereign NodeId of author
    sequence            INTEGER NOT NULL,   -- Monotonic sequence counter from actor
    op_type             INTEGER NOT NULL,   -- 1=Create, 2=CrdtDelta, 3=Assign, 4=Tombstone
    delta_bytes         BLOB NOT NULL,      -- Serialized delta payload
    hlc_physical        INTEGER NOT NULL,   -- HLC physical UTC millisecond component
    hlc_logical         INTEGER NOT NULL,   -- HLC logical counter component
    dependencies_bytes  BLOB NOT NULL,      -- Binary JSON array of parent OpIds
    signature           BLOB NOT NULL,      -- 64-byte Ed25519 signature
    created_at          INTEGER NOT NULL,   -- Local physical ingest time
    status              INTEGER NOT NULL DEFAULT 1, -- 1=Applied, 2=WaitingDep, 3=Rejected
    FOREIGN KEY(document_id) REFERENCES documents(document_id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ops_doc_actor_seq 
    ON operations(document_id, actor_node_id, sequence);
CREATE INDEX IF NOT EXISTS idx_ops_doc_hlc 
    ON operations(document_id, hlc_physical, hlc_logical);
CREATE INDEX IF NOT EXISTS idx_ops_status 
    ON operations(status);

-- ============================================================================
-- 3. APPLIED OPERATIONS TRACKER
-- Fast O(1) deduplication check for incoming operations.
-- ============================================================================
CREATE TABLE IF NOT EXISTS applied_operations (
    document_id         BLOB NOT NULL,
    op_id               BLOB NOT NULL,
    applied_at          INTEGER NOT NULL,
    PRIMARY KEY(document_id, op_id)
);

-- ============================================================================
-- 4. REPLICA CAUSAL STATE
-- Document-level state vector and compaction tracking.
-- ============================================================================
CREATE TABLE IF NOT EXISTS replica_state (
    document_id         BLOB PRIMARY KEY,
    state_vector_bytes  BLOB NOT NULL,      -- Serialized StateVector
    latest_hlc_physical INTEGER NOT NULL,
    latest_hlc_logical  INTEGER NOT NULL,
    compaction_watermark INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY(document_id) REFERENCES documents(document_id) ON DELETE CASCADE
);

-- ============================================================================
-- 5. PEER SYNCHRONIZATION WATERMARKS
-- Remembers last confirmed synchronization points per peer.
-- ============================================================================
CREATE TABLE IF NOT EXISTS peer_sync_state (
    peer_node_id        BLOB NOT NULL,      -- 32-byte NodeId of remote peer
    document_id         BLOB NOT NULL,      -- Target document identifier
    last_synced_seq     INTEGER NOT NULL,   -- Highest contiguous sequence received
    last_synced_hlc_p   INTEGER NOT NULL,
    last_synced_hlc_l   INTEGER NOT NULL,
    last_sync_time      INTEGER NOT NULL,   -- Unix epoch milliseconds
    sync_status         TEXT NOT NULL,      -- 'CONVERGED', 'PENDING', 'INTERRUPTED'
    PRIMARY KEY(peer_node_id, document_id)
);

-- ============================================================================
-- 6. ACTIVE SYNC SESSIONS & CURSORS
-- Tracks in-flight synchronization handshakes and resumption offsets.
-- ============================================================================
CREATE TABLE IF NOT EXISTS sync_sessions (
    session_id          BLOB PRIMARY KEY,   -- 16-byte session identifier
    peer_node_id        BLOB NOT NULL,
    started_at          INTEGER NOT NULL,
    state               TEXT NOT NULL,      -- 'NEGOTIATING', 'TRANSFERRING', etc.
    bytes_sent          INTEGER NOT NULL DEFAULT 0,
    bytes_received      INTEGER NOT NULL DEFAULT 0,
    ops_sent            INTEGER NOT NULL DEFAULT 0,
    ops_received        INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS sync_cursors (
    peer_node_id        BLOB NOT NULL,
    document_id         BLOB NOT NULL,
    cursor_seq          INTEGER NOT NULL,
    cursor_hlc_p        INTEGER NOT NULL,
    cursor_hlc_l        INTEGER NOT NULL,
    PRIMARY KEY(peer_node_id, document_id)
);

-- ============================================================================
-- 7. DOCUMENT SNAPSHOTS & CHECKPOINTS
-- Retains compacted baselines for fast-sync and historical recovery.
-- ============================================================================
CREATE TABLE IF NOT EXISTS document_snapshots (
    snapshot_id         BLOB PRIMARY KEY,
    document_id         BLOB NOT NULL,
    snapshot_version    INTEGER NOT NULL,
    compacted_up_to_seq INTEGER NOT NULL,
    state_vector_bytes  BLOB NOT NULL,
    snapshot_payload    BLOB NOT NULL,
    created_at          INTEGER NOT NULL,
    FOREIGN KEY(document_id) REFERENCES documents(document_id) ON DELETE CASCADE
);

-- ============================================================================
-- 8. CAUSAL TOMBSTONES REGISTRY
-- Preserves deletion markers to prevent resurrection by lagged peers.
-- ============================================================================
CREATE TABLE IF NOT EXISTS tombstones (
    entity_id           BLOB PRIMARY KEY,   -- DocumentId or field-level identifier
    entity_type         TEXT NOT NULL,      -- 'DOCUMENT', 'FIELD', 'ITEM'
    deleted_at          INTEGER NOT NULL,
    hlc_physical        INTEGER NOT NULL,
    hlc_logical         INTEGER NOT NULL,
    deleted_by          BLOB NOT NULL,      -- NodeId of author who issued deletion
    gc_after            INTEGER NOT NULL    -- Absolute timestamp after which GC is legal
);
CREATE INDEX IF NOT EXISTS idx_tombstones_gc ON tombstones(gc_after);
```

---

## 3. Rust Document Record Model

```rust
use fieldnet_core::{NodeId, Timestamp};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct DocumentId(pub [u8; 32]);

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum CrdtType {
    LwwRegister = 1,
    PnCounter = 2,
    OrSet = 3,
    CrdtMap = 4,
    RgaSequence = 5,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DocumentRecord {
    pub document_id: DocumentId,
    pub namespace: String,
    pub schema_version: u32,
    pub owner_node_id: NodeId,
    pub version: u64,
    pub local_revision: u64,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    pub is_deleted: bool,
    pub crdt_type: CrdtType,
    pub payload_bytes: Vec<u8>,
    pub state_vector: StateVector,
}
```

---

## 4. Operation Record & Lifecycle State Machine

### 4.1 Rust Operation Record
```rust
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct OperationId(pub [u8; 32]);

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum OpStatus {
    Applied = 1,
    WaitingDependency = 2,
    Rejected = 3,
    Tombstone = 4,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OperationRecord {
    pub op_id: OperationId,
    pub document_id: DocumentId,
    pub actor_node_id: NodeId,
    pub sequence: u64,
    pub op_type: u8,
    pub delta_bytes: Vec<u8>,
    pub hlc: HybridLogicalClock,
    pub causal_dependencies: Vec<OperationId>,
    #[serde(with = "hex")]
    pub signature: [u8; 64],
    pub created_at: u64,
    pub status: OpStatus,
}
```

### 4.2 Operation Lifecycle State Machine
```
   [ Local Mutation Intent ]
              │
              ▼
         ┌─────────┐
         │ CREATED │ (In-memory, signed with author's Ed25519 key)
         └────┬────┘
              │ SQLite Transaction
              ▼
         ┌─────────┐
         │ DURABLE │ (Committed to operations table in SQLite)
         └────┬────┘
              │ Enqueued in local sync runner
              ▼
      ┌──────────────┐
      │ PENDING_SYNC │ (Awaiting peer rendezvous or DTN dispatch)
      └───────┬──────┘
              │ Dispatched over Phase 05 session / staged in DtnQueue
              ▼
          ┌──────┐
          │ SENT │ (In transit across mesh)
          └──────┴──────────────────┐
                                    │ Network ACK received from peer
                                    ▼
                             ┌──────────────┐
                             │ ACKNOWLEDGED │ (Confirmed at remote watermark)
                             └──────────────┘

   [ Inbound Remote Operation ]
              │
              ▼
       Validate Signature & Auth
        /                  \
   FAIL/                    \ PASS
      ▼                      ▼
┌──────────┐      Check Causal Dependencies
│ REJECTED │       /                      \
└──────────┘  MISSING/                    \ PRESENT
                    ▼                      ▼
         ┌────────────────────┐       ┌─────────┐
         │ WAITING_DEPENDENCY │       │ APPLIED │ (Merged into SQLite document)
         └────────────────────┘       └─────────┘
```

---

## 5. Causal Metadata Storage & Serialization

### 5.1 Hybrid Logical Clock (HLC) Serialization
An HLC timestamp is strictly serialized as 12 big-endian bytes:
```
[0..8]  Physical Component: u64 big-endian milliseconds since UNIX epoch
[8..12] Logical Component:  u32 big-endian monotonic logical counter
```
Comparison is scalar and lexicographical:
$$HLC_1 < HLC_2 \iff (HLC_1.p < HLC_2.p) \lor (HLC_1.p == HLC_2.p \land HLC_1.l < HLC_2.l)$$

### 5.2 State Vector Storage
A `StateVector` maps contributing author `NodeId`s to their highest applied contiguous sequence:
```rust
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateVector {
    pub entries: HashMap<NodeId, u64>,
}
```
- **Serialization:** Serialized via compact binary bincode or canonical sorted JSON array `[ [NodeId, Sequence], ... ]`.
- **Storage:** Stored in `replica_state.state_vector_bytes` and `documents.state_vector_bytes`.
- **Merge Operation:** $V_{merged}[k] = \max(V_1[k], V_2[k])$.

---

## 6. Synchronization Protocol Messages

All Phase 06 synchronization messages are framed as payloads inside the existing Phase 05 `Message` envelope using a dedicated protocol type: `MessageType::Sync = 6`.

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SyncMessage {
    /// 1. Initial Handshake Greeting
    Hello {
        protocol_version: u32,
        supported_namespaces: Vec<String>,
        capabilities: Vec<String>,
    },

    /// 2. Compact State Summary for a collection of documents
    StateSummary {
        summaries: Vec<DocumentSummaryEntry>,
    },

    /// 3. Targeted Delta Request for missing operations
    DeltaRequest {
        document_id: DocumentId,
        have_vector: StateVector,
        max_operations: u32,
    },

    /// 4. Paged Delta Response containing batched operations
    DeltaResponse {
        document_id: DocumentId,
        operations: Vec<OperationRecord>,
        has_more: bool,
    },

    /// 5. Immediate Live Operation Push (event-driven)
    LiveOperation {
        operation: OperationRecord,
    },

    /// 6. Synchronized Watermark Acknowledgment
    SyncAck {
        document_id: DocumentId,
        up_to_sequence: u64,
        confirmed_hlc: HybridLogicalClock,
        resulting_state_vector: StateVector,
    },

    /// 7. Snapshot Baseline for Fast-Sync / Compaction Recovery
    SnapshotDelivery {
        document_id: DocumentId,
        snapshot_version: u64,
        state_vector: StateVector,
        snapshot_bytes: Vec<u8>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DocumentSummaryEntry {
    pub document_id: DocumentId,
    pub version: u64,
    pub state_vector: StateVector,
    pub latest_hlc: HybridLogicalClock,
}
```

---

## 7. Synchronization Handshake & Session Protocol

```
    Node A (Initiator)                                 Node B (Responder)
        │                                                      │
        │ 1. SyncMessage::Hello                                │
        ├─────────────────────────────────────────────────────>│
        │                                                      │
        │ 2. SyncMessage::Hello                                │
        │<─────────────────────────────────────────────────────┤
        │                                                      │
        │ 3. SyncMessage::StateSummary (Vector for docs)       │
        ├─────────────────────────────────────────────────────>│
        │                                                      │
        │ 4. SyncMessage::StateSummary (Vector for docs)       │
        │<─────────────────────────────────────────────────────┤
        │                                                      │
        │ [Calculate Diffs: Detect A \ B and B \ A]            │
        │                                                      │
        │ 5. SyncMessage::DeltaRequest(doc_id, have_vector)    │
        ├─────────────────────────────────────────────────────>│
        │                                                      │
        │ 6. SyncMessage::DeltaResponse(ops batch, has_more)   │
        │<─────────────────────────────────────────────────────┤
        │                                                      │
        │ [Validate Pipeline -> Commit to SQLite Tx]           │
        │                                                      │
        │ 7. SyncMessage::SyncAck(up_to_seq, confirmed_vector) │
        ├─────────────────────────────────────────────────────>│
```

### Disconnection Halfway Through Handshake
- If the transport link fails before `SyncAck` is returned, Node A retains all operations committed to SQLite.
- `peer_sync_state` preserves the last successfully committed cursor.
- Upon reconnection, `have_vector` accurately reflects the partial progress; zero duplicate work is performed.

---

## 8. Incremental Synchronization & Paging

1. **No Full Database Dumps:** Nodes exchange only state summaries and request missing sequence gaps.
2. **Page Bounds:** `DeltaResponse` batches are capped at:
   - Maximum 100 operations per page.
   - Maximum 512 KB payload bytes per page.
3. **Paging Flag:** `has_more: bool` indicates whether additional pages remain. The requester issues subsequent `DeltaRequest`s with updated cursors until `has_more == false`.

---

## 9. Operation Validation Pipeline

Every incoming operation must traverse a strict, fail-closed 14-step validation pipeline before committing to the database:

```
[ Inbound Frame ]
       │
       ▼
 1. Frame Size & Header Check      ─── FAIL ───> Discard Frame, Log Network Warning
       │ PASS
       ▼
 2. Protocol Version Check         ─── FAIL ───> Reject with VersionMismatch Error
       │ PASS
       ▼
 3. Operation Size Limit (≤ 1MB)   ─── FAIL ───> Reject with PayloadTooLarge Error
       │ PASS
       ▼
 4. Author Identity Derivation     ─── FAIL ───> Reject: NodeId ≠ BLAKE3(PubKey)
       │ PASS
       ▼
 5. Ed25519 Cryptographic Sig     ─── FAIL ───> Reject: Signature Verification Failed
       │ PASS
       ▼
 6. Document Capability Check      ─── FAIL ───> Reject: Unauthorized Author (Fail-Closed)
       │ PASS
       ▼
 7. Freshness & Expiration Check   ─── FAIL ───> Reject: Operation Expired / Clock Skew
       │ PASS
       ▼
 8. Deterministic OpId Hash Check  ─── FAIL ───> Reject: OpId Mismatch
       │ PASS
       ▼
 9. Duplicate Check (applied_ops)  ─── DUP  ───> Drop Delta, Send Re-ACK, Exit Early
       │ NEW
       ▼
10. Causal Dependency Check        ─── MISS ──> Stage in `WaitingDependency` Buffer
       │ PRESENT
       ▼
11. CRDT Schema Compatibility      ─── FAIL ───> Reject: Incompatible CRDT Type
       │ PASS
       ▼
12. Atomic SQLite Transaction      ─── FAIL ───> Rollback Transaction, Alert Storage Error
       │ COMMIT
       ▼
13. Peer Sync Watermark Updated
       │
       ▼
14. Cryptographic SyncAck Issued
```

---

## 10. Atomicity & Transaction Boundaries

To guarantee complete crash consistency, local and remote mutations execute inside a **single atomic SQLite transaction**:

```rust
// Conceptual SQLite Atomic Transaction
pub fn apply_operation_tx(
    conn: &mut rusqlite::Connection,
    op: &ChangeOperation,
    materialized_payload: &[u8],
    new_state_vector: &StateVector,
    peer_id: &NodeId,
) -> Result<(), StorageError> {
    let tx = conn.transaction()?;

    // 1. Insert immutable operation
    tx.execute(
        "INSERT INTO operations (op_id, document_id, actor_node_id, sequence, op_type, delta_bytes, hlc_physical, hlc_logical, dependencies_bytes, signature, created_at, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)",
        params![op.op_id.0, op.document_id.0, op.actor_node_id.as_bytes(), op.sequence, op.op_type as u8, op.delta, op.hlc.physical, op.hlc.logical, serde_json::to_vec(&op.causal_dependencies)?, op.signature, op.created_at],
    )?;

    // 2. Record in applied_operations deduplication table
    tx.execute(
        "INSERT INTO applied_operations (document_id, op_id, applied_at) VALUES (?, ?, ?)",
        params![op.document_id.0, op.op_id.0, Timestamp::now().as_millis()],
    )?;

    // 3. Update materialized document state
    tx.execute(
        "UPDATE documents SET payload_bytes = ?, state_vector_bytes = ?, version = version + 1, updated_at = ? WHERE document_id = ?",
        params![materialized_payload, bincode::serialize(new_state_vector)?, Timestamp::now().as_millis(), op.document_id.0],
    )?;

    // 4. Advance replica state
    tx.execute(
        "UPDATE replica_state SET state_vector_bytes = ?, latest_hlc_physical = ?, latest_hlc_logical = ? WHERE document_id = ?",
        params![bincode::serialize(new_state_vector)?, op.hlc.physical, op.hlc.logical, op.document_id.0],
    )?;

    // 5. Update peer watermark
    tx.execute(
        "INSERT INTO peer_sync_state (peer_node_id, document_id, last_synced_seq, last_synced_hlc_p, last_synced_hlc_l, last_sync_time, sync_status) VALUES (?, ?, ?, ?, ?, ?, 'SYNCING') ON CONFLICT(peer_node_id, document_id) DO UPDATE SET last_synced_seq = excluded.last_synced_seq, last_synced_hlc_p = excluded.last_synced_hlc_p, last_synced_hlc_l = excluded.last_synced_hlc_l, last_sync_time = excluded.last_sync_time",
        params![peer_id.as_bytes(), op.document_id.0, op.sequence, op.hlc.physical, op.hlc.logical, Timestamp::now().as_millis()],
    )?;

    tx.commit()?;
    Ok(())
}
```

---

## 11. Sync Engine State Machine

```
                 Peer Discovered
  ┌──────┐ ─────────────────────────> ┌──────────────┐
  │ IDLE │                            │ DISCOVERING  │
  └──────┘ <───────────────────────── └──────┬───────┘
     ▲              Disconnection            │ Peer Authenticated
     │                                       ▼
     │                                ┌─────────────┐
     │ ┌───────────────────────────── │ NEGOTIATING │ (SyncHello exchange)
     │ │                              └──────┬──────┘
     │ │                                     │ Namespaces match
     │ │                                     ▼
     │ │                              ┌───────────┐
     │ │ ┌─────────────────────────── │ COMPARING │ (StateVector exchange)
     │ │ │                            └──────┬────┘
     │ │ │                                   │ Diffs detected
     │ │ │                                   ▼
     │ │ │                            ┌────────────┐
     │ │ │ ┌───────────────────────── │ REQUESTING │ (DeltaRequest dispatched)
     │ │ │ │                          └──────┬─────┘
     │ │ │ │                                 │ Batches arriving
     │ │ │ │                                 ▼
     │ │ │ │                          ┌───────────┐
     │ │ │ │ ┌─────────────────────── │ RECEIVING │ (DeltaResponse streaming)
     │ │ │ │ │                        └──────┬────┘
     │ │ │ │ │                               │ Validating
     │ │ │ │ │                               ▼
     │ │ │ │ │                        ┌──────────┐
     │ │ │ │ │ ┌───────────────────── │ APPLYING │ (SQLite Tx commit)
     │ │ │ │ │ │                      └──────┬───┘
     │ │ │ │ │ │                             │
     │ │ │ │ │ │                             ▼
     │ │ │ │ │ │                      ┌───────────────┐
     │ │ │ │ │ │                      │ ACKNOWLEDGING │ (SyncAck issued)
     │ │ │ │ │ │                      └──────┬────────┘
     │ │ │ │ │ │                             │ All diffs drained
     │ │ │ │ │ │                             ▼
     │ │ │ │ │ │                      ┌───────────┐
     │ │ │ │ │ │                      │ CONVERGED │
     │ │ │ │ │ │                      └───────────┘
     ▼ ▼ ▼ ▼ ▼ ▼
  ┌─────────────┐   Reconnect / Resume
  │ INTERRUPTED │ ──────────────────────> [COMPARING]
  └─────────────┘
```

---

## 12. Peer Synchronization State Ledger

For each connected or known peer, FIELDNET maintains a persistent record in `peer_sync_state`:
- `peer_node_id`: Sovereign `NodeId` of remote peer.
- `document_id`: Associated document.
- `last_synced_seq`: Highest contiguous sequence received from that peer.
- `last_synced_hlc`: Highest HLC timestamp confirmed.
- `sync_status`: `CONVERGED`, `PENDING`, `PARTIAL`, or `INTERRUPTED`.

---

## 13. Operation Batching Invariants

1. **Max Operations per Batch:** 100 operations.
2. **Max Bytes per Batch:** 512 KB.
3. **Deterministic Ordering:** Operations within a batch are ordered strictly by causal sequence $(\text{actor\_id}, \text{sequence})$.
4. **Partial Batch Failure Isolation:** If operation 5 in a batch of 10 fails validation, operations 1–4 are committed; operations 5–10 are rejected. The response ACK specifies sequence 4 as the new watermark.

---

## 14. Duplicate Handling & Idempotency Guarantees

1. **Duplicate Operation:** Rejected by SQLite `applied_operations` primary key constraint. Re-ACK returned to peer to allow peer watermark advancement.
2. **Duplicate Batch:** All operations detected as applied; zero mutations occur; identical `SyncAck` returned.
3. **Duplicate Snapshot:** If snapshot sequence $\le$ current local sequence, snapshot is discarded.
4. **Duplicate ACK:** Ignored safely without altering watermark or triggering retransmission.

---

## 15. Out-of-Order Dependencies & DTN Buffering

When operation $O_2$ arrives with causal dependency $O_1$ ($O_1 \notin \text{applied\_operations}$):
1. **Durable Staging:** $O_2$ is committed to `operations` table with status `OpStatus::WaitingDependency = 2`.
2. **Missing Ancestor Request:** Sync engine dispatches a `SyncMessage::DeltaRequest` targeting parent $O_1$.
3. **Cascade Resolution:** When $O_1$ arrives and commits:
   - Queries `SELECT * FROM operations WHERE status = 2 AND dependencies_bytes LIKE '%O_1%'`.
   - Recursively applies all unblocked dependent operations in topological order.

---

## 16. Snapshot Protocol

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotRecord {
    pub snapshot_id: [u8; 32],
    pub document_id: DocumentId,
    pub snapshot_version: u64,
    pub compacted_up_to_seq: u64,
    pub state_vector: StateVector,
    pub snapshot_payload: Vec<u8>,
    pub state_hash: [u8; 32],
}
```
- **Fast-Syncing:** Delivered to newly provisioned nodes or nodes that have been offline longer than the compaction retention window.
- **Integrity:** `state_hash == BLAKE3(snapshot_payload)`.

---

## 17. Safe Change Log Compaction

Compaction permanently prunes old rows from `operations`. Compaction up to sequence $K$ is legal **if and only if**:
1. A valid snapshot exists in `document_snapshots` covering sequence $\ge K$.
2. All active peers in `peer_sync_state` have confirmed synchronization past $K$:
   $$\forall P_i \in \text{Peers}: \quad \text{last\_synced\_seq}(P_i) \ge K$$
3. For offline peers, sequence $K$ is older than the absolute grace period $\Delta_{gc} = 30\text{ days}$.

---

## 18. CRDT Implementation Contract

```rust
pub trait Crdt {
    type State: Clone + Serialize + for<'de> Deserialize<'de>;
    type Delta: Clone + Serialize + for<'de> Deserialize<'de>;
    type Value;

    /// Evaluates current state to produce application value.
    fn read(&self) -> Self::Value;

    /// Generates a mutation delta from application intent.
    fn generate_delta(&self, intent: &Self::Value) -> Self::Delta;

    /// Merges an operation delta into local state.
    /// Invariant: Must be Commutative, Associative, and Idempotent.
    fn apply_delta(&mut self, delta: &Self::Delta) -> Result<(), CrdtError>;

    /// Merges another full state (e.g. from snapshot).
    fn merge(&mut self, other: Self::State) -> Result<(), CrdtError>;

    /// Produces a deterministic state hash for Merkle comparison.
    fn state_hash(&self) -> [u8; 32];
}
```
**Strict Boundary:** The `Crdt` trait has zero dependency on sockets, TCP, UDP, DTN queues, or routing.

---

## 19. Conflict Resolver Contract

```rust
#[async_trait]
pub trait ConflictResolver: Send + Sync {
    /// Evaluates two concurrent, non-CRDT values and produces resolution.
    async fn resolve(
        &self,
        document_id: &DocumentId,
        local_value: &[u8],
        remote_value: &[u8],
        strategy: ConflictStrategy,
    ) -> Result<ConflictResolution, StorageError>;
}

pub enum ConflictStrategy {
    LastWriteWins,
    MultiValueBranch,
    ApplicationDefined,
    ManualUserReview,
}

pub struct ConflictResolution {
    pub resolved_payload: Vec<u8>,
    pub resolution_op_type: OperationType,
}
```

---

## 20. Application-Facing Local Data API

Applications interact with high-level asynchronous traits:

```rust
#[async_trait]
pub trait DocumentStore: Send + Sync {
    async fn create(&self, namespace: &str, initial_payload: &[u8]) -> Result<DocumentRecord, StorageError>;
    async fn get(&self, doc_id: &DocumentId) -> Result<Option<DocumentRecord>, StorageError>;
    async fn update(&self, doc_id: &DocumentId, delta: &[u8]) -> Result<OperationId, StorageError>;
    async fn delete(&self, doc_id: &DocumentId) -> Result<OperationId, StorageError>;
    async fn list(&self, namespace: &str) -> Result<Vec<DocumentRecord>, StorageError>;
}

#[async_trait]
pub trait SyncManager: Send + Sync {
    async fn sync_status(&self, doc_id: &DocumentId) -> Result<SyncStatus, StorageError>;
    async fn sync_now(&self) -> Result<(), StorageError>;
    fn subscribe(&self, doc_id: &DocumentId) -> broadcast::Receiver<DocumentChangeEvent>;
}
```

---

## 21. Canonical Event Definitions

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DocumentChangeEvent {
    DocumentCreated { doc_id: DocumentId, namespace: String, owner: NodeId },
    DocumentUpdated { doc_id: DocumentId, op_id: OperationId, version: u64 },
    DocumentDeleted { doc_id: DocumentId, deleted_by: NodeId },
    ConflictDetected { doc_id: DocumentId, local_op: OperationId, remote_op: OperationId },
    ConflictResolved { doc_id: DocumentId, resolution_op: OperationId },
    SyncConverged { doc_id: DocumentId, peer: NodeId, state_vector: StateVector },
}
```

---

## 22. Safe Resource Limits

| Parameter | Default Limit | Overflow Handling |
|:---|:---|:---|
| **Max Document Size** | 4,194,304 bytes (4 MB) | `StorageError::DocumentTooLarge`, write rejected |
| **Max Operation Delta** | 1,048,576 bytes (1 MB) | `StorageError::PayloadTooLarge`, delta rejected |
| **Max Batch Size** | 524,288 bytes (512 KB) | Paged into multiple `DeltaResponse` frames |
| **Max Ops per Batch** | 100 operations | Paged with `has_more = true` |
| **Max Unapplied Buffer**| 500 operations / document | Backpressure: reject further incoming deltas |
| **Max Snapshot Size** | 16,777,216 bytes (16 MB) | Reject snapshot creation; trigger partition |

---

## 23. Security & Cryptographic Contract

1. **Network Authentication $\neq$ Data Authorization:**
   - Network layer (Phase 04/05) validates connecting node identity.
   - Storage layer (Phase 06) verifies that `actor_node_id` holds write permission for `document.namespace`.
2. **Author Cryptographic Signature:**
   - Every operation is signed: $\text{Ed25519Verify}(\text{actor\_id}, \text{signing\_bytes}, \text{signature}) == \text{Valid}$.
3. **Confidentiality:** All sync protocol messages travel over ChaCha20-Poly1305 AEAD sessions.

---

## 24. Compatibility & Versioning

```rust
pub const CURRENT_SYNC_PROTOCOL_VERSION: u32 = 1;
pub const CURRENT_OPERATION_FORMAT_VERSION: u32 = 1;
pub const CURRENT_SNAPSHOT_FORMAT_VERSION: u32 = 1;
```
If a peer communicates with an incompatible `protocol_version`, the session fails gracefully with `IncompatibleVersion` and closes without modifying database state.

---

## 25. Failure and Recovery Matrix

| Failure Condition | Detection Mechanism | Persisted State | Recovery Action |
|:---|:---|:---|:---|
| **Process Crash / SIGKILL** | SQLite WAL recovery on restart | Uncommitted tx rolled back; committed tx durable | Re-open SQLite; drain WAL; resume sync engine |
| **Network Disconnect Mid-Batch** | TCP socket EOF / Phase 05 error | Committed ops durable; `peer_sync_state` at last ACK | Resume from last recorded `cursor_seq` upon reconnect |
| **Invalid Operation Signature** | `Ed25519::verify` fails | Operation dropped; not inserted | Log security warning; flag peer; ignore delta |
| **Missing Dependency ($O_1$)** | Dependency lookup in SQLite | $O_2$ saved with `status = 2` | Issue `DeltaRequest` for $O_1$; cascade apply |
| **Disk Exhaustion** | SQLite `SQLITE_FULL` error | Write fails; database consistent | Reject new writes; signal backpressure; alert user |
| **Malformed JSON/Bincode** | Deserialization error | Frame dropped | Discard message; do not crash runtime |

---

## 26. Concrete Test Contracts for Phase 06 Implementation

| Test ID | Name | Test Scenario & Verification Target |
|:---|:---|:---|
| **TEST 01** | `test_local_write_survives_restart` | Write document offline; terminate runtime; reboot from same data dir; assert document and change log row are identical. |
| **TEST 02** | `test_two_node_bidirectional_sync` | Node A and Node B connect; A sends document; B receives, verifies, and acknowledges; state vectors converge. |
| **TEST 03** | `test_offline_modification_sync` | A edits doc offline; B connects later; A transmits deltas; B applies and reaches convergence. |
| **TEST 04** | `test_concurrent_crdt_convergence` | A increments counter by 2 offline; B increments counter by 3 offline; reconnect; assert both read 5. |
| **TEST 05** | `test_idempotency_duplicate_ops` | Deliver operation $O_1$ three times; assert applied count is 1 and state changes only once. |
| **TEST 06** | `test_out_of_order_dependency_hold` | Send $O_2$ before parent $O_1$; verify $O_2$ is staged as `WaitingDependency`; deliver $O_1$; assert $O_2$ unblocks. |
| **TEST 07** | `test_interrupted_sync_resumes` | Abort TCP connection halfway through 10-op batch; reconnect; verify sync resumes from watermark without duplicate application. |
| **TEST 08** | `test_unauthorized_operation_rejected` | Inject operation from `NodeId` lacking namespace write capabilities; assert rejected fail-closed. |
| **TEST 09** | `test_tampered_signature_dropped` | Corrupt 1 byte of operation signature; assert operation dropped and runtime never panics. |
| **TEST 10** | `test_tombstone_prevents_resurrection` | A deletes doc; B (offline) reconnects with old version; assert tombstone dominates and document remains deleted. |
| **TEST 11** | `test_snapshot_creation_and_fast_sync` | Compact log at sequence 100; connect new blank node; deliver snapshot; assert node reaches valid state. |
| **TEST 12** | `test_dtn_store_and_forward_sync` | A edits doc destined for B; travels via Mule Node C in DTN queue; C connects to B; B receives and applies. |
| **TEST 13** | `test_three_node_mesh_convergence` | A, B, C form linear mesh $A \leftrightarrow B \leftrightarrow C$; A edits doc; verifies C receives via B and ACKs. |
| **TEST 14** | `test_lww_and_mv_register_resolution` | Concurrent register updates on A and B; verify LWW resolves by HLC, and MVR retains both branches. |

---

## 27. Phase 06 Implementation BUILD Sequence

The Phase 06 implementation is strictly divided into six bounded, progressive BUILD passes:

```
┌────────────────────────────────────────────────────────────────────────┐
│ BUILD-01: Local Database & Document Store Engine                       │
│   - SQLite WAL schema migrations, document table CRUD                  │
│   - DocumentId, DocumentRecord, local revision tracking                │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
┌───────────────────────────────────▼────────────────────────────────────┐
│ BUILD-02: Change Log & Causality Engine                                │
│   - Immutable operations table, atomic SQLite mutation transactions    │
│   - Hybrid Logical Clock (HLC) & StateVector implementation            │
│   - Ed25519 operation signing and verification pipeline                │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
┌───────────────────────────────────▼────────────────────────────────────┐
│ BUILD-03: CRDT Primitives & Conflict Resolution                        │
│   - PnCounter, OrSet, CrdtMap, LwwRegister, MvRegister, RgaSequence    │
│   - Mathematical semilattice property tests (proptest)                 │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
┌───────────────────────────────────▼────────────────────────────────────┐
│ BUILD-04: Synchronization Protocol & Sync Engine                       │
│   - SyncMessage framing over Phase 05 authenticated sessions           │
│   - Handshake, state vector diffing, paged DeltaResponse streaming     │
│   - Watermarking, cursor tracking, and sync state machine              │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
┌───────────────────────────────────▼────────────────────────────────────┐
│ BUILD-05: Snapshots, Log Compaction & DTN Integration                  │
│   - Checkpointing, stable watermark log pruning, tombstones            │
│   - Asynchronous sync transport via Phase 05 SQLite DtnQueue           │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
┌───────────────────────────────────▼────────────────────────────────────┐
│ BUILD-06: End-to-End Fault Injection, Hardening & Verification         │
│   - 14 integration test contracts executed over live network sockets   │
│   - Disconnection recovery, crash tests, and final DoD sign-off        │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 28. Phase 06 Definition of Done

Phase 06 will be declared **COMPLETE** if and only if all of the following criteria are proven by automated integration tests:

- [ ] Local documents can be created, read, updated, and deleted with zero network connectivity.
- [ ] Every mutation commits atomically to the append-only SQLite change log.
- [ ] Two nodes editing independently while offline converge to identical state upon reconnection.
- [ ] CRDT counters, sets, maps, registers, and sequences satisfy commutativity, associativity, and idempotency.
- [ ] Out-of-order operations buffer safely in SQLite and resolve when dependencies arrive.
- [ ] Duplicate and replayed sync operations are detected and safely ignored.
- [ ] Incremental synchronization exchanges only missing deltas (no whole-database transfers).
- [ ] Interrupted synchronization resumes cleanly from persisted watermarks.
- [ ] Multi-hop store-and-forward DTN queues successfully transport sync operations.
- [ ] Process SIGKILL during sync commit recovers cleanly with zero database corruption.
- [ ] Checkpoint snapshots allow fast-syncing of newly provisioned or heavily lagging nodes.
- [ ] Log compaction reclaims disk space while preserving correctness.
- [ ] Document authorization checks reject unauthorized operations fail-closed.
- [ ] Malformed or tampered remote synchronization payloads cannot crash or panic the runtime.
- [ ] 100% of automated tests pass without mocks.
- [ ] `IMPLEMENTATION_STATUS.md` and documentation accurately match implementation.
