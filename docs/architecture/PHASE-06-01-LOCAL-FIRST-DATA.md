# FIELDNET — Phase 06 / Architecture Document 01
**Title:** Local-First Data Model & Distributed Synchronization Architecture  
**Document ID:** `PHASE-06-01-LOCAL-FIRST-DATA`  
**Status:** ARCHITECTURE COMPLETE — AWAITING IMPLEMENTATION  
**Authoritative References:** `FIELDNET_MASTER_SPEC.md`, `docs/architecture/01-system-model.md` through `05-protocol-routing-dtn.md`, `docs/implementation/BUILD-01.md`, `BUILD-02.md`, `BUILD-03.md`.

---

## 1. Executive Summary & Core Goals

FIELDNET Phase 06 establishes the foundation for distributed data persistence and synchronization across autonomous nodes.

### The Fundamental Principle: Local State is Primary
In FIELDNET, **local state is primary, and synchronization is an opportunistic extension of local operation.**

A FIELDNET node must be fully capable of creating, reading, modifying, querying, and deleting its local data without requiring:
- An active Internet connection
- A centralized database, coordinator, or cloud server
- A permanent or synchronous peer network connection
- Synchronous quorum agreement across the mesh

When network connectivity becomes available—whether via direct Wi-Fi/Bluetooth links, local LAN connections, or multi-hop DTN routes established in Phase 05—nodes autonomously exchange mutation deltas and converge mathematically toward identical, causally consistent states.

```
┌────────────────────────────────────────────────────────────────────────┐
│                        Local Application Progress                      │
│       Create / Read / Update / Delete / Query (Zero Latency)          │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                     Local SQLite Database (WAL Mode)                   │
│   - Materialized Document State                                        │
│   - Immutable Append-Only Change Log                                   │
│   - Local Revision Counters & Hybrid Logical Clocks                    │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ Opportunistic / Event-Driven
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                      Synchronization Subsystem                         │
│   - State Vector Diffing (Missing Delta Detection)                     │
│   - Conflict Resolution (CRDT Layer & LWW/MVR Policies)                │
│   - Causal Dependency Resolution                                       │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                      Phase 05 Networking Subsystem                     │
│   - Authenticated Sessions (X25519 / ChaCha20-Poly1305)                │
│   - Distance-Vector Multi-Hop Routing                                  │
│   - Delay-Tolerant Store-and-Forward (SQLite DTN Queue)                │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Canonical Data Model

### 2.1 Layered Representation Separation
To prevent conflating local edits with replicated state, FIELDNET strictly separates six distinct data facets:

```
┌─────────────────────────────────────────────────────────────────────────┐
│ 1. Application Data:        Domain payload (JSON, text, binary structs) │
├─────────────────────────────────────────────────────────────────────────┤
│ 2. Metadata:                Document ID, object type, owner, timestamps │
├─────────────────────────────────────────────────────────────────────────┤
│ 3. Synchronization Metadata:Version vectors, HLC, causal dependency DAG │
├─────────────────────────────────────────────────────────────────────────┤
│ 4. Change History:          Immutable append-only signed operation log  │
├─────────────────────────────────────────────────────────────────────────┤
│ 5. Local State:             Locally materialized working view           │
├─────────────────────────────────────────────────────────────────────────┤
│ 6. Replicated State:        Mathematically merged CRDT state vector     │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Logical Document Structure
A logical document is defined independently of any physical device, IP address, transport link, or current provider node. A document retains its identity across migration, replication, and physical node restarts.

```rust
/// Sovereign 32-byte content/entropy document identifier.
/// Derived as BLAKE3(namespace || owner_node_id || entropy).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct DocumentId(pub [u8; 32]);

/// Canonical representation of a FIELDNET document.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Document {
    /// Global sovereign document identifier.
    pub document_id: DocumentId,

    /// Semantic namespace/type tag (e.g., "org.fieldnet.text", "org.fieldnet.task").
    pub object_type: String,

    /// Sovereign NodeId of the original document creator.
    pub owner: NodeId,

    /// Monotonically increasing logical version counter.
    pub version: u64,

    /// Node-local mutation sequence counter.
    pub local_revision: u64,

    /// Physical UTC creation timestamp (milliseconds since UNIX epoch).
    pub created_at: Timestamp,

    /// Physical UTC last update timestamp.
    pub updated_at: Timestamp,

    /// Deletion tombstone marker (preserves causal history during deletion).
    pub is_deleted: bool,

    /// Serialized document state (materialized view).
    pub payload: Vec<u8>,

    /// Causal state vector summarizing applied operations.
    pub state_vector: StateVector,
}
```

---

## 3. Local Persistence Architecture

### 3.1 Storage Engine: SQLite in WAL Mode
The local storage subsystem uses SQLite compiled in Write-Ahead Logging (`WAL`) mode with `NORMAL` synchronous settings, ensuring crash-safe atomic transactions with concurrent non-blocking reads and high write throughput.

### 3.2 Relational Schema
```sql
-- 1. Materialized Documents Table
CREATE TABLE IF NOT EXISTS documents (
    document_id         BLOB PRIMARY KEY,   -- 32-byte BLAKE3 identifier
    object_type         TEXT NOT NULL,      -- Semantic schema/type string
    owner_node_id       BLOB NOT NULL,      -- 32-byte NodeId of creator
    version             INTEGER NOT NULL,   -- Global logical version
    local_revision      INTEGER NOT NULL,   -- Node-local update counter
    created_at          INTEGER NOT NULL,   -- Unix epoch milliseconds
    updated_at          INTEGER NOT NULL,   -- Unix epoch milliseconds
    is_deleted          INTEGER NOT NULL,   -- 0 = active, 1 = tombstone
    payload_bytes       BLOB NOT NULL,      -- Serialized materialized payload
    state_vector_bytes  BLOB NOT NULL       -- Serialized causal StateVector
);

CREATE INDEX IF NOT EXISTS idx_docs_type ON documents(object_type);
CREATE INDEX IF NOT EXISTS idx_docs_updated ON documents(updated_at);

-- 2. Append-Only Change Log Table
CREATE TABLE IF NOT EXISTS change_log (
    op_id               BLOB PRIMARY KEY,   -- 32-byte BLAKE3 operation hash
    document_id         BLOB NOT NULL,      -- Target document identifier
    source_node_id      BLOB NOT NULL,      -- Authoring NodeId
    sequence            INTEGER NOT NULL,   -- Monotonic sequence from author
    op_type             INTEGER NOT NULL,   -- 1=Create, 2=Update, 3=Delete, 4=CRDT
    delta_bytes         BLOB NOT NULL,      -- Serialized mutation delta
    hlc_physical        INTEGER NOT NULL,   -- HLC physical millisecond component
    hlc_logical         INTEGER NOT NULL,   -- HLC logical counter component
    dependencies_bytes  BLOB NOT NULL,      -- List of parent op_ids (JSON/bincode)
    signature           BLOB NOT NULL,      -- 64-byte Ed25519 signature from author
    created_at          INTEGER NOT NULL,   -- Local ingestion epoch milliseconds
    is_applied          INTEGER NOT NULL DEFAULT 1,
    FOREIGN KEY(document_id) REFERENCES documents(document_id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_changelog_doc_src_seq 
    ON change_log(document_id, source_node_id, sequence);
CREATE INDEX IF NOT EXISTS idx_changelog_doc_hlc 
    ON change_log(document_id, hlc_physical, hlc_logical);

-- 3. Peer Synchronization Watermarks
CREATE TABLE IF NOT EXISTS peer_sync_state (
    peer_node_id        BLOB NOT NULL,      -- 32-byte NodeId of remote peer
    document_id         BLOB NOT NULL,      -- Target document identifier
    last_synced_seq     INTEGER NOT NULL,   -- Last confirmed sequence from peer
    last_synced_hlc_p   INTEGER NOT NULL,   -- Last confirmed HLC physical time
    last_synced_hlc_l   INTEGER NOT NULL,   -- Last confirmed HLC logical count
    last_sync_time      INTEGER NOT NULL,   -- Local epoch time of synchronization
    sync_status         TEXT NOT NULL,      -- 'CONVERGED', 'PENDING', etc.
    PRIMARY KEY(peer_node_id, document_id)
);

-- 4. Conflict Staging Records
CREATE TABLE IF NOT EXISTS conflict_records (
    conflict_id         BLOB PRIMARY KEY,   -- 32-byte identifier
    document_id         BLOB NOT NULL,      -- Associated document
    local_op_id         BLOB NOT NULL,      -- Conflicting local operation
    remote_op_id        BLOB NOT NULL,      -- Conflicting remote operation
    detected_at         INTEGER NOT NULL,   -- Local epoch time of detection
    strategy            TEXT NOT NULL,      -- 'LWW', 'CRDT', 'MANUAL', etc.
    resolved_op_id      BLOB,               -- Resulting resolution operation
    status              TEXT NOT NULL       -- 'UNRESOLVED', 'RESOLVED'
);

-- 5. Document Snapshots & Compaction Checkpoints
CREATE TABLE IF NOT EXISTS document_snapshots (
    snapshot_id         BLOB PRIMARY KEY,   -- 32-byte snapshot identifier
    document_id         BLOB NOT NULL,      -- Target document
    snapshot_version    INTEGER NOT NULL,   -- Logical document version at snapshot
    compacted_up_to_hlc INTEGER NOT NULL,   -- HLC physical boundary of compaction
    state_vector_bytes  BLOB NOT NULL,      -- StateVector at snapshot boundary
    snapshot_payload    BLOB NOT NULL,      -- Full serialized document state
    created_at          INTEGER NOT NULL    -- Ingestion epoch milliseconds
);
```

---

## 4. Append-Oriented Change Log

### 4.1 Mutation Invariant
Every state modification—whether originated locally by the user application or received from a remote peer—is represented as an immutable change operation before it is applied to the materialized document view:

$$\text{Local Write} \longrightarrow \text{Durable Operation} \longrightarrow \text{Sync Queue} \longrightarrow \text{Network Transfer} \longrightarrow \text{Remote Validation} \longrightarrow \text{CRDT Apply}$$

### 4.2 Operation Definition
```rust
/// Sovereign 32-byte operation identifier.
/// Derived as BLAKE3(document_id || source_node || sequence || hlc || delta).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct OperationId(pub [u8; 32]);

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum OperationType {
    Create = 1,
    Update = 2,
    Delete = 3,
    CrdtDelta = 4,
}

/// Immutable, cryptographically signed change log entry.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChangeOperation {
    pub op_id: OperationId,
    pub document_id: DocumentId,
    pub source_node: NodeId,
    pub sequence: u64,
    pub op_type: OperationType,
    pub delta: Vec<u8>,
    pub hlc: HybridLogicalClock,
    pub causal_dependencies: Vec<OperationId>,
    #[serde(with = "hex")]
    pub signature: [u8; 64],
}

impl ChangeOperation {
    /// Computes canonical signing bytes covering all immutable operation fields.
    pub fn signing_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(self.document_id.0.as_ref());
        bytes.extend_from_slice(self.source_node.as_bytes());
        bytes.extend_from_slice(&self.sequence.to_be_bytes());
        bytes.push(self.op_type as u8);
        bytes.extend_from_slice(&self.hlc.physical.to_be_bytes());
        bytes.extend_from_slice(&self.hlc.logical.to_be_bytes());
        for dep in &self.causal_dependencies {
            bytes.extend_from_slice(&dep.0);
        }
        bytes.extend_from_slice(&self.delta);
        bytes
    }
}
```

---

## 5. Causality Model: Hybrid Logical Clocks (HLC)

### 5.1 Clock Mechanism Evaluation
| Mechanism | Ordering Capability | Vector Size Overhead | Physical Time Correlation | Decision |
|:---|:---|:---|:---|:---:|
| **Lamport Clocks** | Partial / Total arbitrary | $O(1)$ | None (pure logical) | Insufficient for time queries |
| **Vector Clocks** | Strict Causality ($A < B, A \parallel B$) | $O(N)$ (grows with nodes) | None | Excessive overhead in open mesh |
| **Hybrid Logical Clocks** | Strict Causality + Monotonic Time | $O(1)$ scalar (12 bytes) | Bounded to physical wall clock | **SELECTED PRIMARY** |

### 5.2 HLC Definition & Invariants
An HLC timestamp consists of a physical component (Unix epoch milliseconds) and a logical counter component:

```rust
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct HybridLogicalClock {
    pub physical: u64,
    pub logical: u32,
}

impl HybridLogicalClock {
    /// Increments local clock upon a local write event.
    pub fn send(&mut self, physical_now: u64) {
        if physical_now > self.physical {
            self.physical = physical_now;
            self.logical = 0;
        } else {
            self.logical += 1;
        }
    }

    /// Advances local clock upon receiving a remote HLC timestamp.
    pub fn receive(&mut self, remote: &HybridLogicalClock, physical_now: u64) {
        let max_physical = self.physical.max(remote.physical).max(physical_now);
        if max_physical == self.physical && max_physical == remote.physical {
            self.logical = self.logical.max(remote.logical) + 1;
        } else if max_physical == self.physical {
            self.logical += 1;
        } else if max_physical == remote.physical {
            self.logical = remote.logical + 1;
        } else {
            self.logical = 0;
        }
        self.physical = max_physical;
    }
}
```

### 5.3 Causal Relation Determination
For two operations $O_1$ and $O_2$:
1. **Direct Causal Precedence ($O_1 \to O_2$):** If $O_1.\text{op\_id} \in O_2.\text{causal\_dependencies}$, then $O_1$ causally precedes $O_2$.
2. **HLC Ordering:** If $O_1.\text{hlc} < O_2.\text{hlc}$ and neither has causal dependency on the other, they are concurrent ($O_1 \parallel O_2$), but HLC provides a deterministic tie-breaker:
   $$\text{Winner} = \begin{cases} O_1 & \text{if } O_1.\text{hlc} > O_2.\text{hlc} \\ \max(O_1.\text{source}, O_2.\text{source}) & \text{if } O_1.\text{hlc} == O_2.\text{hlc} \end{cases}$$

---

## 6. Synchronization Protocol Over Phase 05

FIELDNET Synchronization does not create a new transport or socket abstraction. It executes directly on top of Phase 05 authenticated sessions, multi-hop routing, and persistent DTN queues.

### 6.1 Protocol Messages
```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SyncMessage {
    /// Initial synchronization greeting listing supported namespaces.
    Hello {
        protocol_version: u32,
        supported_namespaces: Vec<String>,
    },

    /// Compact state summary listing document IDs and state vector summaries.
    StateSummary {
        summaries: Vec<DocumentStateSummary>,
    },

    /// Request for specific missing operations or causal ranges.
    DeltaRequest {
        document_id: DocumentId,
        known_vector: StateVector,
        limit: u32,
    },

    /// Batched payload of missing change log operations.
    DeltaResponse {
        document_id: DocumentId,
        operations: Vec<ChangeOperation>,
        has_more: bool,
    },

    /// Immediate, unsolicited push of newly created local operation.
    LiveOperation {
        operation: ChangeOperation,
    },

    /// Delivery & application confirmation for synchronization state watermark.
    SyncAck {
        document_id: DocumentId,
        up_to_op_id: OperationId,
        applied_state_vector: StateVector,
    },

    /// Snapshot baseline for fast-syncing nodes missing historical compaction log.
    SnapshotDelivery {
        document_id: DocumentId,
        snapshot_version: u64,
        state_vector: StateVector,
        snapshot_payload: Vec<u8>,
    },
}
```

### 6.2 Transport Mapping
When transmitting sync traffic between nodes:
- Messages are serialized to canonical binary JSON.
- Wrapped into a Phase 05 `Message` envelope with `MessageType::Data` (or dedicated `MessageType::Sync = 6`).
- Transported via Phase 05 `ConnectionManager` (if directly connected) or staged into the SQLite `DtnQueue` (if destination is across multi-hop or currently unreachable).

---

## 7. Synchronization Handshake & Incremental Diffing

When Node A meets Node B, they NEVER exchange the entire database. They execute an incremental, state-vector-based negotiation:

```
  Node A                                                         Node B
    │                                                              │
    │ 1. SyncMessage::Hello                                        │
    ├─────────────────────────────────────────────────────────────>│
    │                                                              │
    │ 2. SyncMessage::StateSummary (StateVectors for shared docs)  │
    │<─────────────────────────────────────────────────────────────┤
    │                                                              │
    │ [Evaluate Delta: Compute A \ B and B \ A]                     │
    │                                                              │
    │ 3. SyncMessage::DeltaRequest (Request B's missing ops)       │
    ├─────────────────────────────────────────────────────────────>│
    │                                                              │
    │ 4. SyncMessage::DeltaResponse (Operations missing on A)      │
    │<─────────────────────────────────────────────────────────────┤
    │                                                              │
    │ [Validate Signatures -> Causal Sort -> Apply to SQLite]      │
    │                                                              │
    │ 5. SyncMessage::SyncAck (A confirms ops applied to watermark)│
    ├─────────────────────────────────────────────────────────────>│
```

---

## 8. Sync State Machine

Every document maintains a discrete synchronization state relative to the surrounding environment:

```
                 Local Mutation
  ┌───────┐ ───────────────────────> ┌─────────┐
  │ LOCAL │                          │ PENDING │
  └───────┘ <─────────────────────── └────┬────┘
      │        All Sync Cancelled         │
      │                                   │ Peer Discovered / DTN Triggered
      │                                   ▼
      │                              ┌─────────┐
      │                              │ SYNCING │
      │                              └────┬────┘
      │                                   │
      │                  ┌────────────────┴────────────────┐
      │                  │                                 │
      │                  ▼                                 ▼
      │        ┌──────────────────┐               ┌────────────┐
      └──────> │ PARTIALLY_SYNCED │               │ CONVERGED  │
               └─────────┬────────┘               └────────────┘
                         │                               ▲
                         │ Concurrent Non-CRDT Conflict   │ Automatic / Manual
                         ▼                               │ Resolution
                    ┌────────────┐                       │
                    │ CONFLICTED │ ──────────────────────┘
                    └────────────┘
```

### State Definitions
1. **`LOCAL`:** Document created locally; no remote peers configured or tracking this document.
2. **`PENDING`:** Local mutations recorded in SQLite change log; awaiting transmission to known peers.
3. **`SYNCING`:** Active exchange of `StateSummary`, `DeltaRequest`, or `DeltaResponse` in progress.
4. **`PARTIALLY_SYNCED`:** Synchronized with one or more available peers, but known network partitions keep other peers unreached. **This is a healthy, expected operating state in FIELDNET.**
5. **`CONVERGED`:** State vectors and operation logs are identical across all currently known, authorized replicas.
6. **`CONFLICTED`:** Concurrent conflicting mutations detected on non-CRDT fields requiring deterministic resolution policy or user intervention.

---

## 9. Conflict Model & Resolution Policies

FIELDNET distinguishes between **automatic CRDT convergence** and **semantic conflict resolution**:

### 9.1 Multi-Tier Resolution Hierarchy
```
                                 Incoming Mutation
                                         │
                                         ▼
                            Is Target Field a CRDT?
                                    /         \
                             YES   /           \   NO
                                  ▼             ▼
                         Apply CRDT Merge    Evaluate Causality (HLC + DAG)
                         (Math Convergence)     /                   \
                                               / Concurrent ($A \parallel B$)
                                        Causal ($A \to B$)           \
                                             /                        ▼
                                            ▼                Is Field LWW Register?
                                     Overwrite Older                 /        \
                                                              YES   /          \   NO
                                                                   ▼            ▼
                                                            Apply Highest HLC  Store in Conflict Table
                                                            (Tie: Max NodeId)  (Multi-Value Register / Manual)
```

1. **CRDT Automatic Convergence:** Commutative, associative, idempotent data types converge without conflict.
2. **Last-Write-Wins (LWW) Register:** Explicitly selected for metadata and scalar fields. The mutation with higher HLC wins. If HLC is identical, the lexicographically higher `NodeId` wins deterministically.
3. **Multi-Value Register (MVR):** Retains all concurrent values ($A \parallel B$) as discrete branches until an explicit application merge operation is submitted.
4. **Application-Defined Merge Hooks:** Application-specific resolvers registered for custom semantic reconciliations.
5. **User-Assisted Resolution:** Conflicting branches are staged in the `conflict_records` table, allowing user interaction without blocking node runtime operations.

---

## 10. CRDT Abstraction Layer

### 10.1 Generic CRDT Trait
```rust
pub trait Crdt {
    type State: Clone + Serialize + for<'de> Deserialize<'de>;
    type Operation: Clone + Serialize + for<'de> Deserialize<'de>;
    type Value;

    /// Evaluates current state to produce a read value.
    fn read(&self) -> Self::Value;

    /// Generates a mutation delta from a local application intent.
    fn generate_op(&self, intent: &Self::Value) -> Self::Operation;

    /// Applies a local or remote delta to the CRDT state.
    /// Invariant: Must be Commutative, Associative, and Idempotent.
    fn apply_op(&mut self, op: &Self::Operation) -> Result<(), CrdtError>;

    /// Merges full state from another replica (e.g. during snapshot restore).
    fn merge_state(&mut self, other: Self::State) -> Result<(), CrdtError>;

    /// Serializes state for persistence or transmission.
    fn to_bytes(&self) -> Result<Vec<u8>, CrdtError>;
}
```

### 10.2 Supported Core CRDT Primitives
1. **`PnCounter` (Positive-Negative Counter):** Supports concurrent increments and decrements via two state vectors ($P, N$).
2. **`OrSet<T>` (Observed-Remove Set):** Elements tagged with unique monotonic UUIDs/HLCs upon addition. Removal adds observed tags to a tombstone set.
3. **`LwwRegister<T>`:** Single value tagged with HLC timestamp. Overwrites occur only if new HLC is strictly higher.
4. **`MvRegister<T>`:** Preserves concurrent values as a vector until merged.
5. **`CrdtMap<K, V: Crdt>`:** Recursive map supporting nested CRDT types for complex structured documents.
6. **`RgaSequence<T>` (Replicated Growable Array):** Causal tree-based sequence supporting concurrent character and item insertions/deletions for collaborative text and lists.

---

## 11. Eventual Consistency & Convergence Guarantee

### Strong Eventual Consistency (SEC)
FIELDNET implements **Strong Eventual Consistency (SEC)**:
> Any two replicas that have received and applied the exact same set of operations are guaranteed to be in identical logical state, regardless of the order in which those operations were transmitted, routed, or applied.

### Distinction of State Milestones
- **Packet Delivered:** The Phase 05 network layer transported bytes to the destination node.
- **Durable:** The operation is safely committed to the recipient's SQLite change log.
- **Causally Ready:** All ancestor operations referenced in `causal_dependencies` are present locally.
- **Applied:** The operation has been merged into the materialized view.
- **Converged:** The local state vector matches all known replica watermarks.

---

## 12. State Comparison & Merkle Summaries

To discover differences without transferring raw operations:

### 12.1 State Vector Summaries
Each node maintains a `StateVector` mapping each contributing author `NodeId` to their latest known sequence number:

$$\text{StateVector} = \{ \text{Node}_A \mapsto \text{Seq}_5, \text{Node}_B \mapsto \text{Seq}_8 \}$$

When comparing State Vectors:
- If $\text{Vector}_A == \text{Vector}_B$: Replicas are identical ($O(1)$ equality check).
- If $\text{Vector}_A \setminus \text{Vector}_B \neq \emptyset$: Node A has operations Node B is missing. Node B requests $[\text{Vector}_B[\text{Node}_A] + 1 \dots \text{Vector}_A[\text{Node}_A]]$.

### 12.2 Merkle DAG Foundation for Large Datasets
For documents with extensive historical depth, operation logs are structured as a Merkle Directed Acyclic Graph (DAG) where each operation includes the `BLAKE3` hash of its causal parents. Root state hashes enable instantaneous $O(1)$ divergence detection across thousands of documents.

---

## 13. Delay-Tolerant Networking (DTN) Integration

Distributed synchronization in FIELDNET is natively delay-tolerant:

```
┌──────────────────┐           Store-and-Forward           ┌──────────────────┐
│      Node A      │ ────────────────────────────────────> │  Mule Node C     │
│  (Edits Doc X)   │       (DTN SQLite Queue on C)         │  (Moves to site) │
└──────────────────┘                                       └────────┬─────────┘
                                                                    │ Opportunistic Link
                                                                    ▼
                                                           ┌──────────────────┐
                                                           │      Node B      │
                                                           │  (Receives Ops)  │
                                                           └──────────────────┘
```

1. Node A edits document $X$ while disconnected from Node B.
2. Node A creates a signed `ChangeOperation` and writes it to local SQLite.
3. Node A meets intermediate Mule Node C.
4. Node A transmits the sync operation to Node C via Phase 05 `ConnectionManager`.
5. Node C does not own or understand the document payload; it stages the Phase 05 `Message` in its persistent SQLite `DtnQueue`.
6. Node C travels to a remote site and connects to Node B.
7. Node C dispatches the queued message to Node B.
8. Node B unpacks the `SyncMessage::LiveOperation`, verifies Node A's Ed25519 signature, checks causal dependencies, and applies the operation to its local database.

---

## 14. Offline-First Operational Guarantees

| Network Condition | System Behavior | Guarantees Maintained |
|:---|:---|:---|
| **Zero Peers Known** | Full local read/write execution | Zero latency, ACID durability in SQLite |
| **Peer Disconnection** | Transition to `PARTIALLY_SYNCED` or `PENDING` | Local progress continues unhindered |
| **Partial Partition** | Subsets of mesh synchronize locally | Cluster convergence, causal ordering intact |
| **Interrupted Sync** | Transaction rollback in SQLite | Zero data corruption, resumes from watermark |
| **Out-of-Order Delivery** | Staged in `unapplied_operations` buffer | Operations applied strictly in causal order |
| **Duplicate Delivery** | Ignored via `UNIQUE(document_id, op_id)` | Perfect idempotency, no double-application |

---

## 15. Duplication Detection & Idempotency

Idempotency is enforced at three distinct layers:
1. **Transport Layer:** Phase 05 `MessageDeduplicator` discards retransmitted network frames.
2. **Database Engine Layer:** The `change_log` table enforces `UNIQUE(document_id, op_id)`. Re-insertions fail gracefully with an `InsertIgnored` result.
3. **CRDT Application Layer:** All CRDT mutations satisfy $f(f(S, \Delta), \Delta) = f(S, \Delta)$. Applying an identical delta twice yields zero change to materialized state.

---

## 16. Snapshots & Change Log Compaction

An append-only log cannot grow indefinitely on resource-constrained devices:

### 16.1 Compaction Lifecycle
1. **Stable State Watermark:** A logical sequence boundary $S_{compact}$ is stable when:
   $$\forall \text{ active peer } P_i, \quad \text{LastSyncedSequence}(P_i) \ge S_{compact}$$
2. **Snapshot Creation:** The materialized document state at $S_{compact}$ is serialized and committed to `document_snapshots`.
3. **Log Truncation:** Historical operations with sequence $\le S_{compact}$ are purged from `change_log`.
4. **Tombstone Garbage Collection:** Deletion markers older than a configurable grace period ($\Delta_{gc} = 30\text{ days}$) are purged once all known peers acknowledge the tombstone.
5. **Fast-Syncing:** A newly joined peer or heavily lagging node receives the latest `SnapshotDelivery` followed by incremental deltas created after the snapshot.

---

## 17. Security, Trust & Cryptographic Boundaries

1. **Sovereign Author Signatures:**
   - Every operation is digitally signed by its authoring node: $\text{Ed25519Sign}_{SK_{author}}(\text{signing\_bytes})$.
   - Receiving nodes verify that `NodeId::from_ed25519_public_key(&pubkey) == op.source_node`.
   - Relays and intermediate DTN nodes cannot tamper with document deltas without invalidating the cryptographic signature.
2. **Namespace Authorization:**
   - Operation application checks the authoring `NodeId` against the document's capability ACL. Unauthorized updates are rejected fail-closed.
3. **Transport Encryption:**
   - Sync messages in transit are encapsulated in Phase 05 ChaCha20-Poly1305 AEAD sessions.
4. **Zero-Leakage Observability:**
   - Tracing events log only `document_id`, `op_id`, `source_node`, and sequence numbers. User data payloads and cryptographic keys are never emitted.

---

## 18. Application API Boundary

Applications interact with high-level asynchronous traits, completely shielded from network sockets, DTN queues, routing tables, and cryptographic handshakes:

```rust
#[async_trait]
pub trait DocumentStore: Send + Sync {
    /// Creates a new sovereign document.
    async fn create(&self, object_type: &str, initial_payload: &[u8]) -> Result<Document, StorageError>;

    /// Retrieves materialized document state by DocumentId.
    async fn get(&self, doc_id: &DocumentId) -> Result<Option<Document>, StorageError>;

    /// Submits a local modification delta.
    async fn update(&self, doc_id: &DocumentId, delta: &[u8]) -> Result<OperationId, StorageError>;

    /// Marks a document deleted using a causal tombstone.
    async fn delete(&self, doc_id: &DocumentId) -> Result<OperationId, StorageError>;

    /// Subscribes to real-time document mutation events.
    fn subscribe(&self, doc_id: &DocumentId) -> broadcast::Receiver<DocumentChangeEvent>;
}

#[async_trait]
pub trait SyncManager: Send + Sync {
    /// Returns current synchronization status for a document.
    async fn status(&self, doc_id: &DocumentId) -> Result<SyncStatus, StorageError>;

    /// Explicitly triggers synchronization with all reachable peers.
    async fn sync_now(&self) -> Result<(), StorageError>;

    /// Subscribes to synchronization lifecycle events (convergence, conflicts).
    fn subscribe_sync_events(&self) -> broadcast::Receiver<SyncLifecycleEvent>;
}
```

---

## 19. Failure Modes & Graceful Degradation

| Failure Condition | Detection Mechanism | Recovery & Degradation Strategy |
|:---|:---|:---|
| **SQLite File Corruption** | SQLite error code on connection open | Fail-safe: recover from last `document_snapshot`; replay verified WAL |
| **Transaction Failure / Power Loss** | WAL journal rollback upon restart | Partial write rolled back cleanly; node restarts in consistent state |
| **Invalid Operation Signature** | `Ed25519::verify` fails on remote op | Operation discarded; security event logged; peer flagged; runtime stable |
| **Missing Causal Dependency** | `causal_dependencies` not found locally | Staged in `unapplied_operations`; issues `DeltaRequest` for missing parent |
| **Storage Quota Reached** | Disk write error / quota tripwire | Graceful backpressure: refuse incoming deltas; trigger compaction; alert user |
| **Malformed Payload Flood** | Deserialization error / length limit | Reject frame; close malicious peer session; no panic or buffer overflow |

---

## 20. Observability & Structured Tracing

FIELDNET Phase 06 emits structured events containing correlation identifiers:

```rust
// Structured Event Examples
tracing::info!(
    event = "local_mutation_committed",
    document_id = %doc_id,
    op_id = %op.op_id,
    hlc = %op.hlc,
    payload_len = op.delta.len()
);

tracing::info!(
    event = "sync_delta_applied",
    document_id = %doc_id,
    source_node = %op.source_node,
    op_id = %op.op_id,
    sequence = op.sequence
);

tracing::warn!(
    event = "concurrent_conflict_detected",
    document_id = %doc_id,
    local_op = %local_id,
    remote_op = %remote_id,
    strategy = "LWW_RESOLVED"
);

tracing::info!(
    event = "document_converged",
    document_id = %doc_id,
    state_vector = ?vector
);
```

---

## 21. Testing Strategy & Verification Plan

Prior to implementation, the test matrix mandates coverage across 12 distinct categories:

1. **Unit Tests:** HLC clock progression, state vector comparisons, operation signature verification.
2. **Local Persistence Tests:** SQLite transaction commits, rollbacks, and concurrent read/write throughput.
3. **Mathematical CRDT Convergence Tests:** Property-based testing (`proptest`) verifying commutativity, associativity, and idempotency across arbitrary permutation sequences.
4. **Causal Dependency Resolution:** Validating that out-of-order operations are buffered and applied strictly after their parents arrive.
5. **Idempotency Verification:** Injecting identical operations 100 times; verifying state vector and materialized payload remain invariant.
6. **Offline Progression:** Modifying documents completely offline; verifying local read consistency.
7. **Two-Node Convergence:** Node A and Node B edit independently offline, reconnect, exchange deltas, and converge.
8. **Three-Node Multi-Hop DTN Sync:** Node A edits document; reaches Node B via Mule Node C store-and-forward.
9. **Conflict Resolution Scenarios:** Exercising LWW, Multi-Value Register branching, and manual resolution hooks.
10. **Compaction & Snapshot Recovery:** Truncating change log at stable watermark and restoring full state from snapshot.
11. **Fault Injection:** Sudden SIGKILL during active synchronization transaction; verifying clean WAL recovery on restart.
12. **Fuzz Testing:** Supplying corrupted bincode/JSON payloads to synchronization parsers; verifying zero panics.

---

## 22. Phase 06 Implementation Boundary

### In-Scope for Phase 06 Implementation
- `crates/fieldnet-storage`: SQLite WAL engine, document tables, change log persistence.
- `crates/fieldnet-crdt`: Standard CRDT primitives (PnCounter, OrSet, LwwRegister, MvRegister, CrdtMap, RgaSequence).
- `crates/fieldnet-sync`: Sync protocol engine, state vector diffing, causal dependency resolver, DTN sync integration.

### Strictly Out-of-Scope (Deferred to Later Phases)
- Distributed content-addressed file chunking & streaming (Deferred to **Phase 07**).
- Local web gateway, HTTP proxies, and `fieldnet.local` resolution (Deferred to **Phase 08**).
- Sandboxed WASM execution environments (Deferred to **Phase 09**).
- Distributed compute & local AI inference (Deferred to **Phase 10**).
- Internet egress proxies (Deferred to **Phase 11**).

---

## 23. Open Architectural Questions

1. **Causal History Pruning in Disconnected Partitions:** If a peer remains disconnected for months, how long should active replicas retain change history before forcing that peer to perform a full snapshot reset upon return?
   - *Working Decision:* 30-day compaction watermark grace period ($\Delta_{gc}$). Beyond that window, lagging peers fast-sync via `SnapshotDelivery`.
2. **Partial Namespace Subscriptions:** Can a node subscribe to a single document without synchronizing the entire document table?
   - *Working Decision:* Yes. `SyncMessage::DeltaRequest` is strictly scoped per `DocumentId` and semantic `object_type`.

---

## 24. Phase 06 Acceptance Criteria

Phase 06 implementation will be declared COMPLETE only when:
- [ ] Local documents can be created, read, updated, and deleted with zero network connectivity.
- [ ] Every mutation is durably recorded in the append-only SQLite change log.
- [ ] Two nodes editing independently while offline converge to identical state upon reconnection.
- [ ] Concurrent edits are deterministically resolved via CRDT or configured conflict policy.
- [ ] Multi-hop store-and-forward DTN synchronization works across intermediate relays.
- [ ] Duplicate and replayed sync operations produce zero data corruption (strict idempotency).
- [ ] Out-of-order operations are held until causal dependencies arrive.
- [ ] Snapshot creation and log compaction successfully reclaim storage.
- [ ] Malformed or tampered remote synchronization payloads cannot crash or panic the runtime.
- [ ] 100% of automated tests pass without mocks.
