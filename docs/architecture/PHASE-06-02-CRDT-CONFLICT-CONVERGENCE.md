# FIELDNET — Phase 06 / Architecture Document 02
**Title:** CRDT Model, Conflict Resolution & Convergence Semantics  
**Document ID:** `PHASE-06-02-CRDT-CONFLICT-CONVERGENCE`  
**Status:** ARCHITECTURE COMPLETE — AWAITING IMPLEMENTATION  
**Authoritative References:** `FIELDNET_MASTER_SPEC.md`, `docs/architecture/01-system-model.md` through `05-protocol-routing-dtn.md`, `docs/architecture/PHASE-06-01-LOCAL-FIRST-DATA.md`, `docs/implementation/BUILD-01.md` through `BUILD-03.md`.

---

## 1. Distributed State Model

FIELDNET models distributed application state as a causally ordered, immutable progression of cryptographically signed operations that converge deterministically into a local materialized view.

### 1.1 Structural Hierarchy
```
┌────────────────────────────────────────────────────────────────────────┐
│                        Logical Document (DocumentId)                   │
│   - Globally unique 32-byte sovereign identifier:                      │
│     BLAKE3(namespace || creator_node_id || entropy)                    │
│   - Completely decoupled from hosting node, device, IP, or transport   │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                  Current Materialized State (SQLite Row)               │
│   - Fast local read projection (e.g. JSON document, text, counter)     │
│   - Derived deterministically by folding the operation log             │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                    Operation Log (Change Log History)                  │
│   - Immutable, append-only, signed delta stream                        │
│   - Partial order governed by causal dependency DAG and HLC            │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                        Causal & Sync Metadata                          │
│   - Version Vectors (actor_id -> max_sequence)                         │
│   - Hybrid Logical Clock timestamps (physical_millis, logical_count)   │
│   - Tombstone registry for safely deleted elements                     │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                         Replica State (Per-Peer)                       │
│   - Watermarks: last confirmed sequence synchronized per peer          │
│   - In-flight operations buffered in Phase 05 DTN queue                │
└────────────────────────────────────────────────────────────────────────┘
```

### 1.2 Separation of Concerns
1. **Materialized State:** The fast, indexed representation queried by applications in local SQLite tables (`documents`).
2. **Operation History:** The authoritative append-only journal of all state transitions (`change_log`).
3. **Replica Metadata:** Local operational accounting (unapplied buffers, retry counters, dirty flags).
4. **Synchronization Metadata:** The exchangeable causal summary (`VersionVector`, `HLC`, Merkle state roots) used to compute minimal difference sets between replicas.

---

## 2. Canonical Operation Model

Every state modification is an immutable, self-contained, digitally signed `ChangeOperation`.

### 2.1 Operation Structure
```rust
/// Globally unique 32-byte cryptographic operation identifier.
/// Derived as: BLAKE3(document_id || actor_id || sequence || hlc_bytes || payload_hash)
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct OperationId(pub [u8; 32]);

/// Semantic category of the operation.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum OperationType {
    /// Document creation with initial state.
    Create = 1,
    /// Commutative / associative CRDT delta application.
    CrdtDelta = 2,
    /// Field-level or register-level assignment.
    RegisterAssign = 3,
    /// Causal tombstone marking deletion.
    DeleteTombstone = 4,
}

/// The atomic unit of distributed mutation in FIELDNET.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChangeOperation {
    /// Cryptographic identifier of this operation.
    pub op_id: OperationId,

    /// Target document identifier.
    pub document_id: DocumentId,

    /// Sovereign NodeId of the author who created this operation.
    pub actor_id: NodeId,

    /// Monotonically increasing sequence number from this actor for this document.
    pub sequence: u64,

    /// Operation classification.
    pub op_type: OperationType,

    /// Specific CRDT delta or value payload (e.g. PN-Counter delta, OR-Set tag, RGA char).
    pub payload: Vec<u8>,

    /// Hybrid Logical Clock timestamp when the operation was committed locally.
    pub hlc: HybridLogicalClock,

    /// Causal dependencies: list of immediate ancestor OperationIds that this
    /// operation causally observed and depends upon (Merkle-DAG edges).
    pub causal_dependencies: Vec<OperationId>,

    /// Physical UTC creation timestamp (milliseconds since UNIX epoch).
    pub created_at: u64,

    /// Ed25519 digital signature of the author over canonical signing bytes.
    #[serde(with = "hex")]
    pub signature: [u8; 64],
}

impl ChangeOperation {
    /// Produces the immutable byte slice used for signature generation and verification.
    pub fn signing_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(self.document_id.0.as_ref());
        bytes.extend_from_slice(self.actor_id.as_bytes());
        bytes.extend_from_slice(&self.sequence.to_be_bytes());
        bytes.push(self.op_type as u8);
        bytes.extend_from_slice(&self.hlc.physical.to_be_bytes());
        bytes.extend_from_slice(&self.hlc.logical.to_be_bytes());
        for dep in &self.causal_dependencies {
            bytes.extend_from_slice(&dep.0);
        }
        bytes.extend_from_slice(&self.created_at.to_be_bytes());
        bytes.extend_from_slice(&self.payload);
        bytes
    }
}
```

---

## 3. Causality Model & Clock Comparison

### 3.1 Systematic Mechanism Evaluation

| Criteria | Lamport Timestamps | Classic Vector Clocks | Hybrid Logical Clocks (HLC) + Causal DAG |
|:---|:---|:---|:---|
| **Storage Complexity** | $O(1)$ scalar | $O(N)$ dynamic map ($N = \text{actors}$) | **$O(1)$ scalar (12 bytes) + compact DAG edges** |
| **Concurrency Detection** | Cannot detect concurrency ($A < B \lor B < A$) | Full ($A < B, B < A, A \parallel B$) | **Full causality via DAG + Monotonic HLC total order** |
| **Physical Time Querying** | No correlation to real time | No correlation to real time | **Bounded to physical UTC milliseconds** |
| **Actor Churn Scaling** | Excellent | Poor (vectors grow with every transient peer) | **Excellent ($O(1)$ clock state, localized DAG)** |
| **Replay & Skew Tolerance** | Vulnerable to jump | Vulnerable to actor reset | **Bounded drift ($\Delta_{max} = 60\text{s}$), monotonic fallback** |
| **Decision** | Rejected: insufficient | Rejected as per-op payload | **SELECTED FOR FIELDNET** |

### 3.2 Selected Causality Strategy
FIELDNET adopts a **Hybrid Logical Clock (HLC) combined with explicit Causal Dependency DAG references**:
1. **Per-Operation Causality:** Every operation carries its author's HLC $(physical: \text{u64}, logical: \text{u32})$ and a list of `causal_dependencies` (hashes of directly observed prior operations).
2. **Causal Precedence ($O_1 \to O_2$):** $O_1$ happens-before $O_2$ if and only if $O_1$ is reachable by following `causal_dependencies` backward from $O_2$.
3. **Concurrency ($O_1 \parallel O_2$):** If neither $O_1 \to O_2$ nor $O_2 \to O_1$, the operations are concurrent.
4. **Deterministic Tie-Breaking:** If two concurrent operations compete for a non-CRDT register, the total order is resolved deterministically by HLC, breaking ties by `actor_id`:
   $$\text{Precedence}(O_1, O_2) = \begin{cases} O_1 > O_2 & \text{if } O_1.\text{hlc} > O_2.\text{hlc} \\ O_1 > O_2 & \text{if } O_1.\text{hlc} == O_2.\text{hlc} \land O_1.\text{actor\_id} > O_2.\text{actor\_id} \\ O_2 > O_1 & \text{otherwise} \end{cases}$$

---

## 4. Version Vector Semantics & State Comparison

While individual operations rely on HLC and DAG edges, **document synchronization state** uses compact **Version Vectors** mapping known author `NodeId`s to their highest applied contiguous sequence number.

### 4.1 Comparison Semantics
Let $V_A$ and $V_B$ be version vectors for a document:

```
  Dominates (A > B):      ∀ k ∈ B: V_A[k] ≥ V_B[k]  AND  ∃ k: V_A[k] > V_B[k]
                          ==> Replica A is strictly causally newer than Replica B.
                              Replica B needs deltas from A.

  Dominated By (A < B):   ∀ k ∈ A: V_B[k] ≥ V_A[k]  AND  ∃ k: V_B[k] > V_A[k]
                          ==> Replica B is strictly causally newer than Replica A.
                              Replica A needs deltas from B.

  Equal (A == B):         ∀ k: V_A[k] == V_B[k]
                          ==> Replicas have identical causal coverage. No exchange needed.

  Concurrent (A || B):    ∃ i: V_A[i] > V_B[i]  AND  ∃ j: V_B[j] > V_A[j]
                          ==> Independent concurrent mutations occurred on both replicas.
                              Bidirectional delta exchange required.
```

### 4.2 Three-Node Concrete Scenarios

#### Scenario 1: Unidirectional Propagation ($A \to B$)
- Initial state: $V_A = \{ A: 0 \}, V_B = \{ A: 0 \}$.
- Node A generates local operation $O_{A1}$ ($sequence: 1$).
  - $V_A = \{ A: 1 \}$.
- A synchronizes to B:
  - $V_A = \{ A: 1 \}$ dominates $V_B = \{ A: 0 \}$.
  - B receives and applies $O_{A1}$.
  - $V_B$ becomes $\{ A: 1 \}$.
  - Result: $V_A == V_B$ (Converged).

#### Scenario 2: Multi-Hop Propagation ($A \to B \to C$)
- From Scenario 1, Node B generates local operation $O_{B1}$ ($sequence: 1$, depending on $O_{A1}$).
  - $V_B = \{ A: 1, B: 1 \}$.
- Node B synchronizes to Node C ($V_C = \{ A: 0, B: 0 \}$):
  - $V_B$ dominates $V_C$.
  - C receives $O_{A1}$ and $O_{B1}$ in order.
  - $V_C$ becomes $\{ A: 1, B: 1 \}$. Causality $A \to B \to C$ is preserved.

#### Scenario 3: Concurrent Partitions ($A \parallel C$)
- Starting from common state $V = \{ A: 1, B: 1 \}$:
  - Partition occurs: A and C are disconnected from each other.
  - Node A commits local edit $O_{A2}$: $V_A = \{ A: 2, B: 1 \}$.
  - Node C commits local edit $O_{C1}$: $V_C = \{ A: 1, B: 1, C: 1 \}$.
- Comparison between A and C:
  - $V_A[A] = 2 > V_C[A] = 1$
  - $V_C[C] = 1 > V_A[C] = 0$
  - Classification: **$V_A \parallel V_C$ (Concurrent Updates)**.
- Reconciliation:
  - Node A transmits $[\{ A: 2 \}]$ to C.
  - Node C transmits $[\{ C: 1 \}]$ to A.
  - Both apply missing deltas; resulting merged vector: $V = \{ A: 2, B: 1, C: 1 \}$ on both nodes.

---

## 5. CRDT Taxonomy Supported by FIELDNET

FIELDNET supports five primary CRDT families, avoiding the trap of forcing every application into a single monolithic data structure:

```
┌────────────────────────────────────────────────────────────────────────┐
│                        FIELDNET CRDT Families                          │
├───────────────────┬───────────────────┬────────────────────────────────┤
│ CRDT Family       │ Type Name         │ Primary Use Case               │
├───────────────────┼───────────────────┼────────────────────────────────┤
│ 1. Counter        │ PnCounter         │ Metrics, likes, tallies, quotas│
│ 2. Set            │ OrSet<T>          │ Tags, member lists, rosters    │
│ 3. Map            │ CrdtMap<K, V>     │ Structured records, profiles   │
│ 4. Register       │ LwwRegister<T>    │ Titles, statuses, settings     │
│                   │ MvRegister<T>     │ Ambiguous / branched values    │
│ 5. Sequence/Text  │ RgaSequence<T>    │ Collaborative text, logs, items│
└───────────────────┴───────────────────┴────────────────────────────────┘
```

---

## 6. Distributed Counter (`PnCounter`)

### 6.1 Why Raw Scalar Transmission Fails
If Node A has counter value 5 and increments by 1 (to 6), and Node B independently increments by 2 (to 7), transmitting the final scalars (6 and 7) leads to an unresolvable collision where either 6 overwrites 7 (losing 2 increments) or 7 overwrites 6 (losing 1 increment).

### 6.2 State Representation & Merge Semantics
A Positive-Negative Counter separates increments ($P$) and decrements ($N$) per actor:

```rust
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PnCounter {
    /// Positive increments per actor: NodeId -> positive count
    pub p: HashMap<NodeId, u64>,
    /// Negative decrements per actor: NodeId -> negative count
    pub n: HashMap<NodeId, u64>,
}

impl PnCounter {
    /// Computes the effective integer value of the counter.
    pub fn value(&self) -> i64 {
        let p_sum: u64 = self.p.values().sum();
        let n_sum: u64 = self.n.values().sum();
        (p_sum as i64) - (n_sum as i64)
    }

    /// Merges state from a remote replica using the mathematical join (max per actor).
    pub fn merge(&mut self, other: &PnCounter) {
        for (actor, &val) in &other.p {
            let entry = self.p.entry(*actor).or_insert(0);
            *entry = (*entry).max(val);
        }
        for (actor, &val) in &other.n {
            let entry = self.n.entry(*actor).or_insert(0);
            *entry = (*entry).max(val);
        }
    }
}
```

### 6.3 Mathematical Properties
- **Commutative:** $\text{merge}(A, B) = \text{merge}(B, A)$
- **Associative:** $\text{merge}(\text{merge}(A, B), C) = \text{merge}(A, \text{merge}(B, C))$
- **Idempotent:** $\text{merge}(A, A) = A$
- **Duplicate Safety:** Duplicate arrivals of the same increment operation do not change the actor's monotonic value.

---

## 7. Distributed Set (`OrSet`)

### 7.1 Concurrent Add and Remove Ambiguity
If Node A executes `ADD(item)` while Node B executes `REMOVE(item)` concurrently, standard sets have no mathematical means to distinguish whether the removal observed the addition or vice versa.

### 7.2 Observed-Remove Set (Add-Wins OR-Set)
FIELDNET implements an **Add-Wins Observed-Remove Set (OR-Set)**:
- Adding an item assigns it a globally unique tag: `(item, Tag(actor_id, hlc))`.
- Removing an item removes all tags for that item that were **observed locally at the time of removal**, moving them to a tombstone set.
- If a concurrent `ADD` generated a new, unobserved tag on another node, the item survives upon merge.

```rust
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct ElementTag {
    pub actor_id: NodeId,
    pub hlc: HybridLogicalClock,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct OrSet<T: Hash + Eq + Clone> {
    /// Active elements: Element -> Set of active unique tags
    pub entries: HashMap<T, HashSet<ElementTag>>,
    /// Tombstone set of observed deletions
    pub tombstones: HashSet<ElementTag>,
}

impl<T: Hash + Eq + Clone> OrSet<T> {
    /// Checks membership: item exists if it has at least one active non-tombstoned tag.
    pub fn contains(&self, item: &T) -> bool {
        if let Some(tags) = self.entries.get(item) {
            tags.iter().any(|t| !self.tombstones.contains(t))
        } else {
            false
        }
    }

    /// State-based merge: union active tags, union tombstones, prune tombstoned tags.
    pub fn merge(&mut self, other: &OrSet<T>) {
        self.tombstones.extend(other.tombstones.iter().copied());
        for (item, tags) in &other.entries {
            let local_tags = self.entries.entry(item.clone()).or_default();
            local_tags.extend(tags.iter().copied());
        }
        // Retain only tags not present in tombstones
        for tags in self.entries.values_mut() {
            tags.retain(|t| !self.tombstones.contains(t));
        }
        self.entries.retain(|_, tags| !tags.is_empty());
    }
}
```

---

## 8. Distributed Map (`CrdtMap`)

### 8.1 Field-Level Independence Invariant
Updating field `user.status` on Node A must **never** conflict with or overwrite field `user.score` updated concurrently on Node B. Treating an entire JSON object as a single register is strictly forbidden.

### 8.2 Structure
A `CrdtMap` is a recursive map of field names to nested CRDT values:
- Each field maintains its own causal metadata (`HLC`, `VersionVector`).
- Deleting a key inserts a field-level tombstone with an HLC timestamp.
- Nested structures are modeled as recursive `CrdtMap` instances, allowing granular collaboration inside complex domain objects.

---

## 9. Register Semantics: LWW vs. Multi-Value

For scalar fields (strings, numbers, booleans) that cannot be split into sets or counters, FIELDNET provides two distinct register models:

### 9.1 Last-Write-Wins Register (`LwwRegister`)
- **State:** `(value: T, hlc: HybridLogicalClock, author: NodeId)`
- **Merge Rule:** The value with the higher HLC timestamp takes precedence. If HLC is equal, the higher `NodeId` wins deterministically.
- **Architectural Caveat:** LWW is lossy for concurrent writes. It is suitable for telemetry, sensor readings, and low-contention metadata (e.g. document title, user avatar URL), but **must not** be used where concurrent inputs must both be preserved.

### 9.2 Multi-Value Register (`MvRegister`)
- **State:** `Vec<(value: T, hlc: HybridLogicalClock, author: NodeId)>`
- **Merge Rule:** If update $A$ causally succeeds $B$, $A$ overwrites $B$. If $A \parallel B$ (concurrent), both values are retained as discrete branches:
  $$\text{read}() \longrightarrow [\text{val}_A, \text{val}_B]$$
- **Resolution:** The register remains in a branched state until the application or user submits an explicit resolving assignment that causally depends on both $A$ and $B$.

---

## 10. Sequence & Collaborative Text Abstraction

### 10.1 Replicated Growable Array (RGA) Model
Collaborative text and ordered lists require stable character/item identity:
- Array indexing ($0, 1, 2, \dots$) is dynamic and local; sending "insert at index 3" fails under concurrent edits.
- Every element receives an immutable **Node Anchor**:
  $$\text{ElementId} = (\text{actor\_id}, \text{hlc})$$
- An insertion specifies its immediate left predecessor:
  $$\text{InsertAfter}(\text{predecessor\_id}, \text{new\_id}, \text{character})$$
- Concurrent insertions after the same predecessor are deterministically ordered by comparing $\text{HLC}$ (descending), breaking ties by $\text{actor\_id}$.

### 10.2 Deletion & Tombstones
- Deleting an element flags it with a deletion tombstone but preserves its anchor identity in the sequence tree so concurrent neighbor insertions resolve correctly.

---

## 11. Deletion Semantics & Tombstone Lifecycle

### 11.1 The Resurrection Problem
If Node A deletes document $X$ and immediately deletes all local references to $X$, when Node B (which has been offline) reconnects, Node B will offer its copy of document $X$. Because Node A has forgotten $X$, Node A would interpret $X$ as a brand new document, resurrecting deleted data.

### 11.2 Tombstone Rules
1. **Deletion is an Operation:** Deleting a document or entity creates a tombstone operation:
   $$\text{ChangeOperation} \{ \text{op\_type}: \text{DeleteTombstone}, \text{hlc}: \text{HLC}_{now}, \dots \}$$
2. **Causal Dominance:** A tombstone operation causally dominates all prior operations on that entity.
3. **Retention Period ($\Delta_{gc}$):** Tombstones must be retained for at least $\Delta_{gc} = 30\text{ days}$, or until all known peers have acknowledged observing the tombstone.

---

## 12. Conflict Classification & State Machine

Every incoming remote operation is classified into one of five deterministic categories:

```
                           Incoming Remote Operation
                                       │
                                       ▼
                       Has this op_id been applied?
                                  /         \
                           YES   /           \   NO
                                ▼             ▼
                           [DUPLICATE]    Evaluate Causality vs Local State
                           (Ignore Op)        /           |           \
                                             /            |            \
                                  Causally Ahead    Concurrent ($||$)  Causally Older
                                         │                │                    │
                                         ▼                ▼                    ▼
                                 [CAUSAL_UPDATE]     Is target a CRDT?     [STALE_OP]
                                 (Apply Directly)     /            \       (Ignore Op)
                                                     /              \
                                              YES   /                \   NO
                                                   ▼                  ▼
                                           [CONCURRENT_MERGE]  [UNMERGEABLE_CONFLICT]
                                           (Apply Math Join)   (Stage in Conflict Table)
```

---

## 13. Conflict Resolution Engine

For `UNMERGEABLE_CONFLICT` occurrences:
1. **LWW Policy:** If the document schema declares LWW for this field, the higher HLC wins automatically.
2. **Multi-Value Staging:** If MVR is configured, concurrent values are preserved in `conflict_records`.
3. **Application Hook:** The `ConflictResolver` callback is invoked asynchronously:
   ```rust
   #[async_trait]
   pub trait ConflictResolver: Send + Sync {
       async fn resolve(
           &self,
           document_id: &DocumentId,
           local_value: &[u8],
           remote_value: &[u8],
       ) -> ConflictResolution;
   }
   ```
4. **Resolution Persistence:** The chosen resolution is committed as a new `ChangeOperation` referencing both conflicting operations in its `causal_dependencies`, permanently closing the conflict.

---

## 14. Formal Convergence Guarantees

### 14.1 Strong Eventual Consistency (SEC) Theorem
Let $\mathcal{O}$ be the complete set of valid, authenticated operations issued across the mesh. Any two replicas $R_1$ and $R_2$ that have received and applied $\mathcal{O}$ will have identical materialized state:

$$\text{AppliedOps}(R_1) = \text{AppliedOps}(R_2) \implies \text{State}(R_1) \equiv \text{State}(R_2)$$

### 14.2 Semilattice Join Requirements
All CRDT state transitions form a bounded join-semilattice $\langle S, \sqcup \rangle$:
1. **Commutativity:** $x \sqcup y = y \sqcup x$
2. **Associativity:** $(x \sqcup y) \sqcup z = x \sqcup (y \sqcup z)$
3. **Idempotence:** $x \sqcup x = x$

---

## 15. Idempotency & Deduplication

### 15.1 Triple-Layer Idempotency Guard
```
Layer 1: Network Deduplicator (Phase 05 MessageDeduplicator in-memory bloom filter)
   └── Drops duplicate frames arriving over mesh routes.

Layer 2: Database Storage Constraint (SQLite change_log table)
   └── UNIQUE(document_id, op_id) constraint rejects re-insertion.

Layer 3: CRDT State Idempotence (Mathematical semilattice property)
   └── merge(State, Delta) == State if Delta was already applied.
```

---

## 16. Out-of-Order Operation Handling & DTN Staging

In an intermittent, multi-hop mesh, operation $O_2$ may arrive before its ancestor $O_1$ ($O_1 \in O_2.\text{causal\_dependencies}$).

### 16.1 Handling Flow
```
  Remote Op Arrives ──> Are all causal_dependencies present locally?
                                /             \
                         YES   /               \   NO
                              ▼                 ▼
                         Apply to SQLite   Store in `unapplied_operations` table
                                                │
                                                ▼
                                           Issue `SyncMessage::DeltaRequest`
                                           for missing parent via Phase 05 DTN
                                                │
                                                ▼
                                           When parent arrives:
                                           Recursively drain & apply buffer
```

---

## 17. Partial Replication & Subscriptions

A node with limited storage is not required to replicate every document in the environment:

### 17.1 Replica Tiers
- **Full Replica:** Subscribes to and retains complete history and materialized state for all documents in an authorized namespace.
- **Partial / Scoped Replica:** Replicates only explicitly subscribed document IDs or collections (e.g. `docs/workspaces/project-alpha/*`).
- **Cached Replica:** Stores materialized views locally for offline reading; purges history when storage constraints demand.

---

## 18. Document Subscriptions Mechanism

### 18.1 Scoped Synchronization Negotiation
During the `SyncMessage::Hello` and `SyncMessage::StateSummary` phases:
- Nodes announce their `subscribed_document_ids` or namespace filters.
- Replicas omit unrequested documents from `DeltaResponse` batches, ensuring low-bandwidth radio channels are not saturated with irrelevant data.

---

## 19. Snapshot & Operation Interplay

### 19.1 Baseline Checkpointing
When a document's change log grows large, a `document_snapshots` row is generated:
- Stores full serialized materialized state at version vector $V_{snap}$.
- Incremental operations $O_k$ where $\text{sequence} > V_{snap}[\text{author}]$ are applied on top of the snapshot.
- An incoming operation is verified against $V_{snap}$: if $O.\text{sequence} \le V_{snap}[O.\text{author}]$, it is already incorporated in the snapshot and safely skipped.

---

## 20. Safe Operation-Log Compaction

### 20.1 Stability Watermark Definition
An operation log entry cannot be deleted based on age alone. Compaction is permitted up to sequence $K$ if and only if:
1. A valid snapshot exists covering sequence $K$.
2. All active, authorized peers have acknowledged synchronization beyond $K$:
   $$\forall P_i \in \text{Peers}: \quad \text{LastSyncedSeq}(P_i) \ge K$$
3. For disconnected peers offline longer than $\Delta_{gc} = 30\text{ days}$, the log is compacted anyway; upon return, the peer is served a full `SnapshotDelivery`.

---

## 21. Security, Integrity & Authorization Boundaries

### 21.1 Network Authentication vs. Data Authorization
- **Network Authentication (Phase 04/05):** Proves that the connecting node possesses the private key corresponding to its transport `NodeId`.
- **Data Authorization (Phase 03/06):** Proves that the author `NodeId` has explicit capability permission to mutate the specific document namespace.

### 21.2 Cryptographic Integrity Invariant
Every operation in transit or at rest carries author Ed25519 signature:
$$\text{Ed25519Verify}(\text{actor\_id}, \text{op.signing\_bytes}(), \text{op.signature}) == \text{Valid}$$
An invalid signature causes immediate rejection and logs a security audit event. Intermediate DTN relay nodes cannot tamper with document payloads without detection.

---

## 22. Worked Example Scenarios

### Scenario A: Offline Concurrent Counter Increments
- Node A (offline): Increments counter $C$ from 0 to 1 ($P_A[A] = 1$).
- Node B (offline): Increments counter $C$ from 0 to 2 ($P_B[B] = 2$).
- Reconnect: A and B exchange `PnCounter` states.
- Merged state: $P = \{ A: 1, B: 2 \}$. Effective value $= 1 + 2 = 3$.
- **Result:** Both updates preserved; counter converges to 3.

### Scenario B: Concurrent Additions to a Set
- Node A (offline): `ADD("apple")` $\to$ tag $T_1$.
- Node B (offline): `ADD("banana")` $\to$ tag $T_2$.
- Reconnect: A and B merge `OrSet`s.
- **Result:** Set contains both `{"apple", "banana"}`.

### Scenario C: Concurrent Field Updates in a Map
- Document has fields `name: "FIELDNET"`, `status: "idle"`.
- Node A (offline): Updates `status = "active"`.
- Node B (offline): Updates `name = "FIELDNET-Mesh"`.
- Reconnect: `CrdtMap` merges field deltas independently.
- **Result:** Materialized map is `{ name: "FIELDNET-Mesh", status: "active" }`. Zero conflict.

### Scenario D: Concurrent Register Modification
- Node A sets `title = "Alpha"` ($HLC = (1000, 1), NodeId = 0xAA$).
- Node B sets `title = "Beta"` ($HLC = (1000, 2), NodeId = 0xBB$).
- Reconnect: LWW evaluates HLC. $HLC_B > HLC_A$.
- **Result:** `title` converges to `"Beta"` deterministically across both nodes.

### Scenario E: Concurrent Deletion vs. Modification
- Node A deletes document $D$ (creates tombstone $O_{del}$ at $HLC_{1050}$).
- Node B (offline) modifies document $D$ (creates update $O_{up}$ at $HLC_{1020}$).
- Reconnect: $O_{del}$ causally succeeds or has higher HLC than $O_{up}$.
- **Result:** Deletion tombstone prevails; document remains deleted.

### Scenario F: Out-of-Order Operation Arrival
- Node A produces $O_1$ (create) and $O_2$ (update depending on $O_1$).
- Due to multi-hop DTN routing, Node B receives $O_2$ first.
- Node B inspects `causal_dependencies`: observes $O_1$ is missing.
- Node B buffers $O_2$ in `unapplied_operations`.
- Later, $O_1$ arrives: applied to SQLite; triggers cascade apply of $O_2$.
- **Result:** State correctly reflects $O_1$ followed by $O_2$.

### Scenario G: Triplicate Delivery
- Operation $O_1$ arrives directly from A, relayed via B, and retransmitted via DTN.
- First arrival: applied and committed.
- Second and third arrivals: rejected by SQLite `UNIQUE(document_id, op_id)`.
- **Result:** State changes exactly once.

---

## 23. Conceptual Rust API Contracts

```rust
#[async_trait]
pub trait DocumentStore: Send + Sync {
    async fn create(&self, object_type: &str, initial_payload: &[u8]) -> Result<Document, StorageError>;
    async fn get(&self, doc_id: &DocumentId) -> Result<Option<Document>, StorageError>;
    async fn update(&self, doc_id: &DocumentId, delta: &[u8]) -> Result<OperationId, StorageError>;
    async fn delete(&self, doc_id: &DocumentId) -> Result<OperationId, StorageError>;
}

#[async_trait]
pub trait OperationLog: Send + Sync {
    async fn append(&self, op: ChangeOperation) -> Result<bool, StorageError>;
    async fn get_operations_since(&self, doc_id: &DocumentId, seq: u64, limit: usize) -> Result<Vec<ChangeOperation>, StorageError>;
    async fn get_unapplied(&self) -> Result<Vec<ChangeOperation>, StorageError>;
}

pub trait Crdt {
    type State: Clone + Serialize + for<'de> Deserialize<'de>;
    type Delta: Clone + Serialize + for<'de> Deserialize<'de>;
    type Value;

    fn read(&self) -> Self::Value;
    fn generate_delta(&self, intent: &Self::Value) -> Self::Delta;
    fn apply_delta(&mut self, delta: &Self::Delta) -> Result<(), CrdtError>;
    fn merge(&mut self, other: Self::State) -> Result<(), CrdtError>;
}

#[async_trait]
pub trait SyncEngine: Send + Sync {
    async fn generate_summary(&self, doc_id: &DocumentId) -> Result<DocumentStateSummary, StorageError>;
    async fn process_sync_message(&self, from: NodeId, msg: SyncMessage) -> Result<Option<SyncMessage>, StorageError>;
}
```

---

## 24. Testable Invariants for Later Implementation

The eventual Phase 06 implementation must rigorously prove all 12 testable invariants:

1. **Idempotency:** Applying an identical operation $N$ times produces the exact same state as applying it once.
2. **Determinism:** Given identical operation sets, all replicas arrive at byte-for-byte identical state.
3. **Concurrency Detection:** Operations generated in separate partitions are classified as concurrent ($A \parallel B$).
4. **Causality Preservation:** If $O_1 \to O_2$, no replica ever applies $O_2$ before $O_1$.
5. **Convergence:** When all pending operations are delivered, all replicas have matching state vectors.
6. **Signature Verification:** Operations with corrupted or mismatched signatures are rejected immediately.
7. **Authorization Fail-Closed:** Operations authored by a node lacking write permissions are discarded.
8. **Crash Durability:** Power loss or SIGKILL immediately after committing a local write loses zero data.
9. **Out-of-Order Recovery:** Operations arriving ahead of their causal dependencies are staged and applied once ancestors arrive.
10. **Tombstone Integrity:** Deletions are never resurrected by late-arriving concurrent operations unless an explicit add-wins policy applies.
11. **Compaction Safety:** Replicas that compact historical logs up to sequence $K$ continue to serve consistent snapshots to newly joined peers.
12. **Partial Isolation:** Modifying Document X never alters or invalidates cached state for Document Y.

---

## 25. Implementation Boundary & Next Steps

### Implementation Packaging (Phase 06 Implementation Phase)
- **`crates/fieldnet-crdt`**: Core data types (`PnCounter`, `OrSet`, `CrdtMap`, `LwwRegister`, `MvRegister`, `RgaSequence`).
- **`crates/fieldnet-storage`**: SQLite schema migrations, change log indexer, snapshot store.
- **`crates/fieldnet-sync`**: Sync engine state machine, DTN integration, delta diffing.

### Out of Scope
- Distributed content-addressed file chunking (Deferred to **Phase 07**).
- Local web gateway and HTTP routing (Deferred to **Phase 08**).
- WASM sandboxed runtime (Deferred to **Phase 09**).
- Cloud coordination (Strict non-goal).
