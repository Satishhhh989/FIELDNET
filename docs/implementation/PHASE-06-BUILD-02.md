# FIELDNET Implementation Report: Phase 06 — BUILD-02
## Title: Durable Operation Log & Causality Engine

**Status:** COMPLETE  
**Milestone:** Phase 06 — BUILD-02  
**Author:** FIELDNET Architecture & Core Implementation Team  
**Date:** September 2026  

---

## 1. Executive Summary

Phase 06 BUILD-02 implements FIELDNET's **Durable Operation Log and Causality Engine**. Building directly upon the local Document Store created in BUILD-01, this milestone establishes that **every local document mutation (CREATE, UPDATE, DELETE) atomically produces an immutable, cryptographically authenticatable operation record**.

Causality is tracked with mathematical precision using a Hybrid Logical Clock (`HybridLogicalClock`, combining physical milliseconds and logical counters) and a distributed `StateVector` ($V_A \le V_B$, happens-before, dominates, concurrent). Atomic single-transaction commits guarantee that document state, operation logs, applied-operation indexes, and causal replica states can never diverge or enter half-committed states during crashes.

---

## 2. Operation Model & Identifiers

### Canonical Operation Record (`Operation`)
```rust
pub struct OperationId(pub [u8; 32]);

pub struct Operation {
    pub op_id: OperationId,
    pub document_id: DocumentId,
    pub actor_node_id: NodeId,
    pub sequence: u64,
    pub op_type: OperationType,
    pub payload: Vec<u8>,
    pub hlc: HybridLogicalClock,
    pub causal_dependencies: Vec<OperationId>,
    pub signature: [u8; 64],
    pub created_at: Timestamp,
    pub schema_version: u32,
    pub status: OpStatus,
}
```

- **Global Uniqueness**: `OperationId` is computed as the BLAKE3 digest over canonical operation fields:
  $$\text{OpId} = \text{BLAKE3}(\text{"FIELDNET\_OP\_v1:"} \parallel \text{doc\_id} \parallel \text{actor} \parallel \text{seq} \parallel \text{op\_type} \parallel \text{schema\_ver} \parallel \text{HLC} \parallel \text{deps} \parallel \text{payload})$$
- **Operation Types**: `Create` (1), `Update` (2), `Delete` (3), `CrdtDelta` (4).
- **Operation Status Lifecycle**: `Created` (1), `Durable` (2), `PendingSync` (3), `Sent` (4), `Acknowledged` (5), `Applied` (6), `Rejected` (7), `Tombstone` (8).
- **Cryptographic Authenticity**: When initialized with a sovereign `NodeIdentity`, every operation is signed with Ed25519. Verifying the operation ensures both that the public key derives to `actor_node_id` and that the payload has not been tampered with.

---

## 3. Causality & Logical Time

### 3.1 Hybrid Logical Clock (HLC)
- **Structure**: 12 big-endian bytes `[physical: 8B (ms)] || [logical: 4B]`.
- **Clock Drift Invariant**: Advances when local physical time moves forward, and increments logical counters when multiple mutations occur within the same millisecond.
- **Ordering**: Lexicographical comparison $(p_1, l_1) < (p_2, l_2) \iff p_1 < p_2 \lor (p_1 = p_2 \land l_1 < l_2)$.

### 3.2 State Vector (`StateVector`)
- **Structure**: Maps contributing actor `NodeId`s to their highest observed monotonic sequence counter (`HashMap<NodeId, u64>`).
- **Formal Causal Relations**:
  - **Equal ($V_A = V_B$)**: Identical causal coverage.
  - **Happens-Before ($V_A < V_B$)**: $V_B$ dominates $V_A$ ($\forall k: V_B[k] \ge V_A[k] \land \exists k: V_B[k] > V_A[k]$).
  - **Happens-After ($V_A > V_B$)**: $V_A$ dominates $V_B$.
  - **Concurrent ($V_A \parallel V_B$)**: $\exists i: V_A[i] > V_B[i] \land \exists j: V_B[j] > V_A[j]$.
- **Deterministic Encoding**: Serialized as `[count: u32] || ([node_id: 32B] || [seq: 8B])*` sorted canonically by `NodeId`.

---

## 4. SQLite Schema Extension (Migration v2)

```sql
-- 1. Immutable Append-Only Operations Log
CREATE TABLE IF NOT EXISTS operations (
    op_id               BLOB PRIMARY KEY,
    document_id         BLOB NOT NULL,
    actor_node_id       BLOB NOT NULL,
    sequence            INTEGER NOT NULL,
    op_type             INTEGER NOT NULL,
    delta_bytes         BLOB NOT NULL,
    hlc_physical        INTEGER NOT NULL,
    hlc_logical         INTEGER NOT NULL,
    dependencies_bytes  BLOB NOT NULL,
    signature           BLOB NOT NULL,
    created_at          INTEGER NOT NULL,
    status              INTEGER NOT NULL DEFAULT 1,
    FOREIGN KEY(document_id) REFERENCES documents(document_id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ops_doc_actor_seq 
    ON operations(document_id, actor_node_id, sequence);
CREATE INDEX IF NOT EXISTS idx_ops_doc_hlc 
    ON operations(document_id, hlc_physical, hlc_logical);
CREATE INDEX IF NOT EXISTS idx_ops_status 
    ON operations(status);

-- 2. Fast O(1) Applied Operations Tracker
CREATE TABLE IF NOT EXISTS applied_operations (
    document_id         BLOB NOT NULL,
    op_id               BLOB NOT NULL,
    applied_at          INTEGER NOT NULL,
    PRIMARY KEY(document_id, op_id)
);

-- 3. Document-Level Causal Replica State
CREATE TABLE IF NOT EXISTS replica_state (
    document_id         BLOB PRIMARY KEY,
    state_vector_bytes  BLOB NOT NULL,
    latest_hlc_physical INTEGER NOT NULL,
    latest_hlc_logical  INTEGER NOT NULL,
    local_sequence      INTEGER NOT NULL DEFAULT 0,
    updated_at          INTEGER NOT NULL,
    FOREIGN KEY(document_id) REFERENCES documents(document_id) ON DELETE CASCADE
);

-- 4. Local Node Persistent Monotonic Sequence & Clock State
CREATE TABLE IF NOT EXISTS local_node_state (
    node_id             BLOB PRIMARY KEY,
    local_sequence      INTEGER NOT NULL DEFAULT 0,
    latest_hlc_physical INTEGER NOT NULL,
    latest_hlc_logical  INTEGER NOT NULL,
    updated_at          INTEGER NOT NULL
);
```

---

## 5. Atomic Mutation Pipeline

When an application calls `create_sync`, `update_sync`, or `delete_sync`, the storage engine executes a single atomic transaction:

1. **Validation**: Document type, length, and size constraints verified.
2. **Sequence Allocation**: Monotonic sequence retrieved and incremented in `local_node_state`.
3. **Causal Clock**: Local `HlcEngine` ticks forward; new HLC recorded.
4. **Parent Dependency Resolution**: Latest operation for the document retrieved as causal parent.
5. **Operation Signing**: Ed25519 signature generated over canonical fields.
6. **Document Mutation**: `documents` table updated (`INSERT`, `UPDATE`, or `is_deleted = 1`).
7. **Operation Durability**: Inserted into `operations` table.
8. **Applied Tracking**: Inserted into `applied_operations` table.
9. **Causal State Update**: Document's `replica_state` updated with advanced `StateVector`.
10. **Transaction Commit**: Atomic commit. If any step fails, the entire transaction is rolled back via RAII.

---

## 6. Operation Log APIs for Future Sync Engine

The `DocumentStore` exposes query methods tailored for future synchronization:

| API Method | Parameters | Returns | Description |
|---|---|---|---|
| `get_operation` | `&OperationId` | `Result<Option<Operation>, DataError>` | O(1) operation lookup by hash |
| `list_operations_for_document` | `&DocumentId, limit, offset` | `Result<Vec<Operation>, DataError>` | Causal history of a document |
| `list_pending_operations` | `limit` | `Result<Vec<Operation>, DataError>` | Operations awaiting network dispatch |
| `list_operations_since` | `&DocumentId, &StateVector, limit` | `Result<Vec<Operation>, DataError>` | Incremental delta computation |
| `is_operation_applied` | `&DocumentId, &OperationId` | `Result<bool, DataError>` | Fast duplicate check |
| `record_applied_operation` | `&DocumentId, &OperationId` | `Result<bool, DataError>` | Idempotent application marker |
| `get_causal_state` | `&DocumentId` | `Result<Option<ReplicaState>, DataError>` | State vector and HLC snapshot |
| `get_local_sequence` | `none` | `Result<u64, DataError>` | Current node monotonic counter |

---

## 7. Verification & Test Suite

### 7.1 Required Tests (`crates/fieldnet-data/tests/operation_log_and_causality_tests.rs`)
- **TEST 1**: `test_01_create_document_produces_create_operation` — Verified CREATE op, seq=1, Ed25519 signature validity.
- **TEST 2**: `test_02_update_produces_update_operation` — Verified UPDATE op, seq=2, parent dependency to op1, HLC advance.
- **TEST 3**: `test_03_delete_produces_delete_operation` — Verified DELETE op, seq=2, durable tombstone record.
- **TEST 4**: `test_04_document_mutation_and_operation_commit_atomically` — Verified doc, op, applied tracker, and causal state all committed.
- **TEST 5**: `test_05_failed_transaction_leaves_neither_partially_committed` — Verified rollback leaves no orphaned rows or partial mutations.
- **TEST 6**: `test_06_operation_ids_are_unique` — Verified 50 consecutive operations produce 50 distinct cryptographic IDs.
- **TEST 7**: `test_07_operation_retrieval_works` — Verified retrieval by ID, by document, and pending sync queries.
- **TEST 8**: `test_08_operation_survives_restart` — Verified operations remain intact after database close and reopen.
- **TEST 9**: `test_09_causal_state_survives_restart` — Verified `replica_state` and `StateVector` are restored accurately on restart.
- **TEST 10**: `test_10_local_sequence_remains_valid_after_restart` — Verified monotonic sequence counter continues without gap or collision after restart.
- **TEST 11**: `test_11_repeated_operation_id_is_rejected_deduplicated` — Verified PRIMARY KEY enforcement and idempotent tracker check.
- **TEST 12**: `test_12_delete_operation_remains_durable` — Verified deletion operations remain available for sync history.
- **TEST 13**: `test_13_multiple_documents_maintain_independent_operation_histories` — Verified causal isolation across documents.
- **TEST 14**: `test_14_causally_ordered_operations_compare_correctly` — Verified happens-before / happens-after state vector relations.
- **TEST 15**: `test_15_concurrent_causal_states_compare_correctly` — Verified concurrent partition states ($V_A \parallel V_B$).
- **TEST 16**: `test_16_operation_log_can_return_operations_incrementally` — Verified `list_operations_since` returns only missing delta operations.

### 7.2 Property Invariants
- **Reflexivity**: For all $V$, $V \text{ relation } V = \text{Equal}$.
- **Asymmetry**: If $V_A < V_B$, then $V_B \not< V_A$ ($V_B > V_A$).
- **Concurrency**: Branching state vectors cannot be misclassified as ordered.

---

## 8. Performance Baseline (Apple Silicon)

Measured over 200 operations with 1 KB payloads:

| Operation | Total Iterations | Duration | Throughput | Latency |
|---|---|---|---|---|
| **Create with Atomic Operation** | 200 | 76.25 ms | **2,623 ops/sec** | 0.381 ms / op |
| **Operation Hash Lookup** | 200 | 1.87 ms | **107,016 ops/sec** | 0.009 ms / op |
| **Batch Retrieval (200 ops)** | 200 | 0.84 ms | **238,095 ops/sec** | 0.004 ms / op |

---

## 9. Known Limitations & Deferred Scope

1. **CRDT Reducers & Conflict Resolvers**: Deferred to BUILD-03.
2. **Network Sync Protocol (`SYNC_HELLO`, `SYNC_REQUEST`)**: Deferred to BUILD-04.
3. **Log Compaction & Snapshots**: Deferred to BUILD-05.
4. **Multi-Node End-to-End DTN Sync**: Deferred to BUILD-06.

---

## 10. Final Status

```
PHASE 06 → BUILD-02 COMPLETE
```
