# FIELDNET Phase 06 — BUILD-05 Implementation Report
**Title:** Snapshots, Log Compaction, Causal Stability & Recovery  
**Status:** COMPLETE  
**Workspace:** `crates/fieldnet-data`, `crates/fieldnet-runtime`  
**Authoritative References:** `FIELDNET_MASTER_SPEC.md`, `docs/architecture/PHASE-06-01-LOCAL-FIRST-DATA.md`, `docs/architecture/PHASE-06-02-CRDT-CONFLICT-CONVERGENCE.md`, `docs/architecture/PHASE-06-03-SYNC-PROTOCOL-IMPLEMENTATION-CONTRACTS.md`, `docs/implementation/PHASE-06-BUILD-01.md`, `docs/implementation/PHASE-06-BUILD-02.md`, `docs/implementation/PHASE-06-BUILD-03.md`, `docs/implementation/PHASE-06-BUILD-04.md`.

---

## 1. Executive Summary

FIELDNET Phase 06 BUILD-05 implements the long-term storage bounds and disaster recovery mechanisms for FIELDNET's distributed data subsystem. In an operational tactical mesh, nodes experience thousands of localized mutations; retaining an unbounded operation log indefinitely exhaustively consumes disk and memory, degrading recovery and synchronization latency.

BUILD-05 introduces **Durable Snapshots**, **Causal Stability Tracking**, **Safe Log Compaction**, and **Disaster Recovery** while strictly guaranteeing the core invariant:

> **CORE INVARIANT:** COMPACTION MUST NEVER CHANGE VALID LOGICAL STATE.

### Key Subsystem Achievements:
1. **Durable Snapshot Model:** Explicit, versioned, cryptographic snapshots of materialized document/CRDT state storing exact causal coverage (`StateVector`, `compacted_up_to_seq`, `HybridLogicalClock`) with deterministic BLAKE3 integrity hashes.
2. **Snapshot Integrity Verification:** Fail-closed integrity validation on snapshot creation, deserialization, and recovery. Corrupted snapshots are rejected immediately without polluting local memory or mutating storage.
3. **Multi-Generation Retention & Fallback:** Bounded snapshot generation retention (default 2 generations per document). If the newest generation is corrupted, the recovery engine automatically falls back to prior generations and replays newer operations.
4. **Peer Causal Stability Enforcement:** Log compaction evaluates synchronization watermarks across all active mesh peers. An operation $O$ is eligible for removal if and only if:
   $$\text{seq}(O) \le \min\big(\text{compacted\_up\_to\_seq}(S), \min_{P_i \in \text{Peers}} \text{last\_synced\_seq}(P_i)\big)$$
   Operations unacknowledged by lagging active peers are strictly retained.
5. **Permanent Tombstone Retention:** The `tombstones` table and `applied_operations` deduplication index are decoupled from raw operation log pruning. When an operation log is compacted, tombstones and deduplication digests remain permanently intact, preventing stale resurrecting writes from offline replicas.
6. **Atomic Transactional Compaction & Replay:** Snapshot creation and log compaction execute inside isolated SQLite transactions with post-compaction logical state invariance assertions ($\text{State}_{\text{before}} \equiv \text{State}_{\text{after}}$).
7. **Comprehensive Verification:** 20 tests covering snapshot persistence, crash safety, corruption fallback, causal stability across 3 nodes, offline peer isolation, property invariants, and performance baselines.

---

## 2. Architecture & Data Flow

```
┌────────────────────────────────────────────────────────┐
│                   Application Layer                    │
└───────────────────────────┬────────────────────────────┘
                            │ Document & CRDT Mutations
                            ▼
┌────────────────────────────────────────────────────────┐
│              Document Store (DocumentStore)            │
│  - Durable Documents (documents table)                 │
│  - Causality & Replica State (replica_state table)     │
└──────────────┬──────────────────────────┬──────────────┘
               │                          │
        Create │ Snapshot          Safe   │ Compaction
               ▼                          ▼
┌──────────────────────────┐    ┌──────────────────────────────┐
│  DocumentSnapshot Model  │    │      Compaction Engine       │
│  - snapshot_id           │    │  1. Snapshot Coverage Check  │
│  - compacted_up_to_seq   │    │  2. Peer Sync Watermark Min  │
│  - state_vector          │    │  3. Tombstone Safety Check   │
│  - hlc & crdt_type       │    │  4. Atomic Operation Delete  │
│  - deterministic hash    │    │  5. Invariant State Check    │
└──────────────┬───────────┘    └──────────────┬───────────────┘
               │                               │
               ▼                               ▼
┌────────────────────────────────────────────────────────┐
│             SQLite Persistent Storage Engine           │
│  - document_snapshots (generation-indexed)             │
│  - operations (pruned up to max_safe_sequence)         │
│  - tombstones (permanently retained)                   │
│  - compaction_records (audit log)                      │
│  - peer_sync_state (per-document watermarks)           │
└────────────────────────────────────────────────────────┘
```

---

## 3. Snapshot Model & Integrity Verification

### 3.1 Data Structures (`crates/fieldnet-data/src/snapshot/model.rs`)
A snapshot represents an immutable materialized representation of document state at a specific causal frontier:

```rust
pub struct DocumentSnapshot {
    pub snapshot_id: SnapshotId,
    pub document_id: DocumentId,
    pub snapshot_version: u64,
    pub compacted_up_to_seq: u64,
    pub state_vector: StateVector,
    pub hlc: HybridLogicalClock,
    pub crdt_type: String,
    pub state: Vec<u8>,
    pub state_hash: [u8; 32],
    pub schema_version: u32,
    pub created_at: Timestamp,
}
```

### 3.2 Deterministic Cryptographic State Hash
To guarantee tamper resistance and storage bit-rot detection, `compute_state_hash` binds the payload to all causal coverage parameters using BLAKE3:

$$\text{state\_hash} = \text{BLAKE3}\Big(\text{"FIELDNET\_SNAPSHOT\_V1"} \mathbin{\Vert} \text{doc\_id} \mathbin{\Vert} \text{version} \mathbin{\Vert} \text{seq} \mathbin{\Vert} \text{SV}_{\text{bytes}} \mathbin{\Vert} \text{HLC}_{\text{bytes}} \mathbin{\Vert} \text{crdt\_type} \mathbin{\Vert} \text{state}\Big)$$

Integrity validation occurs synchronously:
1. Immediately upon snapshot creation before SQLite commit.
2. Whenever a snapshot is fetched from SQLite via `get_snapshot_sync` or `get_latest_snapshot_sync`.
3. Before restoring a document during recovery via `restore_snapshot_sync`.

---

## 4. Snapshot Coverage & Restoration Semantics

A snapshot never assumes that it covers the entire universe of document mutations. Instead, it explicitly documents `compacted_up_to_seq` and `state_vector`.

### 4.1 Restoration Flow (`restore_snapshot_sync`)
When restoring from snapshot generation $S$:
1. Validate $S.\text{state\_hash}$ against its payload and causal parameters.
2. Materialize $S.\text{state}$ into the `documents` table atomically.
3. Materialize $S.\text{state\_vector}$, $S.\text{hlc}$, and $S.\text{compacted\_up\_to\_seq}$ into `replica_state`.
4. Query the `operations` table for all subsequent operations where $\text{sequence} > S.\text{compacted\_up\_to\_seq}$.
5. Sequentially replay newer operations forward using `reduce_crdt_delta_or_state`, updating the document version and causal state vector.
6. Verify and return the converged logical state.

$$\text{Logical State} \equiv \text{Replay}\big(\text{Snapshot}(S), \{O_i \mid \text{seq}(O_i) > \text{seq}(S)\}\big) \equiv \text{Full Operation History}$$

---

## 5. Causal Stability & Safe Log Compaction

### 5.1 Causal Stability Formula
In a distributed peer-to-peer mesh, a node must never discard historical operations that an active peer has not yet received. FIELDNET inspects `peer_sync_state` watermarks:

Let $\mathcal{P}_{\text{active}}$ be the set of active mesh peers synchronized within the grace period (default 30 days):
$$\text{max\_safe\_sequence} = \begin{cases}
\min\Big(\text{seq}(S_{\text{latest}}), \min_{p \in \mathcal{P}_{\text{active}}} \text{last\_synced\_seq}(p)\Big) & \text{if } \mathcal{P}_{\text{active}} \neq \emptyset \\
\text{seq}(S_{\text{latest}}) & \text{if } \mathcal{P}_{\text{active}} = \emptyset \text{ (local-only)}
\end{cases}$$

### 5.2 Four Compaction Safety Gates (`check_compaction_safety_sync`)
Before any operation is deleted:
1. **Snapshot Gate:** A valid, integrity-verified snapshot covering sequence $\ge \text{max\_safe\_sequence}$ must exist.
2. **Peer Causal Gate:** Active peers must have acknowledged operations up to `max_safe_sequence`. If a peer is lagging at $\text{seq} = M < \text{seq}(S)$, compaction is clamped to $M$; operations $M+1 \dots \text{seq}(S)$ are retained. If an active peer has sequence 0 or incomplete sync status (`SYNCING`), compaction is blocked.
3. **Tombstone Gate:** Tombstones are held in a separate table (`tombstones`) and are never pruned during log compaction.
4. **Non-Zero Work Gate:** At least 1 operation must be eligible for pruning ($\text{eligible\_count} > 0$).

### 5.3 Compaction Execution (`compact_sync`)
1. Enforce safety gates.
2. Record pre-compaction document state $D_{\text{before}}$.
3. Begin SQLite transaction.
4. Compute total delta bytes and dependencies overhead to be reclaimed.
5. Execute `DELETE FROM operations WHERE document_id = ? AND sequence <= ?`.
6. Insert audit entry in `compaction_records`.
7. Commit SQLite transaction.
8. Query post-compaction document state $D_{\text{after}}$.
9. **Assert Invariance:** If $D_{\text{before}}.\text{state} \neq D_{\text{after}}.\text{state}$ or $D_{\text{before}}.\text{version} \neq D_{\text{after}}.\text{version}$, fail-closed with `DataError::CorruptedData`.

---

## 6. Tombstone Retention & Resurrection Prevention

A critical vulnerability in distributed replication is the "tombstone resurrection" bug:
1. Node A deletes document $X$.
2. Node A prunes the delete operation.
3. Offline Node B reconnects with an old update for $X$.
4. Node A treats Node B's update as a valid update and resurrects $X$.

FIELDNET eliminates this hazard via architectural decoupling:
- **`operations` Table:** Stores transient delta logs pruned during compaction.
- **`tombstones` Table:** Stores permanent deletion metadata (deletion timestamp, deleting node ID, logical version).
- **`applied_operations` Table:** Stores BLAKE3 deduplication digests of all applied operations.
- In `apply_remote_operation_sync`, when a remote operation targets a document marked `deleted = true`, the engine rejects it immediately with `DataError::DocumentDeleted` unless the incoming operation is an authorized delete. Compaction never purges tombstones.

---

## 7. Disaster Recovery & Corruption Fallback

When disk corruption or bit-rot affects storage:
1. `recover_document_sync` lists all snapshots for the document in descending generation order.
2. It attempts to validate each snapshot generation's BLAKE3 state hash.
3. If generation $N$ fails integrity validation, it logs a warning (`snapshot.corrupted`) and falls back to generation $N-1$.
4. It restores generation $N-1$ and replays all operations newer than generation $N-1$.
5. If all snapshots are corrupted, it checks if an uncompacted operation history exists from sequence 1 and replays it from origin.
6. If neither a valid snapshot nor a contiguous operation history exists, it fails closed with `DataError::RecoveryFailed`. It never fabricates state.

---

## 8. Schema Migration 5

Added in `crates/fieldnet-data/src/migration.rs`:
```sql
CREATE TABLE IF NOT EXISTS document_snapshots (
    snapshot_id BLOB PRIMARY KEY,
    document_id BLOB NOT NULL,
    snapshot_version INTEGER NOT NULL,
    compacted_up_to_seq INTEGER NOT NULL,
    state_vector_bytes BLOB NOT NULL,
    hlc_physical INTEGER NOT NULL,
    hlc_logical INTEGER NOT NULL,
    crdt_type TEXT NOT NULL,
    snapshot_payload BLOB NOT NULL,
    state_hash BLOB NOT NULL,
    schema_version INTEGER NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_doc_snapshots_lookup
    ON document_snapshots (document_id, snapshot_version DESC);

CREATE TABLE IF NOT EXISTS compaction_records (
    compaction_id BLOB PRIMARY KEY,
    document_id BLOB NOT NULL,
    snapshot_id BLOB NOT NULL,
    compacted_up_to_seq INTEGER NOT NULL,
    ops_removed INTEGER NOT NULL,
    bytes_reclaimed INTEGER NOT NULL,
    compacted_at INTEGER NOT NULL,
    reason TEXT
);
```

---

## 9. Verification & Test Matrix

All 20 tests implemented in `crates/fieldnet-data/tests/snapshot_and_compaction_tests.rs`:

| Test ID | Test Scenario | Verified Condition | Status |
|---|---|---|---|
| `TEST 01` | Create Snapshot | Atomic creation, causal vector recording, BLAKE3 hash computation | **PASS** |
| `TEST 02` | Snapshot Restart Persistence | Snapshot and metadata survive process termination and SQLite reload | **PASS** |
| `TEST 03` | Restore Snapshot | Materialized document and replica state restored accurately from snapshot | **PASS** |
| `TEST 04` | Snapshot + Incremental Replay | $\text{Snapshot} + \text{Remaining Ops} \equiv \text{Full Operation History}$ | **PASS** |
| `TEST 05` | Deterministic State Hash | Identical states produce identical hashes; 1-bit change alters hash | **PASS** |
| `TEST 06` | Corrupted Snapshot Rejection | Bit-flipped payload fails integrity validation on fetch and restore | **PASS** |
| `TEST 07` | Corrupted Generation Fallback | Engine falls back to generation $N-1$ and replays forward to correct state | **PASS** |
| `TEST 08` | Safe Operation Compaction | Compaction deletes operations $\le \text{max\_safe\_seq}$, leaves valid state | **PASS** |
| `TEST 09` | Unstable Operations Retention | Operations newer than snapshot are preserved | **PASS** |
| `TEST 10` | Tombstone Retention | Deleted document cannot be resurrected by offline peer after compaction | **PASS** |
| `TEST 11` | Offline Peer Protection | Lagging peer clamps `max_safe_seq`; unsynced ops are retained | **PASS** |
| `TEST 12` | Invariance of Logical State | State before compaction $\equiv$ state after compaction (OR-Set, Counter) | **PASS** |
| `TEST 13` | Restart After Compaction | Reopened database accepts new mutations and sequences advance cleanly | **PASS** |
| `TEST 14` | Snapshot Creation Rollback | Crash/abort during snapshot leaves documents and snapshots intact | **PASS** |
| `TEST 15` | Compaction Transaction Rollback | Crash/abort during compaction preserves all operations | **PASS** |
| `TEST 16` | Multi-Document Isolation | Compacting Document A leaves Document B's operation log untouched | **PASS** |
| `TEST 17` | Large History Recovery | Replays 5 operations instead of 105 operations after compaction | **PASS** |
| `TEST 18` | 3-Node Causal Stability | Clamps compaction to $\min(\text{Peer B}, \text{Peer C})$; preserves unacknowledged ops | **PASS** |
| `PROPERTY` | Causal Compaction Invariant | Property verified across PN-Counter, OR-Set, and LWW-Register | **PASS** |
| `PERF` | Performance Baseline | Measures snapshot creation, compaction, restore, and storage reclaimed | **PASS** |

---

## 10. Performance Baseline

Measured on Apple Silicon (ARM64) macOS running local SQLite WAL:

```
--- FIELDNET BUILD-05 SNAPSHOT & COMPACTION PERFORMANCE BASELINE ---
Operations Generated:        51 operations
Snapshot Creation Duration:  185.25 µs
Snapshot State Payload Size: 84 bytes
Compaction Duration:         515.66 µs
Operations Reclaimed:        51 operations
Storage Bytes Reclaimed:     13,465 bytes (~13.5 KB per 50 ops)
Restore + Replay Duration:   575.54 µs
------------------------------------------------------------------
```

### Analysis:
- **Snapshot Creation:** Sub-200 µs latency allows background snapshotting without impacting real-time CRDT updates or message routing.
- **Log Compaction:** ~500 µs to evaluate peer watermarks, compute byte totals, prune operation rows, and record compaction metadata.
- **Incremental Restore vs Full Replay:** On a document with 105 operations, restoring the snapshot and replaying 5 operations takes ~0.5 ms, whereas replaying 105 full deserializations and signature verifications scales linearly.

---

## 11. Known Limitations & Next Steps

1. **Automatic Background Compaction Cadence:** Automatic periodic compaction (`CompactionConfig`) can be scheduled via background timers; manual or API-triggered compaction is the primary invocation path in BUILD-05.
2. **Partial Mesh Horizon:** In partitions where a remote peer has been offline for longer than `peer_grace_period_days` (default 30 days), the node considers the peer inactive and allows compaction past its watermark. If that peer reconnects after 30 days, it must perform a full snapshot sync (`SYNC_SNAPSHOT`). Full snapshot network sync protocol handlers will be exercised in integration workflows.
