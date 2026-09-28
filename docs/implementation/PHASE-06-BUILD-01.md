# FIELDNET Implementation Report: Phase 06 — BUILD-01
## Title: Local Database & Durable Document Store

**Status:** COMPLETE  
**Milestone:** Phase 06 — BUILD-01  
**Author:** FIELDNET Architecture & Core Implementation Team  
**Date:** September 2026  

---

## 1. Executive Summary

Phase 06 BUILD-01 implements FIELDNET's foundational **Local-First Data and Durable Document Store** subsystem. This guarantees that every sovereign FIELDNET node can create, read, update, delete (tombstone), and query local documents completely offline—without internet connectivity, remote peers, cloud relays, or central coordinators.

All documents are durably persisted in an embedded SQLite database configured in Write-Ahead Logging (`WAL`) mode with transactional integrity, deterministic schema migrations, and crash recovery. Document mutations survive runtime shutdown and restart, and the data subsystem operates independently of any active network interface.

---

## 2. Crate & Module Architecture

A dedicated workspace crate `crates/fieldnet-data` was created and integrated into the FIELDNET workspace:

```
crates/fieldnet-data/
├── Cargo.toml
├── src/
│   ├── lib.rs              # Public exports and prelude
│   ├── document.rs         # Canonical Document model and DocumentId
│   ├── database.rs         # SQLite connection manager, WAL pragmas, transactions
│   ├── schema.rs           # Schema definitions and resource boundaries
│   ├── migration.rs        # Versioned migration engine and checksum validator
│   ├── storage.rs          # DocumentStore high-level async & sync API
│   └── error.rs            # Structured DataError domain errors
└── tests/
    ├── document_store_tests.rs              # 15 comprehensive required tests
    └── property_invariants_and_perf_test.rs # 7 Invariant tests & performance benchmark
```

`fieldnet-runtime` was extended to integrate `fieldnet-data::DocumentStore`, automatically instantiating a persistent document store under `config.data_dir/documents.db` keyed to the local sovereign `NodeId`.

---

## 3. SQLite Storage Engine & PRAGMAs

The database backend is managed via `Database` wrapping `rusqlite::Connection` with thread-safe synchronization:

```rust
conn.pragma_update(None, "journal_mode", "WAL")?;
conn.pragma_update(None, "synchronous", "NORMAL")?;
conn.pragma_update(None, "foreign_keys", "ON")?;
conn.pragma_update(None, "busy_timeout", 5000)?;
```

- **WAL Mode**: Permits concurrent reads alongside active writes without lock starvation.
- **Synchronous = NORMAL**: Provides crash-resilient durability while minimizing disk sync overhead.
- **Foreign Keys = ON**: Enforces relational consistency for future CRDT operations and snapshots.
- **Busy Timeout = 5000ms**: Eliminates transient busy errors under concurrent async task execution.

The database path is strictly dynamic and lives inside the node's configured data directory (e.g. `<data_dir>/documents.db`), ensuring multi-node isolation on shared filesystems.

---

## 4. Initial Database Schema (Migration v1)

```sql
-- Initial Local-First Documents Table
CREATE TABLE IF NOT EXISTS documents (
    document_id         BLOB PRIMARY KEY,   -- 32-byte BLAKE3 logical identifier
    document_type       TEXT NOT NULL,      -- Generic domain type (e.g. "note", "marker")
    schema_version      INTEGER NOT NULL DEFAULT 1,
    owner_node_id       BLOB NOT NULL,      -- Sovereign NodeId of author
    version             INTEGER NOT NULL DEFAULT 1, -- Monotonic revision counter
    created_at          INTEGER NOT NULL,   -- UTC Epoch milliseconds
    updated_at          INTEGER NOT NULL,   -- UTC Epoch milliseconds
    is_deleted          INTEGER NOT NULL DEFAULT 0, -- 0 = active, 1 = tombstone
    payload_bytes       BLOB NOT NULL       -- Arbitrary state payload (JSON/bytes)
);

CREATE INDEX IF NOT EXISTS idx_documents_type ON documents(document_type);
CREATE INDEX IF NOT EXISTS idx_documents_updated ON documents(updated_at);
CREATE INDEX IF NOT EXISTS idx_documents_deleted ON documents(is_deleted);
CREATE INDEX IF NOT EXISTS idx_documents_owner ON documents(owner_node_id);
```

---

## 5. Versioned Migration Engine

The `MigrationRunner` applies deterministic, non-destructive migrations:
- Creates `schema_migrations (version, name, applied_at, checksum)`.
- Validates cryptographic BLAKE3 checksums of previously applied migrations to detect corruption or out-of-order schema tampering.
- Executes pending migrations within isolated SQLite transactions.
- Startup is strictly idempotent: repeatedly opening an existing database executes 0 unneeded migrations and never destroys existing data.

---

## 6. Document Model & Logical Identity

### Canonical Model
```rust
pub struct DocumentId(pub [u8; 32]);

pub struct Document {
    pub document_id: DocumentId,
    pub document_type: String,
    pub schema_version: u32,
    pub owner_node_id: NodeId,
    pub version: u64,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    pub deleted: bool,
    pub state: Vec<u8>,
}
```

- **Stable Logical Identity**: `DocumentId` is generated using BLAKE3 hashing over random cryptographic entropy and nanosecond timestamps. It does not use filesystem paths, database row IDs, or network addresses.
- **Generic Payload**: `state: Vec<u8>` accommodates arbitrary binary data or JSON objects, validated against a maximum threshold (`MAX_DOCUMENT_SIZE = 4 MB`).

---

## 7. Application-Facing DocumentStore API

`DocumentStore` exposes both asynchronous (Tokio blocking thread pool) and synchronous APIs:

| Method | Parameters | Returns | Description |
|---|---|---|---|
| `create` | `document_type, state` | `Result<Document, DataError>` | Generates ID, inserts document atomically |
| `create_json` | `document_type, value` | `Result<Document, DataError>` | Serializes serde value to JSON and persists |
| `get` | `id` | `Result<Option<Document>, DataError>` | Fetches document including tombstones |
| `get_active` | `id` | `Result<Document, DataError>` | Fetches active document; fails if tombstoned |
| `update` | `id, new_state` | `Result<Document, DataError>` | Atomically updates payload and increments version |
| `update_json` | `id, value` | `Result<Document, DataError>` | Serializes and updates active document |
| `delete` | `id` | `Result<Document, DataError>` | Sets logical tombstone (`deleted = true`) |
| `list` | `DocumentQuery` | `Result<Vec<Document>, DataError>` | Paginated and filtered document retrieval |
| `count` | `document_type, include_deleted`| `Result<usize, DataError>` | Returns count of matching documents |

---

## 8. Atomicity & Deletion Semantics

- **Atomic Transactions**: All mutations execute inside `Database::with_transaction`. If any validation or SQL operation fails, the transaction is immediately rolled back via RAII, preventing partial writes.
- **Logical Deletion**: Deletion sets `is_deleted = 1` and advances the version counter. Raw document state and metadata are retained as a tombstone, ready for Phase 06 CRDT and replication sync.

---

## 9. Verification & Test Suite

### 9.1 Required Tests (`crates/fieldnet-data/tests/document_store_tests.rs`)
- **TEST 1**: `test_01_initialize_empty_database` — Verified initial clean state.
- **TEST 2**: `test_02_create_document` — Verified document creation, initial revision, and timestamps.
- **TEST 3**: `test_03_read_document` — Verified active and raw retrieval.
- **TEST 4**: `test_04_update_document` — Verified atomic updates and version counter increments.
- **TEST 5**: `test_05_delete_document` — Verified tombstone persistence and `get_active` rejection.
- **TEST 6**: `test_06_list_documents` — Verified type filtering, pagination limit/offset, and tombstone inclusion flags.
- **TEST 7**: `test_07_missing_document` — Verified `Ok(None)` on `get` and `DocumentNotFound` on `get_active`.
- **TEST 8**: `test_08_invalid_document_input` — Verified rejection of empty types, invalid characters, and payloads exceeding 4 MB.
- **TEST 9**: `test_09_transaction_rollback` — Verified clean rollback on mid-transaction failure.
- **TEST 10**: `test_10_database_restart_persistence` — Verified active and deleted documents survive database close and reopen.
- **TEST 11**: `test_11_multiple_documents` — Verified 50+ documents concurrently created and indexed.
- **TEST 12**: `test_12_multiple_document_types` — Verified multi-type isolation and typed JSON deserialization.
- **TEST 13**: `test_13_concurrent_reads_writes` — Verified 10 concurrent async worker tasks reading and writing.
- **TEST 14**: `test_14_migration_execution` — Verified version tracking and zero redundant executions.
- **TEST 15**: `test_15_repeated_initialization_of_existing_database` — Verified repeated startups never wipe or corrupt existing rows.

### 9.2 Invariant & Property Tests (`crates/fieldnet-data/tests/property_invariants_and_perf_test.rs`)
1. **Invariant 1**: Document ID remains stable across mutations.
2. **Invariant 2**: Committed document survives database restart.
3. **Invariant 3**: Failed transaction leaves no partial state.
4. **Invariant 4**: Deletion marker never resurrects across restarts.
5. **Invariant 5**: Database initialization is strictly idempotent.
6. **Invariant 6**: Existing data is never overwritten by startup.
7. **Invariant 7**: Invalid input cannot corrupt database integrity.

### 9.3 Runtime Integration Tests (`crates/fieldnet-runtime/tests/runtime_document_store_test.rs`)
- `test_runtime_document_store_lifecycle`: Verifies `FieldnetRuntime::start` initializes `DocumentStore` under `config.data_dir/documents.db`, supports document mutations, and preserves state across full runtime restart.
- `test_document_store_operates_without_networking`: Proves that a FIELDNET node creates and manages local document collections with zero network connectivity or peers.

---

## 10. Performance Baseline Benchmark

Executed on Apple Silicon (M-series, Darwin):

| Operation | Total Iterations | Duration | Throughput | Latency |
|---|---|---|---|---|
| **Create (1 KB payload)** | 200 | 33.95 ms | **5,891 ops/sec** | 0.170 ms / op |
| **Read (1 KB payload)** | 200 | 2.61 ms | **76,683 ops/sec** | 0.013 ms / op |
| **Update (1 KB payload)**| 200 | 6.82 ms | **29,309 ops/sec** | 0.034 ms / op |
| **Delete (Tombstone)** | 200 | 9.16 ms | **21,831 ops/sec** | 0.046 ms / op |

---

## 11. Known Limitations & Deferred Scope

1. **Distributed Replication & Sync**: Synchronization protocol (`SYNC_HELLO`, `SYNC_REQUEST`, `SYNC_RESPONSE`) is deferred to BUILD-04.
2. **Operation Change Log**: Operation appending and causal tracking are deferred to BUILD-02.
3. **CRDT Merge & Conflict Resolution**: LWW-registers, PN-counters, and OR-sets are deferred to BUILD-03.
4. **Compaction & Snapshot Checkpoints**: State compaction and garbage collection are deferred to BUILD-05.

---

## 12. Final Status

```
PHASE 06 → BUILD-01 COMPLETE
```
