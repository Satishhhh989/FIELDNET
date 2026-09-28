//! FIELDNET Local-First Data Subsystem (Phase 06).
//!
//! Provides local document durability, SQLite persistence, versioned migrations,
//! transactional CRUD operations, and logical tombstones.

pub mod causality;
pub mod crdt;
pub mod database;
pub mod document;
pub mod error;
pub mod migration;
pub mod operation;
pub mod schema;
pub mod snapshot;
pub mod storage;
pub mod sync;

pub use causality::{CausalRelation, HlcEngine, HybridLogicalClock, StateVector};
pub use crdt::{
    ConflictClassification, ConflictResolver, ConflictStrategy, Crdt, CrdtMap, CrdtMapDelta,
    CrdtType, ElementTag, FieldRegister, LwwRegister, LwwRegisterDelta, MvRegister,
    MvRegisterBranch, OrSet, OrSetDelta, PnCounter, PnCounterDelta,
};
pub use database::Database;
pub use document::{Document, DocumentId};
pub use error::DataError;
pub use migration::{Migration, MigrationRunner};
pub use operation::{OpStatus, Operation, OperationId, OperationType, ReplicaState};
pub use schema::{
    CURRENT_SCHEMA_VERSION, CURRENT_SYNC_PROTOCOL_VERSION, DEFAULT_PEER_GRACE_PERIOD_DAYS,
    DEFAULT_QUERY_LIMIT, DEFAULT_SNAPSHOT_RETENTION, MAX_DOCUMENT_SIZE, MAX_DOCUMENT_TYPE_LEN,
    MAX_QUERY_LIMIT, MAX_SYNC_BATCH_BYTES, MAX_SYNC_BATCH_OPERATIONS, TABLE_APPLIED_OPERATIONS,
    TABLE_COMPACTION_RECORDS, TABLE_DOCUMENTS, TABLE_DOCUMENT_SNAPSHOTS, TABLE_LOCAL_NODE_STATE,
    TABLE_MIGRATIONS, TABLE_OPERATIONS, TABLE_PEER_SYNC_STATE, TABLE_REPLICA_STATE,
    TABLE_SYNC_CURSORS, TABLE_SYNC_SESSIONS, TABLE_TOMBSTONES, TABLE_UNAPPLIED_OPERATIONS,
};
pub use snapshot::{
    CompactionConfig, CompactionRecord, CompactionSafetyReport, DocumentSnapshot, SnapshotId,
};
pub use storage::{DocumentQuery, DocumentStore, PeerSyncRecord};
pub use sync::{
    DefaultDocumentAuthorizer, DocumentAuthorizer, DocumentSummaryEntry, SyncAckStatus, SyncEngine,
    SyncEngineConfig, SyncMessage, SyncSession, SyncSessionState, SyncTransport,
};
