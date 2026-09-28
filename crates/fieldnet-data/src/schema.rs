//! Schema definitions, table names, and storage boundaries for FIELDNET Local Data.

pub const TABLE_DOCUMENTS: &str = "documents";
pub const TABLE_MIGRATIONS: &str = "schema_migrations";
pub const TABLE_OPERATIONS: &str = "operations";
pub const TABLE_APPLIED_OPERATIONS: &str = "applied_operations";
pub const TABLE_REPLICA_STATE: &str = "replica_state";
pub const TABLE_LOCAL_NODE_STATE: &str = "local_node_state";
pub const TABLE_TOMBSTONES: &str = "tombstones";
pub const TABLE_UNAPPLIED_OPERATIONS: &str = "unapplied_operations";
pub const TABLE_PEER_SYNC_STATE: &str = "peer_sync_state";
pub const TABLE_SYNC_SESSIONS: &str = "sync_sessions";
pub const TABLE_SYNC_CURSORS: &str = "sync_cursors";
pub const TABLE_DOCUMENT_SNAPSHOTS: &str = "document_snapshots";
pub const TABLE_COMPACTION_RECORDS: &str = "compaction_records";

/// Default number of snapshots to retain per document (latest + fallback).
pub const DEFAULT_SNAPSHOT_RETENTION: usize = 2;

/// Default peer inactivity grace period (30 days) before peer is considered offline for compaction.
pub const DEFAULT_PEER_GRACE_PERIOD_DAYS: u64 = 30;

/// Maximum allowed payload size for a single document (4 MB).
pub const MAX_DOCUMENT_SIZE: usize = 4 * 1024 * 1024;

/// Maximum allowed length for document_type / namespace.
pub const MAX_DOCUMENT_TYPE_LEN: usize = 128;

/// Default limit for paginated queries.
pub const DEFAULT_QUERY_LIMIT: usize = 100;

/// Maximum allowable limit for paginated queries to prevent memory exhaustion.
pub const MAX_QUERY_LIMIT: usize = 1000;

/// Default application schema version.
pub const CURRENT_SCHEMA_VERSION: u32 = 1;

/// Current Sync Protocol Version.
pub const CURRENT_SYNC_PROTOCOL_VERSION: u32 = 1;

/// Maximum operations in a single synchronization delta batch.
pub const MAX_SYNC_BATCH_OPERATIONS: usize = 100;

/// Maximum payload bytes in a single synchronization delta batch (512 KB).
pub const MAX_SYNC_BATCH_BYTES: usize = 512 * 1024;
