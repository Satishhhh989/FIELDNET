use fieldnet_core::{NodeId, Timestamp};
use fieldnet_identity::NodeIdentity;
use rusqlite::{params, Row};
use std::path::Path;
use std::sync::Arc;
use tracing::{info, warn};

use crate::causality::{HlcEngine, HybridLogicalClock, StateVector};
use crate::crdt::{
    ConflictClassification, ConflictResolver, Crdt, CrdtMap, CrdtMapDelta, LwwRegister,
    LwwRegisterDelta, OrSet, OrSetDelta, PnCounter, PnCounterDelta,
};
use crate::database::Database;
use crate::document::{Document, DocumentId};
use crate::error::DataError;
use crate::operation::{OpStatus, Operation, OperationId, OperationType, ReplicaState};
use crate::schema::{DEFAULT_QUERY_LIMIT, MAX_DOCUMENT_SIZE, MAX_QUERY_LIMIT};
use crate::snapshot::{CompactionRecord, CompactionSafetyReport, DocumentSnapshot, SnapshotId};

/// Query parameters for listing documents with pagination and filtering.
#[derive(Clone, Debug, Default)]
pub struct DocumentQuery {
    pub document_type: Option<String>,
    pub include_deleted: bool,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

impl DocumentQuery {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_type(mut self, doc_type: impl Into<String>) -> Self {
        self.document_type = Some(doc_type.into());
        self
    }

    pub fn include_deleted(mut self, include: bool) -> Self {
        self.include_deleted = include;
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }
}

/// Persistent record tracking the synchronization watermark for a remote peer.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PeerSyncRecord {
    pub peer_node_id: NodeId,
    pub document_id: DocumentId,
    pub last_synced_seq: u64,
    pub last_synced_hlc: HybridLogicalClock,
    pub last_sync_time: Timestamp,
    pub sync_status: String,
}

/// Durable, local-first Document Store providing transactional CRUD operations,
/// an append-only operation log, causal tracking, and state vectors.
#[derive(Clone)]
pub struct DocumentStore {
    db: Arc<Database>,
    owner_node_id: NodeId,
    identity: Option<Arc<NodeIdentity>>,
    hlc_engine: Arc<HlcEngine>,
}

impl DocumentStore {
    /// Opens or connects to a Document Store SQLite database at the given path.
    pub fn open(path: impl AsRef<Path>, owner_node_id: NodeId) -> Result<Self, DataError> {
        let db = Database::open(path)?;
        let initial_hlc = Self::load_persisted_hlc(&db, &owner_node_id)?;
        let hlc_engine = Arc::new(HlcEngine::new(initial_hlc));

        info!(
            "DocumentStore initialized for local node {} (initial HLC: {})",
            owner_node_id, initial_hlc
        );
        Ok(Self {
            db: Arc::new(db),
            owner_node_id,
            identity: None,
            hlc_engine,
        })
    }

    /// Opens a Document Store with an active sovereign cryptographic identity for signing operations.
    pub fn with_identity(
        path: impl AsRef<Path>,
        identity: Arc<NodeIdentity>,
    ) -> Result<Self, DataError> {
        let owner_node_id = identity.node_id();
        let db = Database::open(path)?;
        let initial_hlc = Self::load_persisted_hlc(&db, &owner_node_id)?;
        let hlc_engine = Arc::new(HlcEngine::new(initial_hlc));

        info!(
            "DocumentStore initialized with sovereign identity for node {}",
            owner_node_id
        );
        Ok(Self {
            db: Arc::new(db),
            owner_node_id,
            identity: Some(identity),
            hlc_engine,
        })
    }

    /// Opens an in-memory Document Store for tests.
    pub fn in_memory(owner_node_id: NodeId) -> Result<Self, DataError> {
        let db = Database::in_memory()?;
        let hlc_engine = Arc::new(HlcEngine::new(HybridLogicalClock::now()));
        Ok(Self {
            db: Arc::new(db),
            owner_node_id,
            identity: None,
            hlc_engine,
        })
    }

    /// Opens an in-memory Document Store with an active identity.
    pub fn in_memory_with_identity(identity: Arc<NodeIdentity>) -> Result<Self, DataError> {
        let owner_node_id = identity.node_id();
        let db = Database::in_memory()?;
        let hlc_engine = Arc::new(HlcEngine::new(HybridLogicalClock::now()));
        Ok(Self {
            db: Arc::new(db),
            owner_node_id,
            identity: Some(identity),
            hlc_engine,
        })
    }

    /// Returns the local sovereign NodeId owning this document store.
    pub fn owner_node_id(&self) -> NodeId {
        self.owner_node_id
    }

    /// Returns a reference to the underlying Database instance.
    pub fn database(&self) -> &Arc<Database> {
        &self.db
    }

    /// Returns the current Hybrid Logical Clock.
    pub fn current_hlc(&self) -> HybridLogicalClock {
        self.hlc_engine.current()
    }

    // ========================================================================
    // SYNCHRONOUS CRUD & OPERATION MUTATIONS
    // ========================================================================

    /// Synchronously creates a document and atomically appends its CREATE operation.
    pub fn create_sync(
        &self,
        document_type: impl Into<String>,
        state: Vec<u8>,
    ) -> Result<Document, DataError> {
        let (doc, _) = self.create_with_op_sync(document_type, state)?;
        Ok(doc)
    }

    /// Synchronously creates a document and returns both the Document and durable Operation.
    pub fn create_with_op_sync(
        &self,
        document_type: impl Into<String>,
        state: Vec<u8>,
    ) -> Result<(Document, Operation), DataError> {
        let doc = Document::new(document_type, self.owner_node_id, state)?;
        doc.validate()?;

        self.db.with_transaction(|tx| {
            // 1. Check uniqueness
            let existing: Option<i64> = tx
                .query_row(
                    "SELECT 1 FROM documents WHERE document_id = ?1",
                    params![doc.document_id.as_bytes().as_slice()],
                    |row| row.get(0),
                )
                .ok();

            if existing.is_some() {
                return Err(DataError::DocumentAlreadyExists(doc.document_id));
            }

            // 2. Advance local node sequence and HLC
            let (seq, hlc) = self.advance_node_sequence_in_tx(tx)?;

            // 3. Construct Operation
            let op = self.construct_operation(
                doc.document_id,
                seq,
                OperationType::Create,
                doc.state.clone(),
                hlc,
                vec![],
            );

            // 4. Persist document
            tx.execute(
                "INSERT INTO documents (
                    document_id, document_type, schema_version, owner_node_id,
                    version, created_at, updated_at, is_deleted, payload_bytes
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    doc.document_id.as_bytes().as_slice(),
                    doc.document_type,
                    doc.schema_version,
                    doc.owner_node_id.as_bytes().as_slice(),
                    doc.version as i64,
                    doc.created_at.as_millis() as i64,
                    doc.updated_at.as_millis() as i64,
                    if doc.deleted { 1 } else { 0 },
                    doc.state,
                ],
            )?;

            // 5. Persist operation in append-only log
            self.insert_operation_in_tx(tx, &op)?;

            // 6. Record in applied_operations
            tx.execute(
                "INSERT INTO applied_operations (document_id, op_id, applied_at) VALUES (?1, ?2, ?3)",
                params![
                    doc.document_id.as_bytes().as_slice(),
                    op.op_id.as_bytes().as_slice(),
                    op.created_at.as_millis() as i64,
                ],
            )?;

            // 7. Initialize replica_state
            let mut sv = StateVector::new();
            sv.set(self.owner_node_id, seq);
            let sv_bytes = sv.to_bytes();

            tx.execute(
                "INSERT INTO replica_state (
                    document_id, state_vector_bytes, latest_hlc_physical,
                    latest_hlc_logical, local_sequence, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    doc.document_id.as_bytes().as_slice(),
                    sv_bytes,
                    hlc.physical as i64,
                    hlc.logical as i64,
                    seq as i64,
                    doc.created_at.as_millis() as i64,
                ],
            )?;

            info!(
                document_id = %doc.document_id,
                op_id = %op.op_id,
                seq = seq,
                hlc = %hlc,
                "Document and CREATE operation committed atomically"
            );

            Ok((doc, op))
        })
    }

    /// Synchronously updates an active document and atomically appends its UPDATE operation.
    pub fn update_sync(&self, id: &DocumentId, new_state: Vec<u8>) -> Result<Document, DataError> {
        let (doc, _) = self.update_with_op_sync(id, new_state)?;
        Ok(doc)
    }

    /// Synchronously updates a document and returns both Document and durable Operation.
    pub fn update_with_op_sync(
        &self,
        id: &DocumentId,
        new_state: Vec<u8>,
    ) -> Result<(Document, Operation), DataError> {
        if new_state.len() > MAX_DOCUMENT_SIZE {
            return Err(DataError::DocumentTooLarge {
                size: new_state.len(),
                max: MAX_DOCUMENT_SIZE,
            });
        }

        self.db.with_transaction(|tx| {
            // 1. Fetch current document
            let mut stmt = tx.prepare(
                "SELECT document_id, document_type, schema_version, owner_node_id,
                        version, created_at, updated_at, is_deleted, payload_bytes
                 FROM documents WHERE document_id = ?1",
            )?;

            let mut rows = stmt.query(params![id.as_bytes().as_slice()])?;
            let row = match rows.next()? {
                Some(r) => r,
                None => return Err(DataError::DocumentNotFound(*id)),
            };

            let mut doc = Self::row_to_document(row)?;
            if doc.deleted {
                return Err(DataError::DocumentDeleted(*id));
            }
            drop(rows);
            drop(stmt);

            // 2. Fetch latest parent operation for this document
            let parent_op: Option<OperationId> = tx
                .query_row(
                    "SELECT op_id FROM operations WHERE document_id = ?1 
                     ORDER BY hlc_physical DESC, hlc_logical DESC, sequence DESC LIMIT 1",
                    params![id.as_bytes().as_slice()],
                    |r| {
                        let bytes: Vec<u8> = r.get(0)?;
                        let mut arr = [0u8; 32];
                        if bytes.len() == 32 {
                            arr.copy_from_slice(&bytes);
                        }
                        Ok(OperationId::from_bytes(arr))
                    },
                )
                .ok();
            let dependencies = parent_op.into_iter().collect::<Vec<_>>();

            // 3. Advance local sequence and HLC
            let (seq, hlc) = self.advance_node_sequence_in_tx(tx)?;

            // 4. Construct Operation
            let op = self.construct_operation(
                *id,
                seq,
                OperationType::Update,
                new_state.clone(),
                hlc,
                dependencies,
            );

            // 5. Update document record
            let now = Timestamp::now();
            let new_version = doc.version + 1;

            tx.execute(
                "UPDATE documents SET payload_bytes = ?1, updated_at = ?2, version = ?3 WHERE document_id = ?4",
                params![
                    new_state,
                    now.as_millis() as i64,
                    new_version as i64,
                    id.as_bytes().as_slice(),
                ],
            )?;

            doc.state = new_state;
            doc.updated_at = now;
            doc.version = new_version;

            // 6. Persist operation
            self.insert_operation_in_tx(tx, &op)?;

            // 7. Track applied_operations
            tx.execute(
                "INSERT INTO applied_operations (document_id, op_id, applied_at) VALUES (?1, ?2, ?3)",
                params![
                    id.as_bytes().as_slice(),
                    op.op_id.as_bytes().as_slice(),
                    now.as_millis() as i64,
                ],
            )?;

            // 8. Update replica_state
            let mut sv = self.load_state_vector_in_tx(tx, id)?;
            sv.advance(self.owner_node_id, seq);
            let sv_bytes = sv.to_bytes();

            tx.execute(
                "INSERT INTO replica_state (
                    document_id, state_vector_bytes, latest_hlc_physical,
                    latest_hlc_logical, local_sequence, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                ON CONFLICT(document_id) DO UPDATE SET
                    state_vector_bytes = excluded.state_vector_bytes,
                    latest_hlc_physical = excluded.latest_hlc_physical,
                    latest_hlc_logical = excluded.latest_hlc_logical,
                    local_sequence = excluded.local_sequence,
                    updated_at = excluded.updated_at",
                params![
                    id.as_bytes().as_slice(),
                    sv_bytes,
                    hlc.physical as i64,
                    hlc.logical as i64,
                    seq as i64,
                    now.as_millis() as i64,
                ],
            )?;

            info!(
                document_id = %id,
                op_id = %op.op_id,
                version = doc.version,
                seq = seq,
                hlc = %hlc,
                "Document and UPDATE operation committed atomically"
            );

            Ok((doc, op))
        })
    }

    /// Synchronously deletes a document logically and atomically appends its DELETE operation.
    pub fn delete_sync(&self, id: &DocumentId) -> Result<Document, DataError> {
        let (doc, _) = self.delete_with_op_sync(id)?;
        Ok(doc)
    }

    /// Synchronously deletes a document and returns both Document and durable Operation.
    pub fn delete_with_op_sync(&self, id: &DocumentId) -> Result<(Document, Operation), DataError> {
        self.db.with_transaction(|tx| {
            let mut stmt = tx.prepare(
                "SELECT document_id, document_type, schema_version, owner_node_id,
                        version, created_at, updated_at, is_deleted, payload_bytes
                 FROM documents WHERE document_id = ?1",
            )?;

            let mut rows = stmt.query(params![id.as_bytes().as_slice()])?;
            let row = match rows.next()? {
                Some(r) => r,
                None => return Err(DataError::DocumentNotFound(*id)),
            };

            let mut doc = Self::row_to_document(row)?;
            drop(rows);
            drop(stmt);

            // Fetch parent op
            let parent_op: Option<OperationId> = tx
                .query_row(
                    "SELECT op_id FROM operations WHERE document_id = ?1 
                     ORDER BY hlc_physical DESC, hlc_logical DESC, sequence DESC LIMIT 1",
                    params![id.as_bytes().as_slice()],
                    |r| {
                        let bytes: Vec<u8> = r.get(0)?;
                        let mut arr = [0u8; 32];
                        if bytes.len() == 32 {
                            arr.copy_from_slice(&bytes);
                        }
                        Ok(OperationId::from_bytes(arr))
                    },
                )
                .ok();
            let dependencies = parent_op.into_iter().collect::<Vec<_>>();

            let (seq, hlc) = self.advance_node_sequence_in_tx(tx)?;

            let op = self.construct_operation(
                *id,
                seq,
                OperationType::Delete,
                vec![],
                hlc,
                dependencies,
            );

            let now = Timestamp::now();
            let new_version = doc.version + 1;

            tx.execute(
                "UPDATE documents SET is_deleted = 1, updated_at = ?1, version = ?2 WHERE document_id = ?3",
                params![
                    now.as_millis() as i64,
                    new_version as i64,
                    id.as_bytes().as_slice(),
                ],
            )?;

            let gc_after = Timestamp::from_millis(now.as_millis() + 30 * 24 * 3600 * 1000);
            tx.execute(
                "INSERT INTO tombstones (entity_id, entity_type, deleted_at, hlc_physical, hlc_logical, deleted_by, gc_after)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(entity_id) DO NOTHING",
                params![
                    id.as_bytes().as_slice(),
                    doc.document_type,
                    now.as_millis() as i64,
                    hlc.physical as i64,
                    hlc.logical as i64,
                    self.owner_node_id.as_bytes().as_slice(),
                    gc_after.as_millis() as i64,
                ],
            )?;

            doc.deleted = true;
            doc.updated_at = now;
            doc.version = new_version;

            self.insert_operation_in_tx(tx, &op)?;

            tx.execute(
                "INSERT INTO applied_operations (document_id, op_id, applied_at) VALUES (?1, ?2, ?3)",
                params![
                    id.as_bytes().as_slice(),
                    op.op_id.as_bytes().as_slice(),
                    now.as_millis() as i64,
                ],
            )?;

            let mut sv = self.load_state_vector_in_tx(tx, id)?;
            sv.advance(self.owner_node_id, seq);
            let sv_bytes = sv.to_bytes();

            tx.execute(
                "INSERT INTO replica_state (
                    document_id, state_vector_bytes, latest_hlc_physical,
                    latest_hlc_logical, local_sequence, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                ON CONFLICT(document_id) DO UPDATE SET
                    state_vector_bytes = excluded.state_vector_bytes,
                    latest_hlc_physical = excluded.latest_hlc_physical,
                    latest_hlc_logical = excluded.latest_hlc_logical,
                    local_sequence = excluded.local_sequence,
                    updated_at = excluded.updated_at",
                params![
                    id.as_bytes().as_slice(),
                    sv_bytes,
                    hlc.physical as i64,
                    hlc.logical as i64,
                    seq as i64,
                    now.as_millis() as i64,
                ],
            )?;

            info!(
                document_id = %id,
                op_id = %op.op_id,
                version = doc.version,
                "Document deleted and DELETE operation committed"
            );

            Ok((doc, op))
        })
    }

    // ========================================================================
    // CRDT SYNCHRONOUS MUTATIONS & REMOTE REPLICATION
    // ========================================================================

    /// Synchronously creates a CRDT document and atomically appends its CREATE operation.
    pub fn create_crdt_sync<C: Crdt>(&self, crdt: &C) -> Result<(Document, Operation), DataError> {
        let state = crdt.serialize_state()?;
        let (doc, op) = self.create_with_op_sync(crdt.crdt_type().as_str(), state)?;
        info!(
            target: "crdt",
            op_id = %op.op_id,
            doc_id = %doc.document_id,
            crdt_type = crdt.crdt_type().as_str(),
            "crdt.operation.generated"
        );
        info!(
            target: "crdt",
            document_id = %doc.document_id,
            "crdt.state.updated"
        );
        Ok((doc, op))
    }

    /// Reads and deserializes a CRDT document by DocumentId.
    pub fn get_crdt_sync<C: Crdt>(&self, id: &DocumentId) -> Result<C, DataError> {
        let doc = self.get_active_sync(id)?;
        let expected_type = C::crdt_family().as_str();
        if doc.document_type != expected_type {
            return Err(DataError::TypeMismatch {
                expected: expected_type.to_string(),
                found: doc.document_type,
            });
        }
        C::deserialize_state(&doc.state)
    }

    /// Synchronously applies a local CRDT delta and atomically records its CRDT_DELTA operation.
    pub fn apply_crdt_delta_sync<C: Crdt>(
        &self,
        id: &DocumentId,
        delta: &C::Delta,
    ) -> Result<(Document, Operation), DataError> {
        self.db.with_transaction(|tx| {
            let mut stmt = tx.prepare(
                "SELECT document_id, document_type, schema_version, owner_node_id,
                        version, created_at, updated_at, is_deleted, payload_bytes
                 FROM documents WHERE document_id = ?1",
            )?;

            let mut rows = stmt.query(params![id.as_bytes().as_slice()])?;
            let row = match rows.next()? {
                Some(r) => r,
                None => return Err(DataError::DocumentNotFound(*id)),
            };

            let mut doc = Self::row_to_document(row)?;
            if doc.deleted {
                return Err(DataError::DocumentDeleted(*id));
            }
            drop(rows);
            drop(stmt);

            let expected_type = C::crdt_family().as_str();
            if doc.document_type != expected_type {
                return Err(DataError::TypeMismatch {
                    expected: expected_type.to_string(),
                    found: doc.document_type,
                });
            }

            let mut crdt = C::deserialize_state(&doc.state)?;
            crdt.apply_delta(delta)?;
            let new_state = crdt.serialize_state()?;

            let parent_op: Option<OperationId> = tx
                .query_row(
                    "SELECT op_id FROM operations WHERE document_id = ?1 
                     ORDER BY hlc_physical DESC, hlc_logical DESC, sequence DESC LIMIT 1",
                    params![id.as_bytes().as_slice()],
                    |r| {
                        let bytes: Vec<u8> = r.get(0)?;
                        let mut arr = [0u8; 32];
                        if bytes.len() == 32 {
                            arr.copy_from_slice(&bytes);
                        }
                        Ok(OperationId::from_bytes(arr))
                    },
                )
                .ok();
            let dependencies = parent_op.into_iter().collect::<Vec<_>>();

            let (seq, hlc) = self.advance_node_sequence_in_tx(tx)?;
            let delta_bytes = serde_json::to_vec(delta)
                .map_err(|e| DataError::SerializationFailure(e.to_string()))?;

            let op = self.construct_operation(
                *id,
                seq,
                OperationType::CrdtDelta,
                delta_bytes,
                hlc,
                dependencies,
            );

            let now = Timestamp::now();
            let new_version = doc.version + 1;

            tx.execute(
                "UPDATE documents SET payload_bytes = ?1, updated_at = ?2, version = ?3 WHERE document_id = ?4",
                params![
                    new_state,
                    now.as_millis() as i64,
                    new_version as i64,
                    id.as_bytes().as_slice(),
                ],
            )?;

            doc.state = new_state;
            doc.updated_at = now;
            doc.version = new_version;

            self.insert_operation_in_tx(tx, &op)?;

            tx.execute(
                "INSERT INTO applied_operations (document_id, op_id, applied_at) VALUES (?1, ?2, ?3)",
                params![
                    id.as_bytes().as_slice(),
                    op.op_id.as_bytes().as_slice(),
                    now.as_millis() as i64,
                ],
            )?;

            let mut sv = self.load_state_vector_in_tx(tx, id)?;
            sv.advance(self.owner_node_id, seq);
            let sv_bytes = sv.to_bytes();

            tx.execute(
                "INSERT INTO replica_state (
                    document_id, state_vector_bytes, latest_hlc_physical,
                    latest_hlc_logical, local_sequence, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                ON CONFLICT(document_id) DO UPDATE SET
                    state_vector_bytes = excluded.state_vector_bytes,
                    latest_hlc_physical = excluded.latest_hlc_physical,
                    latest_hlc_logical = excluded.latest_hlc_logical,
                    local_sequence = excluded.local_sequence,
                    updated_at = excluded.updated_at",
                params![
                    id.as_bytes().as_slice(),
                    sv_bytes,
                    hlc.physical as i64,
                    hlc.logical as i64,
                    seq as i64,
                    now.as_millis() as i64,
                ],
            )?;

            info!(target: "crdt", op_id = %op.op_id, "crdt.operation.generated");
            info!(target: "crdt", op_id = %op.op_id, "crdt.operation.applied");
            info!(target: "crdt", document_id = %id, "crdt.state.updated");

            Ok((doc, op))
        })
    }

    /// Synchronously merges a remote CRDT state into an active local document.
    pub fn merge_crdt_sync<C: Crdt>(
        &self,
        id: &DocumentId,
        remote_crdt: &C,
    ) -> Result<(Document, Operation, ConflictClassification), DataError> {
        self.db.with_transaction(|tx| {
            let mut stmt = tx.prepare(
                "SELECT document_id, document_type, schema_version, owner_node_id,
                        version, created_at, updated_at, is_deleted, payload_bytes
                 FROM documents WHERE document_id = ?1",
            )?;

            let mut rows = stmt.query(params![id.as_bytes().as_slice()])?;
            let row = match rows.next()? {
                Some(r) => r,
                None => return Err(DataError::DocumentNotFound(*id)),
            };

            let mut doc = Self::row_to_document(row)?;
            if doc.deleted {
                return Err(DataError::DocumentDeleted(*id));
            }
            drop(rows);
            drop(stmt);

            let expected_type = C::crdt_family().as_str();
            if doc.document_type != expected_type {
                return Err(DataError::TypeMismatch {
                    expected: expected_type.to_string(),
                    found: doc.document_type,
                });
            }

            let mut crdt = C::deserialize_state(&doc.state)?;
            crdt.merge(remote_crdt)?;
            let new_state = crdt.serialize_state()?;

            let parent_op: Option<OperationId> = tx
                .query_row(
                    "SELECT op_id FROM operations WHERE document_id = ?1 
                     ORDER BY hlc_physical DESC, hlc_logical DESC, sequence DESC LIMIT 1",
                    params![id.as_bytes().as_slice()],
                    |r| {
                        let bytes: Vec<u8> = r.get(0)?;
                        let mut arr = [0u8; 32];
                        if bytes.len() == 32 {
                            arr.copy_from_slice(&bytes);
                        }
                        Ok(OperationId::from_bytes(arr))
                    },
                )
                .ok();
            let dependencies = parent_op.into_iter().collect::<Vec<_>>();

            let (seq, hlc) = self.advance_node_sequence_in_tx(tx)?;
            let payload = remote_crdt.serialize_state()?;

            let op = self.construct_operation(
                *id,
                seq,
                OperationType::CrdtDelta,
                payload,
                hlc,
                dependencies,
            );

            let now = Timestamp::now();
            let new_version = doc.version + 1;

            tx.execute(
                "UPDATE documents SET payload_bytes = ?1, updated_at = ?2, version = ?3 WHERE document_id = ?4",
                params![
                    new_state,
                    now.as_millis() as i64,
                    new_version as i64,
                    id.as_bytes().as_slice(),
                ],
            )?;

            doc.state = new_state;
            doc.updated_at = now;
            doc.version = new_version;

            self.insert_operation_in_tx(tx, &op)?;

            tx.execute(
                "INSERT INTO applied_operations (document_id, op_id, applied_at) VALUES (?1, ?2, ?3)",
                params![
                    id.as_bytes().as_slice(),
                    op.op_id.as_bytes().as_slice(),
                    now.as_millis() as i64,
                ],
            )?;

            let mut sv = self.load_state_vector_in_tx(tx, id)?;
            sv.advance(self.owner_node_id, seq);
            let sv_bytes = sv.to_bytes();

            tx.execute(
                "INSERT INTO replica_state (
                    document_id, state_vector_bytes, latest_hlc_physical,
                    latest_hlc_logical, local_sequence, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                ON CONFLICT(document_id) DO UPDATE SET
                    state_vector_bytes = excluded.state_vector_bytes,
                    latest_hlc_physical = excluded.latest_hlc_physical,
                    latest_hlc_logical = excluded.latest_hlc_logical,
                    local_sequence = excluded.local_sequence,
                    updated_at = excluded.updated_at",
                params![
                    id.as_bytes().as_slice(),
                    sv_bytes,
                    hlc.physical as i64,
                    hlc.logical as i64,
                    seq as i64,
                    now.as_millis() as i64,
                ],
            )?;

            info!(target: "crdt", op_id = %op.op_id, "crdt.conflict.detected");
            info!(target: "crdt", op_id = %op.op_id, "crdt.conflict.resolved");
            info!(target: "crdt", document_id = %id, "crdt.state.updated");

            Ok((doc, op, ConflictClassification::Resolved))
        })
    }

    /// Synchronously applies an operation originating from a remote replica.
    /// Traverses the complete validation pipeline (structure, signature, idempotency, dependencies, CRDT merge).
    pub fn apply_remote_operation_sync(
        &self,
        op: &Operation,
    ) -> Result<ConflictClassification, DataError> {
        // 1. Validate payload size
        if op.payload.len() > MAX_DOCUMENT_SIZE {
            warn!(target: "crdt", op_id = %op.op_id, "crdt.operation.rejected");
            return Err(DataError::DocumentTooLarge {
                size: op.payload.len(),
                max: MAX_DOCUMENT_SIZE,
            });
        }

        // 2. Validate cryptographic signature if local identity matches author
        if op.signature != [0u8; 64] {
            if let Some(ref identity) = self.identity {
                if op.actor_node_id == self.owner_node_id {
                    if let Err(e) = op.verify_signature_with_identity(identity) {
                        warn!(target: "crdt", op_id = %op.op_id, reason = %e, "crdt.operation.rejected");
                        return Err(e);
                    }
                }
            }
        }

        // 3. Validate deterministic OpId digest
        let computed_id = Operation::compute_id(
            &op.document_id,
            &op.actor_node_id,
            op.sequence,
            op.op_type,
            &op.payload,
            &op.hlc,
            &op.causal_dependencies,
            op.schema_version,
        );
        if computed_id != op.op_id {
            warn!(target: "crdt", op_id = %op.op_id, "crdt.operation.rejected");
            return Err(DataError::InvalidOperation(
                "Operation ID does not match computed BLAKE3 digest".to_string(),
            ));
        }

        // 4. Idempotency check: drop if already applied
        if self.is_operation_applied_sync(&op.document_id, &op.op_id)? {
            info!(target: "crdt", op_id = %op.op_id, "crdt.operation.duplicate");
            return Ok(ConflictClassification::NoConflict);
        }

        // 5. Causal dependency check: ensure all parents are present locally
        for dep in &op.causal_dependencies {
            if !self.is_operation_applied_sync(&op.document_id, dep)? {
                warn!(target: "crdt", op_id = %op.op_id, missing = %dep, "crdt.dependency.missing");
                self.stage_unapplied_operation_sync(op, dep)?;
                return Err(DataError::MissingDependency(*dep));
            }
        }

        // 6. Execute mutation inside atomic SQLite transaction
        let classification = self.db.with_transaction(|tx| {
            // Check existing document
            let existing: Option<Document> = tx
                .query_row(
                    "SELECT document_id, document_type, schema_version, owner_node_id,
                            version, created_at, updated_at, is_deleted, payload_bytes
                     FROM documents WHERE document_id = ?1",
                    params![op.document_id.as_bytes().as_slice()],
                    |r| Self::row_to_document(r),
                )
                .ok();

            let doc_type = match &existing {
                Some(doc) => {
                    if doc.deleted && op.op_type != OperationType::Delete {
                        warn!(target: "crdt", op_id = %op.op_id, "crdt.operation.rejected");
                        return Err(DataError::DocumentDeleted(op.document_id));
                    }
                    doc.document_type.clone()
                }
                None => {
                    if op.op_type == OperationType::Create {
                        let detected = Self::detect_crdt_type(&op.payload)
                            .unwrap_or_else(|| "document".to_string());
                        let now = Timestamp::now();
                        tx.execute(
                            "INSERT INTO documents (
                                document_id, document_type, schema_version, owner_node_id,
                                version, created_at, updated_at, is_deleted, payload_bytes
                            ) VALUES (?1, ?2, ?3, ?4, 1, ?5, ?6, 0, ?7)",
                            params![
                                op.document_id.as_bytes().as_slice(),
                                detected,
                                op.schema_version,
                                op.actor_node_id.as_bytes().as_slice(),
                                now.as_millis() as i64,
                                now.as_millis() as i64,
                                op.payload,
                            ],
                        )?;
                        detected
                    } else {
                        warn!(target: "crdt", op_id = %op.op_id, "crdt.operation.rejected");
                        return Err(DataError::DocumentNotFound(op.document_id));
                    }
                }
            };

            // If updating an existing document
            if let Some(mut doc) = existing {
                match op.op_type {
                    OperationType::Create => {
                        // Concurrent create: merge states
                        let new_state = Self::reduce_crdt_delta_or_state(
                            &doc.document_type,
                            &doc.state,
                            &op.payload,
                        )?;
                        doc.state = new_state;
                        doc.version += 1;
                        doc.updated_at = Timestamp::now();
                        tx.execute(
                            "UPDATE documents SET payload_bytes = ?1, updated_at = ?2, version = ?3 WHERE document_id = ?4",
                            params![
                                doc.state,
                                doc.updated_at.as_millis() as i64,
                                doc.version as i64,
                                op.document_id.as_bytes().as_slice(),
                            ],
                        )?;
                    }
                    OperationType::Update => {
                        doc.state = op.payload.clone();
                        doc.version += 1;
                        doc.updated_at = Timestamp::now();
                        tx.execute(
                            "UPDATE documents SET payload_bytes = ?1, updated_at = ?2, version = ?3 WHERE document_id = ?4",
                            params![
                                doc.state,
                                doc.updated_at.as_millis() as i64,
                                doc.version as i64,
                                op.document_id.as_bytes().as_slice(),
                            ],
                        )?;
                    }
                    OperationType::Delete => {
                        doc.deleted = true;
                        doc.version += 1;
                        doc.updated_at = Timestamp::now();
                        tx.execute(
                            "UPDATE documents SET is_deleted = 1, updated_at = ?1, version = ?2 WHERE document_id = ?3",
                            params![
                                doc.updated_at.as_millis() as i64,
                                doc.version as i64,
                                op.document_id.as_bytes().as_slice(),
                            ],
                        )?;
                        let gc_after = Timestamp::from_millis(
                            Timestamp::now().as_millis() + 30 * 24 * 3600 * 1000,
                        );
                        tx.execute(
                            "INSERT INTO tombstones (entity_id, entity_type, deleted_at, hlc_physical, hlc_logical, deleted_by, gc_after)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                             ON CONFLICT(entity_id) DO NOTHING",
                            params![
                                op.document_id.as_bytes().as_slice(),
                                doc.document_type,
                                Timestamp::now().as_millis() as i64,
                                op.hlc.physical as i64,
                                op.hlc.logical as i64,
                                op.actor_node_id.as_bytes().as_slice(),
                                gc_after.as_millis() as i64,
                            ],
                        )?;
                    }
                    OperationType::CrdtDelta => {
                        let new_state = Self::reduce_crdt_delta_or_state(
                            &doc.document_type,
                            &doc.state,
                            &op.payload,
                        )?;
                        doc.state = new_state;
                        doc.version += 1;
                        doc.updated_at = Timestamp::now();
                        tx.execute(
                            "UPDATE documents SET payload_bytes = ?1, updated_at = ?2, version = ?3 WHERE document_id = ?4",
                            params![
                                doc.state,
                                doc.updated_at.as_millis() as i64,
                                doc.version as i64,
                                op.document_id.as_bytes().as_slice(),
                            ],
                        )?;
                    }
                }
            }

            // Persist operation in operations table
            self.insert_operation_in_tx(tx, op)?;

            // Record in applied_operations
            tx.execute(
                "INSERT INTO applied_operations (document_id, op_id, applied_at) VALUES (?1, ?2, ?3)",
                params![
                    op.document_id.as_bytes().as_slice(),
                    op.op_id.as_bytes().as_slice(),
                    Timestamp::now().as_millis() as i64,
                ],
            )?;

            // Classification based on causal state prior to applying this operation
            let prior_sv = self.load_state_vector_in_tx(tx, &op.document_id)?;

            let current_hlc: Option<(i64, i64)> = tx
                .query_row(
                    "SELECT latest_hlc_physical, latest_hlc_logical FROM replica_state WHERE document_id = ?1",
                    params![op.document_id.as_bytes().as_slice()],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .ok();

            let local_latest_hlc = match current_hlc {
                Some((p, l)) => HybridLogicalClock::new(p as u64, l as u32),
                None => HybridLogicalClock::new(0, 0),
            };

            let is_crdt = Self::is_crdt_type(&doc_type);
            let classification = ConflictResolver::classify_operation(
                &prior_sv,
                &op.actor_node_id,
                op.sequence,
                &local_latest_hlc,
                &op.hlc,
                is_crdt,
            );

            // Update replica_state with advanced sequence and maximum HLC
            let mut sv = prior_sv;
            sv.advance(op.actor_node_id, op.sequence);
            let sv_bytes = sv.to_bytes();

            let new_hlc = local_latest_hlc.max(op.hlc);

            tx.execute(
                "INSERT INTO replica_state (
                    document_id, state_vector_bytes, latest_hlc_physical,
                    latest_hlc_logical, local_sequence, updated_at
                ) VALUES (?1, ?2, ?3, ?4, 0, ?5)
                ON CONFLICT(document_id) DO UPDATE SET
                    state_vector_bytes = excluded.state_vector_bytes,
                    latest_hlc_physical = excluded.latest_hlc_physical,
                    latest_hlc_logical = excluded.latest_hlc_logical,
                    updated_at = excluded.updated_at",
                params![
                    op.document_id.as_bytes().as_slice(),
                    sv_bytes,
                    new_hlc.physical as i64,
                    new_hlc.logical as i64,
                    Timestamp::now().as_millis() as i64,
                ],
            )?;

            // Remove from unapplied_operations if it was previously buffered
            tx.execute(
                "DELETE FROM unapplied_operations WHERE op_id = ?1",
                params![op.op_id.as_bytes().as_slice()],
            )?;

            info!(
                target: "crdt",
                op_id = %op.op_id,
                classification = ?classification,
                "crdt.conflict.detected"
            );
            info!(target: "crdt", op_id = %op.op_id, "crdt.operation.applied");
            info!(target: "crdt", document_id = %op.document_id, "crdt.state.updated");

            Ok(classification)
        })?;

        // Advance local clock engine to absorb remote timestamp
        self.hlc_engine.update(op.hlc);

        Ok(classification)
    }

    /// Stages an operation that cannot yet be applied due to a missing causal dependency.
    pub fn stage_unapplied_operation_sync(
        &self,
        op: &Operation,
        missing_dep: &OperationId,
    ) -> Result<(), DataError> {
        let op_bytes =
            serde_json::to_vec(op).map_err(|e| DataError::SerializationFailure(e.to_string()))?;
        self.db.with_connection(|conn| {
            conn.execute(
                "INSERT INTO unapplied_operations (
                    op_id, document_id, actor_node_id, missing_dep_op_id, operation_bytes, received_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                ON CONFLICT(op_id) DO UPDATE SET missing_dep_op_id = excluded.missing_dep_op_id",
                params![
                    op.op_id.as_bytes().as_slice(),
                    op.document_id.as_bytes().as_slice(),
                    op.actor_node_id.as_bytes().as_slice(),
                    missing_dep.as_bytes().as_slice(),
                    op_bytes,
                    Timestamp::now().as_millis() as i64,
                ],
            )?;
            Ok(())
        })
    }

    /// Lists all unapplied operations staged for a document.
    pub fn list_unapplied_operations_sync(
        &self,
        document_id: &DocumentId,
    ) -> Result<Vec<Operation>, DataError> {
        self.db.with_connection(|conn| {
            let mut stmt = conn.prepare(
                "SELECT operation_bytes FROM unapplied_operations WHERE document_id = ?1 ORDER BY received_at ASC",
            )?;
            let mut rows = stmt.query(params![document_id.as_bytes().as_slice()])?;
            let mut ops = Vec::new();
            while let Some(row) = rows.next()? {
                let bytes: Vec<u8> = row.get(0)?;
                let op: Operation = serde_json::from_slice(&bytes)?;
                ops.push(op);
            }
            Ok(ops)
        })
    }

    /// Retries buffered operations that were waiting on dependencies.
    /// Returns list of OperationIds that successfully applied.
    pub fn retry_unapplied_operations_sync(
        &self,
        document_id: &DocumentId,
    ) -> Result<Vec<OperationId>, DataError> {
        let pending = self.list_unapplied_operations_sync(document_id)?;
        let mut applied = Vec::new();
        for op in pending {
            let mut ready = true;
            for dep in &op.causal_dependencies {
                if !self.is_operation_applied_sync(&op.document_id, dep)? {
                    ready = false;
                    break;
                }
            }
            if ready {
                if let Ok(_) = self.apply_remote_operation_sync(&op) {
                    applied.push(op.op_id);
                }
            }
        }
        Ok(applied)
    }

    /// Persists a causal deletion tombstone for a document or element.
    pub fn record_tombstone_sync(
        &self,
        entity_id: &DocumentId,
        entity_type: &str,
        hlc: HybridLogicalClock,
        deleted_by: NodeId,
        gc_after: Timestamp,
    ) -> Result<(), DataError> {
        self.db.with_connection(|conn| {
            conn.execute(
                "INSERT INTO tombstones (
                    entity_id, entity_type, deleted_at, hlc_physical, hlc_logical, deleted_by, gc_after
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                ON CONFLICT(entity_id) DO UPDATE SET
                    deleted_at = excluded.deleted_at,
                    hlc_physical = excluded.hlc_physical,
                    hlc_logical = excluded.hlc_logical,
                    deleted_by = excluded.deleted_by,
                    gc_after = excluded.gc_after",
                params![
                    entity_id.as_bytes().as_slice(),
                    entity_type,
                    Timestamp::now().as_millis() as i64,
                    hlc.physical as i64,
                    hlc.logical as i64,
                    deleted_by.as_bytes().as_slice(),
                    gc_after.as_millis() as i64,
                ],
            )?;
            Ok(())
        })
    }

    /// Checks if an entity is recorded in the tombstones table.
    pub fn is_tombstoned_sync(&self, entity_id: &DocumentId) -> Result<bool, DataError> {
        self.db.with_connection(|conn| {
            let count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM tombstones WHERE entity_id = ?1",
                params![entity_id.as_bytes().as_slice()],
                |r| r.get(0),
            )?;
            if count > 0 {
                return Ok(true);
            }
            let is_deleted: Option<i64> = conn
                .query_row(
                    "SELECT is_deleted FROM documents WHERE document_id = ?1",
                    params![entity_id.as_bytes().as_slice()],
                    |r| r.get(0),
                )
                .ok();
            Ok(is_deleted == Some(1))
        })
    }

    /// Records or updates peer synchronization watermark.
    pub fn record_peer_sync_watermark_sync(
        &self,
        peer_id: &NodeId,
        doc_id: &DocumentId,
        seq: u64,
        hlc: HybridLogicalClock,
        status: &str,
    ) -> Result<(), DataError> {
        let now = Timestamp::now().as_millis() as i64;
        self.db.with_connection(|conn| {
            conn.execute(
                "INSERT INTO peer_sync_state (
                    peer_node_id, document_id, last_synced_seq,
                    last_synced_hlc_p, last_synced_hlc_l, last_sync_time, sync_status
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                ON CONFLICT(peer_node_id, document_id) DO UPDATE SET
                    last_synced_seq = excluded.last_synced_seq,
                    last_synced_hlc_p = excluded.last_synced_hlc_p,
                    last_synced_hlc_l = excluded.last_synced_hlc_l,
                    last_sync_time = excluded.last_sync_time,
                    sync_status = excluded.sync_status",
                params![
                    peer_id.as_bytes().as_slice(),
                    doc_id.as_bytes().as_slice(),
                    seq as i64,
                    hlc.physical as i64,
                    hlc.logical as i64,
                    now,
                    status
                ],
            )?;
            Ok(())
        })
    }

    /// Fetches the peer sync watermark for a given peer and document.
    pub fn get_peer_sync_watermark_sync(
        &self,
        peer_id: &NodeId,
        doc_id: &DocumentId,
    ) -> Result<Option<PeerSyncRecord>, DataError> {
        self.db.with_connection(|conn| {
            let mut stmt = conn.prepare(
                "SELECT peer_node_id, document_id, last_synced_seq,
                        last_synced_hlc_p, last_synced_hlc_l, last_sync_time, sync_status
                 FROM peer_sync_state WHERE peer_node_id = ?1 AND document_id = ?2",
            )?;
            let mut rows = stmt.query(params![
                peer_id.as_bytes().as_slice(),
                doc_id.as_bytes().as_slice()
            ])?;
            if let Some(row) = rows.next()? {
                let peer_bytes: Vec<u8> = row.get(0)?;
                let doc_bytes: Vec<u8> = row.get(1)?;
                let seq: i64 = row.get(2)?;
                let hlc_p: i64 = row.get(3)?;
                let hlc_l: i64 = row.get(4)?;
                let sync_time: i64 = row.get(5)?;
                let status: String = row.get(6)?;

                let mut peer_arr = [0u8; 32];
                peer_arr.copy_from_slice(&peer_bytes);
                let mut doc_arr = [0u8; 32];
                doc_arr.copy_from_slice(&doc_bytes);

                Ok(Some(PeerSyncRecord {
                    peer_node_id: NodeId::from_bytes(peer_arr),
                    document_id: DocumentId::from_bytes(doc_arr),
                    last_synced_seq: seq as u64,
                    last_synced_hlc: HybridLogicalClock::new(hlc_p as u64, hlc_l as u32),
                    last_sync_time: Timestamp::from_millis(sync_time as u64),
                    sync_status: status,
                }))
            } else {
                Ok(None)
            }
        })
    }

    /// Lists all peer sync records for a peer.
    pub fn list_peer_sync_watermarks_sync(
        &self,
        peer_id: &NodeId,
    ) -> Result<Vec<PeerSyncRecord>, DataError> {
        self.db.with_connection(|conn| {
            let mut stmt = conn.prepare(
                "SELECT peer_node_id, document_id, last_synced_seq,
                        last_synced_hlc_p, last_synced_hlc_l, last_sync_time, sync_status
                 FROM peer_sync_state WHERE peer_node_id = ?1",
            )?;
            let mut rows = stmt.query(params![peer_id.as_bytes().as_slice()])?;
            let mut results = Vec::new();
            while let Some(row) = rows.next()? {
                let peer_bytes: Vec<u8> = row.get(0)?;
                let doc_bytes: Vec<u8> = row.get(1)?;
                let seq: i64 = row.get(2)?;
                let hlc_p: i64 = row.get(3)?;
                let hlc_l: i64 = row.get(4)?;
                let sync_time: i64 = row.get(5)?;
                let status: String = row.get(6)?;

                let mut peer_arr = [0u8; 32];
                peer_arr.copy_from_slice(&peer_bytes);
                let mut doc_arr = [0u8; 32];
                doc_arr.copy_from_slice(&doc_bytes);

                results.push(PeerSyncRecord {
                    peer_node_id: NodeId::from_bytes(peer_arr),
                    document_id: DocumentId::from_bytes(doc_arr),
                    last_synced_seq: seq as u64,
                    last_synced_hlc: HybridLogicalClock::new(hlc_p as u64, hlc_l as u32),
                    last_sync_time: Timestamp::from_millis(sync_time as u64),
                    sync_status: status,
                });
            }
            Ok(results)
        })
    }

    /// Lists all peer sync records for a specific document.
    pub fn list_peer_sync_watermarks_for_document_sync(
        &self,
        document_id: &DocumentId,
    ) -> Result<Vec<PeerSyncRecord>, DataError> {
        self.db.with_connection(|conn| {
            let mut stmt = conn.prepare(
                "SELECT peer_node_id, document_id, last_synced_seq,
                        last_synced_hlc_p, last_synced_hlc_l, last_sync_time, sync_status
                 FROM peer_sync_state WHERE document_id = ?1",
            )?;
            let mut rows = stmt.query(params![document_id.as_bytes().as_slice()])?;
            let mut results = Vec::new();
            while let Some(row) = rows.next()? {
                let peer_bytes: Vec<u8> = row.get(0)?;
                let doc_bytes: Vec<u8> = row.get(1)?;
                let seq: i64 = row.get(2)?;
                let hlc_p: i64 = row.get(3)?;
                let hlc_l: i64 = row.get(4)?;
                let sync_time: i64 = row.get(5)?;
                let status: String = row.get(6)?;

                let mut peer_arr = [0u8; 32];
                peer_arr.copy_from_slice(&peer_bytes);
                let mut doc_arr = [0u8; 32];
                doc_arr.copy_from_slice(&doc_bytes);

                results.push(PeerSyncRecord {
                    peer_node_id: NodeId::from_bytes(peer_arr),
                    document_id: DocumentId::from_bytes(doc_arr),
                    last_synced_seq: seq as u64,
                    last_synced_hlc: HybridLogicalClock::new(hlc_p as u64, hlc_l as u32),
                    last_sync_time: Timestamp::from_millis(sync_time as u64),
                    sync_status: status,
                });
            }
            Ok(results)
        })
    }

    /// Records a resumption sync cursor for a peer and document.
    pub fn record_sync_cursor_sync(
        &self,
        peer_id: &NodeId,
        doc_id: &DocumentId,
        seq: u64,
        hlc: HybridLogicalClock,
    ) -> Result<(), DataError> {
        self.db.with_connection(|conn| {
            conn.execute(
                "INSERT INTO sync_cursors (peer_node_id, document_id, cursor_seq, cursor_hlc_p, cursor_hlc_l)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(peer_node_id, document_id) DO UPDATE SET
                    cursor_seq = excluded.cursor_seq,
                    cursor_hlc_p = excluded.cursor_hlc_p,
                    cursor_hlc_l = excluded.cursor_hlc_l",
                params![
                    peer_id.as_bytes().as_slice(),
                    doc_id.as_bytes().as_slice(),
                    seq as i64,
                    hlc.physical as i64,
                    hlc.logical as i64,
                ],
            )?;
            Ok(())
        })
    }

    /// Fetches the resumption sync cursor for a peer and document.
    pub fn get_sync_cursor_sync(
        &self,
        peer_id: &NodeId,
        doc_id: &DocumentId,
    ) -> Result<Option<(u64, HybridLogicalClock)>, DataError> {
        self.db.with_connection(|conn| {
            let mut stmt = conn.prepare(
                "SELECT cursor_seq, cursor_hlc_p, cursor_hlc_l
                 FROM sync_cursors WHERE peer_node_id = ?1 AND document_id = ?2",
            )?;
            let mut rows = stmt.query(params![
                peer_id.as_bytes().as_slice(),
                doc_id.as_bytes().as_slice()
            ])?;
            if let Some(row) = rows.next()? {
                let seq: i64 = row.get(0)?;
                let hlc_p: i64 = row.get(1)?;
                let hlc_l: i64 = row.get(2)?;
                Ok(Some((
                    seq as u64,
                    HybridLogicalClock::new(hlc_p as u64, hlc_l as u32),
                )))
            } else {
                Ok(None)
            }
        })
    }

    /// Deletes the sync cursor once fully converged.
    pub fn delete_sync_cursor_sync(
        &self,
        peer_id: &NodeId,
        doc_id: &DocumentId,
    ) -> Result<(), DataError> {
        self.db.with_connection(|conn| {
            conn.execute(
                "DELETE FROM sync_cursors WHERE peer_node_id = ?1 AND document_id = ?2",
                params![peer_id.as_bytes().as_slice(), doc_id.as_bytes().as_slice()],
            )?;
            Ok(())
        })
    }

    // ========================================================================
    // SNAPSHOT, LOG COMPACTION & RECOVERY OPERATIONS (BUILD-05)
    // ========================================================================

    /// Creates an immutable point-in-time snapshot of the materialized document state and causality.
    pub fn create_snapshot_sync(
        &self,
        document_id: &DocumentId,
    ) -> Result<DocumentSnapshot, DataError> {
        let doc = self
            .get_sync(document_id)?
            .ok_or_else(|| DataError::DocumentNotFound(*document_id))?;

        let replica_state =
            self.get_causal_state_sync(document_id)?
                .unwrap_or_else(|| ReplicaState {
                    document_id: *document_id,
                    state_vector: StateVector::new(),
                    latest_hlc: HybridLogicalClock::now(),
                    local_sequence: 0,
                    updated_at: Timestamp::now(),
                });

        let highest_op_seq: u64 = self.db.with_connection(|conn| {
            let seq: Option<i64> = conn
                .query_row(
                    "SELECT MAX(sequence) FROM operations WHERE document_id = ?1",
                    params![document_id.as_bytes().as_slice()],
                    |r| r.get(0),
                )
                .ok()
                .flatten();
            Ok(seq.unwrap_or(replica_state.local_sequence as i64) as u64)
        })?;

        let compacted_up_to_seq = highest_op_seq.max(replica_state.local_sequence);

        let latest_snapshot_version: u64 = self.db.with_connection(|conn| {
            let ver: Option<i64> = conn
                .query_row(
                    "SELECT MAX(snapshot_version) FROM document_snapshots WHERE document_id = ?1",
                    params![document_id.as_bytes().as_slice()],
                    |r| r.get(0),
                )
                .ok()
                .flatten();
            Ok(ver.unwrap_or(0) as u64)
        })?;

        let snapshot_version = latest_snapshot_version + 1;
        let created_at = Timestamp::now();
        let schema_version = doc.schema_version;
        let crdt_type = doc.document_type.clone();
        let state = doc.state.clone();
        let state_vector = replica_state.state_vector.clone();
        let hlc = replica_state.latest_hlc;

        let state_hash = DocumentSnapshot::compute_state_hash(
            document_id,
            snapshot_version,
            compacted_up_to_seq,
            &state_vector,
            &hlc,
            &crdt_type,
            &state,
            schema_version,
        );

        let snapshot_id = SnapshotId::from_bytes(state_hash);

        let snapshot = DocumentSnapshot {
            snapshot_id,
            document_id: *document_id,
            snapshot_version,
            compacted_up_to_seq,
            state_vector: state_vector.clone(),
            hlc,
            crdt_type: crdt_type.clone(),
            state: state.clone(),
            state_hash,
            schema_version,
            created_at,
        };

        // Persist inside atomic SQLite transaction, ensuring previous snapshots remain intact
        self.db.with_transaction(|tx| {
            tx.execute(
                "INSERT INTO document_snapshots (
                    snapshot_id, document_id, snapshot_version, compacted_up_to_seq,
                    state_vector_bytes, hlc_physical, hlc_logical, crdt_type,
                    snapshot_payload, state_hash, schema_version, created_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    snapshot_id.as_bytes().as_slice(),
                    document_id.as_bytes().as_slice(),
                    snapshot_version as i64,
                    compacted_up_to_seq as i64,
                    state_vector.to_bytes(),
                    hlc.physical as i64,
                    hlc.logical as i64,
                    crdt_type,
                    state,
                    state_hash.as_slice(),
                    schema_version as i64,
                    created_at.as_millis() as i64,
                ],
            )?;

            // Prune snapshots older than retention threshold (default 2: current + fallback)
            let retain_count = crate::schema::DEFAULT_SNAPSHOT_RETENTION;
            tx.execute(
                "DELETE FROM document_snapshots WHERE document_id = ?1 AND snapshot_id NOT IN (
                    SELECT snapshot_id FROM document_snapshots WHERE document_id = ?1
                    ORDER BY snapshot_version DESC LIMIT ?2
                )",
                params![document_id.as_bytes().as_slice(), retain_count as i64],
            )?;

            Ok(())
        })?;

        info!(
            target: "snapshot",
            document_id = %document_id,
            snapshot_id = %snapshot_id,
            version = snapshot_version,
            seq = compacted_up_to_seq,
            "snapshot.created"
        );

        Ok(snapshot)
    }

    /// Fetches the newest valid snapshot for a document, validating its integrity.
    pub fn get_latest_snapshot_sync(
        &self,
        document_id: &DocumentId,
    ) -> Result<Option<DocumentSnapshot>, DataError> {
        let snap_res = self.db.with_connection(|conn| {
            let mut stmt = conn.prepare(
                "SELECT snapshot_id, document_id, snapshot_version, compacted_up_to_seq,
                        state_vector_bytes, hlc_physical, hlc_logical, crdt_type,
                        snapshot_payload, state_hash, schema_version, created_at
                 FROM document_snapshots
                 WHERE document_id = ?1
                 ORDER BY snapshot_version DESC
                 LIMIT 1",
            )?;

            let mut rows = stmt.query(params![document_id.as_bytes().as_slice()])?;
            if let Some(row) = rows.next()? {
                Ok(Some(Self::row_to_document_snapshot(row)?))
            } else {
                Ok(None)
            }
        })?;

        if let Some(ref snapshot) = snap_res {
            if let Err(e) = snapshot.validate_integrity() {
                warn!(
                    target: "snapshot",
                    snapshot_id = %snapshot.snapshot_id,
                    document_id = %snapshot.document_id,
                    error = %e,
                    "snapshot.corrupted"
                );
                return Err(e);
            }
            info!(
                target: "snapshot",
                snapshot_id = %snapshot.snapshot_id,
                document_id = %snapshot.document_id,
                version = snapshot.snapshot_version,
                "snapshot.validated"
            );
        }

        Ok(snap_res)
    }

    /// Fetches a specific snapshot by its unique SnapshotId, validating its integrity.
    pub fn get_snapshot_sync(
        &self,
        snapshot_id: &SnapshotId,
    ) -> Result<Option<DocumentSnapshot>, DataError> {
        let snap_res = self.db.with_connection(|conn| {
            let mut stmt = conn.prepare(
                "SELECT snapshot_id, document_id, snapshot_version, compacted_up_to_seq,
                        state_vector_bytes, hlc_physical, hlc_logical, crdt_type,
                        snapshot_payload, state_hash, schema_version, created_at
                 FROM document_snapshots
                 WHERE snapshot_id = ?1",
            )?;

            let mut rows = stmt.query(params![snapshot_id.as_bytes().as_slice()])?;
            if let Some(row) = rows.next()? {
                Ok(Some(Self::row_to_document_snapshot(row)?))
            } else {
                Ok(None)
            }
        })?;

        if let Some(ref snapshot) = snap_res {
            if let Err(e) = snapshot.validate_integrity() {
                warn!(
                    target: "snapshot",
                    snapshot_id = %snapshot.snapshot_id,
                    document_id = %snapshot.document_id,
                    error = %e,
                    "snapshot.corrupted"
                );
                return Err(e);
            }
        }

        Ok(snap_res)
    }

    /// Lists all snapshots for a document in descending generation order.
    pub fn list_snapshots_sync(
        &self,
        document_id: &DocumentId,
    ) -> Result<Vec<DocumentSnapshot>, DataError> {
        self.db.with_connection(|conn| {
            let mut stmt = conn.prepare(
                "SELECT snapshot_id, document_id, snapshot_version, compacted_up_to_seq,
                        state_vector_bytes, hlc_physical, hlc_logical, crdt_type,
                        snapshot_payload, state_hash, schema_version, created_at
                 FROM document_snapshots
                 WHERE document_id = ?1
                 ORDER BY snapshot_version DESC",
            )?;

            let mut rows = stmt.query(params![document_id.as_bytes().as_slice()])?;
            let mut snapshots = Vec::new();
            while let Some(row) = rows.next()? {
                snapshots.push(Self::row_to_document_snapshot(row)?);
            }
            Ok(snapshots)
        })
    }

    /// Restores materialized document and causal state from a snapshot, replaying newer operations.
    pub fn restore_snapshot_sync(&self, snapshot_id: &SnapshotId) -> Result<Document, DataError> {
        let snapshot = self
            .get_snapshot_sync(snapshot_id)?
            .ok_or_else(|| DataError::SnapshotNotFound(snapshot_id.to_string()))?;

        snapshot.validate_integrity()?;

        let doc_id = snapshot.document_id;
        let mut restored_doc = self.db.with_transaction(|tx| {
            let now = Timestamp::now();

            // 1. Materialize snapshot document state
            tx.execute(
                "INSERT INTO documents (
                    document_id, document_type, schema_version, owner_node_id,
                    version, created_at, updated_at, is_deleted, payload_bytes
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0, ?8)
                ON CONFLICT(document_id) DO UPDATE SET
                    document_type = excluded.document_type,
                    schema_version = excluded.schema_version,
                    version = excluded.version,
                    updated_at = excluded.updated_at,
                    is_deleted = 0,
                    payload_bytes = excluded.payload_bytes",
                params![
                    doc_id.as_bytes().as_slice(),
                    snapshot.crdt_type,
                    snapshot.schema_version as i64,
                    self.owner_node_id.as_bytes().as_slice(),
                    snapshot.compacted_up_to_seq.max(1) as i64,
                    snapshot.created_at.as_millis() as i64,
                    now.as_millis() as i64,
                    snapshot.state,
                ],
            )?;

            // 2. Restore replica_state
            tx.execute(
                "INSERT INTO replica_state (
                    document_id, state_vector_bytes, latest_hlc_physical,
                    latest_hlc_logical, local_sequence, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                ON CONFLICT(document_id) DO UPDATE SET
                    state_vector_bytes = excluded.state_vector_bytes,
                    latest_hlc_physical = excluded.latest_hlc_physical,
                    latest_hlc_logical = excluded.latest_hlc_logical,
                    local_sequence = excluded.local_sequence,
                    updated_at = excluded.updated_at",
                params![
                    doc_id.as_bytes().as_slice(),
                    snapshot.state_vector.to_bytes(),
                    snapshot.hlc.physical as i64,
                    snapshot.hlc.logical as i64,
                    snapshot.compacted_up_to_seq as i64,
                    now.as_millis() as i64,
                ],
            )?;

            // Fetch restored document
            let mut stmt = tx.prepare(
                "SELECT document_id, document_type, schema_version, owner_node_id,
                        version, created_at, updated_at, is_deleted, payload_bytes
                 FROM documents WHERE document_id = ?1",
            )?;
            let mut rows = stmt.query(params![doc_id.as_bytes().as_slice()])?;
            let row = rows.next()?.ok_or(DataError::DocumentNotFound(doc_id))?;
            Ok(Self::row_to_document(row)?)
        })?;

        // 3. Replay any operations newer than the snapshot
        let newer_ops = self.db.with_connection(|conn| {
            let mut stmt = conn.prepare(
                "SELECT op_id, document_id, actor_node_id, sequence, op_type, delta_bytes,
                        hlc_physical, hlc_logical, dependencies_bytes, signature, created_at, status
                 FROM operations
                 WHERE document_id = ?1 AND sequence > ?2
                 ORDER BY hlc_physical ASC, hlc_logical ASC, sequence ASC",
            )?;

            let mut rows = stmt.query(params![
                doc_id.as_bytes().as_slice(),
                snapshot.compacted_up_to_seq as i64
            ])?;

            let mut ops = Vec::new();
            while let Some(row) = rows.next()? {
                ops.push(Self::row_to_operation(row)?);
            }
            Ok(ops)
        })?;

        for op in newer_ops {
            let current_bytes = restored_doc.state.clone();
            let new_bytes = match op.op_type {
                OperationType::Create | OperationType::Update | OperationType::CrdtDelta => {
                    if Self::is_crdt_type(&restored_doc.document_type) {
                        Self::reduce_crdt_delta_or_state(
                            &restored_doc.document_type,
                            &current_bytes,
                            &op.payload,
                        )?
                    } else {
                        op.payload.clone()
                    }
                }
                OperationType::Delete => {
                    restored_doc.deleted = true;
                    vec![]
                }
            };
            restored_doc.state = new_bytes;
            restored_doc.version += 1;
            restored_doc.updated_at = Timestamp::now();

            let final_state = restored_doc.state.clone();
            let final_version = restored_doc.version;
            let final_deleted = restored_doc.deleted;
            let final_now = restored_doc.updated_at;

            self.db.with_transaction(|tx| {
                tx.execute(
                    "UPDATE documents SET payload_bytes = ?1, version = ?2, is_deleted = ?3, updated_at = ?4 WHERE document_id = ?5",
                    params![
                        final_state,
                        final_version as i64,
                        if final_deleted { 1 } else { 0 },
                        final_now.as_millis() as i64,
                        doc_id.as_bytes().as_slice(),
                    ],
                )?;

                let mut sv = self.load_state_vector_in_tx(tx, &doc_id)?;
                sv.advance(op.actor_node_id, op.sequence);
                tx.execute(
                    "UPDATE replica_state SET state_vector_bytes = ?1, latest_hlc_physical = ?2, latest_hlc_logical = ?3 WHERE document_id = ?4",
                    params![
                        sv.to_bytes(),
                        op.hlc.physical as i64,
                        op.hlc.logical as i64,
                        doc_id.as_bytes().as_slice(),
                    ],
                )?;
                Ok(())
            })?;
        }

        info!(
            target: "snapshot",
            document_id = %doc_id,
            snapshot_id = %snapshot_id,
            "snapshot.restored"
        );

        Ok(restored_doc)
    }

    /// Evaluates whether log compaction is causally and structurally safe for a document.
    pub fn check_compaction_safety_sync(
        &self,
        document_id: &DocumentId,
    ) -> Result<CompactionSafetyReport, DataError> {
        let mut reasons_unsafe = Vec::new();

        // 1. Verify that a valid snapshot exists
        let latest_snapshot = match self.get_latest_snapshot_sync(document_id) {
            Ok(Some(s)) => Some(s),
            Ok(None) => {
                reasons_unsafe.push("No snapshot exists for document".to_string());
                None
            }
            Err(e) => {
                reasons_unsafe.push(format!("Latest snapshot is corrupted: {}", e));
                None
            }
        };

        let latest_snapshot_seq = latest_snapshot
            .as_ref()
            .map(|s| s.compacted_up_to_seq)
            .unwrap_or(0);

        // 2. Query peer causal stability
        let now = Timestamp::now().as_millis();
        let grace_period_ms =
            (crate::schema::DEFAULT_PEER_GRACE_PERIOD_DAYS * 24 * 3600 * 1000) as u64;

        let peers = self.list_peer_sync_watermarks_for_document_sync(document_id)?;
        let mut min_peer_synced_seq: Option<u64> = None;

        for peer in peers {
            let elapsed = now.saturating_sub(peer.last_sync_time.as_millis());
            let is_active = elapsed < grace_period_ms;

            if is_active {
                if let Some(cur_min) = min_peer_synced_seq {
                    min_peer_synced_seq = Some(cur_min.min(peer.last_synced_seq));
                } else {
                    min_peer_synced_seq = Some(peer.last_synced_seq);
                }

                if peer.sync_status != "CONVERGED" && peer.sync_status != "DUPLICATE" {
                    reasons_unsafe.push(format!(
                        "Peer {} has incomplete sync status: {}",
                        peer.peer_node_id, peer.sync_status
                    ));
                }
            }
        }

        // 3. Determine max safe sequence
        let max_safe_sequence = match min_peer_synced_seq {
            Some(peer_seq) => {
                let ceiling = latest_snapshot_seq.min(peer_seq);
                if peer_seq == 0 {
                    reasons_unsafe.push(
                        "Active peer has not synchronized any operations (sequence 0)".to_string(),
                    );
                }
                ceiling
            }
            None => latest_snapshot_seq,
        };

        // 4. Count eligible operations in storage
        let eligible_count: usize = self.db.with_connection(|conn| {
            let count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM operations WHERE document_id = ?1 AND sequence <= ?2",
                params![document_id.as_bytes().as_slice(), max_safe_sequence as i64],
                |r| r.get(0),
            )?;
            Ok(count as usize)
        })?;

        if eligible_count == 0 && reasons_unsafe.is_empty() {
            reasons_unsafe
                .push("No eligible operations to compact (already compacted)".to_string());
        }

        let is_safe = reasons_unsafe.is_empty() && eligible_count > 0 && max_safe_sequence > 0;

        Ok(CompactionSafetyReport {
            is_safe,
            max_safe_sequence,
            eligible_operations_count: eligible_count,
            latest_snapshot_seq,
            min_peer_synced_seq,
            reasons_unsafe,
        })
    }

    /// Safely compacts eligible historical operations up to the causally stable sequence boundary.
    pub fn compact_sync(&self, document_id: &DocumentId) -> Result<CompactionRecord, DataError> {
        let report = self.check_compaction_safety_sync(document_id)?;

        if !report.is_safe {
            info!(
                target: "compaction",
                document_id = %document_id,
                reasons = ?report.reasons_unsafe,
                "compaction.skipped"
            );
            return Err(DataError::CompactionSafetyViolation(
                report.reasons_unsafe.join("; "),
            ));
        }

        info!(
            target: "compaction",
            document_id = %document_id,
            max_safe_seq = report.max_safe_sequence,
            eligible_count = report.eligible_operations_count,
            "compaction.started"
        );

        let latest_snapshot = self
            .get_latest_snapshot_sync(document_id)?
            .ok_or_else(|| DataError::CompactionSafetyViolation("Snapshot missing".into()))?;

        let doc_before = self
            .get_sync(document_id)?
            .ok_or_else(|| DataError::DocumentNotFound(*document_id))?;

        let (bytes_reclaimed, ops_removed) = self.db.with_transaction(|tx| {
            // Compute bytes reclaimed
            let mut stmt = tx.prepare(
                "SELECT delta_bytes, dependencies_bytes FROM operations
                 WHERE document_id = ?1 AND sequence <= ?2",
            )?;
            let mut rows = stmt.query(params![
                document_id.as_bytes().as_slice(),
                report.max_safe_sequence as i64
            ])?;

            let mut total_bytes = 0usize;
            let mut count = 0usize;
            while let Some(r) = rows.next()? {
                let d_bytes: Vec<u8> = r.get(0)?;
                let dep_bytes: Vec<u8> = r.get(1)?;
                total_bytes += d_bytes.len() + dep_bytes.len() + 128; // row overhead
                count += 1;
            }

            // Prune operations
            tx.execute(
                "DELETE FROM operations WHERE document_id = ?1 AND sequence <= ?2",
                params![
                    document_id.as_bytes().as_slice(),
                    report.max_safe_sequence as i64
                ],
            )?;

            // Insert compaction record
            let mut comp_id = [0u8; 16];
            rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut comp_id);
            let now = Timestamp::now();

            tx.execute(
                "INSERT INTO compaction_records (
                    compaction_id, document_id, snapshot_id, compacted_up_to_seq,
                    ops_removed, bytes_reclaimed, compacted_at, reason
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    comp_id.as_slice(),
                    document_id.as_bytes().as_slice(),
                    latest_snapshot.snapshot_id.as_bytes().as_slice(),
                    report.max_safe_sequence as i64,
                    count as i64,
                    total_bytes as i64,
                    now.as_millis() as i64,
                    "Automated causal compaction",
                ],
            )?;

            Ok((total_bytes, count))
        })?;

        // Invariant verification: Compaction must NEVER change logical document state!
        let doc_after = self
            .get_sync(document_id)?
            .ok_or_else(|| DataError::DocumentNotFound(*document_id))?;

        if doc_before.state != doc_after.state || doc_before.version != doc_after.version {
            warn!(
                target: "compaction",
                document_id = %document_id,
                "compaction.failed (state mutation detected)"
            );
            return Err(DataError::CorruptedData(
                "Compaction invariant violated: state changed during compaction".into(),
            ));
        }

        info!(
            target: "compaction",
            document_id = %document_id,
            ops_removed = ops_removed,
            bytes_reclaimed = bytes_reclaimed,
            "compaction.completed"
        );

        let mut comp_id = [0u8; 16];
        rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut comp_id);

        Ok(CompactionRecord {
            compaction_id: comp_id,
            document_id: *document_id,
            snapshot_id: latest_snapshot.snapshot_id,
            compacted_up_to_seq: report.max_safe_sequence,
            ops_removed,
            bytes_reclaimed,
            compacted_at: Timestamp::now(),
            reason: Some("Causal stability compaction".to_string()),
        })
    }

    /// Recovers a document after corruption by attempting snapshot fallback and forward replay.
    pub fn recover_document_sync(&self, document_id: &DocumentId) -> Result<Document, DataError> {
        info!(target: "recovery", document_id = %document_id, "recovery.started");

        let snapshots = self.list_snapshots_sync(document_id)?;

        // Try snapshots in descending generation order
        for snapshot in snapshots {
            if let Ok(()) = snapshot.validate_integrity() {
                info!(
                    target: "recovery",
                    document_id = %document_id,
                    snapshot_id = %snapshot.snapshot_id,
                    version = snapshot.snapshot_version,
                    "snapshot.validated"
                );

                match self.restore_snapshot_sync(&snapshot.snapshot_id) {
                    Ok(doc) => {
                        info!(
                            target: "recovery",
                            document_id = %document_id,
                            snapshot_id = %snapshot.snapshot_id,
                            "recovery.completed"
                        );
                        return Ok(doc);
                    }
                    Err(e) => {
                        warn!(
                            target: "recovery",
                            document_id = %document_id,
                            snapshot_id = %snapshot.snapshot_id,
                            error = %e,
                            "Recovery step failed, trying previous snapshot"
                        );
                    }
                }
            } else {
                warn!(
                    target: "recovery",
                    document_id = %document_id,
                    snapshot_id = %snapshot.snapshot_id,
                    version = snapshot.snapshot_version,
                    "snapshot.corrupted (skipping corrupted snapshot generation)"
                );
            }
        }

        // Fallback: If no valid snapshot could be restored, check if full uncompacted operation history exists
        let ops = self.list_operations_for_document_sync(document_id, None, None)?;
        if !ops.is_empty() && ops[0].sequence == 1 {
            info!(target: "recovery", document_id = %document_id, "Replaying full uncompacted operation history");
            let mut state = vec![];
            let mut ver = 0u64;
            let mut deleted = false;
            let doc_type = self
                .get_sync(document_id)?
                .map(|d| d.document_type)
                .unwrap_or_else(|| "document".to_string());

            for op in &ops {
                match op.op_type {
                    OperationType::Create | OperationType::Update | OperationType::CrdtDelta => {
                        if Self::is_crdt_type(&doc_type) {
                            state =
                                Self::reduce_crdt_delta_or_state(&doc_type, &state, &op.payload)?;
                        } else {
                            state = op.payload.clone();
                        }
                        deleted = false;
                    }
                    OperationType::Delete => {
                        deleted = true;
                        state = vec![];
                    }
                }
                ver += 1;
            }

            let now = Timestamp::now();
            self.db.with_transaction(|tx| {
                tx.execute(
                    "INSERT INTO documents (
                        document_id, document_type, schema_version, owner_node_id,
                        version, created_at, updated_at, is_deleted, payload_bytes
                    ) VALUES (?1, ?2, 1, ?3, ?4, ?5, ?6, ?7, ?8)
                    ON CONFLICT(document_id) DO UPDATE SET
                        version = excluded.version,
                        updated_at = excluded.updated_at,
                        is_deleted = excluded.is_deleted,
                        payload_bytes = excluded.payload_bytes",
                    params![
                        document_id.as_bytes().as_slice(),
                        doc_type,
                        self.owner_node_id.as_bytes().as_slice(),
                        ver as i64,
                        now.as_millis() as i64,
                        now.as_millis() as i64,
                        if deleted { 1 } else { 0 },
                        state,
                    ],
                )?;
                Ok(())
            })?;

            let recovered = self.get_sync(document_id)?.unwrap();
            info!(target: "recovery", document_id = %document_id, "recovery.completed");
            return Ok(recovered);
        }

        warn!(target: "recovery", document_id = %document_id, "recovery.failed");
        Err(DataError::RecoveryFailed(
            *document_id,
            "No valid snapshot and incomplete operation history available for recovery".to_string(),
        ))
    }

    /// Prunes snapshots for a document beyond the specified retention count.
    pub fn prune_snapshots_sync(
        &self,
        document_id: &DocumentId,
        retain_count: usize,
    ) -> Result<usize, DataError> {
        self.db.with_connection(|conn| {
            let pruned = conn.execute(
                "DELETE FROM document_snapshots WHERE document_id = ?1 AND snapshot_id NOT IN (
                    SELECT snapshot_id FROM document_snapshots WHERE document_id = ?1
                    ORDER BY snapshot_version DESC LIMIT ?2
                )",
                params![document_id.as_bytes().as_slice(), retain_count as i64],
            )?;
            Ok(pruned)
        })
    }

    /// Corrupts a snapshot payload in storage for integrity and recovery testing.
    #[doc(hidden)]
    pub fn corrupt_snapshot_payload_for_testing(
        &self,
        snapshot_id: &SnapshotId,
    ) -> Result<(), DataError> {
        self.db.with_connection(|conn| {
            conn.execute(
                "UPDATE document_snapshots SET snapshot_payload = X'DEADBEEFCAFE' WHERE snapshot_id = ?1",
                params![snapshot_id.as_bytes().as_slice()],
            )?;
            Ok(())
        })
    }

    /// Synchronously applies a remote operation, verifying its signature against the author's public key.
    pub fn apply_remote_operation_with_pubkey_sync(
        &self,
        op: &Operation,
        pubkey: &[u8; 32],
    ) -> Result<ConflictClassification, DataError> {
        if op.signature != [0u8; 64] {
            if let Err(e) = op.verify_signature_with_pubkey(pubkey) {
                warn!(target: "crdt", op_id = %op.op_id, reason = %e, "crdt.operation.rejected");
                return Err(e);
            }
        }
        self.apply_remote_operation_sync(op)
    }

    // ========================================================================
    // OPERATION RETRIEVAL & QUERY APIS
    // ========================================================================

    /// Retrieves an operation by its unique OperationId.
    pub fn get_operation_sync(&self, op_id: &OperationId) -> Result<Option<Operation>, DataError> {
        self.db.with_connection(|conn| {
            let mut stmt = conn.prepare(
                "SELECT op_id, document_id, actor_node_id, sequence, op_type, delta_bytes,
                        hlc_physical, hlc_logical, dependencies_bytes, signature, created_at, status
                 FROM operations WHERE op_id = ?1",
            )?;

            let mut rows = stmt.query(params![op_id.as_bytes().as_slice()])?;
            if let Some(row) = rows.next()? {
                Ok(Some(Self::row_to_operation(row)?))
            } else {
                Ok(None)
            }
        })
    }

    /// Lists operations for a specific document in ascending causal sequence order.
    pub fn list_operations_for_document_sync(
        &self,
        document_id: &DocumentId,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<Vec<Operation>, DataError> {
        let limit = limit.unwrap_or(DEFAULT_QUERY_LIMIT).min(MAX_QUERY_LIMIT);
        let offset = offset.unwrap_or(0);

        self.db.with_connection(|conn| {
            let mut stmt = conn.prepare(
                "SELECT op_id, document_id, actor_node_id, sequence, op_type, delta_bytes,
                        hlc_physical, hlc_logical, dependencies_bytes, signature, created_at, status
                 FROM operations WHERE document_id = ?1
                 ORDER BY hlc_physical ASC, hlc_logical ASC, sequence ASC
                 LIMIT ?2 OFFSET ?3",
            )?;

            let mut rows = stmt.query(params![
                document_id.as_bytes().as_slice(),
                limit as i64,
                offset as i64
            ])?;

            let mut ops = Vec::new();
            while let Some(row) = rows.next()? {
                ops.push(Self::row_to_operation(row)?);
            }
            Ok(ops)
        })
    }

    /// Lists operations that are pending synchronization to peers.
    pub fn list_pending_operations_sync(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<Operation>, DataError> {
        let limit = limit.unwrap_or(DEFAULT_QUERY_LIMIT).min(MAX_QUERY_LIMIT);

        self.db.with_connection(|conn| {
            let mut stmt = conn.prepare(
                "SELECT op_id, document_id, actor_node_id, sequence, op_type, delta_bytes,
                        hlc_physical, hlc_logical, dependencies_bytes, signature, created_at, status
                 FROM operations 
                 WHERE status IN (?1, ?2)
                 ORDER BY created_at ASC
                 LIMIT ?3",
            )?;

            let mut rows = stmt.query(params![
                OpStatus::Durable.as_u8() as i64,
                OpStatus::PendingSync.as_u8() as i64,
                limit as i64
            ])?;

            let mut ops = Vec::new();
            while let Some(row) = rows.next()? {
                ops.push(Self::row_to_operation(row)?);
            }
            Ok(ops)
        })
    }

    /// Lists operations for a document that are causally newer than a given StateVector.
    pub fn list_operations_since_sync(
        &self,
        document_id: &DocumentId,
        since: &StateVector,
        limit: Option<usize>,
    ) -> Result<Vec<Operation>, DataError> {
        let all_ops =
            self.list_operations_for_document_sync(document_id, Some(MAX_QUERY_LIMIT), None)?;
        let mut filtered: Vec<Operation> = all_ops
            .into_iter()
            .filter(|op| op.sequence > since.get(&op.actor_node_id))
            .collect();
        if let Some(l) = limit {
            filtered.truncate(l);
        }
        Ok(filtered)
    }

    /// Checks if an operation has already been applied locally.
    pub fn is_operation_applied_sync(
        &self,
        document_id: &DocumentId,
        op_id: &OperationId,
    ) -> Result<bool, DataError> {
        self.db.with_connection(|conn| {
            let count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM applied_operations WHERE document_id = ?1 AND op_id = ?2",
                params![
                    document_id.as_bytes().as_slice(),
                    op_id.as_bytes().as_slice()
                ],
                |r| r.get(0),
            )?;
            Ok(count > 0)
        })
    }

    /// Records an operation as applied. Returns `true` if newly recorded, `false` if already present.
    pub fn record_applied_operation_sync(
        &self,
        document_id: &DocumentId,
        op_id: &OperationId,
    ) -> Result<bool, DataError> {
        self.db.with_transaction(|tx| {
            let existing: Option<i64> = tx
                .query_row(
                    "SELECT 1 FROM applied_operations WHERE document_id = ?1 AND op_id = ?2",
                    params![
                        document_id.as_bytes().as_slice(),
                        op_id.as_bytes().as_slice()
                    ],
                    |r| r.get(0),
                )
                .ok();

            if existing.is_some() {
                return Ok(false);
            }

            tx.execute(
                "INSERT INTO applied_operations (document_id, op_id, applied_at) VALUES (?1, ?2, ?3)",
                params![
                    document_id.as_bytes().as_slice(),
                    op_id.as_bytes().as_slice(),
                    Timestamp::now().as_millis() as i64,
                ],
            )?;

            Ok(true)
        })
    }

    /// Retrieves the current causal replica state for a document.
    pub fn get_causal_state_sync(
        &self,
        document_id: &DocumentId,
    ) -> Result<Option<ReplicaState>, DataError> {
        self.db.with_connection(|conn| {
            let mut stmt = conn.prepare(
                "SELECT document_id, state_vector_bytes, latest_hlc_physical,
                        latest_hlc_logical, local_sequence, updated_at
                 FROM replica_state WHERE document_id = ?1",
            )?;

            let mut rows = stmt.query(params![document_id.as_bytes().as_slice()])?;
            if let Some(row) = rows.next()? {
                Ok(Some(Self::row_to_replica_state(row)?))
            } else {
                Ok(None)
            }
        })
    }

    /// Returns the current monotonic sequence counter for the local node.
    pub fn get_local_sequence_sync(&self) -> Result<u64, DataError> {
        self.db.with_connection(|conn| {
            let seq: Option<i64> = conn
                .query_row(
                    "SELECT local_sequence FROM local_node_state WHERE node_id = ?1",
                    params![self.owner_node_id.as_bytes().as_slice()],
                    |r| r.get(0),
                )
                .ok();
            Ok(seq.unwrap_or(0) as u64)
        })
    }

    // ========================================================================
    // DOCUMENT READ & QUERY OPERATIONS
    // ========================================================================

    pub fn get_sync(&self, id: &DocumentId) -> Result<Option<Document>, DataError> {
        self.db.with_connection(|conn| {
            let mut stmt = conn.prepare(
                "SELECT document_id, document_type, schema_version, owner_node_id,
                        version, created_at, updated_at, is_deleted, payload_bytes
                 FROM documents WHERE document_id = ?1",
            )?;

            let mut rows = stmt.query(params![id.as_bytes().as_slice()])?;
            if let Some(row) = rows.next()? {
                Ok(Some(Self::row_to_document(row)?))
            } else {
                Ok(None)
            }
        })
    }

    pub fn get_active_sync(&self, id: &DocumentId) -> Result<Document, DataError> {
        match self.get_sync(id)? {
            Some(doc) => {
                if doc.deleted {
                    Err(DataError::DocumentDeleted(*id))
                } else {
                    Ok(doc)
                }
            }
            None => Err(DataError::DocumentNotFound(*id)),
        }
    }

    pub fn list_sync(&self, query: DocumentQuery) -> Result<Vec<Document>, DataError> {
        let limit = query
            .limit
            .unwrap_or(DEFAULT_QUERY_LIMIT)
            .min(MAX_QUERY_LIMIT);
        let offset = query.offset.unwrap_or(0);

        self.db.with_connection(|conn| {
            let mut sql = String::from(
                "SELECT document_id, document_type, schema_version, owner_node_id,
                        version, created_at, updated_at, is_deleted, payload_bytes
                 FROM documents WHERE 1=1",
            );

            if !query.include_deleted {
                sql.push_str(" AND is_deleted = 0");
            }

            if query.document_type.is_some() {
                sql.push_str(" AND document_type = ?1");
            }

            sql.push_str(" ORDER BY updated_at DESC LIMIT ?2 OFFSET ?3");

            let mut stmt = conn.prepare(&sql)?;
            let mut rows = if let Some(ref doc_type) = query.document_type {
                stmt.query(params![doc_type, limit as i64, offset as i64])?
            } else {
                let sql_no_type = format!(
                    "SELECT document_id, document_type, schema_version, owner_node_id,
                            version, created_at, updated_at, is_deleted, payload_bytes
                     FROM documents WHERE {} ORDER BY updated_at DESC LIMIT ?1 OFFSET ?2",
                    if query.include_deleted {
                        "1=1"
                    } else {
                        "is_deleted = 0"
                    }
                );
                let mut stmt_no_type = conn.prepare(&sql_no_type)?;
                let doc_rows = stmt_no_type
                    .query_map(params![limit as i64, offset as i64], Self::row_to_document)?;
                let mut docs = Vec::new();
                for r in doc_rows {
                    docs.push(r?);
                }
                return Ok(docs);
            };

            let mut docs = Vec::new();
            while let Some(row) = rows.next()? {
                docs.push(Self::row_to_document(row)?);
            }

            Ok(docs)
        })
    }

    pub fn count_sync(
        &self,
        document_type: Option<&str>,
        include_deleted: bool,
    ) -> Result<usize, DataError> {
        self.db.with_connection(|conn| {
            let count: i64 = match (document_type, include_deleted) {
                (Some(dt), true) => conn.query_row(
                    "SELECT COUNT(*) FROM documents WHERE document_type = ?1",
                    params![dt],
                    |r| r.get(0),
                )?,
                (Some(dt), false) => conn.query_row(
                    "SELECT COUNT(*) FROM documents WHERE document_type = ?1 AND is_deleted = 0",
                    params![dt],
                    |r| r.get(0),
                )?,
                (None, true) => {
                    conn.query_row("SELECT COUNT(*) FROM documents", [], |r| r.get(0))?
                }
                (None, false) => conn.query_row(
                    "SELECT COUNT(*) FROM documents WHERE is_deleted = 0",
                    [],
                    |r| r.get(0),
                )?,
            };
            Ok(count as usize)
        })
    }

    // ========================================================================
    // ASYNCHRONOUS CRUD OPERATIONS (Spawns blocking tasks on Tokio pool)
    // ========================================================================

    pub async fn create(
        &self,
        document_type: impl Into<String> + Send + 'static,
        state: Vec<u8>,
    ) -> Result<Document, DataError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.create_sync(document_type, state))
            .await
            .map_err(|e| DataError::StorageFailure(format!("Task spawn error: {}", e)))?
    }

    pub async fn create_with_op(
        &self,
        document_type: impl Into<String> + Send + 'static,
        state: Vec<u8>,
    ) -> Result<(Document, Operation), DataError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.create_with_op_sync(document_type, state))
            .await
            .map_err(|e| DataError::StorageFailure(format!("Task spawn error: {}", e)))?
    }

    pub async fn get(&self, id: DocumentId) -> Result<Option<Document>, DataError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.get_sync(&id))
            .await
            .map_err(|e| DataError::StorageFailure(format!("Task spawn error: {}", e)))?
    }

    pub async fn get_active(&self, id: DocumentId) -> Result<Document, DataError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.get_active_sync(&id))
            .await
            .map_err(|e| DataError::StorageFailure(format!("Task spawn error: {}", e)))?
    }

    pub async fn update(&self, id: DocumentId, new_state: Vec<u8>) -> Result<Document, DataError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.update_sync(&id, new_state))
            .await
            .map_err(|e| DataError::StorageFailure(format!("Task spawn error: {}", e)))?
    }

    pub async fn update_with_op(
        &self,
        id: DocumentId,
        new_state: Vec<u8>,
    ) -> Result<(Document, Operation), DataError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.update_with_op_sync(&id, new_state))
            .await
            .map_err(|e| DataError::StorageFailure(format!("Task spawn error: {}", e)))?
    }

    pub async fn delete(&self, id: DocumentId) -> Result<Document, DataError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.delete_sync(&id))
            .await
            .map_err(|e| DataError::StorageFailure(format!("Task spawn error: {}", e)))?
    }

    pub async fn delete_with_op(&self, id: DocumentId) -> Result<(Document, Operation), DataError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.delete_with_op_sync(&id))
            .await
            .map_err(|e| DataError::StorageFailure(format!("Task spawn error: {}", e)))?
    }

    pub async fn list(&self, query: DocumentQuery) -> Result<Vec<Document>, DataError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.list_sync(query))
            .await
            .map_err(|e| DataError::StorageFailure(format!("Task spawn error: {}", e)))?
    }

    pub async fn count(
        &self,
        document_type: Option<String>,
        include_deleted: bool,
    ) -> Result<usize, DataError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.count_sync(document_type.as_deref(), include_deleted)
        })
        .await
        .map_err(|e| DataError::StorageFailure(format!("Task spawn error: {}", e)))?
    }

    pub async fn get_operation(&self, op_id: OperationId) -> Result<Option<Operation>, DataError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.get_operation_sync(&op_id))
            .await
            .map_err(|e| DataError::StorageFailure(format!("Task spawn error: {}", e)))?
    }

    pub async fn list_operations_for_document(
        &self,
        document_id: DocumentId,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<Vec<Operation>, DataError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.list_operations_for_document_sync(&document_id, limit, offset)
        })
        .await
        .map_err(|e| DataError::StorageFailure(format!("Task spawn error: {}", e)))?
    }

    // ========================================================================
    // SNAPSHOT & COMPACTION ASYNC APIS
    // ========================================================================

    pub async fn create_snapshot(
        &self,
        document_id: DocumentId,
    ) -> Result<DocumentSnapshot, DataError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.create_snapshot_sync(&document_id))
            .await
            .map_err(|e| DataError::StorageFailure(format!("Task spawn error: {}", e)))?
    }

    pub async fn get_latest_snapshot(
        &self,
        document_id: DocumentId,
    ) -> Result<Option<DocumentSnapshot>, DataError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.get_latest_snapshot_sync(&document_id))
            .await
            .map_err(|e| DataError::StorageFailure(format!("Task spawn error: {}", e)))?
    }

    pub async fn get_snapshot(
        &self,
        snapshot_id: SnapshotId,
    ) -> Result<Option<DocumentSnapshot>, DataError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.get_snapshot_sync(&snapshot_id))
            .await
            .map_err(|e| DataError::StorageFailure(format!("Task spawn error: {}", e)))?
    }

    pub async fn list_snapshots(
        &self,
        document_id: DocumentId,
    ) -> Result<Vec<DocumentSnapshot>, DataError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.list_snapshots_sync(&document_id))
            .await
            .map_err(|e| DataError::StorageFailure(format!("Task spawn error: {}", e)))?
    }

    pub async fn restore_snapshot(&self, snapshot_id: SnapshotId) -> Result<Document, DataError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.restore_snapshot_sync(&snapshot_id))
            .await
            .map_err(|e| DataError::StorageFailure(format!("Task spawn error: {}", e)))?
    }

    pub async fn check_compaction_safety(
        &self,
        document_id: DocumentId,
    ) -> Result<CompactionSafetyReport, DataError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.check_compaction_safety_sync(&document_id))
            .await
            .map_err(|e| DataError::StorageFailure(format!("Task spawn error: {}", e)))?
    }

    pub async fn compact(&self, document_id: DocumentId) -> Result<CompactionRecord, DataError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.compact_sync(&document_id))
            .await
            .map_err(|e| DataError::StorageFailure(format!("Task spawn error: {}", e)))?
    }

    pub async fn recover_document(&self, document_id: DocumentId) -> Result<Document, DataError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.recover_document_sync(&document_id))
            .await
            .map_err(|e| DataError::StorageFailure(format!("Task spawn error: {}", e)))?
    }

    pub async fn prune_snapshots(
        &self,
        document_id: DocumentId,
        retain_count: usize,
    ) -> Result<usize, DataError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.prune_snapshots_sync(&document_id, retain_count))
            .await
            .map_err(|e| DataError::StorageFailure(format!("Task spawn error: {}", e)))?
    }

    // ========================================================================
    // INTERNAL TRANSACTION HELPERS
    // ========================================================================

    fn advance_node_sequence_in_tx(
        &self,
        tx: &mut rusqlite::Transaction,
    ) -> Result<(u64, HybridLogicalClock), DataError> {
        let current_seq: Option<i64> = tx
            .query_row(
                "SELECT local_sequence FROM local_node_state WHERE node_id = ?1",
                params![self.owner_node_id.as_bytes().as_slice()],
                |r| r.get(0),
            )
            .ok();

        let next_seq = (current_seq.unwrap_or(0) as u64) + 1;
        let hlc = self.hlc_engine.tick();
        let now = Timestamp::now();

        tx.execute(
            "INSERT INTO local_node_state (
                node_id, local_sequence, latest_hlc_physical, latest_hlc_logical, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(node_id) DO UPDATE SET
                local_sequence = excluded.local_sequence,
                latest_hlc_physical = excluded.latest_hlc_physical,
                latest_hlc_logical = excluded.latest_hlc_logical,
                updated_at = excluded.updated_at",
            params![
                self.owner_node_id.as_bytes().as_slice(),
                next_seq as i64,
                hlc.physical as i64,
                hlc.logical as i64,
                now.as_millis() as i64,
            ],
        )?;

        Ok((next_seq, hlc))
    }

    fn construct_operation(
        &self,
        document_id: DocumentId,
        sequence: u64,
        op_type: OperationType,
        payload: Vec<u8>,
        hlc: HybridLogicalClock,
        causal_dependencies: Vec<OperationId>,
    ) -> Operation {
        if let Some(ref identity) = self.identity {
            Operation::new_local(
                document_id,
                identity,
                sequence,
                op_type,
                payload,
                hlc,
                causal_dependencies,
            )
        } else {
            let schema_version = crate::schema::CURRENT_SCHEMA_VERSION;
            let op_id = Operation::compute_id(
                &document_id,
                &self.owner_node_id,
                sequence,
                op_type,
                &payload,
                &hlc,
                &causal_dependencies,
                schema_version,
            );

            Operation {
                op_id,
                document_id,
                actor_node_id: self.owner_node_id,
                sequence,
                op_type,
                payload,
                hlc,
                causal_dependencies,
                signature: [0u8; 64],
                created_at: Timestamp::now(),
                schema_version,
                status: OpStatus::Durable,
            }
        }
    }

    fn insert_operation_in_tx(
        &self,
        tx: &mut rusqlite::Transaction,
        op: &Operation,
    ) -> Result<(), DataError> {
        let deps_bytes = Self::encode_dependencies(&op.causal_dependencies);

        tx.execute(
            "INSERT INTO operations (
                op_id, document_id, actor_node_id, sequence, op_type, delta_bytes,
                hlc_physical, hlc_logical, dependencies_bytes, signature, created_at, status
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                op.op_id.as_bytes().as_slice(),
                op.document_id.as_bytes().as_slice(),
                op.actor_node_id.as_bytes().as_slice(),
                op.sequence as i64,
                op.op_type.as_u8() as i64,
                op.payload,
                op.hlc.physical as i64,
                op.hlc.logical as i64,
                deps_bytes,
                op.signature.as_slice(),
                op.created_at.as_millis() as i64,
                op.status.as_u8() as i64,
            ],
        )?;

        Ok(())
    }

    fn load_state_vector_in_tx(
        &self,
        tx: &rusqlite::Transaction,
        document_id: &DocumentId,
    ) -> Result<StateVector, DataError> {
        let res: Option<Vec<u8>> = tx
            .query_row(
                "SELECT state_vector_bytes FROM replica_state WHERE document_id = ?1",
                params![document_id.as_bytes().as_slice()],
                |r| r.get(0),
            )
            .ok();

        match res {
            Some(bytes) => StateVector::from_bytes(&bytes),
            None => Ok(StateVector::new()),
        }
    }

    fn load_persisted_hlc(
        db: &Database,
        node_id: &NodeId,
    ) -> Result<HybridLogicalClock, DataError> {
        db.with_connection(|conn| {
            let res: Option<(i64, i64)> = conn
                .query_row(
                    "SELECT latest_hlc_physical, latest_hlc_logical FROM local_node_state WHERE node_id = ?1",
                    params![node_id.as_bytes().as_slice()],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .ok();

            if let Some((p, l)) = res {
                Ok(HybridLogicalClock::new(p as u64, l as u32))
            } else {
                Ok(HybridLogicalClock::now())
            }
        })
    }

    fn encode_dependencies(deps: &[OperationId]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(4 + deps.len() * 32);
        bytes.extend_from_slice(&(deps.len() as u32).to_be_bytes());
        for dep in deps {
            bytes.extend_from_slice(dep.as_bytes());
        }
        bytes
    }

    fn decode_dependencies(bytes: &[u8]) -> Result<Vec<OperationId>, DataError> {
        if bytes.len() < 4 {
            return Ok(Vec::new());
        }
        let count = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        let expected_len = 4 + count * 32;
        if bytes.len() != expected_len {
            return Err(DataError::CorruptedData(format!(
                "Invalid dependencies_bytes length: expected {}, got {}",
                expected_len,
                bytes.len()
            )));
        }

        let mut deps = Vec::with_capacity(count);
        let mut offset = 4;
        for _ in 0..count {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bytes[offset..offset + 32]);
            deps.push(OperationId::from_bytes(arr));
            offset += 32;
        }
        Ok(deps)
    }

    fn row_to_document(row: &Row) -> Result<Document, rusqlite::Error> {
        let id_bytes: Vec<u8> = row.get(0)?;
        let doc_type: String = row.get(1)?;
        let schema_ver: u32 = row.get(2)?;
        let owner_bytes: Vec<u8> = row.get(3)?;
        let version: i64 = row.get(4)?;
        let created_at_ms: i64 = row.get(5)?;
        let updated_at_ms: i64 = row.get(6)?;
        let is_deleted: i64 = row.get(7)?;
        let payload: Vec<u8> = row.get(8)?;

        let mut id_arr = [0u8; 32];
        if id_bytes.len() == 32 {
            id_arr.copy_from_slice(&id_bytes);
        }

        let mut owner_arr = [0u8; 32];
        if owner_bytes.len() == 32 {
            owner_arr.copy_from_slice(&owner_bytes);
        }

        Ok(Document {
            document_id: DocumentId::from_bytes(id_arr),
            document_type: doc_type,
            schema_version: schema_ver,
            owner_node_id: NodeId::from_bytes(owner_arr),
            version: version as u64,
            created_at: Timestamp::from_millis(created_at_ms as u64),
            updated_at: Timestamp::from_millis(updated_at_ms as u64),
            deleted: is_deleted != 0,
            state: payload,
        })
    }

    fn row_to_operation(row: &Row) -> Result<Operation, rusqlite::Error> {
        let op_id_bytes: Vec<u8> = row.get(0)?;
        let doc_id_bytes: Vec<u8> = row.get(1)?;
        let actor_bytes: Vec<u8> = row.get(2)?;
        let sequence: i64 = row.get(3)?;
        let op_type_u8: i64 = row.get(4)?;
        let payload: Vec<u8> = row.get(5)?;
        let hlc_p: i64 = row.get(6)?;
        let hlc_l: i64 = row.get(7)?;
        let deps_bytes: Vec<u8> = row.get(8)?;
        let sig_bytes: Vec<u8> = row.get(9)?;
        let created_at_ms: i64 = row.get(10)?;
        let status_u8: i64 = row.get(11)?;

        let mut op_id_arr = [0u8; 32];
        if op_id_bytes.len() == 32 {
            op_id_arr.copy_from_slice(&op_id_bytes);
        }

        let mut doc_id_arr = [0u8; 32];
        if doc_id_bytes.len() == 32 {
            doc_id_arr.copy_from_slice(&doc_id_bytes);
        }

        let mut actor_arr = [0u8; 32];
        if actor_bytes.len() == 32 {
            actor_arr.copy_from_slice(&actor_bytes);
        }

        let mut sig_arr = [0u8; 64];
        if sig_bytes.len() == 64 {
            sig_arr.copy_from_slice(&sig_bytes);
        }

        let causal_deps = Self::decode_dependencies(&deps_bytes).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(8, rusqlite::types::Type::Blob, Box::new(e))
        })?;

        let op_type = OperationType::from_u8(op_type_u8 as u8).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                4,
                rusqlite::types::Type::Integer,
                Box::new(e),
            )
        })?;

        let status = OpStatus::from_u8(status_u8 as u8).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                11,
                rusqlite::types::Type::Integer,
                Box::new(e),
            )
        })?;

        Ok(Operation {
            op_id: OperationId::from_bytes(op_id_arr),
            document_id: DocumentId::from_bytes(doc_id_arr),
            actor_node_id: NodeId::from_bytes(actor_arr),
            sequence: sequence as u64,
            op_type,
            payload,
            hlc: HybridLogicalClock::new(hlc_p as u64, hlc_l as u32),
            causal_dependencies: causal_deps,
            signature: sig_arr,
            created_at: Timestamp::from_millis(created_at_ms as u64),
            schema_version: crate::schema::CURRENT_SCHEMA_VERSION,
            status,
        })
    }

    fn row_to_replica_state(row: &Row) -> Result<ReplicaState, rusqlite::Error> {
        let doc_id_bytes: Vec<u8> = row.get(0)?;
        let sv_bytes: Vec<u8> = row.get(1)?;
        let hlc_p: i64 = row.get(2)?;
        let hlc_l: i64 = row.get(3)?;
        let local_seq: i64 = row.get(4)?;
        let updated_at_ms: i64 = row.get(5)?;

        let mut doc_id_arr = [0u8; 32];
        if doc_id_bytes.len() == 32 {
            doc_id_arr.copy_from_slice(&doc_id_bytes);
        }

        let sv = StateVector::from_bytes(&sv_bytes).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Blob, Box::new(e))
        })?;

        Ok(ReplicaState {
            document_id: DocumentId::from_bytes(doc_id_arr),
            state_vector: sv,
            latest_hlc: HybridLogicalClock::new(hlc_p as u64, hlc_l as u32),
            local_sequence: local_seq as u64,
            updated_at: Timestamp::from_millis(updated_at_ms as u64),
        })
    }

    fn row_to_document_snapshot(row: &Row) -> Result<DocumentSnapshot, rusqlite::Error> {
        let snap_id_bytes: Vec<u8> = row.get(0)?;
        let doc_id_bytes: Vec<u8> = row.get(1)?;
        let version: i64 = row.get(2)?;
        let seq: i64 = row.get(3)?;
        let sv_bytes: Vec<u8> = row.get(4)?;
        let hlc_p: i64 = row.get(5)?;
        let hlc_l: i64 = row.get(6)?;
        let crdt_type: String = row.get(7)?;
        let payload: Vec<u8> = row.get(8)?;
        let hash_bytes: Vec<u8> = row.get(9)?;
        let schema_ver: i64 = row.get(10)?;
        let created_at_ms: i64 = row.get(11)?;

        let mut snap_arr = [0u8; 32];
        if snap_id_bytes.len() == 32 {
            snap_arr.copy_from_slice(&snap_id_bytes);
        }

        let mut doc_arr = [0u8; 32];
        if doc_id_bytes.len() == 32 {
            doc_arr.copy_from_slice(&doc_id_bytes);
        }

        let mut hash_arr = [0u8; 32];
        if hash_bytes.len() == 32 {
            hash_arr.copy_from_slice(&hash_bytes);
        }

        let sv = StateVector::from_bytes(&sv_bytes).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(4, rusqlite::types::Type::Blob, Box::new(e))
        })?;

        Ok(DocumentSnapshot {
            snapshot_id: SnapshotId::from_bytes(snap_arr),
            document_id: DocumentId::from_bytes(doc_arr),
            snapshot_version: version as u64,
            compacted_up_to_seq: seq as u64,
            state_vector: sv,
            hlc: HybridLogicalClock::new(hlc_p as u64, hlc_l as u32),
            crdt_type,
            state: payload,
            state_hash: hash_arr,
            schema_version: schema_ver as u32,
            created_at: Timestamp::from_millis(created_at_ms as u64),
        })
    }

    fn reduce_crdt_delta_or_state(
        doc_type: &str,
        current_state: &[u8],
        payload: &[u8],
    ) -> Result<Vec<u8>, DataError> {
        match doc_type {
            "pn_counter" | "counter" => {
                let mut counter: PnCounter = serde_json::from_slice(current_state)
                    .map_err(|e| DataError::SerializationFailure(e.to_string()))?;
                if let Ok(delta) = serde_json::from_slice::<PnCounterDelta>(payload) {
                    counter.apply_delta(&delta)?;
                } else if let Ok(remote) = serde_json::from_slice::<PnCounter>(payload) {
                    counter.merge(&remote)?;
                } else {
                    return Err(DataError::InvalidOperation(
                        "Payload is neither valid PnCounterDelta nor PnCounter".to_string(),
                    ));
                }
                counter.serialize_state()
            }
            "or_set" | "set" => {
                let mut set: OrSet<String> = serde_json::from_slice(current_state)
                    .map_err(|e| DataError::SerializationFailure(e.to_string()))?;
                if let Ok(delta) = serde_json::from_slice::<OrSetDelta<String>>(payload) {
                    set.apply_delta(&delta)?;
                } else if let Ok(remote) = serde_json::from_slice::<OrSet<String>>(payload) {
                    set.merge(&remote)?;
                } else {
                    return Err(DataError::InvalidOperation(
                        "Payload is neither valid OrSetDelta nor OrSet".to_string(),
                    ));
                }
                set.serialize_state()
            }
            "lww_register" | "register" => {
                let mut reg: LwwRegister<serde_json::Value> = serde_json::from_slice(current_state)
                    .map_err(|e| DataError::SerializationFailure(e.to_string()))?;
                if let Ok(delta) =
                    serde_json::from_slice::<LwwRegisterDelta<serde_json::Value>>(payload)
                {
                    reg.apply_delta(&delta)?;
                } else if let Ok(remote) =
                    serde_json::from_slice::<LwwRegister<serde_json::Value>>(payload)
                {
                    reg.merge(&remote)?;
                } else {
                    return Err(DataError::InvalidOperation(
                        "Payload is neither valid LwwRegisterDelta nor LwwRegister".to_string(),
                    ));
                }
                reg.serialize_state()
            }
            "crdt_map" | "map" => {
                let mut map: CrdtMap = serde_json::from_slice(current_state)
                    .map_err(|e| DataError::SerializationFailure(e.to_string()))?;
                if let Ok(delta) = serde_json::from_slice::<CrdtMapDelta>(payload) {
                    map.apply_delta(&delta)?;
                } else if let Ok(remote) = serde_json::from_slice::<CrdtMap>(payload) {
                    map.merge(&remote)?;
                } else {
                    return Err(DataError::InvalidOperation(
                        "Payload is neither valid CrdtMapDelta nor CrdtMap".to_string(),
                    ));
                }
                map.serialize_state()
            }
            other => Err(DataError::TypeMismatch {
                expected: "supported CRDT type".to_string(),
                found: other.to_string(),
            }),
        }
    }

    fn is_crdt_type(doc_type: &str) -> bool {
        matches!(
            doc_type,
            "pn_counter"
                | "counter"
                | "or_set"
                | "set"
                | "lww_register"
                | "register"
                | "crdt_map"
                | "map"
        )
    }

    fn detect_crdt_type(payload: &[u8]) -> Option<String> {
        if serde_json::from_slice::<PnCounter>(payload).is_ok() {
            Some("pn_counter".to_string())
        } else if serde_json::from_slice::<OrSet<String>>(payload).is_ok() {
            Some("or_set".to_string())
        } else if serde_json::from_slice::<CrdtMap>(payload).is_ok() {
            Some("crdt_map".to_string())
        } else if serde_json::from_slice::<LwwRegister<serde_json::Value>>(payload).is_ok() {
            Some("lww_register".to_string())
        } else {
            None
        }
    }
}
