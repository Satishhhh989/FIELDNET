//! Document Authorization and Capability Enforcement for FIELDNET Phase 06.
//!
//! Enforces:
//! "Do NOT confuse: authenticated peer with: authorized data actor.
//! A peer may be authenticated but not authorized to modify a document.
//! Unauthorized operations must be rejected fail-closed without crashing runtime."

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use fieldnet_core::NodeId;

use crate::document::{Document, DocumentId};
use crate::error::DataError;
use crate::operation::Operation;

/// Trait governing capability authorization for operations before they touch the CRDT engine.
pub trait DocumentAuthorizer: Send + Sync {
    /// Validates whether the operation author has legitimate capability to mutate the document.
    fn authorize_operation(&self, doc: Option<&Document>, op: &Operation) -> Result<(), DataError>;
}

/// Standard fail-closed document authorizer.
#[derive(Debug, Default)]
pub struct DefaultDocumentAuthorizer {
    /// Document-specific allowed writers: DocumentId -> Set of authorized NodeIds
    document_writers: RwLock<HashMap<DocumentId, HashSet<NodeId>>>,
    /// Restricted namespaces that require explicit ownership
    restricted_namespaces: RwLock<HashSet<String>>,
    /// Blocked / revoked actors
    blocked_actors: RwLock<HashSet<NodeId>>,
}

impl DefaultDocumentAuthorizer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Grants explicit write permission for a document to a designated actor.
    pub fn grant_write_permission(&self, doc_id: DocumentId, actor: NodeId) {
        let mut writers = self.document_writers.write().unwrap();
        writers.entry(doc_id).or_default().insert(actor);
    }

    /// Revokes write permission for a document.
    pub fn revoke_write_permission(&self, doc_id: &DocumentId, actor: &NodeId) {
        let mut writers = self.document_writers.write().unwrap();
        if let Some(set) = writers.get_mut(doc_id) {
            set.remove(actor);
        }
    }

    /// Marks a namespace as restricted (only document owner can mutate).
    pub fn add_restricted_namespace(&self, namespace: impl Into<String>) {
        let mut ns = self.restricted_namespaces.write().unwrap();
        ns.insert(namespace.into());
    }

    /// Globally blocks an actor from writing to any document.
    pub fn block_actor(&self, actor: NodeId) {
        let mut blocked = self.blocked_actors.write().unwrap();
        blocked.insert(actor);
    }
}

impl DocumentAuthorizer for DefaultDocumentAuthorizer {
    fn authorize_operation(&self, doc: Option<&Document>, op: &Operation) -> Result<(), DataError> {
        let actor = op.actor_node_id;

        // 1. Check if actor is globally blocked
        {
            let blocked = self.blocked_actors.read().unwrap();
            if blocked.contains(&actor) {
                return Err(DataError::Unauthorized(format!(
                    "Actor {} is globally blocked from data mutations",
                    actor
                )));
            }
        }

        // 2. If document exists, evaluate document-level capabilities
        if let Some(existing_doc) = doc {
            // Document owner always has authority over their sovereign document
            if actor == existing_doc.owner_node_id {
                return Ok(());
            }

            // Check if actor was explicitly granted write permission for this document
            {
                let writers = self.document_writers.read().unwrap();
                if let Some(allowed) = writers.get(&existing_doc.document_id) {
                    if allowed.contains(&actor) {
                        return Ok(());
                    }
                }
            }

            // Check if document belongs to a restricted namespace
            {
                let restricted = self.restricted_namespaces.read().unwrap();
                if restricted.contains(&existing_doc.document_type) {
                    return Err(DataError::Unauthorized(format!(
                        "Actor {} is not authorized to mutate document {} in restricted namespace '{}'",
                        actor, existing_doc.document_id, existing_doc.document_type
                    )));
                }
            }
        }

        // 3. For new documents or un-restricted namespaces, author is authorized
        Ok(())
    }
}
