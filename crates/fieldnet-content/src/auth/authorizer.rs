use rusqlite::{params, Connection, OptionalExtension};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tracing::{debug, warn};

use fieldnet_core::{NodeId, Timestamp};

use super::action::ContentAction;
use super::token::CapabilityToken;
use crate::error::ContentError;
use crate::events::ContentEvent;
use crate::identity::ContentId;

/// Authoritative content authorization engine (Phase 07 BUILD 06).
/// Enforces demarcated authorization boundaries:
/// Network authentication != Content authorization.
pub struct ContentAuthorizer {
    conn: Arc<Mutex<Connection>>,
    local_node_id: NodeId,
    event_tx: broadcast::Sender<ContentEvent>,
}

impl ContentAuthorizer {
    /// Initializes the `ContentAuthorizer` and ensures SQLite authorization tables exist.
    pub fn new(
        conn: Arc<Mutex<Connection>>,
        local_node_id: NodeId,
        event_tx: broadcast::Sender<ContentEvent>,
    ) -> Result<Arc<Self>, ContentError> {
        {
            let conn_guard = conn
                .lock()
                .map_err(|e| ContentError::LockError(e.to_string()))?;
            conn_guard.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS content_acls (
                    content_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    actions_mask INTEGER NOT NULL,
                    created_at INTEGER NOT NULL,
                    PRIMARY KEY (content_id, node_id)
                );
                CREATE TABLE IF NOT EXISTS content_public (
                    content_id TEXT PRIMARY KEY,
                    is_public INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_content_acls_cid ON content_acls(content_id);
                "#,
            )?;
        }

        Ok(Arc::new(Self {
            conn,
            local_node_id,
            event_tx,
        }))
    }

    /// Returns the local node identity.
    pub fn local_node_id(&self) -> NodeId {
        self.local_node_id
    }

    /// Authorizes a content request before any bytes or metadata are served.
    pub fn authorize(
        &self,
        requester: &NodeId,
        action: ContentAction,
        content_id: &ContentId,
        token: Option<&CapabilityToken>,
    ) -> Result<(), ContentError> {
        // 1. Local sovereign node always authorized for all operations
        if requester == &self.local_node_id {
            return Ok(());
        }

        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;

        // 2. Content creator check: Content creator has sovereign authority over their content
        let creator_opt: Option<Option<String>> = conn
            .query_row(
                "SELECT creator_node_id FROM content_index WHERE content_id = ?1",
                params![content_id.to_hex()],
                |row| row.get(0),
            )
            .optional()?;

        if let Some(Some(creator_hex)) = &creator_opt {
            if let Ok(creator_bytes) = hex::decode(creator_hex) {
                if let Ok(creator_id) = NodeId::from_slice(&creator_bytes) {
                    if &creator_id == requester {
                        return Ok(());
                    }
                }
            }
        }

        // 3. Public content access check (Read only)
        if action == ContentAction::Read {
            let is_public: Option<i64> = conn
                .query_row(
                    "SELECT is_public FROM content_public WHERE content_id = ?1",
                    params![content_id.to_hex()],
                    |row| row.get(0),
                )
                .optional()?;

            if is_public.unwrap_or(0) != 0 {
                return Ok(());
            }
        }

        // 4. Capability token verification
        if let Some(tok) = token {
            let now = Timestamp::now();
            if let Ok(()) = tok.validate(requester, content_id, action, now) {
                // Verify that token issuer is authorized to delegate (is local node or creator)
                let is_valid_issuer = tok.issuer == self.local_node_id
                    || creator_opt
                        .as_ref()
                        .and_then(|c| c.as_ref())
                        .map(|c| c == &tok.issuer.to_hex())
                        .unwrap_or(false);

                if is_valid_issuer {
                    debug!(
                        requester = %requester,
                        content_id = %content_id,
                        action = %action,
                        token_id = hex::encode(tok.token_id),
                        "Authorized content request via capability token"
                    );
                    return Ok(());
                }
            }
        }

        // 5. Persistent ACL lookup
        let acl_mask: Option<u32> = conn
            .query_row(
                "SELECT actions_mask FROM content_acls WHERE content_id = ?1 AND node_id = ?2",
                params![content_id.to_hex(), requester.to_hex()],
                |row| row.get(0),
            )
            .optional()?;

        if let Some(mask) = acl_mask {
            if (mask & action.to_bitmask()) != 0 {
                debug!(
                    requester = %requester,
                    content_id = %content_id,
                    action = %action,
                    "Authorized content request via ACL"
                );
                return Ok(());
            }
        }

        // 6. Deny access fail-closed
        warn!(
            requester = %requester,
            content_id = %content_id,
            action = %action,
            "Denying unauthorized content access"
        );

        let _ = self.event_tx.send(ContentEvent::AuthorizationDenied {
            content_id: *content_id,
            peer: *requester,
            action: action.to_string(),
            reason: "Access denied by policy".to_string(),
        });

        Err(ContentError::AuthorizationDenied(format!(
            "Node {} unauthorized for {:?} on content {}",
            requester, action, content_id
        )))
    }

    /// Grants permissions in the ACL for a specific peer and content.
    pub fn grant_permission(
        &self,
        content_id: &ContentId,
        node_id: &NodeId,
        actions: &[ContentAction],
    ) -> Result<(), ContentError> {
        let mask = ContentAction::actions_to_bitmask(actions);
        let now = Timestamp::now().as_millis() as i64;

        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;

        conn.execute(
            r#"
            INSERT INTO content_acls (content_id, node_id, actions_mask, created_at)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(content_id, node_id) DO UPDATE SET
                actions_mask = (content_acls.actions_mask | excluded.actions_mask)
            "#,
            params![content_id.to_hex(), node_id.to_hex(), mask, now],
        )?;

        Ok(())
    }

    /// Revokes all ACL permissions for a peer on a content object.
    pub fn revoke_permission(
        &self,
        content_id: &ContentId,
        node_id: &NodeId,
    ) -> Result<(), ContentError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;

        conn.execute(
            "DELETE FROM content_acls WHERE content_id = ?1 AND node_id = ?2",
            params![content_id.to_hex(), node_id.to_hex()],
        )?;

        Ok(())
    }

    /// Sets whether a content object is public (accessible for READ without explicit token/ACL).
    pub fn set_public(&self, content_id: &ContentId, is_public: bool) -> Result<(), ContentError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;

        conn.execute(
            r#"
            INSERT INTO content_public (content_id, is_public)
            VALUES (?1, ?2)
            ON CONFLICT(content_id) DO UPDATE SET
                is_public = excluded.is_public
            "#,
            params![content_id.to_hex(), if is_public { 1 } else { 0 }],
        )?;

        Ok(())
    }

    /// Checks if a content object is marked public.
    pub fn is_public(&self, content_id: &ContentId) -> Result<bool, ContentError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| ContentError::LockError(e.to_string()))?;

        let res: Option<i64> = conn
            .query_row(
                "SELECT is_public FROM content_public WHERE content_id = ?1",
                params![content_id.to_hex()],
                |row| row.get(0),
            )
            .optional()?;

        Ok(res.unwrap_or(0) != 0)
    }
}
