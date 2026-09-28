use fieldnet_core::{NodeId, Timestamp};
use fieldnet_identity::NodeIdentity;
use serde::{Deserialize, Serialize};

use super::action::ContentAction;
use crate::error::ContentError;
use crate::identity::ContentId;

/// A cryptographically signed capability token granting specific content permissions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityToken {
    /// Unique 16-byte identifier for this token.
    pub token_id: [u8; 16],
    /// Sovereign node identity that issued and signed this token.
    pub issuer: NodeId,
    /// Ed25519 public key of the issuer.
    #[serde(with = "hex")]
    pub issuer_pubkey: [u8; 32],
    /// Target subject node identity granted permissions (`None` for wildcard/bearer).
    pub subject: Option<NodeId>,
    /// Specific ContentId granted (`None` for all content authored by issuer).
    pub content_id: Option<ContentId>,
    /// Granted content actions.
    pub actions: Vec<ContentAction>,
    /// Creation timestamp.
    pub issued_at: Timestamp,
    /// Expiration timestamp.
    pub expires_at: Timestamp,
    /// Ed25519 signature computed over canonical bytes.
    #[serde(with = "hex")]
    pub signature: [u8; 64],
}

impl CapabilityToken {
    /// Creates a new signed capability token.
    pub fn new(
        issuer_identity: &NodeIdentity,
        subject: Option<NodeId>,
        content_id: Option<ContentId>,
        actions: Vec<ContentAction>,
        ttl_seconds: u64,
    ) -> Self {
        let mut token_id = [0u8; 16];
        rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut token_id);

        let now = Timestamp::now();
        let expires_at = now.add_seconds(ttl_seconds);

        let mut token = Self {
            token_id,
            issuer: issuer_identity.node_id(),
            issuer_pubkey: issuer_identity.public_key_bytes(),
            subject,
            content_id,
            actions,
            issued_at: now,
            expires_at,
            signature: [0u8; 64],
        };

        token.sign(issuer_identity);
        token
    }

    /// Computes canonical binary bytes for signing and verification.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(128);
        buf.extend_from_slice(b"FIELDNET_CAPABILITY_TOKEN_V1");
        buf.extend_from_slice(&self.token_id);
        buf.extend_from_slice(self.issuer.as_bytes());
        buf.extend_from_slice(&self.issuer_pubkey);

        match &self.subject {
            Some(s) => {
                buf.push(1);
                buf.extend_from_slice(s.as_bytes());
            }
            None => buf.push(0),
        }

        match &self.content_id {
            Some(c) => {
                buf.push(1);
                buf.extend_from_slice(c.as_bytes());
            }
            None => buf.push(0),
        }

        let bitmask = ContentAction::actions_to_bitmask(&self.actions);
        buf.extend_from_slice(&bitmask.to_le_bytes());
        buf.extend_from_slice(&self.issued_at.as_millis().to_le_bytes());
        buf.extend_from_slice(&self.expires_at.as_millis().to_le_bytes());

        buf
    }

    /// Signs the token using the issuer's sovereign Ed25519 identity.
    pub fn sign(&mut self, identity: &NodeIdentity) {
        let canonical = self.canonical_bytes();
        let sig = identity.sign(&canonical);
        self.signature = sig;
    }

    /// Verifies the token's cryptographic integrity and signature.
    pub fn verify_signature(&self) -> Result<(), ContentError> {
        // Verify that the issuer_pubkey matches the issuer NodeId
        let expected_node_id = NodeId::from_ed25519_public_key(&self.issuer_pubkey);
        if expected_node_id != self.issuer {
            return Err(ContentError::InvalidToken(
                "Issuer public key does not match issuer NodeId".to_string(),
            ));
        }

        let canonical = self.canonical_bytes();
        NodeIdentity::verify(&self.issuer_pubkey, &canonical, &self.signature).map_err(|e| {
            ContentError::InvalidToken(format!("Signature verification failed: {}", e))
        })
    }

    /// Checks if the token has expired relative to `now`.
    pub fn is_expired(&self, now: Timestamp) -> bool {
        self.expires_at.has_expired(now)
    }

    /// Checks whether the token allows the specified action.
    pub fn allows_action(&self, action: ContentAction) -> bool {
        self.actions.contains(&action)
    }

    /// Checks whether the token covers the specified content ID.
    pub fn allows_content(&self, content_id: &ContentId) -> bool {
        match &self.content_id {
            Some(c) => c == content_id,
            None => true, // Wildcard for all content issued by issuer
        }
    }

    /// Checks whether the token covers the specified requester.
    pub fn allows_subject(&self, requester: &NodeId) -> bool {
        match &self.subject {
            Some(s) => s == requester,
            None => true, // Wildcard bearer
        }
    }

    /// Validates the token completely against requester, content_id, action, and current timestamp.
    pub fn validate(
        &self,
        requester: &NodeId,
        content_id: &ContentId,
        action: ContentAction,
        now: Timestamp,
    ) -> Result<(), ContentError> {
        self.verify_signature()?;

        if self.is_expired(now) {
            return Err(ContentError::TokenExpired {
                expired_at: self.expires_at.as_millis(),
                now: now.as_millis(),
            });
        }

        if !self.allows_subject(requester) {
            return Err(ContentError::InvalidToken(format!(
                "Token subject mismatch: expected {:?}, got {}",
                self.subject, requester
            )));
        }

        if !self.allows_content(content_id) {
            return Err(ContentError::InvalidToken(format!(
                "Token content mismatch: expected {:?}, got {}",
                self.content_id, content_id
            )));
        }

        if !self.allows_action(action) {
            return Err(ContentError::AuthorizationDenied(format!(
                "Token does not grant {:?} permission",
                action
            )));
        }

        Ok(())
    }
}
