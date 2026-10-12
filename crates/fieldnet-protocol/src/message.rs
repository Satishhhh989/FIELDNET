use crate::error::ProtocolError;
use crate::message_type::MessageType;
use fieldnet_core::{MessageId, NodeId, Priority, SessionId, Timestamp};
use fieldnet_identity::NodeIdentity;
use serde::{Deserialize, Serialize};

pub const CURRENT_PROTOCOL_VERSION: u8 = 1;

/// A FIELDNET protocol message envelope.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Message {
    pub version: u8,
    pub message_id: MessageId,
    pub message_type: MessageType,
    pub source: NodeId,
    pub destination: NodeId,
    pub created_at: u64,
    pub expires_at: u64,
    pub ttl: u8,
    pub sequence: u64,
    pub priority: Priority,
    pub payload: Vec<u8>,
    #[serde(with = "hex")]
    pub auth_tag: [u8; 64],
    pub session_id: Option<SessionId>,
}

//
impl Message {
    /// Constructs and signs a new DATA message from local sovereign identity.
    #[allow(clippy::too_many_arguments)]
    pub fn sign_and_build(
        identity: &NodeIdentity,
        destination: NodeId,
        message_type: MessageType,
        payload: Vec<u8>,
        ttl: u8,
        expires_in_millis: u64,
        sequence: u64,
        priority: Priority,
    ) -> Result<Self, ProtocolError> {
        let now = Timestamp::now().as_millis();
        let expires_at = now.saturating_add(expires_in_millis);
        let source = identity.node_id();
        let message_id = MessageId::compute(&source, sequence, now, &payload);

        let mut msg = Self {
            version: CURRENT_PROTOCOL_VERSION,
            message_id,
            message_type,
            source,
            destination,
            created_at: now,
            expires_at,
            ttl,
            sequence,
            priority,
            payload,
            auth_tag: [0u8; 64],
            session_id: None,
        };

        let signing_bytes = msg.canonical_signing_bytes();
        let sig = identity.sign(&signing_bytes);
        msg.auth_tag = sig;

        Ok(msg)
    }

    /// Returns the canonical bytes over which the originator's digital signature is computed.
    /// Excludes mutable relay fields (ttl, session_id, auth_tag itself).
    pub fn canonical_signing_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(128 + self.payload.len());
        bytes.push(self.version);
        bytes.extend_from_slice(self.message_id.as_bytes());
        bytes.push(self.message_type as u8);
        bytes.extend_from_slice(self.source.as_bytes());
        bytes.extend_from_slice(self.destination.as_bytes());
        bytes.extend_from_slice(&self.created_at.to_be_bytes());
        bytes.extend_from_slice(&self.expires_at.to_be_bytes());
        bytes.extend_from_slice(&self.sequence.to_be_bytes());
        bytes.push(self.priority as u8);
        bytes.extend_from_slice(&self.payload);
        bytes
    }

    /// Verifies the digital signature using the source node's Ed25519 public key.
    pub fn verify_signature(&self, source_public_key: &[u8; 32]) -> Result<(), ProtocolError> {
        let derived_node_id = NodeId::from_ed25519_public_key(source_public_key);
        if derived_node_id != self.source {
            return Err(ProtocolError::UnauthorizedSender(format!(
                "Public key does not match message source NodeId: expected {}, got {}",
                self.source, derived_node_id
            )));
        }

        let signing_bytes = self.canonical_signing_bytes();
        NodeIdentity::verify(source_public_key, &signing_bytes, &self.auth_tag)
            .map_err(|e| ProtocolError::InvalidSignature(e.to_string()))
    }

    /// Validates message expiration and TTL.
    pub fn validate_freshness(&self, now: u64) -> Result<(), ProtocolError> {
        if self.version != CURRENT_PROTOCOL_VERSION {
            return Err(ProtocolError::VersionMismatch {
                expected: CURRENT_PROTOCOL_VERSION,
                actual: self.version,
            });
        }
        if now >= self.expires_at {
            return Err(ProtocolError::ExpiredMessage {
                created_at: self.created_at,
                expires_at: self.expires_at,
                now,
            });
        }
        if self.ttl == 0 {
            return Err(ProtocolError::TtlExhausted);
        }
        Ok(())
    }

    /// Checks whether the message has exceeded its expiration time.
    pub fn is_expired(&self) -> bool {
        Timestamp::now().as_millis() >= self.expires_at
    }

    /// Decrements TTL by 1 during relay forwarding.
    pub fn decrement_ttl(&mut self) -> Result<(), ProtocolError> {
        if self.ttl == 0 {
            return Err(ProtocolError::TtlExhausted);
        }
        self.ttl -= 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_sign_verify_and_freshness() {
        let identity = NodeIdentity::generate();
        let dest = NodeId::from_bytes([2u8; 32]);
        let payload = b"Hello from FIELDNET BUILD-01".to_vec();

        let mut msg = Message::sign_and_build(
            &identity,
            dest,
            MessageType::Data,
            payload,
            16,
            60_000,
            1,
            Priority::Normal,
        )
        .unwrap();

        assert_eq!(msg.ttl, 16);
        assert!(msg.verify_signature(&identity.public_key_bytes()).is_ok());

        // Test freshness
        let now = Timestamp::now().as_millis();
        assert!(msg.validate_freshness(now).is_ok());

        // Test TTL decrement
        assert!(msg.decrement_ttl().is_ok());
        assert_eq!(msg.ttl, 15);
        // Signature remains valid because TTL is not in canonical signing bytes!
        assert!(msg.verify_signature(&identity.public_key_bytes()).is_ok());

        // Tampering with payload invalidates signature
        msg.payload[0] ^= 0xFF;
        assert!(msg.verify_signature(&identity.public_key_bytes()).is_err());
    }
}
