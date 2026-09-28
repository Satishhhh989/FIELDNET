use crate::error::CoreError;
use crate::node_id::NodeId;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;

/// A globally unique 32-byte cryptographic Message Identifier.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct MessageId(pub [u8; 32]);

impl MessageId {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn from_slice(slice: &[u8]) -> Result<Self, CoreError> {
        if slice.len() != 32 {
            return Err(CoreError::InvalidIdentifier(format!(
                "Expected 32 bytes for MessageId, got {}",
                slice.len()
            )));
        }
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(slice);
        Ok(Self(bytes))
    }

    /// Computes MessageId = BLAKE3(source || sequence || created_at || payload_hash)
    pub fn compute(source: &NodeId, sequence: u64, created_at: u64, payload: &[u8]) -> Self {
        let payload_hash = blake3::hash(payload);
        let mut hasher = blake3::Hasher::new();
        hasher.update(source.as_bytes());
        hasher.update(&sequence.to_be_bytes());
        hasher.update(&created_at.to_be_bytes());
        hasher.update(payload_hash.as_bytes());
        let hash = hasher.finalize();
        Self(*hash.as_bytes())
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    pub fn short_hex(&self) -> String {
        let h = self.to_hex();
        format!("{}...{}", &h[..8], &h[56..])
    }
}

impl fmt::Debug for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MessageId({})", self.to_hex())
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl FromStr for MessageId {
    type Err = CoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = hex::decode(s).map_err(|e| CoreError::HexDecode(e.to_string()))?;
        Self::from_slice(&bytes)
    }
}

impl Serialize for MessageId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for MessageId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        MessageId::from_str(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_id_compute_and_serde() {
        let node_id = NodeId::from_bytes([1u8; 32]);
        let msg_id = MessageId::compute(&node_id, 1, 1000, b"test payload");
        let hex = msg_id.to_hex();
        assert_eq!(hex.len(), 64);
        let deserialized: MessageId = hex.parse().unwrap();
        assert_eq!(msg_id, deserialized);
    }
}
