use crate::error::CoreError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;

/// A sovereign 32-byte Node Identifier.
/// Derived as the BLAKE3 hash of the node's Ed25519 public key.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct NodeId(pub [u8; 32]);

impl NodeId {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn from_slice(slice: &[u8]) -> Result<Self, CoreError> {
        if slice.len() != 32 {
            return Err(CoreError::InvalidIdentifier(format!(
                "Expected 32 bytes for NodeId, got {}",
                slice.len()
            )));
        }
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(slice);
        Ok(Self(bytes))
    }

    /// Derives a stable NodeId from an Ed25519 public key.
    pub fn from_ed25519_public_key(pubkey_bytes: &[u8; 32]) -> Self {
        let hash = blake3::hash(pubkey_bytes);
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

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId({})", self.to_hex())
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl FromStr for NodeId {
    type Err = CoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = hex::decode(s).map_err(|e| CoreError::HexDecode(e.to_string()))?;
        Self::from_slice(&bytes)
    }
}

impl Serialize for NodeId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for NodeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        NodeId::from_str(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_id_derivation_and_hex() {
        let pubkey = [42u8; 32];
        let node_id = NodeId::from_ed25519_public_key(&pubkey);
        let hex = node_id.to_hex();
        assert_eq!(hex.len(), 64);
        let parsed: NodeId = hex.parse().unwrap();
        assert_eq!(node_id, parsed);
    }
}
