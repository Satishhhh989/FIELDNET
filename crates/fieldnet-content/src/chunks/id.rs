use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use crate::error::ContentError;

/// Length in bytes of a SHA-256 chunk digest.
pub const CHUNK_ID_BYTES_LEN: usize = 32;

/// Length in characters of a SHA-256 chunk hex string.
pub const CHUNK_ID_HEX_LEN: usize = 64;

/// Strongly typed cryptographic identifier for a content chunk.
///
/// Derived from the SHA-256 digest of the raw chunk bytes, ensuring
/// chunks are strictly content-addressed and provider-independent.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChunkId([u8; CHUNK_ID_BYTES_LEN]);

impl ChunkId {
    /// Constructs a `ChunkId` from a 32-byte digest array.
    pub const fn from_bytes(bytes: [u8; CHUNK_ID_BYTES_LEN]) -> Self {
        Self(bytes)
    }

    /// Constructs a `ChunkId` from a byte slice.
    pub fn from_slice(slice: &[u8]) -> Result<Self, ContentError> {
        if slice.len() != CHUNK_ID_BYTES_LEN {
            return Err(ContentError::InvalidContentId(format!(
                "Expected {} bytes for ChunkId, received {}",
                CHUNK_ID_BYTES_LEN,
                slice.len()
            )));
        }
        let mut arr = [0u8; CHUNK_ID_BYTES_LEN];
        arr.copy_from_slice(slice);
        Ok(Self(arr))
    }

    /// Returns a reference to the raw 32-byte digest.
    pub fn as_bytes(&self) -> &[u8; CHUNK_ID_BYTES_LEN] {
        &self.0
    }

    /// Returns a copy of the raw 32-byte digest.
    pub fn to_bytes(&self) -> [u8; CHUNK_ID_BYTES_LEN] {
        self.0
    }

    /// Formats the chunk identifier as a 64-character lowercase hex string.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Parses a 64-character hex string (with optional prefix `fn-chk-` or `sha256:`) into a `ChunkId`.
    pub fn from_hex(s: &str) -> Result<Self, ContentError> {
        let clean = s.trim();
        let stripped = clean
            .strip_prefix("fn-chk-")
            .or_else(|| clean.strip_prefix("sha256:"))
            .or_else(|| clean.strip_prefix("sha256-"))
            .unwrap_or(clean);

        if stripped.len() != CHUNK_ID_HEX_LEN {
            return Err(ContentError::InvalidContentId(format!(
                "Invalid ChunkId hex length: expected {}, got {}",
                CHUNK_ID_HEX_LEN,
                stripped.len()
            )));
        }

        let mut bytes = [0u8; CHUNK_ID_BYTES_LEN];
        hex::decode_to_slice(stripped, &mut bytes).map_err(|e| {
            ContentError::InvalidContentId(format!("Hex decoding failed for ChunkId: {}", e))
        })?;

        Ok(Self(bytes))
    }

    /// Computes two-tier sharding path components `(shard1, shard2, filename)`.
    pub fn shard_components(&self) -> (String, String, String) {
        let hex_str = self.to_hex();
        let shard1 = hex_str[0..2].to_string();
        let shard2 = hex_str[2..4].to_string();
        (shard1, shard2, hex_str)
    }

    /// Computes the relative storage path within a chunk store: `ab/cd/<chunk_id_hex>`.
    pub fn relative_storage_path(&self) -> PathBuf {
        let (shard1, shard2, filename) = self.shard_components();
        Path::new(&shard1).join(shard2).join(filename)
    }
}

impl fmt::Display for ChunkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl fmt::Debug for ChunkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ChunkId({})", self.to_hex())
    }
}

impl FromStr for ChunkId {
    type Err = ContentError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_hex(s)
    }
}

impl Serialize for ChunkId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_hex())
        } else {
            serializer.serialize_bytes(&self.0)
        }
    }
}

impl<'de> Deserialize<'de> for ChunkId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            ChunkId::from_hex(&s).map_err(serde::de::Error::custom)
        } else {
            let bytes = <[u8; CHUNK_ID_BYTES_LEN]>::deserialize(deserializer)?;
            Ok(ChunkId::from_bytes(bytes))
        }
    }
}

/// Metadata descriptor for a single constituent chunk within a manifest.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkDescriptor {
    /// Zero-based sequential chunk index.
    pub index: u32,
    /// Absolute byte offset within the logical content stream.
    pub offset: u64,
    /// Length of this chunk in bytes.
    pub size: u32,
    /// Cryptographic identifier of the chunk bytes: SHA-256(chunk_payload).
    pub chunk_id: ChunkId,
}
