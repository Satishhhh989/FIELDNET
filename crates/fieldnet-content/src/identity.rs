use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use crate::error::ContentError;

/// Length in bytes of a SHA-256 content digest.
pub const CONTENT_ID_BYTES_LEN: usize = 32;

/// Length in characters of a SHA-256 hex string.
pub const CONTENT_ID_HEX_LEN: usize = 64;

/// Strongly typed Content Identity (`ContentId`) representing a SHA-256 cryptographic digest.
///
/// Implements content addressing where data belongs to its cryptographic identity,
/// invariant across devices and storage backends.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentId([u8; CONTENT_ID_BYTES_LEN]);

impl ContentId {
    /// Constructs a `ContentId` from raw 32-byte digest array.
    pub const fn from_bytes(bytes: [u8; CONTENT_ID_BYTES_LEN]) -> Self {
        Self(bytes)
    }

    /// Constructs a `ContentId` from a byte slice, verifying exact 32-byte length.
    pub fn from_slice(slice: &[u8]) -> Result<Self, ContentError> {
        if slice.len() != CONTENT_ID_BYTES_LEN {
            return Err(ContentError::InvalidContentId(format!(
                "Expected {} bytes, received {}",
                CONTENT_ID_BYTES_LEN,
                slice.len()
            )));
        }
        let mut arr = [0u8; CONTENT_ID_BYTES_LEN];
        arr.copy_from_slice(slice);
        Ok(Self(arr))
    }

    /// Returns a reference to the raw 32-byte digest.
    pub fn as_bytes(&self) -> &[u8; CONTENT_ID_BYTES_LEN] {
        &self.0
    }

    /// Returns a copy of the raw 32-byte digest.
    pub fn to_bytes(&self) -> [u8; CONTENT_ID_BYTES_LEN] {
        self.0
    }

    /// Formats the content identifier as a 64-character lowercase hex string.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Parses a 64-character hex string (with optional prefix `fn-cid-` or `sha256:`) into a `ContentId`.
    pub fn from_hex(s: &str) -> Result<Self, ContentError> {
        let clean = s.trim();
        let stripped = clean
            .strip_prefix("fn-cid-")
            .or_else(|| clean.strip_prefix("sha256:"))
            .or_else(|| clean.strip_prefix("sha256-"))
            .unwrap_or(clean);

        if stripped.len() != CONTENT_ID_HEX_LEN {
            return Err(ContentError::InvalidContentId(format!(
                "Invalid ContentId hex length: expected {}, got {}",
                CONTENT_ID_HEX_LEN,
                stripped.len()
            )));
        }

        let mut bytes = [0u8; CONTENT_ID_BYTES_LEN];
        hex::decode_to_slice(stripped, &mut bytes)
            .map_err(|e| ContentError::InvalidContentId(format!("Hex decoding failed: {}", e)))?;

        Ok(Self(bytes))
    }

    /// Computes the deterministic two-tier sharding path components:
    /// Returns `(shard_tier_1, shard_tier_2, object_filename)`
    /// e.g. for `e3b0c442...` returns `("e3", "b0", "e3b0c442...")`.
    pub fn shard_components(&self) -> (String, String, String) {
        let hex_str = self.to_hex();
        let shard1 = hex_str[0..2].to_string();
        let shard2 = hex_str[2..4].to_string();
        (shard1, shard2, hex_str)
    }

    /// Computes the relative storage path within an objects store: `ab/cd/<content_id_hex>`.
    pub fn relative_storage_path(&self) -> PathBuf {
        let (shard1, shard2, filename) = self.shard_components();
        Path::new(&shard1).join(shard2).join(filename)
    }
}

impl fmt::Display for ContentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl fmt::Debug for ContentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ContentId({})", self.to_hex())
    }
}

impl FromStr for ContentId {
    type Err = ContentError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_hex(s)
    }
}

impl Serialize for ContentId {
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

impl<'de> Deserialize<'de> for ContentId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            ContentId::from_hex(&s).map_err(serde::de::Error::custom)
        } else {
            let bytes = <[u8; CONTENT_ID_BYTES_LEN]>::deserialize(deserializer)?;
            Ok(ContentId::from_bytes(bytes))
        }
    }
}
