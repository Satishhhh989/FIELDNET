use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;

use crate::error::ContentError;

/// Length in bytes of a SHA-256 manifest digest.
pub const MANIFEST_ID_BYTES_LEN: usize = 32;

/// Length in characters of a SHA-256 manifest hex string.
pub const MANIFEST_ID_HEX_LEN: usize = 64;

/// Strongly typed cryptographic identifier for a content manifest.
///
/// Deterministically computed as the SHA-256 digest over the canonical manifest structure.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ManifestId([u8; MANIFEST_ID_BYTES_LEN]);

impl ManifestId {
    /// Constructs a `ManifestId` from a 32-byte digest array.
    pub const fn from_bytes(bytes: [u8; MANIFEST_ID_BYTES_LEN]) -> Self {
        Self(bytes)
    }

    /// Constructs a `ManifestId` from a byte slice.
    pub fn from_slice(slice: &[u8]) -> Result<Self, ContentError> {
        if slice.len() != MANIFEST_ID_BYTES_LEN {
            return Err(ContentError::InvalidContentId(format!(
                "Expected {} bytes for ManifestId, received {}",
                MANIFEST_ID_BYTES_LEN,
                slice.len()
            )));
        }
        let mut arr = [0u8; MANIFEST_ID_BYTES_LEN];
        arr.copy_from_slice(slice);
        Ok(Self(arr))
    }

    /// Returns a reference to the raw 32-byte digest.
    pub fn as_bytes(&self) -> &[u8; MANIFEST_ID_BYTES_LEN] {
        &self.0
    }

    /// Returns a copy of the raw 32-byte digest.
    pub fn to_bytes(&self) -> [u8; MANIFEST_ID_BYTES_LEN] {
        self.0
    }

    /// Formats the manifest identifier as a 64-character lowercase hex string.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Parses a 64-character hex string (with optional prefix `fn-mf-` or `sha256:`) into a `ManifestId`.
    pub fn from_hex(s: &str) -> Result<Self, ContentError> {
        let clean = s.trim();
        let stripped = clean
            .strip_prefix("fn-mf-")
            .or_else(|| clean.strip_prefix("sha256:"))
            .or_else(|| clean.strip_prefix("sha256-"))
            .unwrap_or(clean);

        if stripped.len() != MANIFEST_ID_HEX_LEN {
            return Err(ContentError::InvalidContentId(format!(
                "Invalid ManifestId hex length: expected {}, got {}",
                MANIFEST_ID_HEX_LEN,
                stripped.len()
            )));
        }

        let mut bytes = [0u8; MANIFEST_ID_BYTES_LEN];
        hex::decode_to_slice(stripped, &mut bytes).map_err(|e| {
            ContentError::InvalidContentId(format!("Hex decoding failed for ManifestId: {}", e))
        })?;

        Ok(Self(bytes))
    }
}

impl fmt::Display for ManifestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl fmt::Debug for ManifestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ManifestId({})", self.to_hex())
    }
}

impl FromStr for ManifestId {
    type Err = ContentError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_hex(s)
    }
}

impl Serialize for ManifestId {
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

impl<'de> Deserialize<'de> for ManifestId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            ManifestId::from_hex(&s).map_err(serde::de::Error::custom)
        } else {
            let bytes = <[u8; MANIFEST_ID_BYTES_LEN]>::deserialize(deserializer)?;
            Ok(ManifestId::from_bytes(bytes))
        }
    }
}
