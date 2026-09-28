use std::fmt;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use fieldnet_core::{NodeId, Timestamp};
use rand::RngCore;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::error::DataError;
use crate::schema::{CURRENT_SCHEMA_VERSION, MAX_DOCUMENT_SIZE, MAX_DOCUMENT_TYPE_LEN};

/// A globally unique, stable, 32-byte logical document identifier.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct DocumentId(pub [u8; 32]);

impl DocumentId {
    /// Generates a new, cryptographically robust, stable document identifier.
    /// Combines random entropy, high-resolution physical time, and BLAKE3 hashing.
    pub fn generate() -> Self {
        let mut entropy = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut entropy);

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();

        let mut hasher = blake3::Hasher::new();
        hasher.update(&entropy);
        hasher.update(&nanos.to_le_bytes());
        let hash = hasher.finalize();

        let mut id_bytes = [0u8; 32];
        id_bytes.copy_from_slice(hash.as_bytes());
        Self(id_bytes)
    }

    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    pub fn from_hex(s: &str) -> Result<Self, DataError> {
        let bytes = hex::decode(s)
            .map_err(|e| DataError::InvalidDocumentId(format!("Hex decode error: {}", e)))?;
        if bytes.len() != 32 {
            return Err(DataError::InvalidDocumentId(format!(
                "Invalid document ID byte length: expected 32, got {}",
                bytes.len()
            )));
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(Self(arr))
    }
}

impl fmt::Display for DocumentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl FromStr for DocumentId {
    type Err = DataError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_hex(s)
    }
}

/// Canonical Document Model for FIELDNET Local-First Data.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Document {
    /// Stable logical identifier (survives network transport, device restart, and replication).
    pub document_id: DocumentId,

    /// Generic document type identifier (e.g., "note", "report", "incident_marker").
    pub document_type: String,

    /// Schema version for application-level data parsing.
    pub schema_version: u32,

    /// Sovereign NodeId of original author or local creator.
    pub owner_node_id: NodeId,

    /// Node-local monotonic revision version counter.
    pub version: u64,

    /// Physical UTC creation timestamp.
    pub created_at: Timestamp,

    /// Physical UTC last-modified timestamp.
    pub updated_at: Timestamp,

    /// Logical deletion flag (tombstone marker for CRDT/sync compatibility).
    pub deleted: bool,

    /// Serialized document state payload (e.g. JSON or binary representation).
    pub state: Vec<u8>,
}

impl Document {
    /// Creates a new valid Document instance with a newly generated stable ID.
    pub fn new(
        document_type: impl Into<String>,
        owner_node_id: NodeId,
        state: Vec<u8>,
    ) -> Result<Self, DataError> {
        let doc_type = document_type.into();
        let now = Timestamp::now();

        let doc = Self {
            document_id: DocumentId::generate(),
            document_type: doc_type,
            schema_version: CURRENT_SCHEMA_VERSION,
            owner_node_id,
            version: 1,
            created_at: now,
            updated_at: now,
            deleted: false,
            state,
        };

        doc.validate()?;
        Ok(doc)
    }

    /// Convenience builder to create a document from any serde-serializable value as JSON.
    pub fn from_json<T: Serialize>(
        document_type: impl Into<String>,
        owner_node_id: NodeId,
        value: &T,
    ) -> Result<Self, DataError> {
        let state = serde_json::to_vec(value)?;
        Self::new(document_type, owner_node_id, state)
    }

    /// Validates the document against system invariants and resource limits.
    pub fn validate(&self) -> Result<(), DataError> {
        if self.document_type.trim().is_empty() {
            return Err(DataError::InvalidDocumentType(
                "Document type cannot be empty".to_string(),
            ));
        }

        if self.document_type.len() > MAX_DOCUMENT_TYPE_LEN {
            return Err(DataError::InvalidDocumentType(format!(
                "Document type length {} exceeds maximum allowed {}",
                self.document_type.len(),
                MAX_DOCUMENT_TYPE_LEN
            )));
        }

        // Ensure document_type contains valid identifier characters (alphanumeric, dot, underscore, dash)
        if !self
            .document_type
            .chars()
            .all(|c| c.is_alphanumeric() || c == '.' || c == '_' || c == '-')
        {
            return Err(DataError::InvalidDocumentType(format!(
                "Document type '{}' contains invalid characters",
                self.document_type
            )));
        }

        if self.state.len() > MAX_DOCUMENT_SIZE {
            return Err(DataError::DocumentTooLarge {
                size: self.state.len(),
                max: MAX_DOCUMENT_SIZE,
            });
        }

        Ok(())
    }

    /// Deserializes state as JSON into a typed structure.
    pub fn state_json<T: DeserializeOwned>(&self) -> Result<T, DataError> {
        serde_json::from_slice(&self.state).map_err(|e| {
            DataError::SerializationFailure(format!("Failed to deserialize state as JSON: {}", e))
        })
    }

    /// Returns state as UTF-8 string if valid.
    pub fn state_str(&self) -> Result<&str, DataError> {
        std::str::from_utf8(&self.state).map_err(|e| {
            DataError::SerializationFailure(format!("State is not valid UTF-8: {}", e))
        })
    }
}
