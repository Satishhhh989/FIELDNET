use fieldnet_core::{NodeId, Timestamp};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use crate::error::ContentError;
use crate::identity::ContentId;

/// Storage lifecycle state of a content object on local storage.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ContentStorageState {
    /// Object is currently being written to staging and has not yet been verified or committed.
    Staging,
    /// Object has been verified, committed, and is complete in local storage.
    Complete,
    /// Object failed integrity verification or disk corruption was detected.
    Corrupted,
    /// Content advertised by remote peers but not yet transferred.
    Discovered,
    /// Transfer session initiated for content.
    Requested,
    /// Incomplete transfer: some chunks received, awaiting remaining chunks.
    Partial,
    /// All chunks received and cryptographic root verified.
    Verified,
    /// Fully verified and ready for serving and application use.
    Available,
    /// Evicted from local storage under cache pressure (metadata retained).
    Evicted,
}

impl ContentStorageState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Staging => "STAGING",
            Self::Complete => "COMPLETE",
            Self::Corrupted => "CORRUPTED",
            Self::Discovered => "DISCOVERED",
            Self::Requested => "REQUESTED",
            Self::Partial => "PARTIAL",
            Self::Verified => "VERIFIED",
            Self::Available => "AVAILABLE",
            Self::Evicted => "EVICTED",
        }
    }
}

impl fmt::Display for ContentStorageState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for ContentStorageState {
    type Err = ContentError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_uppercase().as_str() {
            "STAGING" => Ok(Self::Staging),
            "COMPLETE" => Ok(Self::Complete),
            "CORRUPTED" => Ok(Self::Corrupted),
            "DISCOVERED" => Ok(Self::Discovered),
            "REQUESTED" => Ok(Self::Requested),
            "PARTIAL" => Ok(Self::Partial),
            "VERIFIED" => Ok(Self::Verified),
            "AVAILABLE" => Ok(Self::Available),
            "EVICTED" => Ok(Self::Evicted),
            other => Err(ContentError::InvalidState {
                expected: "STAGING | COMPLETE | CORRUPTED | ...".to_string(),
                actual: other.to_string(),
            }),
        }
    }
}

/// Strongly typed metadata for stored content.
///
/// Kept strictly separate from raw content bytes and filesystem paths.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentMetadata {
    /// Authoritative content identifier (SHA-256 digest).
    pub content_id: ContentId,
    /// Size in bytes of the content object.
    pub size_bytes: u64,
    /// Current local storage state.
    pub storage_state: ContentStorageState,
    /// Timestamp when content was first created/persisted locally.
    pub created_at: Timestamp,
    /// Timestamp of most recent metadata update.
    pub updated_at: Timestamp,
    /// Timestamp when content was last accessed/retrieved.
    pub last_accessed_at: Timestamp,
    /// Optional MIME type (e.g. "application/json", "image/png").
    pub mime_type: Option<String>,
    /// Schema or metadata version.
    pub version: u32,
    /// Node ID of the node that created or first introduced the content, if known.
    pub creator_node_id: Option<NodeId>,
    /// Whether content is pinned (protected from automated garbage collection and eviction).
    pub is_pinned: bool,
    /// User or application-defined custom key-value metadata.
    pub custom_metadata: HashMap<String, String>,
}

impl ContentMetadata {
    /// Constructs a new `ContentMetadata` record with default attributes for complete content.
    pub fn new_complete(
        content_id: ContentId,
        size_bytes: u64,
        mime_type: Option<String>,
        creator_node_id: Option<NodeId>,
    ) -> Self {
        let now = Timestamp::now();
        Self {
            content_id,
            size_bytes,
            storage_state: ContentStorageState::Complete,
            created_at: now,
            updated_at: now,
            last_accessed_at: now,
            mime_type,
            version: 1,
            creator_node_id,
            is_pinned: false,
            custom_metadata: HashMap::new(),
        }
    }
}
