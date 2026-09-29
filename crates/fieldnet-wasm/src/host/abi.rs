use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Authoritative version of the FIELDNET WASM Host Environment API.
pub const FIELDNET_HOST_API_VERSION: i32 = 1;

/// Maximum payload size permitted for incoming guest host-call requests (10 MiB).
pub const MAX_REQUEST_PAYLOAD_BYTES: usize = 10 * 1024 * 1024;

/// Maximum payload size permitted for outgoing host-call responses (10 MiB).
pub const MAX_RESPONSE_PAYLOAD_BYTES: usize = 10 * 1024 * 1024;

/// Maximum length of identifiers (ApplicationId, ContentId, ServiceId, Topic) to prevent memory exhaustion.
pub const MAX_ID_LENGTH: usize = 256;

/// Operation codes used by the unified `host_call` entrypoint.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(i32)]
pub enum HostOperation {
    StoragePut = 1,
    StorageGet = 2,
    StorageHas = 3,
    StorageDelete = 4,
    ServiceResolve = 5,
    ServiceDescribe = 6,
    EventPublish = 7,
    NetworkSend = 8,
}

impl HostOperation {
    pub fn from_i32(val: i32) -> Option<Self> {
        match val {
            1 => Some(Self::StoragePut),
            2 => Some(Self::StorageGet),
            3 => Some(Self::StorageHas),
            4 => Some(Self::StorageDelete),
            5 => Some(Self::ServiceResolve),
            6 => Some(Self::ServiceDescribe),
            7 => Some(Self::EventPublish),
            8 => Some(Self::NetworkSend),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::StoragePut => "storage.put",
            Self::StorageGet => "storage.get",
            Self::StorageHas => "storage.has",
            Self::StorageDelete => "storage.delete",
            Self::ServiceResolve => "service.resolve",
            Self::ServiceDescribe => "service.describe",
            Self::EventPublish => "event.publish",
            Self::NetworkSend => "network.send",
        }
    }
}

// ---------------------------------------------------------------------------
// Storage ABI Models
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct StoragePutRequest {
    pub data: Vec<u8>,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct StoragePutResponse {
    pub content_id: String,
    pub size_bytes: u64,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct StorageGetRequest {
    pub content_id: String,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct StorageGetResponse {
    pub content_id: String,
    pub data: Vec<u8>,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct StorageHasRequest {
    pub content_id: String,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct StorageHasResponse {
    pub content_id: String,
    pub exists: bool,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct StorageDeleteRequest {
    pub content_id: String,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct StorageDeleteResponse {
    pub content_id: String,
    pub deleted: bool,
}

// ---------------------------------------------------------------------------
// Service ABI Models
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ServiceResolveRequest {
    pub service_id: String,
    #[serde(default)]
    pub min_version: Option<String>,
    #[serde(default)]
    pub required_capabilities: Option<Vec<String>>,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ServiceDescribeRequest {
    pub service_id: String,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ServiceDescription {
    pub service_id: String,
    pub instance_id: String,
    pub provider_node: String,
    pub version: String,
    pub capabilities: Vec<String>,
    pub protocol: String,
    pub endpoint: String,
    pub status: String,
}

// ---------------------------------------------------------------------------
// Event ABI Models
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct EventPublishRequest {
    pub event_type: String,
    pub payload: Vec<u8>,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct EventPublishResponse {
    pub canonical_topic: String,
    pub published: bool,
}

// ---------------------------------------------------------------------------
// Network ABI Models
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct NetworkSendRequest {
    pub destination: String,
    #[serde(default = "default_message_type")]
    pub message_type: String,
    pub payload: Vec<u8>,
    #[serde(default)]
    pub metadata: Option<HashMap<String, String>>,
    #[serde(default)]
    pub ttl: Option<u8>,
    #[serde(default)]
    pub expires_in_ms: Option<u64>,
}

fn default_message_type() -> String {
    "DATA".to_string()
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct NetworkSendResponse {
    pub message_id: String,
    pub queued: bool,
}
