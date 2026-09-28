use fieldnet_core::{MessageId, Timestamp};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum AckStatus {
    Sent = 1,
    Forwarded = 2,
    Received = 3,
    Processed = 4,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct AckPayload {
    pub message_id: MessageId,
    pub status: AckStatus,
    pub timestamp: Timestamp,
    pub note: Option<String>,
}

impl AckPayload {
    pub fn new_received(message_id: MessageId) -> Self {
        Self {
            message_id,
            status: AckStatus::Received,
            timestamp: Timestamp::now(),
            note: None,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}
