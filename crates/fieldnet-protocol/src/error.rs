use thiserror::Error;

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    #[error("Protocol version mismatch: expected {expected}, got {actual}")]
    VersionMismatch { expected: u8, actual: u8 },

    #[error("Malformed message envelope: {0}")]
    MalformedMessage(String),

    #[error("Invalid digital signature: {0}")]
    InvalidSignature(String),

    #[error(
        "Message has expired: created_at {created_at}, expires_at {expires_at}, current {now}"
    )]
    ExpiredMessage {
        created_at: u64,
        expires_at: u64,
        now: u64,
    },

    #[error("TTL exhausted (ttl = 0)")]
    TtlExhausted,

    #[error("Duplicate message detected: {0}")]
    DuplicateMessage(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Deserialization error: {0}")]
    Deserialization(String),

    #[error("Unauthorized sender: {0}")]
    UnauthorizedSender(String),

    #[error("Core error: {0}")]
    Core(String),
}
