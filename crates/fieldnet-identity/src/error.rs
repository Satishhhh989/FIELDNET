use thiserror::Error;

#[derive(Error, Debug)]
pub enum IdentityError {
    #[error("Cryptographic signature error: {0}")]
    Signature(String),

    #[error("I/O error during identity persistence: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Invalid key format: {0}")]
    InvalidKey(String),

    #[error("Core error: {0}")]
    Core(#[from] fieldnet_core::CoreError),
}
