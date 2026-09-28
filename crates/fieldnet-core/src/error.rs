use thiserror::Error;

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum CoreError {
    #[error("Invalid identifier: {0}")]
    InvalidIdentifier(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Hex decoding error: {0}")]
    HexDecode(String),

    #[error("Internal core error: {0}")]
    Internal(String),
}
