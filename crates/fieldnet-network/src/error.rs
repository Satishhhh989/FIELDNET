use thiserror::Error;

#[derive(Error, Debug)]
pub enum NetworkError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Handshake authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("Cryptographic key agreement error: {0}")]
    Crypto(String),

    #[error("AEAD encryption/decryption failed: {0}")]
    Aead(String),

    #[error("Protocol error: {0}")]
    Protocol(#[from] fieldnet_protocol::ProtocolError),

    #[error("Identity error: {0}")]
    Identity(#[from] fieldnet_identity::IdentityError),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Connection closed unexpectedly")]
    ConnectionClosed,

    #[error("Invalid state transition: {0}")]
    InvalidState(String),

    #[error("Peer not found: {0}")]
    PeerNotFound(String),
}
