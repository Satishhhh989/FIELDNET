use crate::error::ServiceError;
use serde::{Deserialize, Serialize};

/// Service communication protocol dialect.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum ServiceProtocol {
    /// Internal multi-hop binary RPC over FIELDNET overlay transport (MessageType::Service = 8).
    NativeFieldnetRpc = 1,
    /// HTTP/1.1 or HTTP/2 protocol.
    Http1 = 2,
    /// Full-duplex WebSocket stream.
    WebSocket = 3,
    /// Phase 07 content-addressed streaming feed.
    ContentStream = 4,
}

impl ServiceProtocol {
    /// Returns the wire protocol byte tag.
    pub const fn as_u8(&self) -> u8 {
        *self as u8
    }

    /// Reconstructs a `ServiceProtocol` from its raw wire byte.
    pub fn from_u8(val: u8) -> Result<Self, ServiceError> {
        match val {
            1 => Ok(Self::NativeFieldnetRpc),
            2 => Ok(Self::Http1),
            3 => Ok(Self::WebSocket),
            4 => Ok(Self::ContentStream),
            other => Err(ServiceError::InvalidEndpoint(format!(
                "Unknown service protocol tag: {}",
                other
            ))),
        }
    }
}

impl std::fmt::Display for ServiceProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NativeFieldnetRpc => write!(f, "NativeFieldnetRpc"),
            Self::Http1 => write!(f, "Http1"),
            Self::WebSocket => write!(f, "WebSocket"),
            Self::ContentStream => write!(f, "ContentStream"),
        }
    }
}

/// Transport-independent reachability endpoint for a service instance.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub enum ServiceEndpoint {
    /// Routed strictly across the FIELDNET mesh overlay to `provider_node_id`.
    /// No physical port or IP socket is exposed to the local host network.
    InternalRpc,
    /// Listening on a local TCP port on the provider host (e.g. localhost:8081).
    LocalPort(u16),
    /// Listening on a local Unix domain socket path.
    LocalSocket(String),
    /// Mounted as a virtual path on the Local Web Gateway (e.g. "/services/search").
    GatewayPath(String),
}

impl ServiceEndpoint {
    /// Produces a deterministic canonical byte representation for cryptographic signing.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        match self {
            Self::InternalRpc => {
                bytes.push(1u8); // tag
            }
            Self::LocalPort(port) => {
                bytes.push(2u8);
                bytes.extend_from_slice(&port.to_be_bytes());
            }
            Self::LocalSocket(path) => {
                bytes.push(3u8);
                let path_bytes = path.as_bytes();
                bytes.extend_from_slice(&(path_bytes.len() as u16).to_be_bytes());
                bytes.extend_from_slice(path_bytes);
            }
            Self::GatewayPath(path) => {
                bytes.push(4u8);
                let path_bytes = path.as_bytes();
                bytes.extend_from_slice(&(path_bytes.len() as u16).to_be_bytes());
                bytes.extend_from_slice(path_bytes);
            }
        }
        bytes
    }
}
