pub mod connection;
pub mod discovery;
pub mod error;
pub mod framing;
pub mod session;

pub use connection::{ConnectionEvent, ConnectionManager, ConnectionState};
pub use discovery::{
    DiscoveredPeer, DiscoveryBeacon, DiscoveryConfig, DiscoveryService, DEFAULT_DISCOVERY_PORT,
    MULTICAST_GROUP, PROTOCOL_VERSION,
};
pub use error::NetworkError;
pub use framing::{read_frame, write_frame};
pub use session::{
    handshake_as_initiator, handshake_as_responder, HandshakeMessage, SecureSession, SessionRx,
    SessionTx,
};
