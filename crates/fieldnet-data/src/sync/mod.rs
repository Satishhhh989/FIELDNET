//! FIELDNET Phase 06 Peer-to-Peer Synchronization Subsystem.
//!
//! Submodules:
//! - `protocol`: Wire framing, SyncMessage types, StateSummary, DeltaRequest/Response, SyncAck.
//! - `session`: Synchronization session state machine and tracking.
//! - `authorizer`: Sovereign document capability and authorization checks.
//! - `engine`: SyncEngine coordinator and SyncTransport trait.

pub mod authorizer;
pub mod engine;
pub mod protocol;
pub mod session;

pub use authorizer::{DefaultDocumentAuthorizer, DocumentAuthorizer};
pub use engine::{SyncEngine, SyncEngineConfig, SyncTransport};
pub use protocol::{DocumentSummaryEntry, SyncAckStatus, SyncMessage};
pub use session::{SyncSession, SyncSessionState};
