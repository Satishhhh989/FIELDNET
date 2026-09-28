//! Distributed Content Transfer Engine & Resumable Transfers (Phase 07 Build 04).
//!
//! Provides the central `TransferManager`, `TransferSession` state machine,
//! sliding window chunk scheduling with backpressure, persistent progress checkpointing,
//! provider-side authorized streaming, chunk cryptographic verification,
//! and resumable content transfers surviving network drops and process crashes.

pub mod manager;
pub mod persistence;
pub mod protocol;
pub mod provider;
pub mod scheduler;
pub mod session;
pub mod state;

pub use manager::{TransferConfig, TransferManager};
pub use persistence::TransferPersistence;
pub use protocol::{
    AckStatus, ContentAck, ContentChunk, ContentRequest, ContentResponse, TransferCancel,
    TransferComplete, TransferError, TransferErrorCode, TransferId, TransferResponseStatus,
};
pub use provider::{ProviderResponse, TransferProviderService};
pub use scheduler::{ChunkScheduler, SchedulerConfig};
pub use session::TransferSession;
pub use state::TransferState;
