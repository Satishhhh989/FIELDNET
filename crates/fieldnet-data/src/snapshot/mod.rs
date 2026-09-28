//! Snapshot, Log Compaction, Causal Stability, and Disaster Recovery Subsystem.

pub mod config;
pub mod model;

pub use config::CompactionConfig;
pub use model::{CompactionRecord, CompactionSafetyReport, DocumentSnapshot, SnapshotId};
