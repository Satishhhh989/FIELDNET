use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

/// Observability metrics for the `ServiceResolver`.
#[derive(Debug, Default)]
pub struct ResolverMetrics {
    pub resolutions_total: AtomicU64,
    pub resolutions_success: AtomicU64,
    pub resolutions_failed: AtomicU64,
    pub failures_not_found: AtomicU64,
    pub failures_version: AtomicU64,
    pub failures_capability: AtomicU64,
    pub failures_protocol: AtomicU64,
    pub failures_unauthorized: AtomicU64,
    pub failures_unreachable: AtomicU64,
    pub failures_unavailable: AtomicU64,
}

/// Point-in-time immutable snapshot of resolver metrics.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
pub struct ResolverMetricsSnapshot {
    pub resolutions_total: u64,
    pub resolutions_success: u64,
    pub resolutions_failed: u64,
    pub failures_not_found: u64,
    pub failures_version: u64,
    pub failures_capability: u64,
    pub failures_protocol: u64,
    pub failures_unauthorized: u64,
    pub failures_unreachable: u64,
    pub failures_unavailable: u64,
}

impl ResolverMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inc_total(&self) {
        self.resolutions_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_success(&self) {
        self.resolutions_success.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_failed(&self) {
        self.resolutions_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_not_found(&self) {
        self.failures_not_found.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_version(&self) {
        self.failures_version.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_capability(&self) {
        self.failures_capability.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_protocol(&self) {
        self.failures_protocol.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_unauthorized(&self) {
        self.failures_unauthorized.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_unreachable(&self) {
        self.failures_unreachable.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_unavailable(&self) {
        self.failures_unavailable.fetch_add(1, Ordering::Relaxed);
    }

    /// Captures an immutable snapshot of all counters.
    pub fn snapshot(&self) -> ResolverMetricsSnapshot {
        ResolverMetricsSnapshot {
            resolutions_total: self.resolutions_total.load(Ordering::Relaxed),
            resolutions_success: self.resolutions_success.load(Ordering::Relaxed),
            resolutions_failed: self.resolutions_failed.load(Ordering::Relaxed),
            failures_not_found: self.failures_not_found.load(Ordering::Relaxed),
            failures_version: self.failures_version.load(Ordering::Relaxed),
            failures_capability: self.failures_capability.load(Ordering::Relaxed),
            failures_protocol: self.failures_protocol.load(Ordering::Relaxed),
            failures_unauthorized: self.failures_unauthorized.load(Ordering::Relaxed),
            failures_unreachable: self.failures_unreachable.load(Ordering::Relaxed),
            failures_unavailable: self.failures_unavailable.load(Ordering::Relaxed),
        }
    }
}
