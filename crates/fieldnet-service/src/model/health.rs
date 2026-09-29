use fieldnet_core::Timestamp;
use serde::{Deserialize, Serialize};

use super::state::ServiceState;

/// Telemetry tracking liveness, performance, and failure rates for a service instance.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ServiceHealth {
    /// Current authoritative lifecycle state.
    pub state: ServiceState,
    /// Count of consecutive failed calls.
    pub consecutive_failures: u32,
    /// Timestamp of most recent successful interaction.
    pub last_success_at: Option<Timestamp>,
    /// Timestamp of most recent failure.
    pub last_failure_at: Option<Timestamp>,
    /// Most recent observed round-trip latency in milliseconds.
    pub observed_rtt_ms: u32,
    /// Exponential moving average of latency in milliseconds.
    pub moving_avg_latency_ms: u32,
    /// Reliability score in permille (0 to 1000, where 1000 = 100% success).
    pub reliability_permille: u32,
}

impl ServiceHealth {
    /// Creates a new `ServiceHealth` instance with default metrics.
    pub fn new(state: ServiceState) -> Self {
        Self {
            state,
            consecutive_failures: 0,
            last_success_at: None,
            last_failure_at: None,
            observed_rtt_ms: 0,
            moving_avg_latency_ms: 0,
            reliability_permille: 1000, // Starts at 100% reliability
        }
    }

    /// Records a successful service interaction and updates moving averages.
    pub fn record_success(&mut self, latency_ms: u32, now: Timestamp) {
        self.consecutive_failures = 0;
        self.last_success_at = Some(now);
        self.observed_rtt_ms = latency_ms;

        // Update exponential moving average: 80% previous + 20% sample
        if self.moving_avg_latency_ms == 0 {
            self.moving_avg_latency_ms = latency_ms;
        } else {
            self.moving_avg_latency_ms =
                ((self.moving_avg_latency_ms as u64 * 4 + latency_ms as u64) / 5) as u32;
        }

        // Recover reliability score towards 1000
        self.reliability_permille = (self.reliability_permille + 50).min(1000);

        // If currently degraded, recover to Available
        if self.state == ServiceState::Degraded {
            let _ = self.state.transition_to(ServiceState::Available);
        }
    }

    /// Records an interaction failure, updating consecutive counts and penalizing reliability.
    ///
    /// Returns `Some(new_state)` if this failure triggered an automatic state transition.
    pub fn record_failure(
        &mut self,
        now: Timestamp,
        max_failures_before_unavailable: u32,
    ) -> Option<ServiceState> {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.last_failure_at = Some(now);

        // Penalize reliability score
        self.reliability_permille = self.reliability_permille.saturating_sub(150);

        let previous_state = self.state;

        if self.consecutive_failures >= max_failures_before_unavailable {
            if self.state != ServiceState::Unavailable {
                let _ = self.state.transition_to(ServiceState::Unavailable);
            }
        } else if self.state == ServiceState::Available {
            let _ = self.state.transition_to(ServiceState::Degraded);
        }

        if self.state != previous_state {
            Some(self.state)
        } else {
            None
        }
    }

    /// Returns true if the service instance is currently callable.
    pub fn is_callable(&self) -> bool {
        self.state.is_callable()
    }
}
