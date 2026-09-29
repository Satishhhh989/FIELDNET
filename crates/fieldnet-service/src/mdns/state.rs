use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

/// Lifecycle states of the FIELDNET mDNS / DNS-SD subsystem.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MdnsState {
    /// Subsystem is disabled in configuration.
    Disabled,
    /// Subsystem is initializing sockets and network interfaces.
    Starting,
    /// Subsystem is actively responding to queries and announcing services.
    Running,
    /// Subsystem encountered network/multicast degradation but continues best-effort.
    Degraded,
    /// Subsystem is sending goodbye packets and withdrawing advertisements.
    Stopping,
    /// Subsystem is completely stopped.
    Stopped,
}

impl MdnsState {
    const CODE_DISABLED: u8 = 0;
    const CODE_STARTING: u8 = 1;
    const CODE_RUNNING: u8 = 2;
    const CODE_DEGRADED: u8 = 3;
    const CODE_STOPPING: u8 = 4;
    const CODE_STOPPED: u8 = 5;

    fn to_code(self) -> u8 {
        match self {
            Self::Disabled => Self::CODE_DISABLED,
            Self::Starting => Self::CODE_STARTING,
            Self::Running => Self::CODE_RUNNING,
            Self::Degraded => Self::CODE_DEGRADED,
            Self::Stopping => Self::CODE_STOPPING,
            Self::Stopped => Self::CODE_STOPPED,
        }
    }

    fn from_code(code: u8) -> Self {
        match code {
            Self::CODE_DISABLED => Self::Disabled,
            Self::CODE_STARTING => Self::Starting,
            Self::CODE_RUNNING => Self::Running,
            Self::CODE_DEGRADED => Self::Degraded,
            Self::CODE_STOPPING => Self::Stopping,
            _ => Self::Stopped,
        }
    }
}

/// Thread-safe state machine managing the mDNS subsystem lifecycle and metrics.
#[derive(Debug)]
pub struct MdnsStateMachine {
    state: AtomicU8,
    active_announcements: AtomicUsize,
    queries_received: AtomicUsize,
    responses_sent: AtomicUsize,
    errors_encountered: AtomicUsize,
}

impl Default for MdnsStateMachine {
    fn default() -> Self {
        Self::new(MdnsState::Stopped)
    }
}

impl MdnsStateMachine {
    pub fn new(initial_state: MdnsState) -> Self {
        Self {
            state: AtomicU8::new(initial_state.to_code()),
            active_announcements: AtomicUsize::new(0),
            queries_received: AtomicUsize::new(0),
            responses_sent: AtomicUsize::new(0),
            errors_encountered: AtomicUsize::new(0),
        }
    }

    pub fn current(&self) -> MdnsState {
        MdnsState::from_code(self.state.load(Ordering::Acquire))
    }

    pub fn is_running(&self) -> bool {
        matches!(self.current(), MdnsState::Running | MdnsState::Degraded)
    }

    pub fn transition(&self, target: MdnsState) -> Result<(), (MdnsState, MdnsState)> {
        let mut current_code = self.state.load(Ordering::Acquire);
        loop {
            let current = MdnsState::from_code(current_code);
            let valid = match (current, target) {
                // From Stopped or Disabled, can start or disable
                (MdnsState::Stopped, MdnsState::Starting) => true,
                (MdnsState::Stopped, MdnsState::Disabled) => true,
                (MdnsState::Disabled, MdnsState::Starting) => true,
                // From Starting, can reach Running, Degraded, or Stopped (if failed)
                (MdnsState::Starting, MdnsState::Running) => true,
                (MdnsState::Starting, MdnsState::Degraded) => true,
                (MdnsState::Starting, MdnsState::Stopped) => true,
                // From Running, can degrade or stop
                (MdnsState::Running, MdnsState::Degraded) => true,
                (MdnsState::Running, MdnsState::Stopping) => true,
                (MdnsState::Running, MdnsState::Stopped) => true,
                // From Degraded, can recover to Running or stop
                (MdnsState::Degraded, MdnsState::Running) => true,
                (MdnsState::Degraded, MdnsState::Stopping) => true,
                (MdnsState::Degraded, MdnsState::Stopped) => true,
                // From Stopping, can reach Stopped
                (MdnsState::Stopping, MdnsState::Stopped) => true,
                // Idempotent transitions
                (a, b) if a == b => true,
                _ => false,
            };

            if !valid {
                return Err((current, target));
            }

            match self.state.compare_exchange_weak(
                current_code,
                target.to_code(),
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(actual) => current_code = actual,
            }
        }
    }

    pub fn increment_queries(&self) {
        self.queries_received.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_responses(&self) {
        self.responses_sent.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_errors(&self) {
        self.errors_encountered.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_active_announcements(&self, count: usize) {
        self.active_announcements.store(count, Ordering::Relaxed);
    }

    pub fn queries_count(&self) -> usize {
        self.queries_received.load(Ordering::Relaxed)
    }

    pub fn responses_count(&self) -> usize {
        self.responses_sent.load(Ordering::Relaxed)
    }

    pub fn errors_count(&self) -> usize {
        self.errors_encountered.load(Ordering::Relaxed)
    }

    pub fn active_announcements_count(&self) -> usize {
        self.active_announcements.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_state_transitions() {
        let sm = MdnsStateMachine::new(MdnsState::Stopped);
        assert_eq!(sm.current(), MdnsState::Stopped);

        assert!(sm.transition(MdnsState::Starting).is_ok());
        assert_eq!(sm.current(), MdnsState::Starting);

        assert!(sm.transition(MdnsState::Running).is_ok());
        assert_eq!(sm.current(), MdnsState::Running);
        assert!(sm.is_running());

        assert!(sm.transition(MdnsState::Degraded).is_ok());
        assert_eq!(sm.current(), MdnsState::Degraded);
        assert!(sm.is_running());

        assert!(sm.transition(MdnsState::Stopping).is_ok());
        assert_eq!(sm.current(), MdnsState::Stopping);

        assert!(sm.transition(MdnsState::Stopped).is_ok());
        assert_eq!(sm.current(), MdnsState::Stopped);
        assert!(!sm.is_running());
    }

    #[test]
    fn test_invalid_state_transition() {
        let sm = MdnsStateMachine::new(MdnsState::Stopped);
        assert!(sm.transition(MdnsState::Running).is_err());
    }
}
