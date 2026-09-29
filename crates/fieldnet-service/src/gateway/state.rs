use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

use super::error::GatewayError;

/// Lifecycle states of the `LocalWebGateway`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum GatewayState {
    /// Gateway is not running and listener is closed.
    Stopped,
    /// Gateway is initializing internal resources.
    Starting,
    /// Gateway is binding to configured TCP socket.
    Binding,
    /// Gateway is actively serving incoming HTTP requests.
    Running,
    /// Gateway encountered a transient error or degraded network interface.
    Degraded,
    /// Gateway is gracefully draining active connections.
    Stopping,
}

impl fmt::Display for GatewayState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stopped => write!(f, "STOPPED"),
            Self::Starting => write!(f, "STARTING"),
            Self::Binding => write!(f, "BINDING"),
            Self::Running => write!(f, "RUNNING"),
            Self::Degraded => write!(f, "DEGRADED"),
            Self::Stopping => write!(f, "STOPPING"),
        }
    }
}

impl GatewayState {
    /// Validates whether a transition from `self` to `target` is permitted.
    pub fn can_transition_to(&self, target: GatewayState) -> bool {
        match (self, target) {
            (Self::Stopped, Self::Starting) => true,
            (Self::Starting, Self::Binding) => true,
            (Self::Starting, Self::Stopped) => true,
            (Self::Binding, Self::Running) => true,
            (Self::Binding, Self::Degraded) => true,
            (Self::Binding, Self::Stopping) => true,
            (Self::Running, Self::Degraded) => true,
            (Self::Running, Self::Stopping) => true,
            (Self::Degraded, Self::Running) => true,
            (Self::Degraded, Self::Stopping) => true,
            (Self::Stopping, Self::Stopped) => true,
            // Re-entrant / idempotency transitions
            (s1, s2) if *s1 == s2 => true,
            _ => false,
        }
    }
}

/// Thread-safe state tracker for the `LocalWebGateway`.
#[derive(Clone, Debug)]
pub struct GatewayStateMachine {
    state: Arc<RwLock<GatewayState>>,
    active_requests: Arc<AtomicUsize>,
    active_connections: Arc<AtomicUsize>,
}

impl Default for GatewayStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl GatewayStateMachine {
    /// Creates a new state machine initialized to `Stopped`.
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(GatewayState::Stopped)),
            active_requests: Arc::new(AtomicUsize::new(0)),
            active_connections: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Returns the current state of the gateway.
    pub async fn current(&self) -> GatewayState {
        *self.state.read().await
    }

    /// Attempts to transition to the target state.
    /// Returns `Err(GatewayError::InvalidStateTransition)` if the transition is illegal.
    pub async fn transition(&self, target: GatewayState) -> Result<(), GatewayError> {
        let mut guard = self.state.write().await;
        if guard.can_transition_to(target) {
            tracing::info!("LocalWebGateway state transition: {} -> {}", *guard, target);
            *guard = target;
            Ok(())
        } else {
            Err(GatewayError::InvalidStateTransition {
                from: guard.to_string(),
                to: target.to_string(),
            })
        }
    }

    /// Increments and returns active connection count.
    pub fn inc_connections(&self) -> usize {
        self.active_connections.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Decrements and returns active connection count.
    pub fn dec_connections(&self) -> usize {
        self.active_connections
            .fetch_sub(1, Ordering::SeqCst)
            .saturating_sub(1)
    }

    /// Returns the current active connection count.
    pub fn connections(&self) -> usize {
        self.active_connections.load(Ordering::SeqCst)
    }

    /// Increments and returns active in-flight request count.
    pub fn inc_requests(&self) -> usize {
        self.active_requests.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Decrements and returns active in-flight request count.
    pub fn dec_requests(&self) -> usize {
        self.active_requests
            .fetch_sub(1, Ordering::SeqCst)
            .saturating_sub(1)
    }

    /// Returns the current active in-flight request count.
    pub fn requests(&self) -> usize {
        self.active_requests.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_valid_lifecycle_transitions() {
        let sm = GatewayStateMachine::new();
        assert_eq!(sm.current().await, GatewayState::Stopped);

        sm.transition(GatewayState::Starting).await.unwrap();
        assert_eq!(sm.current().await, GatewayState::Starting);

        sm.transition(GatewayState::Binding).await.unwrap();
        assert_eq!(sm.current().await, GatewayState::Binding);

        sm.transition(GatewayState::Running).await.unwrap();
        assert_eq!(sm.current().await, GatewayState::Running);

        sm.transition(GatewayState::Degraded).await.unwrap();
        assert_eq!(sm.current().await, GatewayState::Degraded);

        sm.transition(GatewayState::Running).await.unwrap();
        assert_eq!(sm.current().await, GatewayState::Running);

        sm.transition(GatewayState::Stopping).await.unwrap();
        assert_eq!(sm.current().await, GatewayState::Stopping);

        sm.transition(GatewayState::Stopped).await.unwrap();
        assert_eq!(sm.current().await, GatewayState::Stopped);
    }

    #[tokio::test]
    async fn test_invalid_lifecycle_transition_fails_safely() {
        let sm = GatewayStateMachine::new();
        // Cannot jump directly from Stopped to Running
        let err = sm.transition(GatewayState::Running).await.unwrap_err();
        assert!(matches!(err, GatewayError::InvalidStateTransition { .. }));
        assert_eq!(sm.current().await, GatewayState::Stopped);
    }
}
