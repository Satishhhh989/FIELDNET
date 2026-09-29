use fieldnet_core::Timestamp;
use fieldnet_service::{ServiceHealth, ServiceState};

#[test]
fn test_valid_service_state_transitions() {
    let mut state = ServiceState::Discovered;

    // Discovered -> Authenticated
    assert!(state.can_transition_to(ServiceState::Authenticated));
    assert!(state.transition_to(ServiceState::Authenticated).is_ok());
    assert_eq!(state, ServiceState::Authenticated);

    // Authenticated -> Available
    assert!(state.can_transition_to(ServiceState::Available));
    assert!(state.transition_to(ServiceState::Available).is_ok());
    assert_eq!(state, ServiceState::Available);

    // Available -> Degraded
    assert!(state.can_transition_to(ServiceState::Degraded));
    assert!(state.transition_to(ServiceState::Degraded).is_ok());
    assert_eq!(state, ServiceState::Degraded);

    // Degraded -> Available (recovered)
    assert!(state.can_transition_to(ServiceState::Available));
    assert!(state.transition_to(ServiceState::Available).is_ok());
    assert_eq!(state, ServiceState::Available);

    // Available -> Unavailable (peer drop)
    assert!(state.can_transition_to(ServiceState::Unavailable));
    assert!(state.transition_to(ServiceState::Unavailable).is_ok());
    assert_eq!(state, ServiceState::Unavailable);

    // Unavailable -> Available (peer reconnect)
    assert!(state.can_transition_to(ServiceState::Available));
    assert!(state.transition_to(ServiceState::Available).is_ok());
    assert_eq!(state, ServiceState::Available);

    // Available -> Expired (TTL expired)
    assert!(state.can_transition_to(ServiceState::Expired));
    assert!(state.transition_to(ServiceState::Expired).is_ok());
    assert_eq!(state, ServiceState::Expired);

    // Expired -> Removed
    assert!(state.can_transition_to(ServiceState::Removed));
    assert!(state.transition_to(ServiceState::Removed).is_ok());
    assert_eq!(state, ServiceState::Removed);
}

#[test]
fn test_invalid_service_state_transitions() {
    // Cannot jump from Discovered directly to Available
    let mut s1 = ServiceState::Discovered;
    assert!(!s1.can_transition_to(ServiceState::Available));
    assert!(s1.transition_to(ServiceState::Available).is_err());

    // Cannot transition from Removed (terminal state) to anything
    let mut s2 = ServiceState::Removed;
    assert!(!s2.can_transition_to(ServiceState::Available));
    assert!(!s2.can_transition_to(ServiceState::Discovered));
    assert!(s2.transition_to(ServiceState::Available).is_err());

    // Cannot jump from Authenticated to Expired directly
    let mut s3 = ServiceState::Authenticated;
    assert!(!s3.can_transition_to(ServiceState::Expired));
    assert!(s3.transition_to(ServiceState::Expired).is_err());
}

#[test]
fn test_service_health_telemetry_and_failure_degradation() {
    let mut health = ServiceHealth::new(ServiceState::Available);
    assert!(health.is_callable());
    assert_eq!(health.reliability_permille, 1000);

    let now = Timestamp::now();

    // 1. Record success
    health.record_success(25, now);
    assert_eq!(health.observed_rtt_ms, 25);
    assert_eq!(health.moving_avg_latency_ms, 25);
    assert_eq!(health.consecutive_failures, 0);

    // Record another success
    health.record_success(50, now);
    assert_eq!(health.observed_rtt_ms, 50);
    // (25 * 4 + 50) / 5 = 150 / 5 = 30
    assert_eq!(health.moving_avg_latency_ms, 30);

    // 2. First failure -> Transitions to Degraded
    let transition1 = health.record_failure(now, 3);
    assert_eq!(transition1, Some(ServiceState::Degraded));
    assert_eq!(health.state, ServiceState::Degraded);
    assert!(health.is_callable()); // Degraded is still callable
    assert_eq!(health.consecutive_failures, 1);

    // 3. Second failure -> Remains Degraded
    let transition2 = health.record_failure(now, 3);
    assert_eq!(transition2, None);
    assert_eq!(health.state, ServiceState::Degraded);
    assert_eq!(health.consecutive_failures, 2);

    // 4. Third failure -> Transitions to Unavailable
    let transition3 = health.record_failure(now, 3);
    assert_eq!(transition3, Some(ServiceState::Unavailable));
    assert_eq!(health.state, ServiceState::Unavailable);
    assert!(!health.is_callable()); // Unavailable is NOT callable
    assert_eq!(health.consecutive_failures, 3);

    // 5. Success recovers health back to Available
    health.record_success(15, now);
    assert_eq!(health.consecutive_failures, 0);
}
