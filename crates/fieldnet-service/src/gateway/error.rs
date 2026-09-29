use std::net::SocketAddr;
use thiserror::Error;

use crate::error::ServiceError;

/// Gateway-specific errors encompassing HTTP parsing, security validation,
/// service dispatch, and runtime lifecycle events.
#[derive(Debug, Error)]
pub enum GatewayError {
    #[error("Gateway invalid state transition: from '{from}' to '{to}'")]
    InvalidStateTransition { from: String, to: String },

    #[error("Gateway listener bind error on {addr}: {source}")]
    BindError {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },

    #[error("Gateway server error: {0}")]
    ServerError(String),

    #[error("Service error: {0}")]
    Service(#[from] ServiceError),

    #[error("Content error: {0}")]
    Content(#[from] fieldnet_content::ContentError),

    #[error("Request body too large: {size} > {max} bytes")]
    PayloadTooLarge { size: usize, max: usize },

    #[error("Header size too large: {size} > {max} bytes")]
    HeaderTooLarge { size: usize, max: usize },

    #[error("URI too long: {len} > {max} bytes")]
    UriTooLong { len: usize, max: usize },

    #[error("Request timeout after {timeout_ms}ms")]
    Timeout { timeout_ms: u64 },

    #[error("Host header invalid or disallowed: '{host}'")]
    InvalidHost { host: String },

    #[error("Forbidden arbitrary proxy target: '{target}'")]
    ArbitraryProxyForbidden { target: String },

    #[error("Cross-origin request rejected: '{origin}'")]
    CorsRejected { origin: String },

    #[error("CSRF protection rejected request: missing required validation")]
    CsrfRejected,

    #[error("Session unauthorized: {reason}")]
    SessionUnauthorized { reason: String },

    #[error("Session forbidden: {reason}")]
    SessionForbidden { reason: String },

    #[error("Pairing rejected: {reason}")]
    PairingRejected { reason: String },

    #[error("Pairing attempt rate limit exceeded")]
    PairingRateLimited,

    #[error("Client disconnected")]
    ClientDisconnected,

    #[error("Connection limit exceeded: max {max}")]
    ConnectionLimitExceeded { max: usize },

    #[error("Concurrent request limit exceeded: max {max}")]
    RequestLimitExceeded { max: usize },

    #[error("Request execution indeterminate on state-changing operation: {0}")]
    ExecutionIndeterminate(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

impl GatewayError {
    /// Maps the error to an authoritative HTTP status code.
    pub fn status_code(&self) -> u16 {
        match self {
            Self::InvalidHost { .. } => 400,
            Self::ArbitraryProxyForbidden { .. } => 400,
            Self::SessionUnauthorized { .. } | Self::PairingRejected { .. } => 401,
            Self::CorsRejected { .. } | Self::CsrfRejected | Self::SessionForbidden { .. } => 403,
            Self::PayloadTooLarge { .. } => 413,
            Self::UriTooLong { .. } => 414,
            Self::HeaderTooLarge { .. } => 431,
            Self::ConnectionLimitExceeded { .. }
            | Self::RequestLimitExceeded { .. }
            | Self::PairingRateLimited => 429,
            Self::Timeout { .. } | Self::ExecutionIndeterminate(_) => 504,
            Self::ClientDisconnected => 499,
            Self::Service(ServiceError::ServiceNotFound(_))
            | Self::Service(ServiceError::InstanceNotFound(_)) => 404,
            Self::Service(ServiceError::Unauthorized { .. }) => 403,
            Self::Service(ServiceError::NoReachableProvider { .. })
            | Self::Service(ServiceError::AllProvidersUnavailable { .. }) => 503,
            Self::Service(ServiceError::InvalidQuery(_))
            | Self::Service(ServiceError::InvalidServiceId(_)) => 400,
            Self::Content(fieldnet_content::ContentError::ContentNotFound(_)) => 404,
            _ => 502,
        }
    }

    /// Returns a machine-readable error code string.
    pub fn error_code(&self) -> &'static str {
        match self {
            Self::InvalidHost { .. } => "INVALID_HOST",
            Self::ArbitraryProxyForbidden { .. } => "ARBITRARY_PROXY_FORBIDDEN",
            Self::CorsRejected { .. } => "CORS_REJECTED",
            Self::CsrfRejected => "CSRF_REJECTED",
            Self::SessionUnauthorized { .. } => "SESSION_UNAUTHORIZED",
            Self::SessionForbidden { .. } => "SESSION_FORBIDDEN",
            Self::PairingRejected { .. } => "PAIRING_REJECTED",
            Self::PairingRateLimited => "PAIRING_RATE_LIMITED",
            Self::PayloadTooLarge { .. } => "PAYLOAD_TOO_LARGE",
            Self::UriTooLong { .. } => "URI_TOO_LONG",
            Self::HeaderTooLarge { .. } => "HEADER_TOO_LARGE",
            Self::ConnectionLimitExceeded { .. } => "CONNECTION_LIMIT_EXCEEDED",
            Self::RequestLimitExceeded { .. } => "REQUEST_LIMIT_EXCEEDED",
            Self::Timeout { .. } => "GATEWAY_TIMEOUT",
            Self::ExecutionIndeterminate(_) => "EXECUTION_INDETERMINATE",
            Self::ClientDisconnected => "CLIENT_DISCONNECTED",
            Self::Service(ServiceError::ServiceNotFound(_)) => "SERVICE_NOT_FOUND",
            Self::Service(ServiceError::InstanceNotFound(_)) => "INSTANCE_NOT_FOUND",
            Self::Service(ServiceError::Unauthorized { .. }) => "UNAUTHORIZED",
            Self::Service(ServiceError::NoReachableProvider { .. }) => "NO_REACHABLE_PROVIDER",
            Self::Service(ServiceError::AllProvidersUnavailable { .. }) => {
                "ALL_PROVIDERS_UNAVAILABLE"
            }
            Self::Content(fieldnet_content::ContentError::ContentNotFound(_)) => {
                "CONTENT_NOT_FOUND"
            }
            _ => "GATEWAY_ERROR",
        }
    }
}
