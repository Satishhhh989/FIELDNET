use serde::{Deserialize, Serialize};
use std::fmt;

/// Guest-visible error codes returned across the WASM host boundary.
///
/// Positive integer values are reserved for successful operations (e.g. byte counts).
/// Negative integer values represent structured, non-leaking guest error conditions.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[repr(i32)]
pub enum GuestErrorCode {
    InvalidRequest = -1,
    PermissionDenied = -2,
    NotFound = -3,
    InvalidId = -4,
    QuotaExceeded = -5,
    ServiceUnavailable = -6,
    NetworkUnavailable = -7,
    ContentUnavailable = -8,
    Timeout = -9,
    InternalError = -10,
    BufferTooSmall = -11,
}

impl GuestErrorCode {
    pub fn as_i32(&self) -> i32 {
        *self as i32
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::InvalidRequest => "INVALID_REQUEST",
            Self::PermissionDenied => "PERMISSION_DENIED",
            Self::NotFound => "NOT_FOUND",
            Self::InvalidId => "INVALID_ID",
            Self::QuotaExceeded => "QUOTA_EXCEEDED",
            Self::ServiceUnavailable => "SERVICE_UNAVAILABLE",
            Self::NetworkUnavailable => "NETWORK_UNAVAILABLE",
            Self::ContentUnavailable => "CONTENT_UNAVAILABLE",
            Self::Timeout => "TIMEOUT",
            Self::InternalError => "INTERNAL_ERROR",
            Self::BufferTooSmall => "BUFFER_TOO_SMALL",
        }
    }
}

impl fmt::Display for GuestErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Internal host error containing rich diagnostics that are logged internally
/// but mapped cleanly to safe `GuestErrorCode` values before crossing into guest memory.
#[derive(Debug)]
pub struct HostError {
    code: GuestErrorCode,
    detail: String,
}

impl HostError {
    pub fn new(code: GuestErrorCode, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }

    pub fn code(&self) -> GuestErrorCode {
        self.code
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }

    pub fn invalid_request(detail: impl Into<String>) -> Self {
        Self::new(GuestErrorCode::InvalidRequest, detail)
    }

    pub fn permission_denied(detail: impl Into<String>) -> Self {
        Self::new(GuestErrorCode::PermissionDenied, detail)
    }

    pub fn not_found(detail: impl Into<String>) -> Self {
        Self::new(GuestErrorCode::NotFound, detail)
    }

    pub fn invalid_id(detail: impl Into<String>) -> Self {
        Self::new(GuestErrorCode::InvalidId, detail)
    }

    pub fn quota_exceeded(detail: impl Into<String>) -> Self {
        Self::new(GuestErrorCode::QuotaExceeded, detail)
    }

    pub fn service_unavailable(detail: impl Into<String>) -> Self {
        Self::new(GuestErrorCode::ServiceUnavailable, detail)
    }

    pub fn network_unavailable(detail: impl Into<String>) -> Self {
        Self::new(GuestErrorCode::NetworkUnavailable, detail)
    }

    pub fn content_unavailable(detail: impl Into<String>) -> Self {
        Self::new(GuestErrorCode::ContentUnavailable, detail)
    }

    pub fn timeout(detail: impl Into<String>) -> Self {
        Self::new(GuestErrorCode::Timeout, detail)
    }

    pub fn internal(detail: impl Into<String>) -> Self {
        Self::new(GuestErrorCode::InternalError, detail)
    }

    pub fn buffer_too_small(detail: impl Into<String>) -> Self {
        Self::new(GuestErrorCode::BufferTooSmall, detail)
    }
}

impl fmt::Display for HostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code.as_str(), self.detail)
    }
}

impl std::error::Error for HostError {}
