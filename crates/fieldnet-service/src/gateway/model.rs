use fieldnet_core::Timestamp;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// Supported HTTP methods at the gateway boundary.
#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub enum GatewayMethod {
    Get,
    Head,
    Post,
    Put,
    Delete,
    Patch,
    Options,
    Other(String),
}

impl GatewayMethod {
    /// Returns true if the method is safe according to RFC 7231 (read-only, no side effects).
    pub fn is_safe(&self) -> bool {
        matches!(self, Self::Get | Self::Head | Self::Options)
    }

    /// Returns true if the method is idempotent (repeated identical calls produce same effect).
    pub fn is_idempotent(&self) -> bool {
        matches!(
            self,
            Self::Get | Self::Head | Self::Options | Self::Put | Self::Delete
        )
    }

    /// Converts standard string into `GatewayMethod`.
    pub fn from_str_method(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "GET" => Self::Get,
            "HEAD" => Self::Head,
            "POST" => Self::Post,
            "PUT" => Self::Put,
            "DELETE" => Self::Delete,
            "PATCH" => Self::Patch,
            "OPTIONS" => Self::Options,
            other => Self::Other(other.to_string()),
        }
    }
}

impl fmt::Display for GatewayMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Get => write!(f, "GET"),
            Self::Head => write!(f, "HEAD"),
            Self::Post => write!(f, "POST"),
            Self::Put => write!(f, "PUT"),
            Self::Delete => write!(f, "DELETE"),
            Self::Patch => write!(f, "PATCH"),
            Self::Options => write!(f, "OPTIONS"),
            Self::Other(o) => write!(f, "{}", o),
        }
    }
}

/// Gateway-level request abstraction isolating HTTP framework types from FIELDNET core.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GatewayRequest {
    /// Unique correlation identifier for this request.
    pub request_id: String,
    /// HTTP method.
    pub method: GatewayMethod,
    /// Complete original URI.
    pub uri: String,
    /// Parsed request path.
    pub path: String,
    /// Parsed query parameters.
    pub query_params: HashMap<String, String>,
    /// Safe request headers.
    pub headers: HashMap<String, String>,
    /// Request payload / body bytes.
    pub body: Vec<u8>,
    /// Execution deadline.
    pub deadline: Timestamp,
    /// Client Origin header if present.
    pub origin: Option<String>,
}

impl GatewayRequest {
    /// Extracts the idempotency key header (`Idempotency-Key`) if provided by the client.
    pub fn idempotency_key(&self) -> Option<&str> {
        self.headers
            .get("idempotency-key")
            .or_else(|| self.headers.get("Idempotency-Key"))
            .map(|s| s.as_str())
    }
}

/// Gateway-level response abstraction.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GatewayResponse {
    /// HTTP status code.
    pub status_code: u16,
    /// Response headers.
    pub headers: HashMap<String, String>,
    /// Content-Type header shortcut.
    pub content_type: Option<String>,
    /// Response body payload.
    pub body: Vec<u8>,
    /// Correlation request identifier.
    pub request_id: String,
}

impl GatewayResponse {
    /// Creates an empty response with the specified status code and request ID.
    pub fn new(status_code: u16, request_id: impl Into<String>) -> Self {
        Self {
            status_code,
            headers: HashMap::new(),
            content_type: None,
            body: Vec::new(),
            request_id: request_id.into(),
        }
    }

    /// Creates a JSON response with status 200 OK.
    pub fn json<T: Serialize>(
        data: &T,
        request_id: impl Into<String>,
    ) -> Result<Self, serde_json::Error> {
        let body = serde_json::to_vec(data)?;
        let mut resp = Self::new(200, request_id);
        resp.headers
            .insert("content-type".to_string(), "application/json".to_string());
        resp.content_type = Some("application/json".to_string());
        resp.body = body;
        Ok(resp)
    }
}

/// Standardized structured error response returned to the browser client.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GatewayErrorResponse {
    /// Machine-readable error code.
    pub error: String,
    /// HTTP status code.
    pub code: u16,
    /// Human-readable explanation.
    pub message: String,
    /// Correlation request ID.
    pub request_id: String,
}

impl GatewayErrorResponse {
    /// Serializes into JSON bytes.
    pub fn to_json_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_else(|_| b"{\"error\":\"INTERNAL_ERROR\"}".to_vec())
    }
}
