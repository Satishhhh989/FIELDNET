use super::config::GatewayConfig;
use super::error::GatewayError;
use super::model::GatewayMethod;

/// Validates the incoming HTTP Host header against allowed hostnames (DNS rebinding mitigation).
pub fn validate_host_header(host: &str, config: &GatewayConfig) -> Result<(), GatewayError> {
    // Strip port if present (e.g., "localhost:8080" -> "localhost", "[::1]:8080" -> "[::1]")
    let host_name = if host.starts_with('[') {
        if let Some(end) = host.find(']') {
            &host[..=end]
        } else {
            host
        }
    } else if let Some(idx) = host.find(':') {
        &host[..idx]
    } else {
        host
    };

    let host_lower = host_name.to_lowercase();

    // Check against config whitelist
    for allowed in &config.allowed_hosts {
        let allowed_lower = allowed.to_lowercase();
        if host_lower == allowed_lower {
            return Ok(());
        }
        // Match wildcard *.local
        if allowed_lower.starts_with("*.") {
            let suffix = &allowed_lower[1..];
            if host_lower.ends_with(suffix) {
                return Ok(());
            }
        }
    }

    // Standard local aliases
    if host_lower == "localhost"
        || host_lower == "127.0.0.1"
        || host_lower == "[::1]"
        || host_lower == "fieldnet.local"
        || host_lower.ends_with(".local")
    {
        return Ok(());
    }

    Err(GatewayError::InvalidHost {
        host: host.to_string(),
    })
}

/// Enforces arbitrary proxy prevention: ensures the path does not target external or raw IP endpoints.
pub fn validate_proxy_target(path: &str, raw_uri: &str) -> Result<(), GatewayError> {
    let lower_uri = raw_uri.to_lowercase();
    let lower_path = path.to_lowercase();

    // Rejection of absolute URI proxy requests (e.g. GET http://google.com/ or GET http://127.0.0.1/)
    if lower_uri.starts_with("http://")
        || lower_uri.starts_with("https://")
        || lower_uri.starts_with("//")
    {
        return Err(GatewayError::ArbitraryProxyForbidden {
            target: raw_uri.to_string(),
        });
    }

    // Rejection of embedded IP or localhost targets in proxy paths
    let forbidden_patterns = [
        "http:/",
        "https:/",
        "127.0.0.1",
        "localhost",
        "192.168.",
        "10.",
        "172.16.",
        "172.17.",
        "172.18.",
        "172.19.",
        "172.20.",
        "172.21.",
        "172.22.",
        "172.23.",
        "172.24.",
        "172.25.",
        "172.26.",
        "172.27.",
        "172.28.",
        "172.29.",
        "172.30.",
        "172.31.",
    ];

    for pattern in forbidden_patterns {
        if lower_path.contains(pattern) || lower_uri.contains(pattern) {
            return Err(GatewayError::ArbitraryProxyForbidden {
                target: path.to_string(),
            });
        }
    }

    Ok(())
}

/// Validates CORS Origin header against allowed origins.
pub fn validate_origin(origin: &str, config: &GatewayConfig) -> bool {
    let origin_lower = origin.to_lowercase();
    for allowed in &config.cors_allowed_origins {
        if origin_lower == allowed.to_lowercase() {
            return true;
        }
    }

    // Also accept matching standard local hosts
    if origin_lower.starts_with("http://localhost:")
        || origin_lower.starts_with("http://127.0.0.1:")
        || origin_lower.starts_with("http://fieldnet.local:")
        || origin_lower == "http://localhost"
        || origin_lower == "http://127.0.0.1"
        || origin_lower == "http://fieldnet.local"
    {
        return true;
    }

    false
}

use axum::body::Body;
use axum::http::header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION};
use axum::response::Response;

/// Extracts Bearer token from incoming HTTP Authorization header, if present.
pub fn extract_bearer_token(headers: &HeaderMap) -> Option<&str> {
    if let Some(auth_val) = headers.get(AUTHORIZATION) {
        if let Ok(s) = auth_val.to_str() {
            let trimmed = s.trim();
            if trimmed.len() > 7 && trimmed[..7].eq_ignore_ascii_case("bearer ") {
                return Some(trimmed[7..].trim());
            }
        }
    }
    None
}

/// Injects authoritative security headers onto an outgoing HTTP response.
pub fn inject_security_headers(resp: &mut Response<Body>) {
    let headers = resp.headers_mut();
    headers.insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        HeaderName::from_static("referrer-policy"),
        HeaderValue::from_static("strict-origin-when-cross-origin"),
    );
    headers.insert(
        HeaderName::from_static("content-security-policy"),
        HeaderValue::from_static("default-src 'self'"),
    );
}

/// Validates anti-CSRF protections on state-changing requests.
pub fn validate_csrf(
    method: &GatewayMethod,
    origin: Option<&str>,
    client_header: Option<&str>,
    config: &GatewayConfig,
) -> Result<(), GatewayError> {
    // Safe idempotent methods do not need CSRF tokens
    if method.is_safe() {
        return Ok(());
    }

    // Custom header `X-Fieldnet-Client: web-gateway` (or similar non-standard header)
    // HTML forms cannot emit custom headers, mitigating standard CSRF
    if client_header.is_some() {
        return Ok(());
    }

    // If Origin is present, verify it is trusted
    if let Some(orig) = origin {
        if validate_origin(orig, config) {
            return Ok(());
        }
        return Err(GatewayError::CorsRejected {
            origin: orig.to_string(),
        });
    }

    // Local loopback client without origin (e.g. curl or internal client)
    if config.auto_pair_localhost {
        return Ok(());
    }

    Err(GatewayError::CsrfRejected)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_host_header_validation() {
        let config = GatewayConfig::default();
        assert!(validate_host_header("localhost", &config).is_ok());
        assert!(validate_host_header("localhost:8080", &config).is_ok());
        assert!(validate_host_header("127.0.0.1:8080", &config).is_ok());
        assert!(validate_host_header("[::1]:8080", &config).is_ok());
        assert!(validate_host_header("fieldnet.local:8080", &config).is_ok());
        assert!(validate_host_header("node-1.fieldnet.local:8080", &config).is_ok());

        // Attacker domains rejected
        assert!(validate_host_header("attacker.com", &config).is_err());
        assert!(validate_host_header("evil.localnet", &config).is_err());
    }

    #[test]
    fn test_arbitrary_proxy_prevention() {
        assert!(validate_proxy_target("/", "/").is_ok());
        assert!(validate_proxy_target("/services/maps/tiles", "/services/maps/tiles").is_ok());
        assert!(validate_proxy_target("/api/v1/services", "/api/v1/services").is_ok());

        // Arbitrary proxy targets rejected
        assert!(validate_proxy_target("http://google.com/", "http://google.com/").is_err());
        assert!(validate_proxy_target("/127.0.0.1/admin", "/127.0.0.1/admin").is_err());
        assert!(validate_proxy_target("/192.168.1.1/setup", "/192.168.1.1/setup").is_err());
        assert!(validate_proxy_target("/http:/attacker.com", "/http:/attacker.com").is_err());
    }
}
