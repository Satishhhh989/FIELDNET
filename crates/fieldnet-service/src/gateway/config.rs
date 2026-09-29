use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

/// Canonical configuration for the FIELDNET `LocalWebGateway`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GatewayConfig {
    /// Whether the Local Web Gateway is enabled on the node.
    pub enabled: bool,
    /// Network socket address to bind (e.g. 127.0.0.1:8080 or 0.0.0.0:8080).
    pub bind_address: SocketAddr,
    /// Maximum allowed HTTP request body size in bytes (default: 10 MiB).
    pub max_request_size_bytes: usize,
    /// Maximum allowed HTTP header block size in bytes (default: 64 KiB).
    pub max_header_size_bytes: usize,
    /// Maximum allowed URI length in bytes (default: 4096 bytes).
    pub max_uri_length_bytes: usize,
    /// Overall request execution deadline in milliseconds (default: 30,000 ms).
    pub request_timeout_ms: u64,
    /// Idle connection timeout in milliseconds (default: 15,000 ms).
    pub idle_timeout_ms: u64,
    /// Maximum concurrent TCP connections accepted by the gateway (default: 512).
    pub max_concurrent_connections: usize,
    /// Maximum concurrent in-flight requests processed by the gateway (default: 256).
    pub max_concurrent_requests: usize,
    /// Whitelist of allowed HTTP Host headers for DNS rebinding mitigation.
    pub allowed_hosts: Vec<String>,
    /// Whitelist of allowed CORS origins for browser security.
    pub cors_allowed_origins: Vec<String>,
    /// Whether localhost / loopback clients bypass pairing token enforcement.
    pub auto_pair_localhost: bool,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bind_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
            max_request_size_bytes: 10 * 1024 * 1024, // 10 MiB
            max_header_size_bytes: 64 * 1024,         // 64 KiB
            max_uri_length_bytes: 4096,               // 4 KiB
            request_timeout_ms: 30_000,
            idle_timeout_ms: 15_000,
            max_concurrent_connections: 512,
            max_concurrent_requests: 256,
            allowed_hosts: vec![
                "localhost".to_string(),
                "127.0.0.1".to_string(),
                "[::1]".to_string(),
                "fieldnet.local".to_string(),
            ],
            cors_allowed_origins: vec![
                "http://localhost:8080".to_string(),
                "http://127.0.0.1:8080".to_string(),
                "http://fieldnet.local:8080".to_string(),
            ],
            auto_pair_localhost: true,
        }
    }
}

impl GatewayConfig {
    /// Creates a configuration bound to an ephemeral port for testing.
    pub fn ephemeral() -> Self {
        Self {
            bind_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0),
            ..Default::default()
        }
    }

    /// Loads a `GatewayConfig` from a JSON file.
    pub fn load_from_file(path: impl AsRef<std::path::Path>) -> Result<Self, std::io::Error> {
        let content = std::fs::read_to_string(path)?;
        serde_json::from_str(&content)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    /// Saves the `GatewayConfig` to a JSON file.
    pub fn save_to_file(&self, path: impl AsRef<std::path::Path>) -> Result<(), std::io::Error> {
        let content = serde_json::to_string_pretty(self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        std::fs::write(path, content)
    }
}
