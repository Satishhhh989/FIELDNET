use serde::{Deserialize, Serialize};
use std::net::Ipv4Addr;
use std::time::Duration;

pub const DEFAULT_MDNS_PORT: u16 = 5353;
pub const DEFAULT_MULTICAST_IPV4: Ipv4Addr = Ipv4Addr::new(224, 0, 0, 251);
pub const DEFAULT_SERVICE_TYPE: &str = "_http._tcp.local";
pub const DEFAULT_HOSTNAME: &str = "fieldnet.local";
pub const DEFAULT_SERVICE_NAME: &str = "FIELDNET Local Web Gateway";
pub const DEFAULT_TTL_SECONDS: u32 = 120;

/// Configuration options for the mDNS / DNS-SD subsystem.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MdnsConfig {
    /// Whether the mDNS subsystem is enabled.
    pub enabled: bool,

    /// Canonical hostname to advertise (e.g. "fieldnet.local").
    pub hostname: String,

    /// Standard DNS-SD service type (e.g. "_http._tcp.local").
    pub service_type: String,

    /// Human-readable service instance name.
    pub service_name: String,

    /// Actual listening TCP port of the LocalWebGateway being advertised.
    pub gateway_port: u16,

    /// UDP port for mDNS queries and responses (standard RFC 6762 is 5353).
    pub mdns_port: u16,

    /// IPv4 multicast address for mDNS (default 224.0.0.251).
    pub multicast_ipv4: Ipv4Addr,

    /// Whether to advertise loopback IP (127.0.0.1). Typically false in LAN mode,
    /// true for single-machine tests or when no external interface is present.
    pub allow_loopback: bool,

    /// Duration to listen for probe conflicts before claiming a hostname.
    pub probe_timeout: Duration,

    /// Periodic announcement refresh interval.
    pub announce_interval: Duration,

    /// Time-to-Live in seconds for advertised DNS-SD resource records.
    pub ttl_seconds: u32,
}

impl Default for MdnsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            hostname: DEFAULT_HOSTNAME.to_string(),
            service_type: DEFAULT_SERVICE_TYPE.to_string(),
            service_name: DEFAULT_SERVICE_NAME.to_string(),
            gateway_port: 0,
            mdns_port: DEFAULT_MDNS_PORT,
            multicast_ipv4: DEFAULT_MULTICAST_IPV4,
            allow_loopback: false,
            probe_timeout: Duration::from_millis(250),
            announce_interval: Duration::from_secs(60),
            ttl_seconds: DEFAULT_TTL_SECONDS,
        }
    }
}

impl MdnsConfig {
    /// Creates a configuration configured specifically for fast, isolated testing.
    pub fn test_mode(mdns_port: u16, gateway_port: u16) -> Self {
        Self {
            enabled: true,
            hostname: DEFAULT_HOSTNAME.to_string(),
            service_type: DEFAULT_SERVICE_TYPE.to_string(),
            service_name: DEFAULT_SERVICE_NAME.to_string(),
            gateway_port,
            mdns_port,
            multicast_ipv4: DEFAULT_MULTICAST_IPV4,
            allow_loopback: true,
            probe_timeout: Duration::from_millis(100),
            announce_interval: Duration::from_secs(10),
            ttl_seconds: 30,
        }
    }

    /// Sets the gateway port to advertise.
    pub fn with_gateway_port(mut self, port: u16) -> Self {
        self.gateway_port = port;
        self
    }

    /// Sets the requested canonical hostname.
    pub fn with_hostname(mut self, hostname: impl Into<String>) -> Self {
        self.hostname = hostname.into();
        self
    }

    /// Sets the mDNS UDP port.
    pub fn with_mdns_port(mut self, port: u16) -> Self {
        self.mdns_port = port;
        self
    }
}
