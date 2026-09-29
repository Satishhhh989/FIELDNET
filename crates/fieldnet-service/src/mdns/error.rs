use thiserror::Error;

/// Errors arising from the mDNS / DNS-SD subsystem.
#[derive(Debug, Error)]
pub enum MdnsError {
    #[error("Failed to bind UDP socket: {0}")]
    SocketBind(String),

    #[error("Failed to join multicast group: {0}")]
    MulticastJoin(String),

    #[error("Failed to parse DNS packet: {0}")]
    PacketParse(String),

    #[error("Failed to build DNS packet: {0}")]
    PacketBuild(String),

    #[error("Invalid hostname '{0}': must end in '.local' and adhere to DNS naming rules")]
    InvalidHostname(String),

    #[error("Invalid service type '{0}': expected format like '_http._tcp.local'")]
    InvalidServiceType(String),

    #[error("Hostname conflict: requested '{requested}', assigned fallback '{fallback}'")]
    HostnameConflict { requested: String, fallback: String },

    #[error("mDNS subsystem is degraded: {0}")]
    Degraded(String),

    #[error("mDNS subsystem is already running")]
    AlreadyRunning,

    #[error("mDNS subsystem is not running")]
    NotRunning,

    #[error("Timeout occurred while waiting for mDNS response")]
    Timeout,

    #[error("Network I/O error: {0}")]
    Io(#[from] std::io::Error),
}
