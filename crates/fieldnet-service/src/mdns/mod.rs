pub mod config;
pub mod conflict;
pub mod error;
pub mod interface;
pub mod packet;
pub mod service;
pub mod state;

pub use config::{MdnsConfig, DEFAULT_HOSTNAME, DEFAULT_MDNS_PORT, DEFAULT_SERVICE_TYPE};
pub use conflict::{deterministic_fallback_hostname, normalize_hostname};
pub use error::MdnsError;
pub use interface::{get_interface_summaries, get_local_interfaces};
pub use packet::{MdnsRecord, MAX_MDNS_PACKET_SIZE};
pub use service::MdnsService;
pub use state::{MdnsState, MdnsStateMachine};
