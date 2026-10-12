use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeStatusDto {
    pub is_running: bool,
    pub lifecycle_phase: String,
    pub uptime_secs: u64,
    pub data_dir: String,
    pub tcp_port: u16,
    pub discovery_port: u16,
    pub node_id: Option<String>,
    pub short_node_id: Option<String>,
    pub gateway_running: bool,
    pub gateway_port: Option<u16>,
    pub mdns_running: bool,
    pub mdns_hostname: Option<String>,
    pub subsystems: SubsystemHealthDto,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubsystemHealthDto {
    pub identity: String,
    pub network: String,
    pub routing: String,
    pub dtn: String,
    pub storage: String,
    pub service_registry: String,
    pub wasm_supervisor: String,
    pub local_gateway: String,
    pub mdns: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeIdentityDto {
    pub node_id: String,
    pub short_id: String,
    pub public_key_hex: String,
    pub algorithm: String,
    pub storage_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransportDto {
    pub name: String,
    pub protocol: String,
    pub endpoint: String,
    pub state: String,
    pub details: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteDto {
    pub destination: String,
    pub short_destination: String,
    pub next_hop: String,
    pub short_next_hop: String,
    pub hops: u8,
    pub metric: u32,
    pub sequence: u64,
    pub updated_at: u64,
    pub expires_at: u64,
    pub is_expired: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkStatusDto {
    pub connectivity_mode: String,
    pub local_tcp_port: u16,
    pub discovery_port: u16,
    pub transports: Vec<TransportDto>,
    pub connected_peers_count: usize,
    pub discovered_peers_count: usize,
    pub routes: Vec<RouteDto>,
    pub dtn_queue_count: usize,
    pub dtn_db_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerDto {
    pub node_id: String,
    pub short_id: String,
    pub connection_state: String, // "Connected", "Discovered", "Disconnected"
    pub transport_address: String,
    pub protocol_version: u32,
    pub discovered_at: u64,
    pub expires_at: u64,
    pub is_direct_neighbor: bool,
    pub next_hop_for_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceDto {
    pub service_id: String,
    pub instance_id: String,
    pub short_instance_id: String,
    pub provider_node_id: String,
    pub short_provider_id: String,
    pub is_local: bool,
    pub protocol: String,
    pub version: String,
    pub state: String, // "Available", "Degraded", "Unavailable"
    pub capabilities: Vec<String>,
    pub endpoint: String,
    pub last_refreshed_at: u64,
    pub reliability_permille: u32,
    pub consecutive_failures: u32,
    pub observed_rtt_ms: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceLimitsDto {
    pub max_fuel: Option<u64>,
    pub max_memory_bytes: Option<u64>,
    pub max_execution_time_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DependencyDto {
    pub dependency_id: String,
    pub version_req: String,
    pub is_optional: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveExecutionDto {
    pub execution_id: String,
    pub application_id: String,
    pub started_at_ms: u64,
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplicationDto {
    pub application_id: String,
    pub version: String,
    pub package_id: String,
    pub package_hash: String,
    pub publisher: String,
    pub signature_status: String,
    pub lifecycle_state: String,
    pub declared_permissions: Vec<String>,
    pub authorized_permissions: Vec<String>,
    pub effective_capabilities: Vec<String>,
    pub resource_limits: ResourceLimitsDto,
    pub dependencies: Vec<DependencyDto>,
    pub active_executions: Vec<ActiveExecutionDto>,
    pub installed_at: u64,
    pub updated_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentItemDto {
    pub content_id: String,
    pub short_id: String,
    pub size_bytes: u64,
    pub is_pinned: bool,
    pub chunk_count: u32,
    pub created_at: u64,
    pub availability: String,
    pub verification_state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStatusDto {
    pub used_bytes: u64,
    pub limit_bytes: u64,
    pub available_bytes: u64,
    pub object_count: usize,
    pub chunk_count: usize,
    pub pinned_count: usize,
    pub storage_dir: String,
    pub items: Vec<ContentItemDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrontendEventDto {
    pub id: String,
    pub event_type: String,
    pub category: String, // "peer", "service", "app", "content", "network", "system"
    pub timestamp: String,
    pub timestamp_ms: u64,
    pub node_id: Option<String>,
    pub summary: String,
    pub details: serde_json::Value,
}
