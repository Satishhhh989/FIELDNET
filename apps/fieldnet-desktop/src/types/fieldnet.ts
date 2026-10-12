// FIELDNET Desktop Environment Shell - Domain Types
// Strictly mirrors crates/fieldnet-desktop/src-tauri/src/dto.rs

export interface SubsystemHealth {
  identity: string;
  network: string;
  routing: string;
  dtn: string;
  storage: string;
  service_registry: string;
  wasm_supervisor: string;
  local_gateway: string;
  mdns: string;
}

export interface RuntimeStatus {
  is_running: boolean;
  lifecycle_phase: string;
  uptime_secs: number;
  data_dir: string;
  tcp_port: number;
  discovery_port: number;
  node_id: string | null;
  short_node_id: string | null;
  gateway_running: boolean;
  gateway_port: number | null;
  mdns_running: boolean;
  mdns_hostname: string | null;
  subsystems: SubsystemHealth;
  error_message: string | null;
}

export interface NodeIdentity {
  node_id: string;
  short_id: string;
  public_key_hex: string;
  algorithm: string;
  storage_path: string;
}

export interface Transport {
  name: string;
  protocol: string;
  endpoint: string;
  state: string;
  details: string;
}

export interface Route {
  destination: string;
  short_destination: string;
  next_hop: string;
  short_next_hop: string;
  hops: number;
  metric: number;
  sequence: number;
  updated_at: number;
  expires_at: number;
  is_expired: boolean;
}

export interface NetworkStatus {
  connectivity_mode: string;
  local_tcp_port: number;
  discovery_port: number;
  transports: Transport[];
  connected_peers_count: number;
  discovered_peers_count: number;
  routes: Route[];
  dtn_queue_count: number;
  dtn_db_path: string;
}

export interface Peer {
  node_id: string;
  short_id: string;
  connection_state: 'Connected' | 'Discovered' | 'Disconnected' | string;
  transport_address: string;
  protocol_version: number;
  discovered_at: number;
  expires_at: number;
  is_direct_neighbor: boolean;
  next_hop_for_count: number;
}

export interface Service {
  service_id: string;
  instance_id: string;
  short_instance_id: string;
  provider_node_id: string;
  short_provider_id: string;
  is_local: boolean;
  protocol: string;
  version: string;
  state: 'Available' | 'Degraded' | 'Unavailable' | string;
  capabilities: string[];
  endpoint: string;
  last_refreshed_at: number;
  reliability_permille: number;
  consecutive_failures: number;
  observed_rtt_ms: number;
}

export interface ServiceResolution {
  service_id: string;
  instance_id: string;
  provider_node_id: string;
  resolved_at: number;
  route_hops: number;
  reason: string;
  protocol: string;
  endpoint: string;
}

export interface ResourceLimits {
  max_fuel?: number | null;
  max_memory_bytes?: number | null;
  max_execution_time_ms?: number | null;
}

export interface Dependency {
  dependency_id: string;
  version_req: string;
  is_optional: boolean;
}

export interface ActiveExecution {
  execution_id: string;
  application_id: string;
  started_at_ms: number;
  state: string;
}

export interface Application {
  application_id: string;
  version: string;
  package_id: string;
  package_hash: string;
  publisher: string;
  signature_status: string;
  lifecycle_state: string;
  declared_permissions: string[];
  authorized_permissions: string[];
  effective_capabilities: string[];
  resource_limits: ResourceLimits;
  dependencies: Dependency[];
  active_executions: ActiveExecution[];
  installed_at: number;
  updated_at: number;
}

export interface ContentItem {
  content_id: string;
  short_id: string;
  size_bytes: number;
  is_pinned: boolean;
  chunk_count: number;
  created_at: number;
  availability: string;
  verification_state: string;
}

export interface StorageStatus {
  used_bytes: number;
  limit_bytes: number;
  available_bytes: number;
  object_count: number;
  chunk_count: number;
  pinned_count: number;
  storage_dir: string;
  items: ContentItem[];
}

export interface FrontendEvent {
  id: string;
  event_type: string;
  category: 'peer' | 'service' | 'app' | 'content' | 'network' | 'system' | string;
  timestamp: string;
  timestamp_ms: number;
  node_id?: string | null;
  summary: string;
  details: Record<string, unknown> | null;
}

export type ViewTab = 'overview' | 'network' | 'peers' | 'storage' | 'services' | 'applications' | 'events';
