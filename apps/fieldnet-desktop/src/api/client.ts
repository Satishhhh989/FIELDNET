// FIELDNET Desktop Environment Shell - Typed Tauri IPC Client
// Communicates with backend commands registered in crates/fieldnet-desktop/src-tauri/src/commands.rs

import { invoke } from '@tauri-apps/api/core';
import type {
  RuntimeStatus,
  NodeIdentity,
  NetworkStatus,
  Peer,
  Service,
  ServiceResolution,
  Application,
  StorageStatus,
  FrontendEvent,
} from '../types/fieldnet';

export const isTauriEnvironment = (): boolean => {
  return typeof window !== 'undefined' && '__TAURI_INTERNALS__' in window;
};

// Fallback values when running in a standalone web browser (e.g. Vite dev preview without Tauri)
const FALLBACK_STATUS: RuntimeStatus = {
  is_running: false,
  lifecycle_phase: 'OFFLINE_STANDALONE_PREVIEW',
  uptime_secs: 0,
  data_dir: 'N/A (Browser Preview)',
  tcp_port: 0,
  discovery_port: 0,
  node_id: null,
  short_node_id: null,
  gateway_running: false,
  gateway_port: null,
  mdns_running: false,
  mdns_hostname: null,
  subsystems: {
    identity: 'Unavailable in browser preview',
    network: 'Unavailable in browser preview',
    routing: 'Unavailable in browser preview',
    dtn: 'Unavailable in browser preview',
    storage: 'Unavailable in browser preview',
    service_registry: 'Unavailable in browser preview',
    wasm_supervisor: 'Unavailable in browser preview',
    local_gateway: 'Unavailable in browser preview',
    mdns: 'Unavailable in browser preview',
  },
  error_message: 'Running outside Tauri native shell. Connect to native desktop runtime for live state.',
};

export const api = {
  // Query Commands
  async getRuntimeStatus(): Promise<RuntimeStatus> {
    if (!isTauriEnvironment()) return FALLBACK_STATUS;
    return await invoke<RuntimeStatus>('get_runtime_status');
  },

  async getNodeIdentity(): Promise<NodeIdentity | null> {
    if (!isTauriEnvironment()) return null;
    return await invoke<NodeIdentity>('get_node_identity');
  },

  async getNetworkStatus(): Promise<NetworkStatus | null> {
    if (!isTauriEnvironment()) return null;
    return await invoke<NetworkStatus>('get_network_status');
  },

  async getPeers(): Promise<Peer[]> {
    if (!isTauriEnvironment()) return [];
    return await invoke<Peer[]>('get_peers');
  },

  async getServices(): Promise<Service[]> {
    if (!isTauriEnvironment()) return [];
    return await invoke<Service[]>('get_services');
  },

  async getApplications(): Promise<Application[]> {
    if (!isTauriEnvironment()) return [];
    return await invoke<Application[]>('get_applications');
  },

  async getStorageStatus(): Promise<StorageStatus | null> {
    if (!isTauriEnvironment()) return null;
    return await invoke<StorageStatus>('get_storage_status');
  },

  async getRuntimeEvents(): Promise<FrontendEvent[]> {
    if (!isTauriEnvironment()) return [];
    return await invoke<FrontendEvent[]>('get_runtime_events');
  },

  // Action Commands
  async startApplication(appId: string): Promise<string> {
    if (!isTauriEnvironment()) throw new Error('Native runtime required to launch applications');
    return await invoke<string>('start_application', { appId });
  },

  async stopApplication(appId: string): Promise<void> {
    if (!isTauriEnvironment()) throw new Error('Native runtime required to stop applications');
    return await invoke<void>('stop_application', { appId });
  },

  async resolveService(serviceId: string): Promise<ServiceResolution | null> {
    if (!isTauriEnvironment()) throw new Error('Native runtime required to resolve services');
    return await invoke<ServiceResolution | null>('resolve_service', { serviceId });
  },

  async sendMessage(destination: string, payload: string): Promise<string> {
    if (!isTauriEnvironment()) throw new Error('Native runtime required to send messages');
    return await invoke<string>('send_message', { destination, payload });
  },

  async disconnectPeer(peerId: string): Promise<void> {
    if (!isTauriEnvironment()) throw new Error('Native runtime required to disconnect peers');
    return await invoke<void>('disconnect_peer', { peerId });
  },

  async refreshDiscovery(): Promise<number> {
    if (!isTauriEnvironment()) return 0;
    return await invoke<number>('refresh_discovery');
  },

  async pinContent(contentId: string): Promise<void> {
    if (!isTauriEnvironment()) throw new Error('Native runtime required to pin content');
    return await invoke<void>('pin_content', { contentId });
  },

  async unpinContent(contentId: string): Promise<void> {
    if (!isTauriEnvironment()) throw new Error('Native runtime required to unpin content');
    return await invoke<void>('unpin_content', { contentId });
  },

  async retryRuntimeStart(): Promise<string> {
    if (!isTauriEnvironment()) throw new Error('Native runtime required');
    return await invoke<string>('retry_runtime_start');
  },
};
