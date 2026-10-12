// FIELDNET Desktop Environment Shell - Runtime State & Event Synchronization Hook

import { useState, useEffect, useCallback, useRef } from 'react';
import { listen, type UnlistenFn } from '@tauri-apps/api/event';
import { api, isTauriEnvironment } from '../api/client';
import type {
  RuntimeStatus,
  NodeIdentity,
  NetworkStatus,
  Peer,
  Service,
  Application,
  StorageStatus,
  FrontendEvent,
} from '../types/fieldnet';

export function useRuntime() {
  const [status, setStatus] = useState<RuntimeStatus | null>(null);
  const [identity, setIdentity] = useState<NodeIdentity | null>(null);
  const [network, setNetwork] = useState<NetworkStatus | null>(null);
  const [peers, setPeers] = useState<Peer[]>([]);
  const [services, setServices] = useState<Service[]>([]);
  const [applications, setApplications] = useState<Application[]>([]);
  const [storage, setStorage] = useState<StorageStatus | null>(null);
  const [events, setEvents] = useState<FrontendEvent[]>([]);

  const [isLoading, setIsLoading] = useState<boolean>(true);
  const [isRefreshingDiscovery, setIsRefreshingDiscovery] = useState<boolean>(false);
  const [actionMessage, setActionMessage] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);

  const mountedRef = useRef<boolean>(true);

  const clearMessages = () => {
    setActionMessage(null);
    setActionError(null);
  };

  const fetchAll = useCallback(async () => {
    try {
      const [
        newStatus,
        newIdentity,
        newNetwork,
        newPeers,
        newServices,
        newApps,
        newStorage,
        newEvents,
      ] = await Promise.all([
        api.getRuntimeStatus(),
        api.getNodeIdentity(),
        api.getNetworkStatus(),
        api.getPeers(),
        api.getServices(),
        api.getApplications(),
        api.getStorageStatus(),
        api.getRuntimeEvents(),
      ]);

      if (mountedRef.current) {
        setStatus(newStatus);
        setIdentity(newIdentity);
        setNetwork(newNetwork);
        setPeers(newPeers);
        setServices(newServices);
        setApplications(newApps);
        setStorage(newStorage);
        setEvents(newEvents);
        setIsLoading(false);
      }
    } catch (err: unknown) {
      if (mountedRef.current) {
        console.error('Failed to sync runtime state:', err);
        setIsLoading(false);
      }
    }
  }, []);

  // Initial fetch and event listener setup
  useEffect(() => {
    mountedRef.current = true;
    fetchAll();

    let unlisten: UnlistenFn | null = null;

    if (isTauriEnvironment()) {
      listen<FrontendEvent>('runtime-event', (event) => {
        if (!mountedRef.current) return;
        const incoming = event.payload;
        setEvents((prev) => [incoming, ...prev.slice(0, 199)]);

        // Opportunistically re-fetch subsystem state based on event category
        if (incoming.category === 'peer') {
          api.getPeers().then((p) => mountedRef.current && setPeers(p)).catch(console.error);
        } else if (incoming.category === 'service') {
          api.getServices().then((s) => mountedRef.current && setServices(s)).catch(console.error);
        } else if (incoming.category === 'app') {
          api.getApplications().then((a) => mountedRef.current && setApplications(a)).catch(console.error);
        } else if (incoming.category === 'content') {
          api.getStorageStatus().then((st) => mountedRef.current && setStorage(st)).catch(console.error);
        } else if (incoming.category === 'network') {
          api.getNetworkStatus().then((n) => mountedRef.current && setNetwork(n)).catch(console.error);
        }
      }).then((u) => {
        unlisten = u;
      }).catch(console.error);
    }

    // Regular heartbeat polling (2.5 seconds)
    const interval = setInterval(() => {
      fetchAll();
    }, 2500);

    return () => {
      mountedRef.current = false;
      clearInterval(interval);
      if (unlisten) {
        unlisten();
      }
    };
  }, [fetchAll]);

  // Actions
  const handleRefreshDiscovery = async () => {
    clearMessages();
    setIsRefreshingDiscovery(true);
    try {
      const count = await api.refreshDiscovery();
      setActionMessage(`Beacon broadcast emitted. ${count} peer(s) currently discovered.`);
      await fetchAll();
    } catch (err: unknown) {
      setActionError(`Discovery broadcast failed: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setIsRefreshingDiscovery(false);
    }
  };

  const handleDisconnectPeer = async (peerId: string) => {
    clearMessages();
    try {
      await api.disconnectPeer(peerId);
      setActionMessage(`Disconnected peer ${peerId.slice(0, 8)}`);
      await fetchAll();
    } catch (err: unknown) {
      setActionError(`Disconnect failed: ${err instanceof Error ? err.message : String(err)}`);
    }
  };

  const handlePinContent = async (contentId: string) => {
    clearMessages();
    try {
      await api.pinContent(contentId);
      setActionMessage(`Pinned content object ${contentId.slice(0, 8)}`);
      await fetchAll();
    } catch (err: unknown) {
      setActionError(`Pin failed: ${err instanceof Error ? err.message : String(err)}`);
    }
  };

  const handleUnpinContent = async (contentId: string) => {
    clearMessages();
    try {
      await api.unpinContent(contentId);
      setActionMessage(`Unpinned content object ${contentId.slice(0, 8)}`);
      await fetchAll();
    } catch (err: unknown) {
      setActionError(`Unpin failed: ${err instanceof Error ? err.message : String(err)}`);
    }
  };

  const handleStartApp = async (appId: string) => {
    clearMessages();
    try {
      const execId = await api.startApplication(appId);
      setActionMessage(`Application ${appId} started (Execution ID: ${execId.slice(0, 8)})`);
      await fetchAll();
    } catch (err: unknown) {
      setActionError(`Start failed: ${err instanceof Error ? err.message : String(err)}`);
    }
  };

  const handleStopApp = async (appId: string) => {
    clearMessages();
    try {
      await api.stopApplication(appId);
      setActionMessage(`Application ${appId} stopped.`);
      await fetchAll();
    } catch (err: unknown) {
      setActionError(`Stop failed: ${err instanceof Error ? err.message : String(err)}`);
    }
  };

  const handleRetryRuntime = async () => {
    clearMessages();
    try {
      const msg = await api.retryRuntimeStart();
      setActionMessage(msg);
      await fetchAll();
    } catch (err: unknown) {
      setActionError(`Runtime start failed: ${err instanceof Error ? err.message : String(err)}`);
    }
  };

  return {
    status,
    identity,
    network,
    peers,
    services,
    applications,
    storage,
    events,
    isLoading,
    isRefreshingDiscovery,
    actionMessage,
    actionError,
    clearMessages,
    refreshAll: fetchAll,
    refreshDiscovery: handleRefreshDiscovery,
    disconnectPeer: handleDisconnectPeer,
    pinContent: handlePinContent,
    unpinContent: handleUnpinContent,
    startApp: handleStartApp,
    stopApp: handleStopApp,
    retryRuntime: handleRetryRuntime,
  };
}
