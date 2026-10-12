import React, { useState } from 'react';
import { useRuntime } from './hooks/useRuntime';
import { Header } from './components/Header';
import { Sidebar } from './components/Sidebar';
import { StatusBar } from './components/StatusBar';
import { OverviewView } from './features/overview/OverviewView';
import { NetworkView } from './features/network/NetworkView';
import { PeersView } from './features/peers/PeersView';
import { StorageView } from './features/storage/StorageView';
import { ServicesView } from './features/services/ServicesView';
import { ApplicationsView } from './features/applications/ApplicationsView';
import { EventsView } from './features/events/EventsView';
import type { ViewTab } from './types/fieldnet';
import { X, CheckCircle2, AlertCircle } from 'lucide-react';

export const App: React.FC = () => {
  const [currentTab, setCurrentTab] = useState<ViewTab>('overview');

  const {
    status,
    identity,
    network,
    peers,
    services,
    applications,
    storage,
    events,
    isRefreshingDiscovery,
    actionMessage,
    actionError,
    clearMessages,
    refreshAll,
    refreshDiscovery,
    disconnectPeer,
    pinContent,
    unpinContent,
    startApp,
    stopApp,
    retryRuntime,
  } = useRuntime();

  return (
    <div className="app-shell">
      {/* Top Header */}
      <Header
        status={status}
        identity={identity}
        isRefreshingDiscovery={isRefreshingDiscovery}
        onRefreshDiscovery={refreshDiscovery}
        onRetryRuntime={retryRuntime}
      />

      {/* Action Notification Banners */}
      {actionMessage && (
        <div className="banner-alert success">
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <CheckCircle2 size={14} />
            <span>{actionMessage}</span>
          </div>
          <button
            onClick={clearMessages}
            style={{ background: 'transparent', border: 'none', color: 'inherit', cursor: 'pointer' }}
          >
            <X size={13} />
          </button>
        </div>
      )}

      {actionError && (
        <div className="banner-alert error">
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <AlertCircle size={14} />
            <span>{actionError}</span>
          </div>
          <button
            onClick={clearMessages}
            style={{ background: 'transparent', border: 'none', color: 'inherit', cursor: 'pointer' }}
          >
            <X size={13} />
          </button>
        </div>
      )}

      {/* Main Body */}
      <div className="app-body">
        <Sidebar
          currentTab={currentTab}
          onSelectTab={setCurrentTab}
          peerCount={peers.length}
          serviceCount={services.length}
          appCount={applications.length}
          storageItemCount={storage?.items.length ?? 0}
          eventCount={events.length}
        />

        <main className="app-content">
          {currentTab === 'overview' && (
            <OverviewView
              status={status}
              identity={identity}
              network={network}
              peers={peers}
              services={services}
              applications={applications}
              storage={storage}
              events={events}
              onNavigateTab={(tab) => setCurrentTab(tab as ViewTab)}
            />
          )}

          {currentTab === 'network' && (
            <NetworkView network={network} onRefresh={refreshAll} />
          )}

          {currentTab === 'peers' && (
            <PeersView
              peers={peers}
              onDisconnectPeer={disconnectPeer}
              onRefreshDiscovery={refreshDiscovery}
              isRefreshing={isRefreshingDiscovery}
            />
          )}

          {currentTab === 'storage' && (
            <StorageView
              storage={storage}
              onPinContent={pinContent}
              onUnpinContent={unpinContent}
            />
          )}

          {currentTab === 'services' && (
            <ServicesView services={services} onRefresh={refreshAll} />
          )}

          {currentTab === 'applications' && (
            <ApplicationsView
              applications={applications}
              onStartApp={startApp}
              onStopApp={stopApp}
            />
          )}

          {currentTab === 'events' && (
            <EventsView events={events} />
          )}
        </main>
      </div>

      {/* Bottom Status Bar */}
      <StatusBar
        status={status}
        storage={storage}
        peerCount={peers.filter((p) => p.connection_state === 'Connected').length}
        serviceCount={services.length}
        appCount={applications.length}
      />
    </div>
  );
};

export default App;
