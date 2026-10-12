import React, { useState } from 'react';
import {
  Shield,
  Radio,
  Users,
  HardDrive,
  Server,
  Boxes,
  Copy,
  Check,
  Activity,
  CheckCircle2,
  AlertCircle,
} from 'lucide-react';
import type {
  RuntimeStatus,
  NodeIdentity,
  NetworkStatus,
  Peer,
  Service,
  Application,
  StorageStatus,
  FrontendEvent,
} from '../../types/fieldnet';

interface OverviewViewProps {
  status: RuntimeStatus | null;
  identity: NodeIdentity | null;
  network: NetworkStatus | null;
  peers: Peer[];
  services: Service[];
  applications: Application[];
  storage: StorageStatus | null;
  events: FrontendEvent[];
  onNavigateTab: (tab: 'network' | 'peers' | 'storage' | 'services' | 'applications' | 'events') => void;
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${(bytes / Math.pow(k, i)).toFixed(1)} ${sizes[i]}`;
}

export const OverviewView: React.FC<OverviewViewProps> = ({
  status,
  identity,
  network,
  peers,
  services,
  applications,
  storage,
  events,
  onNavigateTab,
}) => {
  const [copiedKey, setCopiedKey] = useState(false);

  const isRunning = status?.is_running ?? false;
  const connectedPeers = peers.filter((p) => p.connection_state === 'Connected');

  const copyPublicKey = () => {
    if (identity?.public_key_hex) {
      navigator.clipboard.writeText(identity.public_key_hex);
      setCopiedKey(true);
      setTimeout(() => setCopiedKey(false), 2000);
    }
  };

  const usagePercent =
    storage && storage.limit_bytes > 0
      ? Math.min(100, Math.round((storage.used_bytes / storage.limit_bytes) * 100))
      : 0;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
      {/* View Header */}
      <div className="view-header">
        <div className="view-title-group">
          <h1>System Overview</h1>
          <p>
            Authoritative node status, cryptographic identity, and active mesh subsystems.
          </p>
        </div>
      </div>

      {/* Node Identity Card */}
      <div className="card">
        <div className="card-title">
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <Shield size={14} style={{ color: 'var(--accent-cyan)' }} />
            <span>Node Identity & Cryptography</span>
          </div>
          <span className="badge badge-blue">Ed25519 Sovereign</span>
        </div>

        {identity ? (
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: 16 }}>
            <div>
              <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 4 }}>NODE IDENTIFIER (BASE58)</div>
              <div className="mono" style={{ fontSize: 12, wordBreak: 'break-all', color: 'var(--text-primary)' }}>
                {identity.node_id}
              </div>
            </div>

            <div>
              <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 4 }}>
                PUBLIC KEY (HEX)
              </div>
              <div
                className="mono"
                style={{
                  fontSize: 12,
                  wordBreak: 'break-all',
                  color: 'var(--text-secondary)',
                  display: 'flex',
                  alignItems: 'center',
                  gap: 8,
                }}
              >
                <span>{identity.public_key_hex}</span>
                <button
                  className="btn btn-secondary btn-sm"
                  onClick={copyPublicKey}
                  style={{ padding: '2px 6px', flexShrink: 0 }}
                  title="Copy Public Key"
                >
                  {copiedKey ? <Check size={11} color="var(--accent-emerald)" /> : <Copy size={11} />}
                </button>
              </div>
            </div>

            <div style={{ gridColumn: '1 / -1' }}>
              <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 4 }}>DATA DIRECTORY</div>
              <div className="mono" style={{ fontSize: 11, color: 'var(--text-dim)' }}>
                {identity.storage_path}
              </div>
            </div>
          </div>
        ) : (
          <div className="empty-state" style={{ padding: 24 }}>
            <AlertCircle size={24} className="empty-state-icon" />
            <h3>Identity Uninitialized</h3>
            <p>Runtime is starting or offline. Sovereign cryptographic keypair not loaded.</p>
          </div>
        )}
      </div>

      {/* High-Level Metric Cards */}
      <div className="stats-grid">
        <div className="stat-card" style={{ cursor: 'pointer' }} onClick={() => onNavigateTab('peers')}>
          <div className="stat-label">
            <Users size={12} style={{ color: 'var(--accent-emerald)' }} />
            <span>Connected Peers</span>
          </div>
          <div className="stat-value">{connectedPeers.length}</div>
          <div className="stat-subtext">
            {peers.length} discovered / {network ? `${network.routes.length} routes` : '0 routes'}
          </div>
        </div>

        <div className="stat-card" style={{ cursor: 'pointer' }} onClick={() => onNavigateTab('services')}>
          <div className="stat-label">
            <Server size={12} style={{ color: 'var(--accent-blue)' }} />
            <span>Service Registry</span>
          </div>
          <div className="stat-value">{services.length}</div>
          <div className="stat-subtext">
            {services.filter((s) => s.is_local).length} local / {services.filter((s) => !s.is_local).length} remote
          </div>
        </div>

        <div className="stat-card" style={{ cursor: 'pointer' }} onClick={() => onNavigateTab('storage')}>
          <div className="stat-label">
            <HardDrive size={12} style={{ color: 'var(--accent-cyan)' }} />
            <span>CAS Storage</span>
          </div>
          <div className="stat-value">{storage ? formatBytes(storage.used_bytes) : '0 B'}</div>
          <div className="stat-subtext">
            {storage ? `${storage.object_count} objects (${usagePercent}% used)` : 'Storage offline'}
          </div>
        </div>

        <div className="stat-card" style={{ cursor: 'pointer' }} onClick={() => onNavigateTab('applications')}>
          <div className="stat-label">
            <Boxes size={12} style={{ color: 'var(--accent-purple)' }} />
            <span>WASM Apps</span>
          </div>
          <div className="stat-value">{applications.length}</div>
          <div className="stat-subtext">
            {applications.filter((a) => a.lifecycle_state === 'Running').length} running
          </div>
        </div>
      </div>

      {/* Subsystem Health Grid */}
      <div className="card">
        <div className="card-title">
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <Activity size={14} style={{ color: 'var(--accent-emerald)' }} />
            <span>Subsystem Health & Operational State</span>
          </div>
          <span className={`badge ${isRunning ? 'badge-green' : 'badge-rose'}`}>
            {isRunning ? 'Runtime Active' : 'Offline'}
          </span>
        </div>

        {status ? (
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))', gap: 12 }}>
            <div style={{ background: 'var(--bg-canvas)', padding: 12, borderRadius: 'var(--radius-md)', border: '1px solid var(--border-subtle)' }}>
              <div style={{ fontSize: 11, color: 'var(--text-muted)', fontWeight: 600 }}>IDENTITY SUBSYSTEM</div>
              <div style={{ fontSize: 12, marginTop: 4, color: 'var(--text-primary)', display: 'flex', alignItems: 'center', gap: 6 }}>
                <CheckCircle2 size={13} style={{ color: 'var(--accent-emerald)' }} />
                <span>{status.subsystems.identity}</span>
              </div>
            </div>

            <div style={{ background: 'var(--bg-canvas)', padding: 12, borderRadius: 'var(--radius-md)', border: '1px solid var(--border-subtle)' }}>
              <div style={{ fontSize: 11, color: 'var(--text-muted)', fontWeight: 600 }}>NETWORK TRANSPORT</div>
              <div style={{ fontSize: 12, marginTop: 4, color: 'var(--text-primary)', display: 'flex', alignItems: 'center', gap: 6 }}>
                <CheckCircle2 size={13} style={{ color: 'var(--accent-emerald)' }} />
                <span>{status.subsystems.network}</span>
              </div>
            </div>

            <div style={{ background: 'var(--bg-canvas)', padding: 12, borderRadius: 'var(--radius-md)', border: '1px solid var(--border-subtle)' }}>
              <div style={{ fontSize: 11, color: 'var(--text-muted)', fontWeight: 600 }}>ROUTING & MESH</div>
              <div style={{ fontSize: 12, marginTop: 4, color: 'var(--text-primary)', display: 'flex', alignItems: 'center', gap: 6 }}>
                <CheckCircle2 size={13} style={{ color: 'var(--accent-emerald)' }} />
                <span>{status.subsystems.routing}</span>
              </div>
            </div>

            <div style={{ background: 'var(--bg-canvas)', padding: 12, borderRadius: 'var(--radius-md)', border: '1px solid var(--border-subtle)' }}>
              <div style={{ fontSize: 11, color: 'var(--text-muted)', fontWeight: 600 }}>DTN STORE-AND-FORWARD</div>
              <div style={{ fontSize: 12, marginTop: 4, color: 'var(--text-primary)', display: 'flex', alignItems: 'center', gap: 6 }}>
                <CheckCircle2 size={13} style={{ color: 'var(--accent-emerald)' }} />
                <span>{status.subsystems.dtn}</span>
              </div>
            </div>

            <div style={{ background: 'var(--bg-canvas)', padding: 12, borderRadius: 'var(--radius-md)', border: '1px solid var(--border-subtle)' }}>
              <div style={{ fontSize: 11, color: 'var(--text-muted)', fontWeight: 600 }}>CAS CONTENT STORAGE</div>
              <div style={{ fontSize: 12, marginTop: 4, color: 'var(--text-primary)', display: 'flex', alignItems: 'center', gap: 6 }}>
                <CheckCircle2 size={13} style={{ color: 'var(--accent-emerald)' }} />
                <span>{status.subsystems.storage}</span>
              </div>
            </div>

            <div style={{ background: 'var(--bg-canvas)', padding: 12, borderRadius: 'var(--radius-md)', border: '1px solid var(--border-subtle)' }}>
              <div style={{ fontSize: 11, color: 'var(--text-muted)', fontWeight: 600 }}>SERVICE REGISTRY</div>
              <div style={{ fontSize: 12, marginTop: 4, color: 'var(--text-primary)', display: 'flex', alignItems: 'center', gap: 6 }}>
                <CheckCircle2 size={13} style={{ color: 'var(--accent-emerald)' }} />
                <span>{status.subsystems.service_registry}</span>
              </div>
            </div>

            <div style={{ background: 'var(--bg-canvas)', padding: 12, borderRadius: 'var(--radius-md)', border: '1px solid var(--border-subtle)' }}>
              <div style={{ fontSize: 11, color: 'var(--text-muted)', fontWeight: 600 }}>WASM APPLICATION SUPERVISOR</div>
              <div style={{ fontSize: 12, marginTop: 4, color: 'var(--text-primary)', display: 'flex', alignItems: 'center', gap: 6 }}>
                <CheckCircle2 size={13} style={{ color: 'var(--accent-emerald)' }} />
                <span>{status.subsystems.wasm_supervisor}</span>
              </div>
            </div>

            <div style={{ background: 'var(--bg-canvas)', padding: 12, borderRadius: 'var(--radius-md)', border: '1px solid var(--border-subtle)' }}>
              <div style={{ fontSize: 11, color: 'var(--text-muted)', fontWeight: 600 }}>LOCAL WEB GATEWAY & mDNS</div>
              <div style={{ fontSize: 12, marginTop: 4, color: 'var(--text-primary)', display: 'flex', alignItems: 'center', gap: 6 }}>
                <CheckCircle2 size={13} style={{ color: 'var(--accent-emerald)' }} />
                <span>{status.subsystems.local_gateway} | {status.subsystems.mdns}</span>
              </div>
            </div>
          </div>
        ) : (
          <div className="empty-state" style={{ padding: 20 }}>
            <p>Subsystem telemetry unavailable.</p>
          </div>
        )}
      </div>

      {/* Recent Events Mini Stream */}
      <div className="card">
        <div className="card-title">
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <Radio size={14} style={{ color: 'var(--accent-cyan)' }} />
            <span>Recent Runtime Activity</span>
          </div>
          <button
            className="btn btn-secondary btn-sm"
            onClick={() => onNavigateTab('events')}
            style={{ padding: '2px 8px', fontSize: 11 }}
          >
            View Full Event Stream ({events.length})
          </button>
        </div>

        {events.length > 0 ? (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {events.slice(0, 4).map((evt) => (
              <div
                key={evt.id}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'space-between',
                  background: 'var(--bg-canvas)',
                  padding: '8px 12px',
                  borderRadius: 'var(--radius-sm)',
                  border: '1px solid var(--border-subtle)',
                }}
              >
                <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                  <span className={`badge ${evt.category === 'peer' ? 'badge-green' : evt.category === 'service' ? 'badge-blue' : evt.category === 'app' ? 'badge-purple' : 'badge-gray'}`}>
                    {evt.category.toUpperCase()}
                  </span>
                  <span style={{ fontSize: 12, color: 'var(--text-primary)' }}>{evt.summary}</span>
                </div>
                <span className="mono" style={{ fontSize: 10.5, color: 'var(--text-muted)' }}>
                  {evt.timestamp}
                </span>
              </div>
            ))}
          </div>
        ) : (
          <div className="empty-state" style={{ padding: 18 }}>
            <p>No recent events recorded on bus yet.</p>
          </div>
        )}
      </div>
    </div>
  );
};
