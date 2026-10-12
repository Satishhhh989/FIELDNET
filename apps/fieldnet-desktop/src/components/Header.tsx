import React, { useState } from 'react';
import { Radio, RefreshCw, Copy, Check, ExternalLink, ShieldCheck } from 'lucide-react';
import type { RuntimeStatus, NodeIdentity } from '../types/fieldnet';

interface HeaderProps {
  status: RuntimeStatus | null;
  identity: NodeIdentity | null;
  isRefreshingDiscovery: boolean;
  onRefreshDiscovery: () => void;
  onRetryRuntime: () => void;
}

export const Header: React.FC<HeaderProps> = ({
  status,
  identity,
  isRefreshingDiscovery,
  onRefreshDiscovery,
  onRetryRuntime,
}) => {
  const [copied, setCopied] = useState(false);

  const phase = status?.lifecycle_phase || 'OFFLINE';
  const isRunning = status?.is_running ?? false;

  const getStatusClass = (p: string) => {
    switch (p.toUpperCase()) {
      case 'RUNNING':
        return 'running';
      case 'STARTING':
      case 'INITIALIZING':
      case 'IDENTITY_READY':
      case 'NETWORK_INITIALIZING':
      case 'DISCOVERING':
        return 'starting';
      default:
        return 'offline';
    }
  };

  const copyNodeId = () => {
    if (identity?.node_id) {
      navigator.clipboard.writeText(identity.node_id);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  return (
    <header className="app-header">
      <div className="brand-section">
        <div className="brand-logo">FN</div>
        <div>
          <span className="brand-title">FIELDNET</span>
          <span className="brand-badge" style={{ marginLeft: 8 }}>v0.1.0-alpha</span>
        </div>
      </div>

      <div className="header-meta">
        {/* Runtime Lifecycle Pill */}
        <div className="node-pill">
          <span className={`status-dot ${getStatusClass(phase)}`} />
          <span style={{ fontWeight: 600, letterSpacing: '0.4px' }}>
            {phase.replace(/_/g, ' ')}
          </span>
        </div>

        {/* Node ID Short Pill */}
        {identity ? (
          <div
            className="node-pill"
            style={{ cursor: 'pointer', fontFamily: 'var(--font-mono)' }}
            onClick={copyNodeId}
            title="Click to copy full 64-char Node ID"
          >
            <ShieldCheck size={13} style={{ color: 'var(--accent-cyan)' }} />
            <span style={{ color: 'var(--text-secondary)' }}>ID:</span>
            <span style={{ color: 'var(--text-primary)', fontWeight: 600 }}>{identity.short_id}</span>
            {copied ? (
              <Check size={12} style={{ color: 'var(--accent-emerald)' }} />
            ) : (
              <Copy size={12} style={{ color: 'var(--text-muted)' }} />
            )}
          </div>
        ) : (
          <div className="node-pill" style={{ color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>
            <span>ID: Not Initialized</span>
          </div>
        )}

        {/* Local Web Gateway */}
        {status?.gateway_running && status.gateway_port && (
          <a
            href={`http://127.0.0.1:${status.gateway_port}`}
            target="_blank"
            rel="noreferrer"
            className="gateway-link"
            title="Open Local Web Gateway in browser"
          >
            <ExternalLink size={12} />
            <span>gateway:{status.gateway_port}</span>
          </a>
        )}

        {/* Refresh Beacon Button */}
        {isRunning ? (
          <button
            className="btn btn-secondary btn-sm"
            onClick={onRefreshDiscovery}
            disabled={isRefreshingDiscovery}
            title="Trigger mDNS / UDP Discovery Beacon immediately"
          >
            <Radio size={13} className={isRefreshingDiscovery ? 'status-dot discovering' : ''} />
            <span>{isRefreshingDiscovery ? 'Broadcasting...' : 'Broadcast Beacon'}</span>
          </button>
        ) : (
          <button
            className="btn btn-primary btn-sm"
            onClick={onRetryRuntime}
            title="Attempt to restart FIELDNET Rust runtime"
          >
            <RefreshCw size={13} />
            <span>Start Runtime</span>
          </button>
        )}
      </div>
    </header>
  );
};
