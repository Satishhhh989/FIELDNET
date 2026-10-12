import React, { useState } from 'react';
import { Radio, Send, Database, GitFork, ArrowRight, CheckCircle2, AlertCircle } from 'lucide-react';
import { api } from '../../api/client';
import type { NetworkStatus } from '../../types/fieldnet';

interface NetworkViewProps {
  network: NetworkStatus | null;
  onRefresh: () => void;
}

export const NetworkView: React.FC<NetworkViewProps> = ({ network, onRefresh }) => {
  const [destNode, setDestNode] = useState('');
  const [payload, setPayload] = useState('');
  const [sending, setSending] = useState(false);
  const [sendResult, setSendResult] = useState<string | null>(null);
  const [sendError, setSendError] = useState<string | null>(null);

  const handleSendMessage = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!destNode.trim() || !payload.trim()) return;

    setSending(true);
    setSendResult(null);
    setSendError(null);

    try {
      const msgId = await api.sendMessage(destNode.trim(), payload.trim());
      setSendResult(`Message dispatched successfully! Tracking ID: ${msgId}`);
      setPayload('');
      onRefresh();
    } catch (err: unknown) {
      setSendError(`Failed to send message: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setSending(false);
    }
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
      {/* Header */}
      <div className="view-header">
        <div className="view-title-group">
          <h1>Network & Transports</h1>
          <p>
            Authoritative multi-transport status, mesh routing table, and DTN store-and-forward queue.
          </p>
        </div>
      </div>

      {/* Transports Grid */}
      <div className="card">
        <div className="card-title">
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <Radio size={14} style={{ color: 'var(--accent-cyan)' }} />
            <span>Active Network Transports & Listeners</span>
          </div>
          <span className="badge badge-blue">
            {network ? `${network.transports.length} Listeners` : 'Offline'}
          </span>
        </div>

        {network && network.transports.length > 0 ? (
          <div className="data-table-container">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Transport</th>
                  <th>Protocol</th>
                  <th>Endpoint</th>
                  <th>State</th>
                  <th>Details</th>
                </tr>
              </thead>
              <tbody>
                {network.transports.map((t, idx) => (
                  <tr key={idx}>
                    <td style={{ fontWeight: 600 }}>{t.name}</td>
                    <td>
                      <span className="badge badge-gray">{t.protocol.toUpperCase()}</span>
                    </td>
                    <td className="mono" style={{ color: 'var(--accent-cyan)' }}>
                      {t.endpoint}
                    </td>
                    <td>
                      <span className="badge badge-green">{t.state}</span>
                    </td>
                    <td style={{ color: 'var(--text-secondary)' }}>{t.details}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="empty-state">
            <AlertCircle size={24} className="empty-state-icon" />
            <h3>No Active Transports</h3>
            <p>Runtime network subsystem is not initialized or listeners failed to bind.</p>
          </div>
        )}
      </div>

      {/* Routing Table & DTN Storage */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(360px, 1fr))', gap: 16 }}>
        {/* Routing Table */}
        <div className="card">
          <div className="card-title">
            <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              <GitFork size={14} style={{ color: 'var(--accent-emerald)' }} />
              <span>Mesh Routing Table</span>
            </div>
            <span className="badge badge-gray">{network ? `${network.routes.length} Routes` : '0'}</span>
          </div>

          {network && network.routes.length > 0 ? (
            <div className="data-table-container">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Destination</th>
                    <th>Next Hop</th>
                    <th>Hops</th>
                    <th>Metric</th>
                  </tr>
                </thead>
                <tbody>
                  {network.routes.map((r, idx) => (
                    <tr key={idx}>
                      <td className="mono" title={r.destination}>
                        {r.short_destination}
                      </td>
                      <td className="mono" title={r.next_hop}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                          <ArrowRight size={10} color="var(--text-dim)" />
                          <span>{r.short_next_hop}</span>
                        </div>
                      </td>
                      <td>{r.hops}</td>
                      <td>
                        <span className="badge badge-blue">{r.metric}</span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="empty-state" style={{ padding: 24 }}>
              <p>Routing table is empty. No peer routes established.</p>
            </div>
          )}
        </div>

        {/* DTN Store-and-Forward Status */}
        <div className="card">
          <div className="card-title">
            <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              <Database size={14} style={{ color: 'var(--accent-purple)' }} />
              <span>DTN Store-and-Forward</span>
            </div>
            <span className="badge badge-purple">
              {network ? `${network.dtn_queue_count} Queued Bundles` : '0'}
            </span>
          </div>

          <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            <div style={{ background: 'var(--bg-canvas)', padding: 12, borderRadius: 'var(--radius-md)', border: '1px solid var(--border-subtle)' }}>
              <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 2 }}>PERSISTENT BUNDLE STORE</div>
              <div className="mono" style={{ fontSize: 11, color: 'var(--text-secondary)' }}>
                {network?.dtn_db_path || 'Not initialized'}
              </div>
            </div>

            <div style={{ background: 'var(--bg-canvas)', padding: 12, borderRadius: 'var(--radius-md)', border: '1px solid var(--border-subtle)' }}>
              <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 2 }}>DISCONNECTED BUNDLE DISPATCH</div>
              <div style={{ fontSize: 12, color: 'var(--text-primary)' }}>
                Bundles destined for temporarily unreachable peers remain safely queued in SQLite and automatically forward upon topology convergence.
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Interactive Dispatched Test Message */}
      <div className="card">
        <div className="card-title">
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <Send size={14} style={{ color: 'var(--accent-blue)' }} />
            <span>Interactive Mesh Message Dispatch</span>
          </div>
        </div>

        <form onSubmit={handleSendMessage} style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))', gap: 12 }}>
            <div>
              <label style={{ display: 'block', fontSize: 11, color: 'var(--text-muted)', marginBottom: 4, fontWeight: 600 }}>
                DESTINATION NODE ID (BASE58)
              </label>
              <input
                type="text"
                placeholder="e.g. 5Kq... or full 64-char NodeId"
                value={destNode}
                onChange={(e) => setDestNode(e.target.value)}
                className="mono"
                style={{
                  width: '100%',
                  padding: '8px 12px',
                  background: 'var(--bg-canvas)',
                  border: '1px solid var(--border-medium)',
                  borderRadius: 'var(--radius-sm)',
                  color: 'var(--text-primary)',
                  fontSize: 12,
                }}
              />
            </div>

            <div>
              <label style={{ display: 'block', fontSize: 11, color: 'var(--text-muted)', marginBottom: 4, fontWeight: 600 }}>
                PAYLOAD TEXT
              </label>
              <input
                type="text"
                placeholder="Message payload data..."
                value={payload}
                onChange={(e) => setPayload(e.target.value)}
                style={{
                  width: '100%',
                  padding: '8px 12px',
                  background: 'var(--bg-canvas)',
                  border: '1px solid var(--border-medium)',
                  borderRadius: 'var(--radius-sm)',
                  color: 'var(--text-primary)',
                  fontSize: 12,
                }}
              />
            </div>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end', gap: 10 }}>
            {sendResult && (
              <span style={{ fontSize: 12, color: 'var(--accent-emerald)', display: 'flex', alignItems: 'center', gap: 4 }}>
                <CheckCircle2 size={13} /> {sendResult}
              </span>
            )}
            {sendError && (
              <span style={{ fontSize: 12, color: 'var(--accent-rose)', display: 'flex', alignItems: 'center', gap: 4 }}>
                <AlertCircle size={13} /> {sendError}
              </span>
            )}
            <button
              type="submit"
              className="btn btn-primary"
              disabled={sending || !destNode.trim() || !payload.trim()}
            >
              <Send size={13} />
              <span>{sending ? 'Dispatching...' : 'Dispatch Message'}</span>
            </button>
          </div>
        </form>
      </div>
    </div>
  );
};
