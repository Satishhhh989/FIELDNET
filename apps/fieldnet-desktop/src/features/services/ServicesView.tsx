import React, { useState } from 'react';
import { Server, Search, CheckCircle2, AlertCircle, ShieldCheck } from 'lucide-react';
import { api } from '../../api/client';
import type { Service, ServiceResolution } from '../../types/fieldnet';

interface ServicesViewProps {
  services: Service[];
  onRefresh: () => void;
}

export const ServicesView: React.FC<ServicesViewProps> = ({ services, onRefresh }) => {
  const [queryServiceId, setQueryServiceId] = useState('');
  const [resolving, setResolving] = useState(false);
  const [resolutionResult, setResolutionResult] = useState<ServiceResolution | null>(null);
  const [resolutionError, setResolutionError] = useState<string | null>(null);

  const handleResolveService = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!queryServiceId.trim()) return;

    setResolving(true);
    setResolutionResult(null);
    setResolutionError(null);

    try {
      const res = await api.resolveService(queryServiceId.trim());
      if (res) {
        setResolutionResult(res);
      } else {
        setResolutionError(`Service "${queryServiceId.trim()}" could not be resolved in mesh.`);
      }
      onRefresh();
    } catch (err: unknown) {
      setResolutionError(`Resolution failed: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setResolving(false);
    }
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
      {/* Header */}
      <div className="view-header">
        <div className="view-title-group">
          <h1>Distributed Service Registry</h1>
          <p>
            Locally hosted and peer-advertised service endpoints, capability maps, and health metrics.
          </p>
        </div>
      </div>

      {/* Services Table */}
      <div className="card">
        <div className="card-title">
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <Server size={14} style={{ color: 'var(--accent-blue)' }} />
            <span>Registered Service Records</span>
          </div>
          <span className="badge badge-gray">{services.length} Services</span>
        </div>

        {services.length > 0 ? (
          <div className="data-table-container">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Service ID</th>
                  <th>Origin</th>
                  <th>Provider Node</th>
                  <th>Protocol</th>
                  <th>Version</th>
                  <th>State</th>
                  <th>Reliability</th>
                  <th>Latency</th>
                </tr>
              </thead>
              <tbody>
                {services.map((s) => (
                  <tr key={s.instance_id}>
                    <td>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                        <span style={{ fontWeight: 600, color: 'var(--text-primary)' }}>
                          {s.service_id}
                        </span>
                      </div>
                    </td>
                    <td>
                      {s.is_local ? (
                        <span className="badge badge-blue">Local Node</span>
                      ) : (
                        <span className="badge badge-gray">Remote Peer</span>
                      )}
                    </td>
                    <td>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                        <ShieldCheck size={12} color="var(--accent-cyan)" />
                        <span className="mono" title={s.provider_node_id}>
                          {s.short_provider_id}
                        </span>
                      </div>
                    </td>
                    <td>
                      <span className="badge badge-gray">{s.protocol.toUpperCase()}</span>
                    </td>
                    <td className="mono">{s.version}</td>
                    <td>
                      <span className={`badge ${s.state === 'Available' ? 'badge-green' : s.state === 'Degraded' ? 'badge-amber' : 'badge-rose'}`}>
                        {s.state}
                      </span>
                    </td>
                    <td className="mono">
                      {(s.reliability_permille / 10).toFixed(1)}%
                    </td>
                    <td className="mono">
                      {s.observed_rtt_ms > 0 ? `${s.observed_rtt_ms} ms` : '< 1 ms'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="empty-state">
            <Server size={28} className="empty-state-icon" />
            <h3>No Services Registered</h3>
            <p>
              No local or remote service advertisements have been indexed. Services hosted by local WASM apps or announced by connected peers will appear here.
            </p>
          </div>
        )}
      </div>

      {/* Interactive Service Resolver */}
      <div className="card">
        <div className="card-title">
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <Search size={14} style={{ color: 'var(--accent-cyan)' }} />
            <span>Service Resolution Query Test</span>
          </div>
        </div>

        <form onSubmit={handleResolveService} style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          <div style={{ display: 'flex', gap: 12 }}>
            <input
              type="text"
              placeholder="e.g. fieldnet.gateway or any service identifier..."
              value={queryServiceId}
              onChange={(e) => setQueryServiceId(e.target.value)}
              className="mono"
              style={{
                flex: 1,
                padding: '8px 12px',
                background: 'var(--bg-canvas)',
                border: '1px solid var(--border-medium)',
                borderRadius: 'var(--radius-sm)',
                color: 'var(--text-primary)',
                fontSize: 12,
              }}
            />
            <button
              type="submit"
              className="btn btn-primary"
              disabled={resolving || !queryServiceId.trim()}
            >
              <Search size={13} />
              <span>{resolving ? 'Resolving...' : 'Resolve Service'}</span>
            </button>
          </div>

          {resolutionError && (
            <div style={{ fontSize: 12, color: 'var(--accent-rose)', display: 'flex', alignItems: 'center', gap: 4 }}>
              <AlertCircle size={13} /> {resolutionError}
            </div>
          )}

          {resolutionResult && (
            <div
              style={{
                background: 'var(--bg-canvas)',
                padding: 14,
                borderRadius: 'var(--radius-md)',
                border: '1px solid var(--border-medium)',
                display: 'flex',
                flexDirection: 'column',
                gap: 8,
              }}
            >
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                  <CheckCircle2 size={14} color="var(--accent-emerald)" />
                  <span style={{ fontWeight: 600 }}>Resolved: {resolutionResult.service_id}</span>
                </div>
                <span className="badge badge-green">
                  {resolutionResult.route_hops === 0 ? 'Direct Local Provider' : `${resolutionResult.route_hops} Hops`}
                </span>
              </div>

              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: 10, marginTop: 4 }}>
                <div>
                  <div style={{ fontSize: 10.5, color: 'var(--text-muted)' }}>PROVIDER NODE ID</div>
                  <div className="mono" style={{ fontSize: 11, color: 'var(--text-primary)' }}>
                    {resolutionResult.provider_node_id}
                  </div>
                </div>
                <div>
                  <div style={{ fontSize: 10.5, color: 'var(--text-muted)' }}>ENDPOINT</div>
                  <div className="mono" style={{ fontSize: 11, color: 'var(--accent-cyan)' }}>
                    {resolutionResult.endpoint}
                  </div>
                </div>
                <div>
                  <div style={{ fontSize: 10.5, color: 'var(--text-muted)' }}>SELECTION RATIONALE</div>
                  <div style={{ fontSize: 11, color: 'var(--text-secondary)' }}>
                    {resolutionResult.reason}
                  </div>
                </div>
              </div>
            </div>
          )}
        </form>
      </div>
    </div>
  );
};
