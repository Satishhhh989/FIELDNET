import React, { useState } from 'react';
import { Boxes, Play, Square, Shield, CheckCircle2 } from 'lucide-react';
import type { Application } from '../../types/fieldnet';

interface ApplicationsViewProps {
  applications: Application[];
  onStartApp: (appId: string) => void;
  onStopApp: (appId: string) => void;
}

function formatBytes(bytes?: number | null): string {
  if (!bytes) return 'Unlimited';
  const mb = bytes / (1024 * 1024);
  return `${mb.toFixed(0)} MB`;
}

export const ApplicationsView: React.FC<ApplicationsViewProps> = ({
  applications,
  onStartApp,
  onStopApp,
}) => {
  const [selectedApp, setSelectedApp] = useState<Application | null>(null);

  const getStateBadgeClass = (state: string) => {
    switch (state) {
      case 'Running':
        return 'badge-green';
      case 'Starting':
      case 'Suspended':
        return 'badge-amber';
      case 'Crashed':
        return 'badge-rose';
      default:
        return 'badge-gray';
    }
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
      {/* Header */}
      <div className="view-header">
        <div className="view-title-group">
          <h1>WASM Applications & Sandbox</h1>
          <p>
            Wasmtime-isolated capability-gated applications, resource budgets, and execution supervisor.
          </p>
        </div>
      </div>

      {/* Applications Table */}
      <div className="card">
        <div className="card-title">
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <Boxes size={14} style={{ color: 'var(--accent-purple)' }} />
            <span>Application Registry</span>
          </div>
          <span className="badge badge-gray">{applications.length} Packages</span>
        </div>

        {applications.length > 0 ? (
          <div className="data-table-container">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Application ID</th>
                  <th>Version</th>
                  <th>State</th>
                  <th>Publisher</th>
                  <th>Memory Limit</th>
                  <th>Execution Fuel</th>
                  <th style={{ textAlign: 'right' }}>Actions</th>
                </tr>
              </thead>
              <tbody>
                {applications.map((app) => {
                  const isRunning = app.lifecycle_state === 'Running';
                  return (
                    <tr
                      key={app.application_id}
                      style={{ cursor: 'pointer' }}
                      onClick={() => setSelectedApp(app)}
                    >
                      <td>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                          <span style={{ fontWeight: 600, color: 'var(--text-primary)' }}>
                            {app.application_id}
                          </span>
                        </div>
                      </td>
                      <td className="mono">{app.version}</td>
                      <td>
                        <span className={`badge ${getStateBadgeClass(app.lifecycle_state)}`}>
                          {app.lifecycle_state}
                        </span>
                      </td>
                      <td style={{ color: 'var(--text-secondary)' }}>{app.publisher}</td>
                      <td className="mono">{formatBytes(app.resource_limits.max_memory_bytes)}</td>
                      <td className="mono">
                        {app.resource_limits.max_fuel ? `${(app.resource_limits.max_fuel / 1_000_000).toFixed(0)}M` : 'Default'}
                      </td>
                      <td style={{ textAlign: 'right' }}>
                        {isRunning ? (
                          <button
                            className="btn btn-danger btn-sm"
                            onClick={(e) => {
                              e.stopPropagation();
                              onStopApp(app.application_id);
                            }}
                            title="Stop execution instance"
                          >
                            <Square size={11} />
                            <span>Stop</span>
                          </button>
                        ) : (
                          <button
                            className="btn btn-primary btn-sm"
                            onClick={(e) => {
                              e.stopPropagation();
                              onStartApp(app.application_id);
                            }}
                            title="Spawn isolated Wasmtime instance"
                          >
                            <Play size={11} />
                            <span>Launch</span>
                          </button>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="empty-state">
            <Boxes size={28} className="empty-state-icon" />
            <h3>No WASM Applications Installed</h3>
            <p>
              The application supervisor registry is currently empty. WASM bundles installed from the mesh or registered via CLI will appear here.
            </p>
          </div>
        )}
      </div>

      {/* Selected App Inspector */}
      {selectedApp && (
        <div className="card" style={{ borderColor: 'var(--border-strong)' }}>
          <div className="card-title">
            <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              <Shield size={14} style={{ color: 'var(--accent-purple)' }} />
              <span>Security Permissions & Sandbox: {selectedApp.application_id}</span>
            </div>
            <button
              className="btn btn-secondary btn-sm"
              onClick={() => setSelectedApp(null)}
              style={{ padding: '2px 8px' }}
            >
              Close
            </button>
          </div>

          <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
            <div>
              <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 6, fontWeight: 600 }}>
                AUTHORIZED CAPABILITIES
              </div>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                {selectedApp.authorized_permissions.length > 0 ? (
                  selectedApp.authorized_permissions.map((perm, idx) => (
                    <span key={idx} className="badge badge-green">
                      <CheckCircle2 size={10} /> {perm}
                    </span>
                  ))
                ) : (
                  <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>
                    Zero external permissions granted (strict isolation sandbox).
                  </span>
                )}
              </div>
            </div>

            <div>
              <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 6, fontWeight: 600 }}>
                ACTIVE EXECUTION INSTANCES ({selectedApp.active_executions.length})
              </div>
              {selectedApp.active_executions.length > 0 ? (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                  {selectedApp.active_executions.map((exec) => (
                    <div
                      key={exec.execution_id}
                      style={{
                        background: 'var(--bg-canvas)',
                        padding: '8px 12px',
                        borderRadius: 'var(--radius-sm)',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'space-between',
                      }}
                    >
                      <span className="mono" style={{ fontSize: 11, color: 'var(--text-primary)' }}>
                        Instance: {exec.execution_id}
                      </span>
                      <span className="badge badge-green">{exec.state}</span>
                    </div>
                  ))}
                </div>
              ) : (
                <div style={{ fontSize: 12, color: 'var(--text-dim)' }}>
                  No active Wasmtime instances running for this package.
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
