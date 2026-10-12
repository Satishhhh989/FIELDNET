import React, { useState } from 'react';
import { HardDrive, Pin, PinOff, Copy, Check, Database, CheckCircle2 } from 'lucide-react';
import type { StorageStatus } from '../../types/fieldnet';

interface StorageViewProps {
  storage: StorageStatus | null;
  onPinContent: (contentId: string) => void;
  onUnpinContent: (contentId: string) => void;
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${(bytes / Math.pow(k, i)).toFixed(1)} ${sizes[i]}`;
}

export const StorageView: React.FC<StorageViewProps> = ({
  storage,
  onPinContent,
  onUnpinContent,
}) => {
  const [copiedCid, setCopiedCid] = useState<string | null>(null);

  const copyCid = (cid: string) => {
    navigator.clipboard.writeText(cid);
    setCopiedCid(cid);
    setTimeout(() => setCopiedCid(null), 2000);
  };

  const usagePercent =
    storage && storage.limit_bytes > 0
      ? Math.min(100, Math.round((storage.used_bytes / storage.limit_bytes) * 100))
      : 0;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
      {/* Header */}
      <div className="view-header">
        <div className="view-title-group">
          <h1>Content-Addressed Storage (CAS)</h1>
          <p>
            Cryptographically verified immutable content objects, chunk deduplication, and pin controls.
          </p>
        </div>
      </div>

      {/* Quota & Capacity Stats */}
      <div className="card">
        <div className="card-title">
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <Database size={14} style={{ color: 'var(--accent-cyan)' }} />
            <span>Local Storage Quota & Capacity</span>
          </div>
          <span className="badge badge-blue">{usagePercent}% Utilized</span>
        </div>

        {storage ? (
          <div>
            <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, marginBottom: 4 }}>
              <span style={{ color: 'var(--text-secondary)' }}>
                Used: <strong style={{ color: 'var(--text-primary)' }}>{formatBytes(storage.used_bytes)}</strong>
              </span>
              <span style={{ color: 'var(--text-secondary)' }}>
                Available: <strong style={{ color: 'var(--text-primary)' }}>{formatBytes(storage.available_bytes)}</strong> / Total Limit: {formatBytes(storage.limit_bytes)}
              </span>
            </div>

            <div className="quota-bar-container">
              <div className="quota-bar-fill" style={{ width: `${usagePercent}%` }} />
            </div>

            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: 12, marginTop: 14 }}>
              <div style={{ background: 'var(--bg-canvas)', padding: 10, borderRadius: 'var(--radius-sm)', border: '1px solid var(--border-subtle)' }}>
                <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>TOTAL OBJECTS</div>
                <div className="mono" style={{ fontSize: 16, fontWeight: 700, color: 'var(--text-primary)' }}>
                  {storage.object_count}
                </div>
              </div>

              <div style={{ background: 'var(--bg-canvas)', padding: 10, borderRadius: 'var(--radius-sm)', border: '1px solid var(--border-subtle)' }}>
                <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>PINNED OBJECTS</div>
                <div className="mono" style={{ fontSize: 16, fontWeight: 700, color: 'var(--accent-emerald)' }}>
                  {storage.pinned_count}
                </div>
              </div>

              <div style={{ background: 'var(--bg-canvas)', padding: 10, borderRadius: 'var(--radius-sm)', border: '1px solid var(--border-subtle)' }}>
                <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>TOTAL CHUNKS</div>
                <div className="mono" style={{ fontSize: 16, fontWeight: 700, color: 'var(--text-primary)' }}>
                  {storage.chunk_count}
                </div>
              </div>

              <div style={{ background: 'var(--bg-canvas)', padding: 10, borderRadius: 'var(--radius-sm)', border: '1px solid var(--border-subtle)' }}>
                <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>CAS ON-DISK PATH</div>
                <div className="mono" style={{ fontSize: 11, color: 'var(--text-secondary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {storage.storage_dir}
                </div>
              </div>
            </div>
          </div>
        ) : (
          <div className="empty-state" style={{ padding: 20 }}>
            <p>CAS storage subsystem not initialized.</p>
          </div>
        )}
      </div>

      {/* Content Items List */}
      <div className="card">
        <div className="card-title">
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <HardDrive size={14} style={{ color: 'var(--accent-emerald)' }} />
            <span>Stored Content Objects</span>
          </div>
          <span className="badge badge-gray">{storage?.items.length ?? 0} Items</span>
        </div>

        {storage && storage.items.length > 0 ? (
          <div className="data-table-container">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Content ID (Hash)</th>
                  <th>Size</th>
                  <th>Pin Status</th>
                  <th>Chunks</th>
                  <th>Integrity</th>
                  <th style={{ textAlign: 'right' }}>Actions</th>
                </tr>
              </thead>
              <tbody>
                {storage.items.map((item) => (
                  <tr key={item.content_id}>
                    <td>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                        <span className="mono" style={{ fontWeight: 600 }}>
                          {item.short_id}
                        </span>
                        <button
                          className="btn btn-secondary btn-sm"
                          onClick={() => copyCid(item.content_id)}
                          style={{ padding: '2px 5px' }}
                          title="Copy full Content ID hash"
                        >
                          {copiedCid === item.content_id ? (
                            <Check size={11} color="var(--accent-emerald)" />
                          ) : (
                            <Copy size={11} />
                          )}
                        </button>
                      </div>
                    </td>
                    <td className="mono">{formatBytes(item.size_bytes)}</td>
                    <td>
                      {item.is_pinned ? (
                        <span className="badge badge-green">
                          <Pin size={10} /> PINNED
                        </span>
                      ) : (
                        <span className="badge badge-gray">UNPINNED</span>
                      )}
                    </td>
                    <td className="mono">{item.chunk_count}</td>
                    <td>
                      <span className="badge badge-blue">
                        <CheckCircle2 size={10} /> {item.verification_state}
                      </span>
                    </td>
                    <td style={{ textAlign: 'right' }}>
                      {item.is_pinned ? (
                        <button
                          className="btn btn-secondary btn-sm"
                          onClick={() => onUnpinContent(item.content_id)}
                          title="Unpin content object (allows GC reclamation)"
                        >
                          <PinOff size={11} />
                          <span>Unpin</span>
                        </button>
                      ) : (
                        <button
                          className="btn btn-primary btn-sm"
                          onClick={() => onPinContent(item.content_id)}
                          title="Pin content object (protects from GC reclamation)"
                        >
                          <Pin size={11} />
                          <span>Pin</span>
                        </button>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="empty-state">
            <HardDrive size={28} className="empty-state-icon" />
            <h3>No Content Objects Stored</h3>
            <p>
              Local CAS is currently empty. Content items synchronized across peers or published by applications will appear here with BLAKE3 cryptographic hashes.
            </p>
          </div>
        )}
      </div>
    </div>
  );
};
