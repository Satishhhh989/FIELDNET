import React, { useState } from 'react';
import { Users, WifiOff, Copy, Check, ShieldCheck } from 'lucide-react';
import type { Peer } from '../../types/fieldnet';

interface PeersViewProps {
  peers: Peer[];
  onDisconnectPeer: (peerId: string) => void;
  onRefreshDiscovery: () => void;
  isRefreshing: boolean;
}

export const PeersView: React.FC<PeersViewProps> = ({
  peers,
  onDisconnectPeer,
  onRefreshDiscovery,
  isRefreshing,
}) => {
  const [selectedPeer, setSelectedPeer] = useState<Peer | null>(null);
  const [copiedId, setCopiedId] = useState<string | null>(null);

  const copyPeerId = (id: string) => {
    navigator.clipboard.writeText(id);
    setCopiedId(id);
    setTimeout(() => setCopiedId(null), 2000);
  };

  const getBadgeClass = (state: string) => {
    switch (state) {
      case 'Connected':
        return 'badge-green';
      case 'Discovered':
        return 'badge-amber';
      default:
        return 'badge-rose';
    }
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
      {/* Header */}
      <div className="view-header">
        <div className="view-title-group">
          <h1>Peer Mesh</h1>
          <p>
            Cryptographically authenticated neighbors, link status, and routing topology.
          </p>
        </div>
        <button
          className="btn btn-secondary btn-sm"
          onClick={onRefreshDiscovery}
          disabled={isRefreshing}
        >
          <span>{isRefreshing ? 'Broadcasting Beacon...' : 'Broadcast Beacon'}</span>
        </button>
      </div>

      {/* Main Peers Table */}
      <div className="card">
        <div className="card-title">
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <Users size={14} style={{ color: 'var(--accent-emerald)' }} />
            <span>Discovered & Connected Peers</span>
          </div>
          <span className="badge badge-gray">{peers.length} Total</span>
        </div>

        {peers.length > 0 ? (
          <div className="data-table-container">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Node ID</th>
                  <th>State</th>
                  <th>Transport Address</th>
                  <th>Link Type</th>
                  <th>Protocol</th>
                  <th>Routes Through</th>
                  <th style={{ textAlign: 'right' }}>Actions</th>
                </tr>
              </thead>
              <tbody>
                {peers.map((p) => {
                  const isConnected = p.connection_state === 'Connected';
                  return (
                    <tr
                      key={p.node_id}
                      style={{ cursor: 'pointer' }}
                      onClick={() => setSelectedPeer(p)}
                    >
                      <td>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                          <ShieldCheck size={13} style={{ color: 'var(--accent-cyan)' }} />
                          <span className="mono" style={{ fontWeight: 600 }}>
                            {p.short_id}
                          </span>
                        </div>
                      </td>
                      <td>
                        <span className={`badge ${getBadgeClass(p.connection_state)}`}>
                          {p.connection_state}
                        </span>
                      </td>
                      <td className="mono" style={{ color: 'var(--text-secondary)' }}>
                        {p.transport_address || 'N/A'}
                      </td>
                      <td>
                        {p.is_direct_neighbor ? (
                          <span className="badge badge-blue">Direct Neighbor</span>
                        ) : (
                          <span className="badge badge-gray">Multi-hop</span>
                        )}
                      </td>
                      <td>
                        <span className="mono" style={{ color: 'var(--text-muted)' }}>
                          v{p.protocol_version}
                        </span>
                      </td>
                      <td>
                        <span className="badge badge-gray">
                          {p.next_hop_for_count} route{p.next_hop_for_count === 1 ? '' : 's'}
                        </span>
                      </td>
                      <td style={{ textAlign: 'right' }}>
                        {isConnected && (
                          <button
                            className="btn btn-danger btn-sm"
                            onClick={(e) => {
                              e.stopPropagation();
                              onDisconnectPeer(p.node_id);
                            }}
                            title="Disconnect transport link"
                          >
                            <WifiOff size={11} />
                            <span>Disconnect</span>
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
            <Users size={28} className="empty-state-icon" />
            <h3>No Peers Discovered</h3>
            <p>
              FIELDNET is continuously listening on UDP 4242 and mDNS. Start additional FIELDNET nodes on the same host or local network to establish mesh peering.
            </p>
          </div>
        )}
      </div>

      {/* Selected Peer Inspector Drawer */}
      {selectedPeer && (
        <div className="card" style={{ borderColor: 'var(--border-strong)' }}>
          <div className="card-title">
            <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              <ShieldCheck size={14} style={{ color: 'var(--accent-cyan)' }} />
              <span>Peer Inspector: {selectedPeer.short_id}</span>
            </div>
            <button
              className="btn btn-secondary btn-sm"
              onClick={() => setSelectedPeer(null)}
              style={{ padding: '2px 8px' }}
            >
              Close
            </button>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: 14 }}>
            <div>
              <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 2 }}>FULL NODE IDENTIFIER</div>
              <div
                className="mono"
                style={{
                  fontSize: 11,
                  wordBreak: 'break-all',
                  display: 'flex',
                  alignItems: 'center',
                  gap: 8,
                }}
              >
                <span>{selectedPeer.node_id}</span>
                <button
                  className="btn btn-secondary btn-sm"
                  onClick={() => copyPeerId(selectedPeer.node_id)}
                  style={{ padding: '2px 6px' }}
                >
                  {copiedId === selectedPeer.node_id ? (
                    <Check size={11} color="var(--accent-emerald)" />
                  ) : (
                    <Copy size={11} />
                  )}
                </button>
              </div>
            </div>

            <div>
              <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 2 }}>TRANSPORT ENDPOINT</div>
              <div className="mono" style={{ fontSize: 12, color: 'var(--accent-cyan)' }}>
                {selectedPeer.transport_address || 'None'}
              </div>
            </div>

            <div>
              <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 2 }}>TOPOLOGY ROLE</div>
              <div style={{ fontSize: 12 }}>
                {selectedPeer.is_direct_neighbor ? 'Direct link neighbor' : 'Discovered via beacon'}
              </div>
            </div>

            <div>
              <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 2 }}>NEXT-HOP CAPACITY</div>
              <div style={{ fontSize: 12 }}>
                Provides routing next-hop for {selectedPeer.next_hop_for_count} destination node(s)
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
