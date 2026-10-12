import React from 'react';
import { Users, Server, HardDrive, Clock, Wifi } from 'lucide-react';
import type { RuntimeStatus, StorageStatus } from '../types/fieldnet';

interface StatusBarProps {
  status: RuntimeStatus | null;
  storage: StorageStatus | null;
  peerCount: number;
  serviceCount: number;
  appCount: number;
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${(bytes / Math.pow(k, i)).toFixed(1)} ${sizes[i]}`;
}

function formatUptime(seconds: number): string {
  const hrs = Math.floor(seconds / 3600);
  const mins = Math.floor((seconds % 3600) / 60);
  const secs = seconds % 60;
  return `${hrs.toString().padStart(2, '0')}:${mins.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
}

export const StatusBar: React.FC<StatusBarProps> = ({
  status,
  storage,
  peerCount,
  serviceCount,
  appCount,
}) => {
  const isRunning = status?.is_running ?? false;
  const shortId = status?.short_node_id || 'uninitialized';
  const tcpPort = status?.tcp_port ?? 0;
  const uptime = status?.uptime_secs ?? 0;

  return (
    <footer className="app-statusbar">
      <div className="statusbar-left">
        <div className="status-item">
          <span
            className={`status-dot ${isRunning ? 'running' : 'offline'}`}
            style={{ width: 6, height: 6 }}
          />
          <span className="status-item-label">NODE:</span>
          <span>{shortId}</span>
        </div>
        <div className="status-item">
          <Wifi size={11} style={{ color: 'var(--text-muted)' }} />
          <span className="status-item-label">PORT:</span>
          <span>{tcpPort > 0 ? tcpPort : 'OFFLINE'}</span>
        </div>
      </div>

      <div className="statusbar-center">
        <div className="status-item">
          <Users size={11} style={{ color: 'var(--text-muted)' }} />
          <span className="status-item-label">PEERS:</span>
          <span>{peerCount}</span>
        </div>
        <div className="status-item">
          <Server size={11} style={{ color: 'var(--text-muted)' }} />
          <span className="status-item-label">SERVICES:</span>
          <span>{serviceCount}</span>
        </div>
        <div className="status-item">
          <span className="status-item-label">APPS:</span>
          <span>{appCount}</span>
        </div>
      </div>

      <div className="statusbar-right">
        {storage && (
          <div className="status-item">
            <HardDrive size={11} style={{ color: 'var(--text-muted)' }} />
            <span className="status-item-label">CAS:</span>
            <span>
              {formatBytes(storage.used_bytes)} / {formatBytes(storage.limit_bytes)}
            </span>
          </div>
        )}
        <div className="status-item">
          <Clock size={11} style={{ color: 'var(--text-muted)' }} />
          <span className="status-item-label">UPTIME:</span>
          <span>{formatUptime(uptime)}</span>
        </div>
      </div>
    </footer>
  );
};
