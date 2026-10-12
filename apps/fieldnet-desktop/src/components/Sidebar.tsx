import React from 'react';
import {
  LayoutDashboard,
  Radio,
  Users,
  HardDrive,
  Server,
  Boxes,
  Activity,
} from 'lucide-react';
import type { ViewTab } from '../types/fieldnet';

interface SidebarProps {
  currentTab: ViewTab;
  onSelectTab: (tab: ViewTab) => void;
  peerCount: number;
  serviceCount: number;
  appCount: number;
  storageItemCount: number;
  eventCount: number;
}

export const Sidebar: React.FC<SidebarProps> = ({
  currentTab,
  onSelectTab,
  peerCount,
  serviceCount,
  appCount,
  storageItemCount,
  eventCount,
}) => {
  const navItems = [
    {
      id: 'overview' as ViewTab,
      label: 'Overview',
      icon: LayoutDashboard,
      badge: null,
    },
    {
      id: 'network' as ViewTab,
      label: 'Network & Mesh',
      icon: Radio,
      badge: null,
    },
    {
      id: 'peers' as ViewTab,
      label: 'Peers',
      icon: Users,
      badge: peerCount > 0 ? peerCount : null,
    },
    {
      id: 'storage' as ViewTab,
      label: 'CAS Storage',
      icon: HardDrive,
      badge: storageItemCount > 0 ? storageItemCount : null,
    },
    {
      id: 'services' as ViewTab,
      label: 'Service Registry',
      icon: Server,
      badge: serviceCount > 0 ? serviceCount : null,
    },
    {
      id: 'applications' as ViewTab,
      label: 'WASM Apps',
      icon: Boxes,
      badge: appCount > 0 ? appCount : null,
    },
    {
      id: 'events' as ViewTab,
      label: 'Event Bus',
      icon: Activity,
      badge: eventCount > 0 ? eventCount : null,
    },
  ];

  return (
    <aside className="app-sidebar">
      <div className="sidebar-category-label">Runtime Shell</div>
      {navItems.map((item) => {
        const Icon = item.icon;
        const isActive = currentTab === item.id;
        return (
          <div
            key={item.id}
            className={`nav-item ${isActive ? 'active' : ''}`}
            onClick={() => onSelectTab(item.id)}
          >
            <div className="nav-item-left">
              <Icon size={16} />
              <span>{item.label}</span>
            </div>
            {item.badge !== null && (
              <span className="nav-item-badge">{item.badge}</span>
            )}
          </div>
        );
      })}
    </aside>
  );
};
