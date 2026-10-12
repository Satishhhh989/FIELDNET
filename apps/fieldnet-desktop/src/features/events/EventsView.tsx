import React, { useState } from 'react';
import { Activity, Filter, ChevronDown, ChevronRight, Pause, Play, Search } from 'lucide-react';
import type { FrontendEvent } from '../../types/fieldnet';

interface EventsViewProps {
  events: FrontendEvent[];
}

export const EventsView: React.FC<EventsViewProps> = ({ events }) => {
  const [selectedCategory, setSelectedCategory] = useState<string>('all');
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [expandedEventId, setExpandedEventId] = useState<string | null>(null);
  const [isPaused, setIsPaused] = useState<boolean>(false);
  const [frozenEvents, setFrozenEvents] = useState<FrontendEvent[] | null>(null);

  const togglePause = () => {
    if (isPaused) {
      setIsPaused(false);
      setFrozenEvents(null);
    } else {
      setIsPaused(true);
      setFrozenEvents([...events]);
    }
  };

  const activeEvents = isPaused && frozenEvents ? frozenEvents : events;

  const filteredEvents = activeEvents.filter((evt) => {
    const matchesCategory =
      selectedCategory === 'all' || evt.category.toLowerCase() === selectedCategory.toLowerCase();
    const matchesSearch =
      searchQuery === '' ||
      evt.summary.toLowerCase().includes(searchQuery.toLowerCase()) ||
      evt.event_type.toLowerCase().includes(searchQuery.toLowerCase()) ||
      (evt.node_id && evt.node_id.toLowerCase().includes(searchQuery.toLowerCase()));
    return matchesCategory && matchesSearch;
  });

  const categories = ['all', 'peer', 'service', 'app', 'content', 'network', 'system'];

  const getBadgeClass = (category: string) => {
    switch (category.toLowerCase()) {
      case 'peer':
        return 'badge-green';
      case 'service':
        return 'badge-blue';
      case 'app':
        return 'badge-purple';
      case 'content':
        return 'badge-amber';
      default:
        return 'badge-gray';
    }
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
      {/* Header */}
      <div className="view-header">
        <div className="view-title-group">
          <h1>Event Bus & Telemetry</h1>
          <p>
            Real-time streaming bus bridging authoritative Rust runtime subsystem events to desktop shell.
          </p>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <button
            className={`btn btn-sm ${isPaused ? 'btn-primary' : 'btn-secondary'}`}
            onClick={togglePause}
            title={isPaused ? 'Resume live stream' : 'Pause live stream'}
          >
            {isPaused ? <Play size={12} /> : <Pause size={12} />}
            <span>{isPaused ? 'Resume' : 'Pause'}</span>
          </button>
        </div>
      </div>

      {/* Filter and Search Bar */}
      <div className="card" style={{ padding: '12px 16px' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 12 }}>
          {/* Category Tabs */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' }}>
            <Filter size={13} style={{ color: 'var(--text-muted)' }} />
            {categories.map((cat) => (
              <button
                key={cat}
                className={`btn btn-sm ${selectedCategory === cat ? 'btn-primary' : 'btn-secondary'}`}
                onClick={() => setSelectedCategory(cat)}
                style={{ padding: '3px 8px', fontSize: 11, textTransform: 'capitalize' }}
              >
                {cat}
              </button>
            ))}
          </div>

          {/* Search Input */}
          <div style={{ position: 'relative', width: 220 }}>
            <Search
              size={12}
              style={{
                position: 'absolute',
                left: 8,
                top: '50%',
                transform: 'translateY(-50%)',
                color: 'var(--text-muted)',
              }}
            />
            <input
              type="text"
              placeholder="Search events..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              style={{
                width: '100%',
                padding: '5px 8px 5px 26px',
                background: 'var(--bg-canvas)',
                border: '1px solid var(--border-medium)',
                borderRadius: 'var(--radius-sm)',
                color: 'var(--text-primary)',
                fontSize: 11,
              }}
            />
          </div>
        </div>
      </div>

      {/* Events Stream Table */}
      <div className="card">
        <div className="card-title">
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <Activity size={14} style={{ color: 'var(--accent-cyan)' }} />
            <span>Event Stream ({filteredEvents.length} items)</span>
          </div>
          {isPaused && <span className="badge badge-amber">Stream Paused</span>}
        </div>

        {filteredEvents.length > 0 ? (
          <div className="data-table-container">
            <table className="data-table">
              <thead>
                <tr>
                  <th style={{ width: 30 }}></th>
                  <th style={{ width: 140 }}>Timestamp</th>
                  <th style={{ width: 90 }}>Category</th>
                  <th style={{ width: 160 }}>Type</th>
                  <th>Summary</th>
                </tr>
              </thead>
              <tbody>
                {filteredEvents.map((evt) => {
                  const isExpanded = expandedEventId === evt.id;
                  return (
                    <React.Fragment key={evt.id}>
                      <tr
                        style={{ cursor: 'pointer' }}
                        onClick={() => setExpandedEventId(isExpanded ? null : evt.id)}
                      >
                        <td style={{ color: 'var(--text-muted)' }}>
                          {isExpanded ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
                        </td>
                        <td className="mono" style={{ fontSize: 11, color: 'var(--text-muted)' }}>
                          {evt.timestamp}
                        </td>
                        <td>
                          <span className={`badge ${getBadgeClass(evt.category)}`}>
                            {evt.category.toUpperCase()}
                          </span>
                        </td>
                        <td className="mono" style={{ fontSize: 11, color: 'var(--accent-cyan)' }}>
                          {evt.event_type}
                        </td>
                        <td style={{ color: 'var(--text-primary)' }}>{evt.summary}</td>
                      </tr>

                      {isExpanded && (
                        <tr>
                          <td colSpan={5} style={{ background: 'var(--bg-canvas)', padding: '12px 16px' }}>
                            <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 4 }}>
                              PAYLOAD DETAILS
                            </div>
                            <pre
                              className="mono"
                              style={{
                                fontSize: 11,
                                color: 'var(--text-secondary)',
                                background: 'var(--bg-surface)',
                                padding: 10,
                                borderRadius: 'var(--radius-sm)',
                                overflowX: 'auto',
                                maxHeight: 200,
                              }}
                            >
                              {JSON.stringify(evt.details || {}, null, 2)}
                            </pre>
                          </td>
                        </tr>
                      )}
                    </React.Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="empty-state">
            <Activity size={28} className="empty-state-icon" />
            <h3>No Events Recorded</h3>
            <p>
              Subsystem events (peer links, service discoveries, CAS chunk receipts, WASM lifecycle transitions) will appear here live.
            </p>
          </div>
        )}
      </div>
    </div>
  );
};
