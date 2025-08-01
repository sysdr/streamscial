import React from 'react';

const EventDashboard = ({ events, stats }) => {
  const formatTimestamp = (timestamp) => {
    return new Date(timestamp).toLocaleTimeString();
  };

  const getEventIcon = (eventType) => {
    const icons = {
      'post_created': '📝',
      'post_liked': '❤️',
      'follow_initiated': '👥',
      'comment_added': '💬',
      'user_registered': '🆕',
      'content_shared': '🔄',
      'story_viewed': '👁️',
      'profile_updated': '✏️',
      'content_moderated': '🛡️',
      'session_expired': '⏰'
    };
    return icons[eventType] || '📊';
  };

  return (
    <div className="event-dashboard">
      <div className="stats-section">
        <h3>📊 System Statistics</h3>
        <div className="stats-grid">
          <div className="stat-card">
            <h4>Total Events</h4>
            <span className="stat-number">{stats.total_events || 0}</span>
          </div>
          <div className="stat-card">
            <h4>Feed Users</h4>
            <span className="stat-number">{stats.feed_users || 0}</span>
          </div>
          <div className="stat-card">
            <h4>Notification Users</h4>
            <span className="stat-number">{stats.notification_users || 0}</span>
          </div>
        </div>
        
        {stats.event_stats && (
          <div className="event-types">
            <h4>Event Breakdown</h4>
            {Object.entries(stats.event_stats).map(([type, count]) => (
              <div key={type} className="event-type-stat">
                <span>{getEventIcon(type)} {type}</span>
                <span className="count">{count}</span>
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="events-section">
        <h3>🔄 Recent Events</h3>
        <div className="events-list">
          {events.map((event, index) => (
            <div key={event.event_id || index} className="event-item">
              <div className="event-header">
                <span className="event-icon">
                  {getEventIcon(event.event_type)}
                </span>
                <span className="event-type">{event.event_type}</span>
                <span className="event-time">
                  {formatTimestamp(event.timestamp)}
                </span>
              </div>
              <div className="event-details">
                <span className="user-id">User: {event.user_id}</span>
                <span className="event-id">ID: {event.event_id?.slice(0, 8)}...</span>
              </div>
              {event.data && Object.keys(event.data).length > 0 && (
                <div className="event-data">
                  {JSON.stringify(event.data, null, 2)}
                </div>
              )}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

export default EventDashboard;
