import React, { useState, useEffect } from 'react';
import EventDashboard from './components/EventDashboard';
import EventPublisher from './components/EventPublisher';
import './App.css';

function App() {
  const [events, setEvents] = useState([]);
  const [stats, setStats] = useState({});
  const [ws, setWs] = useState(null);

  useEffect(() => {
    // Connect to WebSocket
    const websocket = new WebSocket('ws://localhost:8000/ws');
    websocket.onopen = () => {
      console.log('WebSocket connected');
      setWs(websocket);
    };
    websocket.onmessage = (event) => {
      const data = JSON.parse(event.data);
      console.log('WebSocket message:', data);
      fetchEvents();
      fetchStats();
    };

    fetchEvents();
    fetchStats();

    return () => {
      if (websocket) {
        websocket.close();
      }
    };
  }, []);

  const fetchEvents = async () => {
    try {
      const response = await fetch('/api/events?limit=20');
      const data = await response.json();
      setEvents(data.events);
    } catch (error) {
      console.error('Failed to fetch events:', error);
    }
  };

  const fetchStats = async () => {
    try {
      const response = await fetch('/api/stats');
      const data = await response.json();
      setStats(data);
    } catch (error) {
      console.error('Failed to fetch stats:', error);
    }
  };

  return (
    <div className="app">
      <header className="app-header">
        <h1>🚀 StreamSocial Event System</h1>
        <p>Real-time Event-Driven Architecture Demo</p>
      </header>
      
      <div className="app-content">
        <div className="left-panel">
          <EventPublisher onEventPublished={() => {
            fetchEvents();
            fetchStats();
          }} />
        </div>
        
        <div className="right-panel">
          <EventDashboard events={events} stats={stats} />
        </div>
      </div>
    </div>
  );
}

export default App;
