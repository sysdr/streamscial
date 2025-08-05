"""
StreamSocial Kafka Monitoring Dashboard - Day 2
Real-time cluster health and metrics visualization
"""

from flask import Flask, render_template, jsonify
from flask_cors import CORS
import json
import time
import threading
import random
import sys
import os

# Add the parent directory to the path so we can import kafka_client
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from kafka import KafkaConsumer
    from kafka.admin import KafkaAdminClient
    from kafka_client import StreamSocialKafkaClient
    KAFKA_AVAILABLE = True
except ImportError:
    print("⚠️ Kafka libraries not available - running in demo mode")
    KAFKA_AVAILABLE = False

# Import system monitor
try:
    from system_monitor import system_monitor
    SYSTEM_MONITOR_AVAILABLE = True
    print("✅ System monitor available")
except ImportError:
    print("⚠️ System monitor not available")
    SYSTEM_MONITOR_AVAILABLE = False

app = Flask(__name__)
CORS(app)

class KafkaMonitor:
    def __init__(self):
        self.metrics = {
            'cluster_health': 'unknown',
            'total_events': 0,
            'events_per_second': 0,
            'broker_count': 0,
            'topics': [],
            'recent_events': [],
            'demo_mode': False,
            'system_events': [],
            'system_summary': {}
        }
        self.running = True
        self.demo_events = 0
        self.start_monitoring()

    def start_monitoring(self):
        """Start background monitoring threads"""
        # Try to initialize Kafka client
        if KAFKA_AVAILABLE:
            try:
                self.client = StreamSocialKafkaClient()
                self.metrics['demo_mode'] = False
                threading.Thread(target=self._monitor_cluster, daemon=True).start()
                threading.Thread(target=self._monitor_events, daemon=True).start()
                print("✅ Kafka monitoring initialized")
            except Exception as e:
                print(f"⚠️ Kafka not available - running in demo mode: {e}")
                self.metrics['demo_mode'] = True
                self.metrics['cluster_health'] = 'demo'
                threading.Thread(target=self._demo_monitoring, daemon=True).start()
        else:
            print("🎭 Running in demo mode - Kafka libraries not available")
            self.metrics['demo_mode'] = True
            self.metrics['cluster_health'] = 'demo'
            threading.Thread(target=self._demo_monitoring, daemon=True).start()
        
        # Start system monitoring if available
        if SYSTEM_MONITOR_AVAILABLE:
            system_monitor.start_monitoring()
            threading.Thread(target=self._monitor_system, daemon=True).start()
            print("🖥️ System monitoring integrated")

    def _monitor_system(self):
        """Monitor system events"""
        while self.running:
            try:
                # Get system events
                system_events = system_monitor.get_recent_events(10)
                self.metrics['system_events'] = system_events
                
                # Get system summary
                system_summary = system_monitor.get_system_summary()
                self.metrics['system_summary'] = system_summary
                
                time.sleep(2)  # Update every 2 seconds
                
            except Exception as e:
                print(f"System monitoring error: {e}")
                time.sleep(5)

    def _demo_monitoring(self):
        """Demo monitoring when Kafka is not available"""
        while self.running:
            # Simulate cluster metrics
            self.metrics['broker_count'] = 3
            self.metrics['topics'] = ['user-actions', 'content-interactions', 'system-events']
            self.metrics['cluster_health'] = 'demo'
            
            # Simulate event generation
            self.demo_events += random.randint(1, 5)
            self.metrics['total_events'] = self.demo_events
            self.metrics['events_per_second'] = random.uniform(10, 50)
            
            # Generate demo events
            demo_event_types = [
                {'topic': 'user-actions', 'action': 'post', 'user_id': random.randint(1, 1000)},
                {'topic': 'user-actions', 'action': 'like', 'user_id': random.randint(1, 1000)},
                {'topic': 'content-interactions', 'interaction': 'view', 'content_id': random.randint(1, 500)},
                {'topic': 'system-events', 'event': 'health_check', 'timestamp': int(time.time() * 1000)}
            ]
            
            event_data = random.choice(demo_event_types)
            demo_event = {
                'topic': event_data['topic'],
                'partition': random.randint(0, 2),
                'offset': self.demo_events,
                'value': event_data,
                'timestamp': time.time()
            }
            
            # Add to recent events (keep last 10)
            self.metrics['recent_events'].append(demo_event)
            if len(self.metrics['recent_events']) > 10:
                self.metrics['recent_events'].pop(0)
            
            time.sleep(2)  # Update every 2 seconds

    def _monitor_cluster(self):
        """Monitor cluster health and metadata"""
        while self.running:
            try:
                metadata = self.client.get_cluster_metadata()
                if metadata:
                    self.metrics['cluster_health'] = 'healthy'
                    self.metrics['broker_count'] = len(metadata['brokers'])
                    self.metrics['topics'] = metadata['topics']
                else:
                    self.metrics['cluster_health'] = 'unhealthy'
                    
            except Exception as e:
                self.metrics['cluster_health'] = 'error'
                print(f"Monitoring error: {e}")
                
            time.sleep(10)  # Check every 10 seconds

    def _monitor_events(self):
        """Monitor event throughput"""
        try:
            consumer = KafkaConsumer(
                'user-actions',
                'content-interactions',
                'system-events',
                bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=1000,
                auto_offset_reset='latest'
            )
            
            events_in_window = []
            window_size = 60  # 1 minute window
            
            for message in consumer:
                current_time = time.time()
                event_data = {
                    'topic': message.topic,
                    'partition': message.partition,
                    'offset': message.offset,
                    'value': message.value,
                    'timestamp': current_time
                }
                
                # Add to recent events (keep last 10)
                self.metrics['recent_events'].append(event_data)
                if len(self.metrics['recent_events']) > 10:
                    self.metrics['recent_events'].pop(0)
                
                # Track events for throughput calculation
                events_in_window.append(current_time)
                
                # Remove events older than window
                events_in_window = [t for t in events_in_window if current_time - t < window_size]
                
                # Update metrics
                self.metrics['total_events'] += 1
                self.metrics['events_per_second'] = len(events_in_window) / window_size
                
        except Exception as e:
            print(f"Event monitoring error: {e}")

monitor = KafkaMonitor()

@app.route('/')
def dashboard():
    """Main dashboard page"""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>StreamSocial Kafka Monitor</title>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            body { 
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
                margin: 0; 
                padding: 20px; 
                background: #f8fafc;
                color: #334155;
            }
            .container { max-width: 1400px; margin: 0 auto; }
            .header { 
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white; 
                padding: 2rem; 
                border-radius: 12px; 
                margin-bottom: 2rem;
                text-align: center;
            }
            .demo-banner {
                background: #f59e0b;
                color: white;
                padding: 1rem;
                border-radius: 8px;
                margin-bottom: 1rem;
                text-align: center;
                font-weight: 500;
            }
            .metrics-grid { 
                display: grid; 
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); 
                gap: 1.5rem; 
                margin-bottom: 2rem;
            }
            .metric-card { 
                background: white; 
                padding: 1.5rem; 
                border-radius: 12px; 
                box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
                border-left: 4px solid #10b981;
            }
            .metric-value { 
                font-size: 2rem; 
                font-weight: bold; 
                margin: 0.5rem 0;
                color: #059669;
            }
            .metric-label { 
                color: #6b7280; 
                font-size: 0.875rem;
                text-transform: uppercase;
                letter-spacing: 0.05em;
            }
            .events-section { 
                background: white; 
                padding: 1.5rem; 
                border-radius: 12px; 
                box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
                margin-bottom: 2rem;
            }
            .event-item { 
                padding: 0.75rem; 
                border-bottom: 1px solid #e5e7eb; 
                font-family: 'Monaco', 'Consolas', monospace;
                font-size: 0.875rem;
            }
            .system-event {
                background: #f0f9ff;
                border-left: 3px solid #3b82f6;
            }
            .kafka-event {
                background: #f0fdf4;
                border-left: 3px solid #10b981;
            }
            .status-healthy { color: #10b981; }
            .status-unhealthy { color: #ef4444; }
            .status-unknown { color: #f59e0b; }
            .status-demo { color: #f59e0b; }
            .refresh-btn {
                background: #3b82f6;
                color: white;
                border: none;
                padding: 0.75rem 1.5rem;
                border-radius: 8px;
                cursor: pointer;
                font-size: 0.875rem;
                font-weight: 500;
            }
            .refresh-btn:hover { background: #2563eb; }
            .tabs {
                display: flex;
                margin-bottom: 1rem;
                border-bottom: 2px solid #e5e7eb;
            }
            .tab {
                padding: 0.75rem 1.5rem;
                cursor: pointer;
                border-bottom: 2px solid transparent;
                transition: all 0.3s ease;
            }
            .tab.active {
                border-bottom-color: #3b82f6;
                color: #3b82f6;
                font-weight: 600;
            }
            .tab-content {
                display: none;
            }
            .tab-content.active {
                display: block;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🚀 StreamSocial Kafka Monitor</h1>
                <p>Real-time cluster health, system metrics, and event tracking</p>
                <button class="refresh-btn" onclick="location.reload()">🔄 Refresh Dashboard</button>
            </div>
            
            <div id="demo-banner" class="demo-banner" style="display: none;">
                🎭 Demo Mode Active - Simulated data for demonstration purposes
            </div>
            
            <div class="metrics-grid">
                <div class="metric-card">
                    <div class="metric-label">Cluster Health</div>
                    <div id="cluster-health" class="metric-value">Loading...</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Active Brokers</div>
                    <div id="broker-count" class="metric-value">0</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Total Events</div>
                    <div id="total-events" class="metric-value">0</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Events/Second</div>
                    <div id="events-per-second" class="metric-value">0.0</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">CPU Usage</div>
                    <div id="cpu-usage" class="metric-value">0%</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Memory Usage</div>
                    <div id="memory-usage" class="metric-value">0%</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Disk Usage</div>
                    <div id="disk-usage" class="metric-value">0%</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Process Count</div>
                    <div id="process-count" class="metric-value">0</div>
                </div>
            </div>
            
            <div class="events-section">
                <div class="tabs">
                    <div class="tab active" onclick="switchTab('all')">📊 All Events</div>
                    <div class="tab" onclick="switchTab('kafka')">🔗 Kafka Events</div>
                    <div class="tab" onclick="switchTab('system')">🖥️ System Events</div>
                </div>
                
                <div id="all-events" class="tab-content active">
                    <h3>📊 Recent Events (All)</h3>
                    <div id="recent-events">Loading events...</div>
                </div>
                
                <div id="kafka-events" class="tab-content">
                    <h3>🔗 Kafka Events</h3>
                    <div id="kafka-events-list">Loading Kafka events...</div>
                </div>
                
                <div id="system-events" class="tab-content">
                    <h3>🖥️ System Events</h3>
                    <div id="system-events-list">Loading system events...</div>
                </div>
            </div>
        </div>

        <script>
            let currentTab = 'all';
            
            function switchTab(tabName) {
                // Update tab styles
                document.querySelectorAll('.tab').forEach(tab => tab.classList.remove('active'));
                document.querySelectorAll('.tab-content').forEach(content => content.classList.remove('active'));
                
                // Show selected tab
                event.target.classList.add('active');
                document.getElementById(tabName + '-events').classList.add('active');
                
                currentTab = tabName;
            }
            
            function updateDashboard() {
                fetch('/api/metrics')
                    .then(response => response.json())
                    .then(data => {
                        // Show/hide demo banner
                        const demoBanner = document.getElementById('demo-banner');
                        if (data.demo_mode) {
                            demoBanner.style.display = 'block';
                        } else {
                            demoBanner.style.display = 'none';
                        }
                        
                        // Update Kafka metrics
                        document.getElementById('cluster-health').textContent = data.cluster_health.toUpperCase();
                        document.getElementById('cluster-health').className = 'metric-value status-' + data.cluster_health;
                        document.getElementById('broker-count').textContent = data.broker_count;
                        document.getElementById('total-events').textContent = data.total_events.toLocaleString();
                        document.getElementById('events-per-second').textContent = data.events_per_second.toFixed(2);
                        
                        // Update system metrics
                        if (data.system_summary) {
                            document.getElementById('cpu-usage').textContent = (data.system_summary.cpu_percent || 0).toFixed(1) + '%';
                            document.getElementById('memory-usage').textContent = (data.system_summary.memory_percent || 0).toFixed(1) + '%';
                            document.getElementById('disk-usage').textContent = (data.system_summary.disk_percent || 0).toFixed(1) + '%';
                            document.getElementById('process-count').textContent = data.system_summary.process_count || 0;
                        }
                        
                        // Update events based on current tab
                        updateEvents(data);
                    })
                    .catch(error => {
                        console.error('Error updating dashboard:', error);
                    });
            }
            
            function updateEvents(data) {
                // Combine Kafka and system events
                const allEvents = [];
                
                // Add Kafka events
                if (data.recent_events) {
                    data.recent_events.forEach(event => {
                        allEvents.push({
                            ...event,
                            source: 'kafka',
                            displayTime: new Date(event.timestamp * 1000).toLocaleTimeString()
                        });
                    });
                }
                
                // Add system events
                if (data.system_events) {
                    data.system_events.forEach(event => {
                        allEvents.push({
                            ...event,
                            source: 'system',
                            displayTime: new Date(event.timestamp).toLocaleTimeString()
                        });
                    });
                }
                
                // Sort by timestamp
                allEvents.sort((a, b) => b.timestamp - a.timestamp);
                
                // Update based on current tab
                if (currentTab === 'all') {
                    const eventsHtml = allEvents.slice(0, 15).map(event => 
                        `<div class="event-item ${event.source}-event">
                            <strong>[${event.displayTime}] ${event.source.toUpperCase()}</strong> - 
                            ${event.source === 'kafka' ? 
                                `${event.topic}[${event.partition}] offset ${event.offset} - ${JSON.stringify(event.value).substring(0, 80)}...` :
                                `${event.event_type}: ${formatSystemEvent(event)}`
                            }
                        </div>`
                    ).join('');
                    document.getElementById('recent-events').innerHTML = eventsHtml || '<div class="event-item">No recent events</div>';
                } else if (currentTab === 'kafka') {
                    const kafkaEvents = allEvents.filter(e => e.source === 'kafka');
                    const eventsHtml = kafkaEvents.slice(0, 10).map(event => 
                        `<div class="event-item kafka-event">
                            <strong>[${event.displayTime}]</strong> ${event.topic}[${event.partition}] offset ${event.offset} - 
                            ${JSON.stringify(event.value).substring(0, 100)}...
                        </div>`
                    ).join('');
                    document.getElementById('kafka-events-list').innerHTML = eventsHtml || '<div class="event-item">No Kafka events</div>';
                } else if (currentTab === 'system') {
                    const systemEvents = allEvents.filter(e => e.source === 'system');
                    const eventsHtml = systemEvents.slice(0, 10).map(event => 
                        `<div class="event-item system-event">
                            <strong>[${event.displayTime}]</strong> ${event.event_type}: ${formatSystemEvent(event)}
                        </div>`
                    ).join('');
                    document.getElementById('system-events-list').innerHTML = eventsHtml || '<div class="event-item">No system events</div>';
                }
            }
            
            function formatSystemEvent(event) {
                switch (event.event_type) {
                    case 'cpu_usage':
                        return `CPU: ${event.cpu_percent}%`;
                    case 'memory_usage':
                        return `Memory: ${event.memory_percent}%`;
                    case 'disk_usage':
                        return `Disk: ${event.disk_percent}%`;
                    case 'network_activity':
                        return `Network: ${(event.bytes_sent / 1024 / 1024).toFixed(2)}MB sent, ${(event.bytes_recv / 1024 / 1024).toFixed(2)}MB received`;
                    case 'process_info':
                        return `${event.total_processes} processes, Top CPU: ${event.top_cpu_processes[0]?.name || 'N/A'}`;
                    case 'system_load':
                        return `Load: ${event.load_1min?.toFixed(2) || 'N/A'} (1min)`;
                    case 'battery_status':
                        return `Battery: ${event.percent}% ${event.power_plugged ? '(plugged)' : '(unplugged)'}`;
                    case 'temperature':
                        return `Temperature sensors: ${Object.keys(event.sensors).length} active`;
                    default:
                        return JSON.stringify(event).substring(0, 100) + '...';
                }
            }

            // Update dashboard every 3 seconds
            updateDashboard();
            setInterval(updateDashboard, 3000);
        </script>
    </body>
    </html>
    """

@app.route('/api/metrics')
def get_metrics():
    """API endpoint for metrics data"""
    return jsonify(monitor.metrics)

@app.route('/api/cluster/test')
def test_cluster():
    """Test cluster connectivity and send sample events"""
    try:
        if monitor.metrics['demo_mode']:
            return jsonify({
                'status': 'demo',
                'message': 'Running in demo mode - Kafka cluster not available'
            })
        
        # Send test events
        client = StreamSocialKafkaClient()
        test_event = {
            'event_type': 'test',
            'message': 'Dashboard connectivity test',
            'timestamp': int(time.time() * 1000)
        }
        
        client.send_event('system-events', test_event)
        
        return jsonify({
            'status': 'success',
            'message': 'Test event sent successfully'
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/api/system/summary')
def get_system_summary():
    """API endpoint for system summary"""
    if SYSTEM_MONITOR_AVAILABLE:
        return jsonify(system_monitor.get_system_summary())
    else:
        return jsonify({'error': 'System monitor not available'})

if __name__ == '__main__':
    print("🖥️ Starting StreamSocial Kafka Monitor...")
    print("📊 Dashboard: http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=False)
