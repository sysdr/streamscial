"""
StreamSocial Real-time Replication Monitoring Dashboard
"""
import json
import time
from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO, emit
import threading
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from monitors.replication_monitor import ReplicationMonitor
from utils.chaos_engineer import ChaosEngineer

app = Flask(__name__, static_folder='static', static_url_path='/static')
app.config['SECRET_KEY'] = 'streamsocial_secret_2025'
socketio = SocketIO(app, cors_allowed_origins="*")

# Global monitoring components
replication_monitor = None
chaos_engineer = None
dashboard_data = {
    'cluster_health': {},
    'recent_events': [],
    'active_failures': {}
}

@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/api/cluster-status')
def cluster_status():
    """Get current cluster status"""
    if replication_monitor:
        return jsonify(replication_monitor.get_monitoring_data())
    return jsonify({'error': 'Monitor not initialized'})

@app.route('/api/inject-failure/<scenario>')
def inject_failure(scenario):
    """Inject a failure scenario"""
    if not chaos_engineer:
        return jsonify({'error': 'Chaos engineer not initialized'})
    
    try:
        failure_id = chaos_engineer.inject_failure(scenario)
        return jsonify({
            'success': True,
            'failure_id': failure_id,
            'message': f'Injected {scenario}'
        })
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/recovery/<failure_id>')
def manual_recovery(failure_id):
    """Trigger manual recovery"""
    if not chaos_engineer:
        return jsonify({'error': 'Chaos engineer not initialized'})
    
    success = chaos_engineer.manual_recover(failure_id)
    return jsonify({'success': success})

@app.route('/api/failure-status')
def failure_status():
    """Get failure status"""
    if chaos_engineer:
        return jsonify(chaos_engineer.get_failure_status())
    return jsonify({'active_failures': 0, 'total_failures': 0, 'failures': {}})

def background_monitoring():
    """Background thread for real-time monitoring"""
    global dashboard_data, replication_monitor
    
    while True:
        try:
            if replication_monitor:
                monitoring_data = replication_monitor.get_monitoring_data()
                dashboard_data.update(monitoring_data)
                
                # Emit real-time updates to all connected clients
                socketio.emit('cluster_update', monitoring_data, broadcast=True)
                print(f"📡 Broadcasting cluster update: {monitoring_data['health_score']['overall_health']:.1f}% health")
            
            time.sleep(5)  # Update every 5 seconds
            
        except Exception as e:
            print(f"Background monitoring error: {e}")
            time.sleep(5)

@socketio.on('connect')
def handle_connect():
    emit('connected', {'message': 'Connected to StreamSocial monitoring'})

@socketio.on('request_status')
def handle_status_request():
    """Handle real-time status requests"""
    if replication_monitor:
        data = replication_monitor.get_monitoring_data()
        emit('cluster_update', data)

def initialize_monitoring():
    """Initialize monitoring components"""
    global replication_monitor, chaos_engineer
    
    bootstrap_servers = "localhost:9091,localhost:9092,localhost:9093"
    
    try:
        replication_monitor = ReplicationMonitor(bootstrap_servers)
        replication_monitor.start_monitoring(interval_seconds=3)
        print("✅ Replication monitor initialized")
        
        chaos_engineer = ChaosEngineer()
        print("✅ Chaos engineer initialized")
        
        # Start background monitoring
        monitor_thread = threading.Thread(target=background_monitoring, daemon=True)
        monitor_thread.start()
        print("✅ Background monitoring started")
        
    except Exception as e:
        print(f"❌ Failed to initialize monitoring: {e}")

if __name__ == '__main__':
    initialize_monitoring()
    socketio.run(app, host='0.0.0.0', port=5000, debug=True, allow_unsafe_werkzeug=True)
