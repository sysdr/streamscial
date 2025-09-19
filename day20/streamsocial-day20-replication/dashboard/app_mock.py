"""
StreamSocial Real-time Replication Monitoring Dashboard (Mock Version)
"""
import json
import time
import random
from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO, emit
import threading

app = Flask(__name__)
app.config['SECRET_KEY'] = 'streamsocial_secret_2025'
socketio = SocketIO(app, cors_allowed_origins="*")

# Mock monitoring data
dashboard_data = {
    'cluster_health': {},
    'recent_events': [],
    'active_failures': {}
}

def generate_mock_data():
    """Generate realistic mock monitoring data"""
    return {
        'timestamp': time.time(),
        'cluster_metadata': {
            'cluster_id': 'mock-cluster-123',
            'controller_id': 1,
            'brokers': {
                '1': {'id': 1, 'host': 'broker-us-1', 'port': 9091, 'rack': 'us-east'},
                '2': {'id': 2, 'host': 'broker-us-2', 'port': 9092, 'rack': 'us-east'},
                '3': {'id': 3, 'host': 'broker-eu-1', 'port': 9093, 'rack': 'eu-west'},
                '4': {'id': 4, 'host': 'broker-eu-2', 'port': 9094, 'rack': 'eu-west'},
                '5': {'id': 5, 'host': 'broker-ap-1', 'port': 9095, 'rack': 'asia-pacific'}
            },
            'topics': {
                'critical_posts': {
                    'partitions': {
                        '0': {'leader': 1, 'replicas': [1, 2, 3, 4, 5], 'isr': [1, 2, 3, 4, 5], 'offline_replicas': [], 'under_replicated': False, 'leader_epoch': 1},
                        '1': {'leader': 2, 'replicas': [1, 2, 3, 4, 5], 'isr': [1, 2, 3, 4, 5], 'offline_replicas': [], 'under_replicated': False, 'leader_epoch': 1},
                        '2': {'leader': 3, 'replicas': [1, 2, 3, 4, 5], 'isr': [1, 2, 3, 4, 5], 'offline_replicas': [], 'under_replicated': False, 'leader_epoch': 1},
                        '3': {'leader': 4, 'replicas': [1, 2, 3, 4, 5], 'isr': [1, 2, 3, 4, 5], 'offline_replicas': [], 'under_replicated': False, 'leader_epoch': 1},
                        '4': {'leader': 5, 'replicas': [1, 2, 3, 4, 5], 'isr': [1, 2, 3, 4, 5], 'offline_replicas': [], 'under_replicated': False, 'leader_epoch': 1},
                        '5': {'leader': 1, 'replicas': [1, 2, 3, 4, 5], 'isr': [1, 2, 3, 4, 5], 'offline_replicas': [], 'under_replicated': False, 'leader_epoch': 1}
                    },
                    'partition_count': 6,
                    'replication_factor': 5
                },
                'user_engagement': {
                    'partitions': {
                        str(i): {'leader': (i % 5) + 1, 'replicas': [1, 2, 3], 'isr': [1, 2, 3], 'offline_replicas': [], 'under_replicated': False, 'leader_epoch': 1}
                        for i in range(12)
                    },
                    'partition_count': 12,
                    'replication_factor': 3
                },
                'analytics_events': {
                    'partitions': {
                        str(i): {'leader': (i % 5) + 1, 'replicas': [1, 2, 3], 'isr': [1, 2, 3], 'offline_replicas': [], 'under_replicated': False, 'leader_epoch': 1}
                        for i in range(24)
                    },
                    'partition_count': 24,
                    'replication_factor': 3
                }
            }
        },
        'health_score': {
            'overall_health': random.uniform(85, 100),
            'total_partitions': 42,
            'healthy_partitions': random.randint(35, 42),
            'under_replicated_partitions': random.randint(0, 3),
            'offline_partitions': 0,
            'status': 'healthy' if random.random() > 0.1 else 'warning'
        },
        'recent_changes': [
            {
                'type': 'isr_change',
                'topic': 'critical_posts',
                'partition': random.randint(0, 5),
                'timestamp': time.time() - random.randint(1, 60),
                'joined_isr': [random.randint(1, 5)],
                'left_isr': [],
                'current_isr': [1, 2, 3, 4, 5],
                'isr_size': 5
            }
        ],
        'replication_lags': {
            'critical_posts': {
                'topic': 'critical_posts',
                'partition_lags': {str(i): random.randint(0, 10) for i in range(6)},
                'max_lag': random.randint(0, 10),
                'avg_lag': random.randint(0, 5)
            },
            'user_engagement': {
                'topic': 'user_engagement',
                'partition_lags': {str(i): random.randint(0, 5) for i in range(12)},
                'max_lag': random.randint(0, 5),
                'avg_lag': random.randint(0, 2)
            }
        }
    }

@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/api/cluster-status')
def cluster_status():
    """Get current cluster status"""
    return jsonify(generate_mock_data())

@app.route('/api/inject-failure/<scenario>')
def inject_failure(scenario):
    """Inject a failure scenario"""
    failure_id = f"{scenario}_{int(time.time())}"
    dashboard_data['active_failures'][failure_id] = {
        'scenario': scenario,
        'status': 'active',
        'start_time': time.time()
    }
    return jsonify({
        'success': True,
        'failure_id': failure_id,
        'message': f'Mock injected {scenario}'
    })

@app.route('/api/recovery/<failure_id>')
def manual_recovery(failure_id):
    """Trigger manual recovery"""
    if failure_id in dashboard_data['active_failures']:
        dashboard_data['active_failures'][failure_id]['status'] = 'recovered'
        return jsonify({'success': True})
    return jsonify({'success': False})

@app.route('/api/failure-status')
def failure_status():
    """Get failure status"""
    active_failures = len([f for f in dashboard_data['active_failures'].values() if f['status'] == 'active'])
    return jsonify({
        'active_failures': active_failures,
        'total_failures': len(dashboard_data['active_failures']),
        'failures': dashboard_data['active_failures']
    })

def background_monitoring():
    """Background thread for real-time monitoring"""
    global dashboard_data
    
    while True:
        try:
            monitoring_data = generate_mock_data()
            dashboard_data.update(monitoring_data)
            
            # Emit real-time updates
            socketio.emit('cluster_update', monitoring_data)
            
        except Exception as e:
            print(f"Background monitoring error: {e}")
        
        time.sleep(5)  # Update every 5 seconds

@socketio.on('connect')
def handle_connect():
    emit('connected', {'message': 'Connected to StreamSocial monitoring (Mock Mode)'})

@socketio.on('request_status')
def handle_status_request():
    """Handle real-time status requests"""
    data = generate_mock_data()
    emit('cluster_update', data)

if __name__ == '__main__':
    print("🚀 Starting StreamSocial Mock Dashboard")
    print("🔗 Dashboard: http://localhost:5000")
    print("⚠️  Note: This is a mock version that simulates data without Docker")
    
    # Start background monitoring
    monitor_thread = threading.Thread(target=background_monitoring, daemon=True)
    monitor_thread.start()
    
    socketio.run(app, host='0.0.0.0', port=5000, debug=True)
