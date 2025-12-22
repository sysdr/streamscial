"""
ACL Monitoring Dashboard - Real-time visualization of access control
"""
from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO
from flask_cors import CORS
import redis
import json
import time
import threading
import os
from typing import Dict, Any

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY', 'streamsocial-acl-secret-dev-only')
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*")

# Redis for real-time metrics
redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)


class ACLMonitor:
    """Monitor ACL access patterns and violations"""
    
    def __init__(self, use_redis=True):
        self.use_redis = use_redis
        self.redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True) if use_redis else None
        self.metrics = {
            'total_requests': 0,
            'granted': 0,
            'denied': 0,
            'by_service': {},
            'by_operation': {},
            'recent_violations': []
        }
        # Load from Redis if available
        if self.use_redis:
            try:
                self._load_from_redis()
            except Exception as e:
                print(f"Warning: Could not load from Redis: {e}")
    
    def _save_to_redis(self):
        """Save metrics to Redis"""
        if not self.use_redis or not self.redis_client:
            return
        try:
            self.redis_client.set('acl_metrics', json.dumps(self.metrics))
        except Exception as e:
            print(f"Warning: Could not save to Redis: {e}")
    
    def _load_from_redis(self):
        """Load metrics from Redis"""
        if not self.use_redis or not self.redis_client:
            return
        try:
            data = self.redis_client.get('acl_metrics')
            if data:
                self.metrics = json.loads(data)
                # Ensure all fields exist
                if 'recent_violations' not in self.metrics:
                    self.metrics['recent_violations'] = []
        except Exception as e:
            print(f"Warning: Could not load from Redis: {e}")
    
    def record_access(self, service: str, operation: str, resource: str, granted: bool):
        """Record access attempt"""
        self.metrics['total_requests'] += 1
        
        if granted:
            self.metrics['granted'] += 1
        else:
            self.metrics['denied'] += 1
            violation = {
                'service': service,
                'operation': operation,
                'resource': resource,
                'timestamp': time.time()
            }
            self.metrics['recent_violations'].append(violation)
            if len(self.metrics['recent_violations']) > 10:
                self.metrics['recent_violations'].pop(0)
        
        # By service
        if service not in self.metrics['by_service']:
            self.metrics['by_service'][service] = {'granted': 0, 'denied': 0}
        
        if granted:
            self.metrics['by_service'][service]['granted'] += 1
        else:
            self.metrics['by_service'][service]['denied'] += 1
        
        # By operation
        if operation not in self.metrics['by_operation']:
            self.metrics['by_operation'][operation] = {'granted': 0, 'denied': 0}
        
        if granted:
            self.metrics['by_operation'][operation]['granted'] += 1
        else:
            self.metrics['by_operation'][operation]['denied'] += 1
        
        # Save to Redis
        self._save_to_redis()
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics"""
        # Reload from Redis to get latest
        if self.use_redis:
            self._load_from_redis()
        return self.metrics


monitor = ACLMonitor()


@app.route('/')
def index():
    """Serve dashboard HTML"""
    return render_template('acl_dashboard.html')


@app.route('/api/metrics')
def get_metrics():
    """Get current ACL metrics"""
    return jsonify(monitor.get_metrics())


@app.route('/api/acls')
def get_acls():
    """Get ACL configuration"""
    # Simulated ACL data
    acls = {
        'post-service': {
            'topics': ['posts.created', 'posts.updated', 'posts.deleted'],
            'operations': ['WRITE'],
            'consumer_groups': []
        },
        'analytics-service': {
            'topics': ['posts.*', 'analytics.metrics'],
            'operations': ['READ', 'WRITE'],
            'consumer_groups': ['analytics-aggregator']
        },
        'moderation-service': {
            'topics': ['posts.*', 'moderation.flags'],
            'operations': ['READ', 'WRITE', 'DELETE'],
            'consumer_groups': ['moderation-scanner']
        }
    }
    return jsonify(acls)


def emit_metrics():
    """Emit metrics via WebSocket"""
    while True:
        try:
            metrics = monitor.get_metrics()
            socketio.emit('metrics_update', metrics)
        except Exception as e:
            print(f"Error emitting metrics: {e}")
        time.sleep(2)


if __name__ == '__main__':
    # Start background metric emission
    metric_thread = threading.Thread(target=emit_metrics, daemon=True)
    metric_thread.start()
    
    socketio.run(app, host='0.0.0.0', port=5055, debug=False, allow_unsafe_werkzeug=True)
