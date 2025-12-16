from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO, emit
from flask_cors import CORS
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.collectors.jmx_collector import JMXCollector
from src.aggregators.cluster_aggregator import MetricsAggregator
from src.alerts.alert_manager import AlertManager
import threading
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY', 'dev-secret-key-change-in-production')
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Initialize components
broker_endpoints = [
    'http://localhost:7071/metrics',
    'http://localhost:7072/metrics',
    'http://localhost:7073/metrics'
]

collector = JMXCollector(broker_endpoints)
aggregator = MetricsAggregator()
alert_manager = AlertManager()

# Global state
current_cluster_metrics = None
current_broker_metrics = []

def metrics_collection_loop():
    """Background thread to collect metrics"""
    global current_cluster_metrics, current_broker_metrics
    
    while True:
        try:
            # Collect from all brokers
            broker_metrics = collector.collect_all()
            
            if broker_metrics:
                # Aggregate cluster metrics
                cluster_metrics = aggregator.aggregate(broker_metrics)
                
                # Evaluate alerts
                alerts = alert_manager.evaluate_metrics(cluster_metrics, broker_metrics)
                
                # Update global state
                current_cluster_metrics = cluster_metrics
                current_broker_metrics = broker_metrics
                
                # Emit to all connected clients (always emit, even if values are small)
                logger.debug(f"Emitting metrics: {cluster_metrics.total_messages_per_sec:.2f} msg/s")
                socketio.emit('metrics_update', {
                    'cluster': {
                        'timestamp': cluster_metrics.timestamp.isoformat(),
                        'messages_per_sec': round(cluster_metrics.total_messages_per_sec, 2),
                        'bytes_in_per_sec': round(cluster_metrics.total_bytes_in_per_sec, 2),
                        'bytes_out_per_sec': round(cluster_metrics.total_bytes_out_per_sec, 2),
                        'avg_request_idle': round(cluster_metrics.avg_request_handler_idle, 2),
                        'avg_network_idle': round(cluster_metrics.avg_network_processor_idle, 2),
                        'under_replicated': cluster_metrics.total_under_replicated,
                        'offline_partitions': cluster_metrics.total_offline_partitions,
                        'total_partitions': cluster_metrics.total_partitions,
                        'broker_count': cluster_metrics.broker_count,
                        'health_status': cluster_metrics.health_status
                    },
                    'brokers': [
                        {
                            'id': b.broker_id,
                            'messages_per_sec': round(b.messages_in_per_sec, 2),
                            'bytes_in': round(b.bytes_in_per_sec, 2),
                            'request_idle': round(b.request_handler_idle_percent, 2),
                            'network_idle': round(b.network_processor_idle_percent, 2),
                            'partitions': b.partition_count,
                            'leaders': b.leader_count,
                            'under_replicated': b.under_replicated_partitions
                        }
                        for b in broker_metrics
                    ],
                    'alerts': alert_manager.get_active_alerts()
                })
                
            time.sleep(5)  # Collect every 5 seconds
            
        except Exception as e:
            logger.error(f"Error in metrics collection: {str(e)}")
            time.sleep(5)

@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/api/cluster/current')
def get_cluster_current():
    """Get current cluster metrics"""
    if current_cluster_metrics:
        return jsonify({
            'timestamp': current_cluster_metrics.timestamp.isoformat(),
            'messages_per_sec': current_cluster_metrics.total_messages_per_sec,
            'bytes_in_per_sec': current_cluster_metrics.total_bytes_in_per_sec,
            'health_status': current_cluster_metrics.health_status
        })
    return jsonify({'error': 'No metrics available'}), 503

@app.route('/api/cluster/history')
def get_cluster_history():
    """Get cluster metrics history"""
    history = aggregator.get_history(minutes=60)
    return jsonify([
        {
            'timestamp': m.timestamp.isoformat(),
            'messages_per_sec': m.total_messages_per_sec,
            'bytes_in_per_sec': m.total_bytes_in_per_sec,
            'avg_request_idle': m.avg_request_handler_idle,
            'health_status': m.health_status
        }
        for m in history
    ])

@app.route('/api/brokers')
def get_brokers():
    """Get current broker metrics"""
    return jsonify([
        {
            'id': b.broker_id,
            'messages_per_sec': b.messages_in_per_sec,
            'partitions': b.partition_count,
            'under_replicated': b.under_replicated_partitions
        }
        for b in current_broker_metrics
    ])

@app.route('/api/alerts')
def get_alerts():
    """Get active alerts"""
    return jsonify({
        'alerts': alert_manager.get_active_alerts(),
        'summary': alert_manager.get_alert_summary()
    })

@socketio.on('connect')
def handle_connect():
    logger.info('Client connected')
    emit('connected', {'status': 'connected'})

@socketio.on('disconnect')
def handle_disconnect():
    logger.info('Client disconnected')

if __name__ == '__main__':
    # Start metrics collection in background thread
    metrics_thread = threading.Thread(target=metrics_collection_loop, daemon=True)
    metrics_thread.start()
    
    logger.info("Starting StreamSocial Broker Monitoring Dashboard on http://localhost:5000")
    socketio.run(app, host='0.0.0.0', port=5000, debug=False, allow_unsafe_werkzeug=True)
