"""
Interactive Query Service for Kafka Streams State Stores
Provides REST API to query engagement scores without external database
"""

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from flask_socketio import SocketIO, emit
import threading
import time
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*")

# Global reference to topology (will be injected)
topology = None


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'service': 'query-service'})


@app.route('/engagement_dashboard.html', methods=['GET'])
def dashboard():
    """Serve dashboard HTML"""
    dashboard_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'dashboards')
    return send_from_directory(dashboard_path, 'engagement_dashboard.html')


@app.route('/scores/<content_id>', methods=['GET'])
def get_score(content_id):
    """Query engagement score for specific content"""
    if not topology:
        return jsonify({'error': 'Topology not initialized'}), 503
    
    score_data = topology.query_state(content_id)
    
    if score_data:
        return jsonify(score_data)
    else:
        return jsonify({'content_id': content_id, 'score': 0, 'message': 'No data yet'}), 404


@app.route('/scores/top', methods=['GET'])
def get_top_scores():
    """Query top scoring content"""
    if not topology:
        return jsonify({'error': 'Topology not initialized'}), 503
    
    limit = request.args.get('limit', default=10, type=int)
    top_scores = topology.query_top_scores(limit)
    
    return jsonify({
        'top_scores': top_scores,
        'count': len(top_scores),
        'timestamp': time.time()
    })


@app.route('/metrics', methods=['GET'])
def get_metrics():
    """Get processing metrics"""
    if not topology:
        return jsonify({'error': 'Topology not initialized'}), 503
    
    return jsonify(topology.metrics)


def broadcast_metrics():
    """Broadcast metrics to WebSocket clients"""
    while True:
        if topology:
            socketio.emit('metrics_update', topology.metrics)
            top_scores = topology.query_top_scores(10)
            socketio.emit('scores_update', {'scores': top_scores})
        time.sleep(2)


@socketio.on('connect')
def handle_connect():
    """Handle WebSocket connection"""
    logger.info("Client connected to WebSocket")
    emit('connected', {'message': 'Connected to Query Service'})


def start_query_service(stream_topology, host='0.0.0.0', port=5001):
    """Start interactive query service"""
    global topology
    topology = stream_topology
    
    # Start metrics broadcaster
    metrics_thread = threading.Thread(target=broadcast_metrics, daemon=True)
    metrics_thread.start()
    
    logger.info(f"Starting query service on {host}:{port}")
    socketio.run(app, host=host, port=port, allow_unsafe_werkzeug=True)


if __name__ == '__main__':
    start_query_service(None)
