"""
REST API for querying recommendations and state stores
"""
from flask import Flask, jsonify, request
from flask_cors import CORS
import threading
import time

app = Flask(__name__)
CORS(app)

# Global topology reference (set by main app)
topology = None

@app.route('/api/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'timestamp': int(time.time())})

@app.route('/api/recommendations/<user_id>', methods=['GET'])
def get_recommendations(user_id):
    """Get recommendations for a user"""
    if not topology:
        return jsonify({'error': 'Topology not initialized'}), 500
    
    recs = topology.get_recommendations(user_id)
    if not recs:
        return jsonify({'user_id': user_id, 'recommendations': [], 'count': 0})
    
    return jsonify(recs)

@app.route('/api/metrics', methods=['GET'])
def get_metrics():
    """Get processing metrics"""
    if not topology:
        return jsonify({'error': 'Topology not initialized'}), 500
    
    metrics = topology.get_metrics()
    return jsonify(metrics)

@app.route('/api/state/<store_name>', methods=['GET'])
def get_state_store_info(store_name):
    """Get state store information"""
    if not topology:
        return jsonify({'error': 'Topology not initialized'}), 500
    
    if store_name not in topology.state_stores:
        return jsonify({'error': 'Store not found'}), 404
    
    store = topology.state_stores[store_name]
    return jsonify({
        'name': store.name,
        'size': store.size(),
        'metrics': store.get_metrics(),
        'changelog_topic': store.changelog_topic
    })

def start_api(topology_ref, host='0.0.0.0', port=8080):
    """Start API server"""
    global topology
    topology = topology_ref
    
    print(f"Starting API server on {host}:{port}...")
    app.run(host=host, port=port, threaded=True)
