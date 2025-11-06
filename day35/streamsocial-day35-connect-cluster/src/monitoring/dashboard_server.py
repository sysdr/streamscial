"""
Real-time Connect Cluster Dashboard Server
WebSocket-based live monitoring interface
"""
from flask import Flask, render_template, jsonify, make_response
from flask_socketio import SocketIO, emit
from flask_cors import CORS
import time
import threading
from .cluster_monitor import ConnectClusterMonitor


app = Flask(__name__, 
            template_folder='../../ui/templates',
            static_folder='../../ui/static')
CORS(app)
socketio = SocketIO(app, 
                   cors_allowed_origins="*",
                   async_mode='threading',
                   logger=True,
                   engineio_logger=False)

# Initialize monitor
monitor = ConnectClusterMonitor([
    'http://localhost:8083',
    'http://localhost:8084',
    'http://localhost:8085'
])


@app.route('/')
def index():
    import time
    # Add cache control headers to force browser refresh
    response = make_response(render_template('dashboard.html'))
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


@app.route('/demo')
def demo():
    # Demo page with static data - no cache
    response = make_response(render_template('demo.html'))
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


@app.route('/api/metrics')
def get_metrics():
    return jsonify(monitor.get_current_metrics())


@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'socketio': 'enabled'})


@socketio.on('connect')
def handle_connect():
    print('Client connected')
    emit('connection_response', {'status': 'connected'})


@socketio.on('disconnect')
def handle_disconnect():
    print('Client disconnected')


def broadcast_metrics():
    """Broadcast metrics to all connected clients"""
    while True:
        try:
            metrics = monitor.get_current_metrics()
            socketio.emit('metrics_update', metrics)
            time.sleep(2)
        except Exception as e:
            print(f"Broadcast error: {e}")
            time.sleep(2)


def start_dashboard(host='0.0.0.0', port=5000):
    """Start dashboard server"""
    # Start monitoring
    monitor.start_monitoring(interval=2)
    
    # Start metrics broadcast thread
    broadcast_thread = threading.Thread(target=broadcast_metrics, daemon=True)
    broadcast_thread.start()
    
    # Start Flask app
    print(f"Dashboard starting at http://{host}:{port}")
    print(f"Socket.IO enabled with async_mode: threading")
    socketio.run(app, 
                 host=host, 
                 port=port, 
                 debug=False, 
                 allow_unsafe_werkzeug=True,
                 use_reloader=False)


if __name__ == '__main__':
    start_dashboard()
