"""Real-time monitoring dashboard for centralized logging"""
from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO, emit
from flask_cors import CORS
import threading
import time
import os
from datetime import datetime
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))
from shared.log_analyzer import LogAnalyzer

# Get the project root directory (parent of src)
PROJECT_ROOT = Path(__file__).parent.parent
TEMPLATE_DIR = PROJECT_ROOT / 'templates'

app = Flask(__name__, template_folder=str(TEMPLATE_DIR))
app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY', 'change-this-in-production')
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

analyzer = LogAnalyzer()

@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/api/stats')
def get_stats():
    """Get current statistics"""
    try:
        error_rate = analyzer.get_error_rate(minutes=1440)  # 24 hours to show all available logs
        latency_stats = analyzer.get_latency_stats()
        top_errors = analyzer.get_top_errors(limit=5)
        component_volume = analyzer.get_component_volume()
        
        # Ensure all values are valid
        if isinstance(error_rate, dict) and 'error' in error_rate:
            error_rate = {'total_logs': 0, 'errors': 0, 'error_rate': 0, 'time_range_minutes': 1440}
        if not isinstance(latency_stats, dict):
            latency_stats = {'avg': 0, 'min': 0, 'max': 0, 'p50': 0, 'p95': 0, 'p99': 0}
        if not isinstance(top_errors, list):
            top_errors = []
        if not isinstance(component_volume, dict):
            component_volume = {}
        
        error_rate_over_time = analyzer.get_error_rate_over_time(minutes=60, intervals=12)
        
        return jsonify({
            'error_rate': error_rate,
            'error_rate_over_time': error_rate_over_time,
            'latency': latency_stats,
            'top_errors': top_errors,
            'component_volume': component_volume,
            'timestamp': datetime.utcnow().isoformat()
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            'error': str(e),
            'error_rate': {'total_logs': 0, 'errors': 0, 'error_rate': 0, 'time_range_minutes': 5},
            'latency': {'avg': 0, 'min': 0, 'max': 0, 'p50': 0, 'p95': 0, 'p99': 0},
            'top_errors': [],
            'timestamp': datetime.utcnow().isoformat()
        }), 500

@app.route('/api/trace/<trace_id>')
def search_trace(trace_id):
    """Search logs by trace ID"""
    try:
        logs = analyzer.search_by_trace_id(trace_id)
        if not isinstance(logs, list):
            logs = []
        return jsonify({
            'trace_id': trace_id,
            'logs': logs,
            'count': len(logs)
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            'error': str(e),
            'trace_id': trace_id,
            'logs': [],
            'count': 0
        }), 500

@app.route('/api/component/<component>')
def get_component_logs(component):
    """Get logs for specific component"""
    try:
        logs = analyzer.get_component_logs(component, limit=20)
        if not isinstance(logs, list):
            logs = []
        return jsonify({
            'component': component,
            'logs': logs,
            'count': len(logs)
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            'error': str(e),
            'component': component,
            'logs': [],
            'count': 0
        }), 500

def background_metrics():
    """Push metrics to connected clients"""
    while True:
        try:
            time.sleep(5)  # Update every 5 seconds
            
            error_rate = analyzer.get_error_rate(minutes=1440)  # 24 hours to show all available logs
            latency_stats = analyzer.get_latency_stats()
            
            socketio.emit('metrics_update', {
                'error_rate': error_rate,
                'latency': latency_stats,
                'timestamp': datetime.utcnow().isoformat()
            })
        except Exception as e:
            print(f"Background metrics error: {e}")
            time.sleep(10)

@socketio.on('connect')
def handle_connect():
    print('Client connected')
    emit('connection_response', {'status': 'connected'})

@socketio.on('disconnect')
def handle_disconnect():
    print('Client disconnected')

if __name__ == '__main__':
    # Start background metrics thread
    metrics_thread = threading.Thread(target=background_metrics, daemon=True)
    metrics_thread.start()
    
    print("Starting dashboard on http://localhost:5000")
    socketio.run(app, host='0.0.0.0', port=5000, debug=False, allow_unsafe_werkzeug=True)
