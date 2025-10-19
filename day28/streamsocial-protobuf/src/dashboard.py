from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO
import json
import time
import threading
from performance_benchmark import PerformanceBenchmark

app = Flask(__name__, template_folder='../templates', static_folder='../static')
socketio = SocketIO(app, cors_allowed_origins="*")

class DashboardService:
    def __init__(self):
        self.benchmark = PerformanceBenchmark()
        self.running = False
        
    def start_live_monitoring(self):
        self.running = True
        thread = threading.Thread(target=self._monitor_performance)
        thread.daemon = True
        thread.start()
    
    def _monitor_performance(self):
        while self.running:
            try:
                results = self.benchmark.run_benchmark(50)  # Smaller batches for real-time
                
                # Prepare data for frontend
                metrics_data = {
                    'timestamp': int(time.time() * 1000),
                    'formats': {}
                }
                
                for format_name, metrics in results.items():
                    metrics_data['formats'][format_name] = {
                        'serialization_time_us': metrics['avg_serialization_time_ns'] / 1000,
                        'deserialization_time_us': metrics['avg_deserialization_time_ns'] / 1000,
                        'payload_size_bytes': metrics['avg_payload_size_bytes'],
                        'total_time_us': metrics['total_time_ns'] / 1000
                    }
                
                socketio.emit('performance_update', metrics_data)
                time.sleep(3)  # Update every 3 seconds
                
            except Exception as e:
                print(f"Error in monitoring: {e}")
                time.sleep(5)

dashboard_service = DashboardService()

@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/api/benchmark')
def run_benchmark():
    results = dashboard_service.benchmark.run_benchmark(1000)
    return jsonify(results)

@socketio.on('connect')
def handle_connect():
    print('Client connected')
    if not dashboard_service.running:
        dashboard_service.start_live_monitoring()

@socketio.on('disconnect')
def handle_disconnect():
    print('Client disconnected')

if __name__ == '__main__':
    socketio.run(app, debug=True, host='0.0.0.0', port=5000, allow_unsafe_werkzeug=True)
