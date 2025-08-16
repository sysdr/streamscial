from flask import Flask, jsonify
from flask_cors import CORS
import threading
import time

app = Flask(__name__)
CORS(app)

class MetricsServer:
    def __init__(self, producer=None, port=8080):
        self.producer = producer
        self.port = port
        self.app = app
        self.setup_routes()

    def setup_routes(self):
        @self.app.route('/metrics')
        def get_metrics():
            if self.producer:
                try:
                    metrics = self.producer.get_metrics()
                    # Add any missing metrics with default values
                    default_metrics = {
                        'cpu_percent': 0,
                        'memory_mb': 0,
                        'queue_size': 0,
                        'active_connections': 0
                    }
                    # Only add metrics that don't already exist
                    for key, value in default_metrics.items():
                        if key not in metrics:
                            metrics[key] = value
                    return jsonify(metrics)
                except Exception as e:
                    print(f"Error updating metrics: {e}")
                    # Return basic metrics on error
                    return jsonify({
                        'messages_sent': 0,
                        'messages_failed': 0,
                        'bytes_sent': 0,
                        'success_rate': 0,
                        'throughput_msg_sec': 0,
                        'throughput_bytes_sec': 0,
                        'avg_latency_ms': 0,
                        'runtime_seconds': 0,
                        'cpu_percent': 0,
                        'memory_mb': 0,
                        'queue_size': 0,
                        'active_connections': 0
                    })
            else:
                # Return dummy metrics for testing
                return jsonify({
                    'messages_sent': 0,
                    'messages_failed': 0,
                    'bytes_sent': 0,
                    'success_rate': 0,
                    'throughput_msg_sec': 0,
                    'throughput_bytes_sec': 0,
                    'avg_latency_ms': 0,
                    'runtime_seconds': 0,
                    'cpu_percent': 0,
                    'memory_mb': 0,
                    'queue_size': 0,
                    'active_connections': 0
                })

        @self.app.route('/health')
        def health_check():
            return jsonify({'status': 'healthy', 'timestamp': time.time()})

    def run(self, host='0.0.0.0', debug=False):
        print(f"🚀 Starting Metrics Server at http://{host}:{self.port}")
        self.app.run(host=host, port=self.port, debug=debug, threaded=True)

if __name__ == '__main__':
    server = MetricsServer()
    server.run(debug=True)
