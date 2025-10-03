"""Real-time monitoring dashboard for low-latency consumer."""

import json
import time
from flask import Flask, render_template, jsonify
import requests
from threading import Thread
import asyncio

app = Flask(__name__, template_folder='../../templates', static_folder='../../static')

class MetricsCollector:
    def __init__(self):
        self.metrics_url = "http://localhost:8080"
        self.latest_metrics = {}
    
    def collect_metrics(self):
        """Collect metrics from Prometheus endpoint."""
        try:
            response = requests.get(f"{self.metrics_url}/metrics", timeout=1)
            if response.status_code == 200:
                # Parse Prometheus metrics (simplified)
                lines = response.text.split('\n')
                for line in lines:
                    if line.startswith('messages_processed_total'):
                        # Extract value
                        parts = line.split(' ')
                        if len(parts) >= 2:
                            self.latest_metrics['messages_processed'] = float(parts[-1])
                    elif line.startswith('processing_latency_seconds'):
                        if 'quantile="0.95"' in line:
                            parts = line.split(' ')
                            if len(parts) >= 2:
                                self.latest_metrics['p95_latency'] = float(parts[-1]) * 1000
        except Exception as e:
            print(f"Metrics collection error: {e}")

collector = MetricsCollector()

@app.route('/')
def dashboard():
    """Main dashboard page."""
    return render_template('dashboard.html')

@app.route('/api/metrics')
def get_metrics():
    """API endpoint for current metrics."""
    collector.collect_metrics()
    
    # Mock some additional metrics for demo
    current_time = time.time()
    return jsonify({
        'timestamp': current_time,
        'messages_processed': collector.latest_metrics.get('messages_processed', 0),
        'average_latency': collector.latest_metrics.get('p95_latency', 0),
        'throughput': collector.latest_metrics.get('messages_processed', 0) / max(1, time.time() - start_time),
        'error_rate': 0.1,  # Mock error rate
        'status': 'healthy'
    })

@app.route('/api/health')
def health_check():
    """Health check endpoint."""
    return jsonify({'status': 'healthy', 'timestamp': time.time()})

start_time = time.time()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
