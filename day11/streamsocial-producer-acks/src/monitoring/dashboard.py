import json
import time
import threading
from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO, emit
import plotly.graph_objs as go
import plotly.utils
from typing import Dict, List

class MetricsDashboard:
    def __init__(self, producer_manager):
        self.app = Flask(__name__, template_folder='../../web/templates', static_folder='../../web/static')
        self.socketio = SocketIO(self.app, cors_allowed_origins="*")
        self.producer_manager = producer_manager
        self.metrics_history = {'critical': [], 'social': [], 'analytics': []}
        
        self._setup_routes()
        self._start_metrics_collection()
    
    def _setup_routes(self):
        @self.app.route('/')
        def dashboard():
            return render_template('dashboard.html')
        
        @self.app.route('/api/metrics')
        def get_metrics():
            return jsonify(self.producer_manager.get_metrics())
        
        @self.app.route('/api/metrics/chart')
        def get_chart_data():
            return jsonify(self._generate_chart_data())
        
        @self.socketio.on('connect')
        def handle_connect():
            emit('connected', {'data': 'Connected to StreamSocial metrics'})
    
    def _start_metrics_collection(self):
        """Start background thread for metrics collection"""
        def collect_metrics():
            while True:
                metrics = self.producer_manager.get_metrics()
                timestamp = time.time()
                
                for producer_type in ['critical', 'social', 'analytics']:
                    metric = metrics[producer_type]
                    avg_latency = sum(metric['latency']) / len(metric['latency']) if metric['latency'] else 0
                    
                    self.metrics_history[producer_type].append({
                        'timestamp': timestamp,
                        'throughput': metric['acked'],
                        'latency': avg_latency,
                        'failure_rate': metric['failed'] / max(metric['sent'], 1) * 100
                    })
                    
                    # Keep only last 100 data points
                    if len(self.metrics_history[producer_type]) > 100:
                        self.metrics_history[producer_type].pop(0)
                
                # Emit real-time data to connected clients
                self.socketio.emit('metrics_update', {
                    'metrics': metrics,
                    'timestamp': timestamp
                })
                
                time.sleep(2)
        
        thread = threading.Thread(target=collect_metrics, daemon=True)
        thread.start()
    
    def _generate_chart_data(self):
        """Generate chart data for the dashboard"""
        charts = {}
        
        for producer_type in ['critical', 'social', 'analytics']:
            history = self.metrics_history[producer_type]
            if not history:
                continue
                
            timestamps = [h['timestamp'] for h in history]
            throughputs = [h['throughput'] for h in history]
            latencies = [h['latency'] for h in history]
            
            charts[producer_type] = {
                'throughput': {
                    'x': timestamps,
                    'y': throughputs,
                    'type': 'scatter',
                    'mode': 'lines',
                    'name': f'{producer_type.title()} Throughput'
                },
                'latency': {
                    'x': timestamps,
                    'y': latencies,
                    'type': 'scatter',
                    'mode': 'lines',
                    'name': f'{producer_type.title()} Latency'
                }
            }
        
        return charts
    
    def run(self, host='0.0.0.0', port=5000, debug=False):
        self.socketio.run(self.app, host=host, port=port, debug=debug)
