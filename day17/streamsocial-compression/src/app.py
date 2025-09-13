import asyncio
import json
import time
import psutil
import threading
from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit
from compression.analyzer import CompressionAnalyzer
from compression.streamsocial_data import StreamSocialDataGenerator
from monitoring.performance_monitor import PerformanceMonitor
import logging

app = Flask(__name__, template_folder='../templates', static_folder='../static')
app.config['SECRET_KEY'] = 'streamsocial-compression-2025'
socketio = SocketIO(app, cors_allowed_origins="*")

# Global instances
analyzer = CompressionAnalyzer()
data_generator = StreamSocialDataGenerator()
performance_monitor = PerformanceMonitor()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.route('/')
def dashboard():
    return render_template('dashboard.html')

@app.route('/api/benchmark', methods=['POST'])
def run_benchmark():
    try:
        data_type = request.json.get('data_type', 'user_profile')
        data_size = int(request.json.get('data_size', 1000))
        iterations = int(request.json.get('iterations', 100))
        
        # Generate test data
        test_data = data_generator.generate_data(data_type, data_size)
        
        # Run compression benchmark
        results = analyzer.benchmark_all_algorithms(test_data, iterations)
        
        # Add system metrics
        results['system_metrics'] = performance_monitor.get_current_metrics()
        
        return jsonify(results)
    except Exception as e:
        logger.error(f"Benchmark error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/realtime-test', methods=['POST'])
def realtime_test():
    try:
        algorithm = request.json.get('algorithm', 'snappy')
        data = request.json.get('data', '').encode('utf-8')
        
        start_time = time.time()
        compressed = analyzer.compress_data(data, algorithm)
        compression_time = time.time() - start_time
        
        start_time = time.time()
        decompressed = analyzer.decompress_data(compressed, algorithm)
        decompression_time = time.time() - start_time
        
        ratio = len(data) / len(compressed) if len(compressed) > 0 else 0
        
        return jsonify({
            'original_size': len(data),
            'compressed_size': len(compressed),
            'compression_ratio': ratio,
            'compression_time_ms': compression_time * 1000,
            'decompression_time_ms': decompression_time * 1000,
            'verified': data == decompressed
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@socketio.on('start_continuous_monitoring')
def handle_continuous_monitoring():
    def monitor_loop():
        while True:
            metrics = performance_monitor.get_current_metrics()
            socketio.emit('metrics_update', metrics)
            time.sleep(1)
    
    thread = threading.Thread(target=monitor_loop, daemon=True)
    thread.start()

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=8080, debug=True)
