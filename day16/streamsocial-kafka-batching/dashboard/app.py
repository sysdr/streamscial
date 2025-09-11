import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO
import threading
import time
import json
import random
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from src.producer.adaptive_producer import AdaptiveKafkaProducer
from src.utils.load_simulator import LoadSimulator

app = Flask(__name__)
app.config['SECRET_KEY'] = 'streamsocial-batching-demo'
socketio = SocketIO(app, cors_allowed_origins="*")

# Global producer instance
producer = None
simulator = None
monitoring_active = False

@app.route('/')
def dashboard():
    return render_template('dashboard.html')

@app.route('/api/metrics')
def get_metrics():
    global producer
    if producer:
        return jsonify(producer.metrics)
    return jsonify({'error': 'Producer not initialized'})

@app.route('/api/config/<config_name>')
def update_config(config_name):
    global producer
    try:
        producer.close()
        producer = AdaptiveKafkaProducer(config_name)
        producer.start_monitoring()
        return jsonify({'status': 'success', 'config': config_name})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/metrics')
def prometheus_metrics():
    return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}

@socketio.on('start_simulation')
def handle_simulation(data):
    global simulator, monitoring_active
    pattern = data.get('pattern', 'steady')
    rate = data.get('rate', 1000)
    duration = data.get('duration', 60)
    
    if simulator and not monitoring_active:
        monitoring_active = True
        thread = threading.Thread(
            target=run_simulation, 
            args=(pattern, rate, duration)
        )
        thread.start()

def run_simulation(pattern, rate, duration):
    global simulator, monitoring_active
    try:
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        result = loop.run_until_complete(
            simulator.simulate_traffic_pattern(rate, duration, pattern)
        )
        
        socketio.emit('simulation_complete', {'result': result})
    except Exception as e:
        socketio.emit('simulation_error', {'error': str(e)})
    finally:
        monitoring_active = False

def emit_realtime_metrics():
    global producer
    while True:
        if producer and producer.running:
            metrics = producer.metrics.copy()
            socketio.emit('metrics_update', metrics)
        time.sleep(1)

if __name__ == '__main__':
    # Initialize producer and simulator
    producer = AdaptiveKafkaProducer('adaptive')
    producer.start_monitoring()
    simulator = LoadSimulator(producer)
    
    # Start metrics broadcasting thread
    metrics_thread = threading.Thread(target=emit_realtime_metrics)
    metrics_thread.daemon = True
    metrics_thread.start()
    
    print("🌐 Starting StreamSocial Batching Dashboard at http://localhost:5000")
    socketio.run(app, debug=False, host='0.0.0.0', port=5000, allow_unsafe_werkzeug=True)
