from flask import Flask, render_template, jsonify
from flask_cors import CORS
from flask_socketio import SocketIO
import threading
import time
import psutil
import json

app = Flask(__name__)
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*")

processor_instance = None
metrics_thread = None

def create_app(processor):
    global processor_instance
    processor_instance = processor
    return app

@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/api/metrics')
def get_metrics():
    if processor_instance:
        metrics = processor_instance.get_metrics()
        system_metrics = {
            'cpu_percent': psutil.cpu_percent(interval=1),
            'memory_percent': psutil.virtual_memory().percent,
            'disk_usage': psutil.disk_usage('/').percent
        }
        return jsonify({
            'processing': metrics,
            'system': system_metrics
        })
    return jsonify({'error': 'Processor not initialized'}), 500

def emit_metrics_loop():
    """Emit metrics via WebSocket"""
    while True:
        if processor_instance:
            metrics = processor_instance.get_metrics()
            socketio.emit('metrics_update', metrics)
        time.sleep(2)

@socketio.on('connect')
def handle_connect():
    global metrics_thread
    if metrics_thread is None:
        metrics_thread = threading.Thread(target=emit_metrics_loop, daemon=True)
        metrics_thread.start()
