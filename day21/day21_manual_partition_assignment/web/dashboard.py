from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO, emit
import json
import redis
import threading
import time
from kafka import KafkaConsumer
from config.kafka_config import KafkaConfig

app = Flask(__name__, template_folder='templates')
app.config['SECRET_KEY'] = 'trend-dashboard-secret'
socketio = SocketIO(app, cors_allowed_origins="*")

redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

class TrendDashboard:
    def __init__(self):
        self.latest_trends = {}
        self.consumer = None
        self.running = False
        
    def start_trend_consumer(self):
        """Start consuming trending results"""
        self.consumer = KafkaConsumer(
            KafkaConfig.TRENDING_RESULTS_TOPIC,
            bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
            group_id='trend-dashboard',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest'
        )
        
        self.running = True
        thread = threading.Thread(target=self._consume_trends)
        thread.daemon = True
        thread.start()
    
    def _consume_trends(self):
        """Consume and emit trending data"""
        for message in self.consumer:
            if not self.running:
                break
                
            trend_data = message.value
            worker_name = trend_data.get('worker_name')
            trends = trend_data.get('trends', [])
            
            # Store latest trends
            self.latest_trends[worker_name] = trends
            
            # Emit to web clients
            socketio.emit('trend_update', {
                'worker': worker_name,
                'trends': trends
            })

dashboard = TrendDashboard()

@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/api/trends')
def get_trends():
    """API endpoint for current trends"""
    return jsonify(dashboard.latest_trends)

@app.route('/api/worker_status')
def worker_status():
    """Get status of all workers"""
    active_workers = redis_client.smembers('active_workers')
    worker_info = {}
    
    for worker in active_workers:
        last_seen = redis_client.get(f'worker:{worker}:last_seen')
        worker_info[worker] = {
            'last_seen': int(last_seen) if last_seen else 0,
            'partition_count': len(redis_client.lrange(f'worker:{worker}:partitions', 0, -1))
        }
    
    return jsonify(worker_info)

@socketio.on('connect')
def handle_connect():
    """Handle client connection"""
    emit('trend_update', dashboard.latest_trends)

if __name__ == '__main__':
    dashboard.start_trend_consumer()
    socketio.run(app, host='0.0.0.0', port=5001, debug=True, allow_unsafe_werkzeug=True)
