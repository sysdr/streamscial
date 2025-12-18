from flask import Flask, render_template, jsonify
from flask_cors import CORS
from flask_socketio import SocketIO
import json
import time
import threading
import sys
import os

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from src.collectors.lag_calculator import LagCalculator
from src.collectors.sla_checker import SLAChecker
from src.producers.instrumented_producer import InstrumentedProducer
from src.consumers.instrumented_consumer import InstrumentedConsumer

app = Flask(__name__)
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*")

# Global state
producers = {}
consumers = {}
lag_calculator = None
sla_checker = None
metrics_data = {
    'producers': [],
    'consumers': [],
    'lag': [],
    'sla': []
}

def initialize_kafka_clients():
    """Initialize Kafka producers and consumers"""
    global lag_calculator, sla_checker, producers, consumers
    
    bootstrap_servers = 'localhost:9092'
    
    # Initialize lag calculator and SLA checker
    lag_calculator = LagCalculator(bootstrap_servers)
    config_path = os.path.join(project_root, 'config', 'sla_config.json')
    sla_checker = SLAChecker(config_path)
    
    # Create producers
    producer_configs = [
        ('posts', 'producer-posts-1'),
        ('likes', 'producer-likes-1'),
        ('comments', 'producer-comments-1'),
        ('notifications', 'producer-notifications-1')
    ]
    
    for topic, client_id in producer_configs:
        producers[client_id] = InstrumentedProducer(bootstrap_servers, topic, client_id)
    
    # Create consumers
    consumer_configs = [
        ('notifications-delivery', ['notifications'], 'consumer-notif-1'),
        ('feed-generator', ['posts', 'likes'], 'consumer-feed-1'),
        ('analytics-pipeline', ['posts', 'likes', 'comments'], 'consumer-analytics-1'),
        ('trending-hashtags', ['posts'], 'consumer-trending-1')
    ]
    
    for group_id, topics, client_id in consumer_configs:
        consumers[client_id] = InstrumentedConsumer(bootstrap_servers, group_id, topics, client_id)
        # Start consumer in thread
        thread = threading.Thread(target=consumers[client_id].consume_messages, daemon=True)
        thread.start()

def simulate_traffic():
    """Simulate traffic to generate metrics"""
    while True:
        for client_id, producer in producers.items():
            # Generate messages at different rates
            rate_multiplier = {'producer-posts-1': 5, 'producer-likes-1': 8, 
                             'producer-comments-1': 3, 'producer-notifications-1': 12}
            
            for _ in range(rate_multiplier.get(client_id, 5)):
                key = f"user_{time.time()}"
                value = {
                    'event_type': producer.topic,
                    'timestamp': int(time.time() * 1000),
                    'data': f'Sample data from {client_id}'
                }
                producer.produce_message(key, value)
        
        time.sleep(0.2)

def collect_metrics():
    """Collect metrics periodically"""
    while True:
        try:
            # Collect producer metrics
            producer_metrics = []
            for client_id, producer in producers.items():
                metrics = producer.get_metrics()
                producer_metrics.append(metrics)
            
            # Collect consumer metrics
            consumer_metrics = []
            for client_id, consumer in consumers.items():
                metrics = consumer.get_metrics()
                consumer_metrics.append(metrics)
            
            # Collect lag metrics
            lag_metrics = []
            sla_metrics = []
            
            for client_id, consumer in consumers.items():
                group_id = consumer.group_id
                for topic in consumer.topics:
                    lag_data = lag_calculator.get_consumer_lag(group_id, topic)
                    if 'error' not in lag_data:
                        lag_metrics.append(lag_data)
                        
                        # Check SLA compliance
                        consumer_m = consumer.get_metrics()
                        sla_status = sla_checker.check_compliance(
                            group_id,
                            lag_data['total_lag'],
                            consumer_m['throughput_rps'],
                            consumer_m['avg_e2e_latency_ms']
                        )
                        sla_metrics.append({
                            'group_id': group_id,
                            'topic': topic,
                            **sla_status
                        })
            
            # Update global metrics
            metrics_data['producers'] = producer_metrics
            metrics_data['consumers'] = consumer_metrics
            metrics_data['lag'] = lag_metrics
            metrics_data['sla'] = sla_metrics
            
            # Emit to websocket clients
            socketio.emit('metrics_update', metrics_data)
            
        except Exception as e:
            print(f"Error collecting metrics: {e}")
        
        time.sleep(5)

@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/api/metrics')
def get_metrics():
    return jsonify(metrics_data)

if __name__ == '__main__':
    # Initialize
    initialize_kafka_clients()
    
    # Start background threads
    threading.Thread(target=simulate_traffic, daemon=True).start()
    threading.Thread(target=collect_metrics, daemon=True).start()
    
    # Run Flask app
    socketio.run(app, host='0.0.0.0', port=5052, debug=False, allow_unsafe_werkzeug=True)
