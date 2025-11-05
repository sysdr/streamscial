from flask import Flask, render_template, jsonify
from flask_cors import CORS
from kafka import KafkaConsumer
import json
import threading
import time
from collections import defaultdict

app = Flask(__name__, template_folder='../web/templates', static_folder='../web/static')
CORS(app)

# Global stats
dashboard_stats = {
    'total_raw': 0,
    'total_normalized': 0,
    'total_filtered': 0,
    'by_source': {'ios': 0, 'android': 0, 'web': 0},
    'by_action': defaultdict(int),
    'recent_messages': [],
    'pipeline_health': 'healthy',
    'transformation_rate': 0
}

def monitor_topics():
    """Background thread to monitor Kafka topics"""
    bootstrap_servers = ['localhost:9093']
    
    while True:
        try:
            # Monitor normalized topic
            consumer = KafkaConsumer(
                'normalized-user-actions',
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset='latest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=1000
            )
            
            for message in consumer:
                data = message.value
                dashboard_stats['total_normalized'] += 1
                dashboard_stats['by_source'][data.get('source', 'unknown')] += 1
                dashboard_stats['by_action'][data.get('action', 'unknown')] += 1
                
                # Keep last 50 messages
                dashboard_stats['recent_messages'].insert(0, {
                    'timestamp': data.get('timestamp'),
                    'source': data.get('source'),
                    'action': data.get('action'),
                    'user_id': data.get('user_id'),
                    'post_id': data.get('post_id')
                })
                dashboard_stats['recent_messages'] = dashboard_stats['recent_messages'][:50]
            
            consumer.close()
            time.sleep(1)
            
        except Exception as e:
            print(f"Monitor error: {e}")
            dashboard_stats['pipeline_health'] = 'degraded'
            time.sleep(5)

@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/api/stats')
def get_stats():
    return jsonify(dashboard_stats)

@app.route('/api/health')
def health():
    return jsonify({'status': 'ok', 'pipeline': dashboard_stats['pipeline_health']})

if __name__ == '__main__':
    # Start monitoring thread
    monitor_thread = threading.Thread(target=monitor_topics, daemon=True)
    monitor_thread.start()
    
    print("🌐 Starting web dashboard on http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=False)
