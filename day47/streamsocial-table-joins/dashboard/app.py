"""
Real-time Monitoring Dashboard for Table-Table Joins
Shows join metrics, state store sizes, and recommendation quality
"""

from flask import Flask, render_template, jsonify
from flask_cors import CORS
from kafka import KafkaConsumer
import json
import threading
from collections import deque
from datetime import datetime
import time

app = Flask(__name__)
CORS(app)

# Global metrics storage
metrics_data = {
    'join_operations': deque(maxlen=50),
    'recommendations_generated': deque(maxlen=50),
    'high_score_rate': deque(maxlen=50),
    'state_store_sizes': {'users': 0, 'content': 0},
    'recent_recommendations': deque(maxlen=10),
    'score_distribution': {'0.5-0.6': 0, '0.6-0.7': 0, '0.7-0.8': 0, '0.8-0.9': 0, '0.9-1.0': 0},
    'timestamps': deque(maxlen=50)
}

def consume_recommendations():
    """Background thread to consume recommendations and update metrics"""
    consumer = KafkaConsumer(
        'content-recommendations',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    for message in consumer:
        rec = message.value
        metrics_data['recent_recommendations'].append(rec)
        
        # Update score distribution
        score = rec['match_score']
        if 0.5 <= score < 0.6:
            metrics_data['score_distribution']['0.5-0.6'] += 1
        elif 0.6 <= score < 0.7:
            metrics_data['score_distribution']['0.6-0.7'] += 1
        elif 0.7 <= score < 0.8:
            metrics_data['score_distribution']['0.7-0.8'] += 1
        elif 0.8 <= score < 0.9:
            metrics_data['score_distribution']['0.8-0.9'] += 1
        elif score >= 0.9:
            metrics_data['score_distribution']['0.9-1.0'] += 1


@app.route('/')
def index():
    return render_template('dashboard.html')


@app.route('/api/metrics')
def get_metrics():
    """API endpoint for real-time metrics"""
    return jsonify({
        'join_operations': list(metrics_data['join_operations']),
        'recommendations_generated': list(metrics_data['recommendations_generated']),
        'high_score_rate': list(metrics_data['high_score_rate']),
        'state_store_sizes': metrics_data['state_store_sizes'],
        'recent_recommendations': list(metrics_data['recent_recommendations']),
        'score_distribution': metrics_data['score_distribution'],
        'timestamps': [t.strftime('%H:%M:%S') for t in metrics_data['timestamps']]
    })


@app.route('/api/update_metrics', methods=['POST'])
def update_metrics():
    """Endpoint for processor to push metrics"""
    from flask import request
    data = request.json
    
    metrics_data['join_operations'].append(data.get('join_operations', 0))
    metrics_data['recommendations_generated'].append(data.get('recommendations_generated', 0))
    
    high_score = data.get('high_score_recommendations', 0)
    total_recs = data.get('recommendations_generated', 1)
    rate = (high_score / total_recs * 100) if total_recs > 0 else 0
    metrics_data['high_score_rate'].append(rate)
    
    metrics_data['state_store_sizes'] = {
        'users': data.get('user_preferences_count', 0),
        'content': data.get('content_metadata_count', 0)
    }
    
    metrics_data['timestamps'].append(datetime.now())
    
    return jsonify({'status': 'success'})


if __name__ == '__main__':
    # Start recommendation consumer thread
    consumer_thread = threading.Thread(target=consume_recommendations, daemon=True)
    consumer_thread.start()
    
    app.run(host='0.0.0.0', port=5000, debug=False)
