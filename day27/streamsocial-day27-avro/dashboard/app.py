from flask import Flask, render_template, jsonify
import json
import time
from datetime import datetime, timedelta
import random

app = Flask(__name__)

# Simulated metrics data
metrics = {
    'messages_processed': 0,
    'avro_messages': 0,
    'json_messages': 0,
    'avg_avro_size': 245,
    'avg_json_size': 487,
    'compression_ratio': 49.7,
    'schema_versions': 3,
    'compatibility_checks': 15
}

@app.route('/')
def dashboard():
    return render_template('dashboard.html')

@app.route('/api/metrics')
def get_metrics():
    # Simulate real-time data
    metrics['messages_processed'] += random.randint(10, 50)
    metrics['avro_messages'] += random.randint(5, 25)
    metrics['json_messages'] += random.randint(2, 10)
    
    return jsonify({
        **metrics,
        'timestamp': datetime.now().isoformat(),
        'throughput': random.randint(1000, 5000),
        'error_rate': round(random.uniform(0, 2), 2)
    })

@app.route('/api/schema-evolution')
def schema_evolution():
    return jsonify({
        'schemas': [
            {'version': 1, 'compatibility': 'BACKWARD', 'active': True},
            {'version': 2, 'compatibility': 'FORWARD', 'active': True},
            {'version': 3, 'compatibility': 'FULL', 'active': True}
        ],
        'evolution_timeline': [
            {'date': '2025-01-15', 'version': 1, 'changes': 'Initial schema'},
            {'date': '2025-02-10', 'version': 2, 'changes': 'Added optional metadata field'},
            {'date': '2025-03-01', 'version': 3, 'changes': 'Added location support'}
        ]
    })

if __name__ == '__main__':
    app.run(debug=True, port=5000)
