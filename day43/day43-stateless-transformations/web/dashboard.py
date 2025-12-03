"""Real-time monitoring dashboard for content moderation pipeline."""

from flask import Flask, render_template, jsonify
from flask_cors import CORS
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from monitoring import metrics

app = Flask(__name__)
CORS(app)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/metrics')
def get_metrics():
    """Return current metrics as JSON."""
    return jsonify(metrics.get_stats())

@app.route('/health')
def health():
    return jsonify({'status': 'healthy'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5050, debug=False)
