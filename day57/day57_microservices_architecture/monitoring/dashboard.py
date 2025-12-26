"""Real-time monitoring dashboard for microservices"""
from flask import Flask, render_template, jsonify
from flask_cors import CORS
import requests
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

SERVICES = {
    'user': 'http://localhost:8001',
    'content': 'http://localhost:8002',
    'notification': 'http://localhost:8003'
}

@app.route('/')
def dashboard():
    return render_template('dashboard.html')

@app.route('/api/metrics')
def get_metrics():
    metrics = {}
    for name, url in SERVICES.items():
        try:
            response = requests.get(f"{url}/metrics", timeout=2)
            metrics[name] = response.json()
        except Exception as e:
            logger.error(f"Failed to get metrics from {name}: {e}")
            metrics[name] = {"error": str(e)}
    
    return jsonify(metrics)

@app.route('/api/health')
def get_health():
    health = {}
    for name, url in SERVICES.items():
        try:
            response = requests.get(f"{url}/health", timeout=2)
            health[name] = response.json()
        except Exception as e:
            health[name] = {"status": "unhealthy", "error": str(e)}
    
    return jsonify(health)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
