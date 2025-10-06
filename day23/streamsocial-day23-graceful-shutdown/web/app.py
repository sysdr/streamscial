#!/usr/bin/env python3

from flask import Flask, render_template, jsonify
import requests
import json
import threading
import time
from datetime import datetime, timedelta
import redis
import random

app = Flask(__name__)

# Global state for dashboard
dashboard_state = {
    "consumers": [],
    "metrics": {},
    "health_status": {},
    "shutdown_events": []
}

def update_dashboard_data():
    """Background thread to update dashboard data"""
    while True:
        try:
            # Get health status
            health_response = requests.get('http://localhost:8080/health', timeout=2)
            dashboard_state["health_status"] = health_response.json()
            
            # Get metrics (simplified parsing)
            try:
                metrics_response = requests.get('http://localhost:8080/metrics', timeout=2)
                dashboard_state["metrics"] = {"raw": metrics_response.text[:500]}
            except:
                pass
                
        except Exception as e:
            dashboard_state["health_status"] = {"status": "error", "error": str(e)}
        
        time.sleep(2)

# Start background data updater
threading.Thread(target=update_dashboard_data, daemon=True).start()

@app.route('/')
def dashboard():
    return render_template('dashboard.html')

@app.route('/api/status')
def api_status():
    return jsonify(dashboard_state)

@app.route('/api/simulate_shutdown')
def simulate_shutdown():
    # Add shutdown simulation event
    shutdown_event = {
        "timestamp": datetime.now().isoformat(),
        "phase": random.choice(["shutdown_requested", "draining", "committing", "cleanup"]),
        "duration": random.uniform(0.5, 5.0),
        "consumer_id": f"consumer-{random.randint(1, 3)}"
    }
    dashboard_state["shutdown_events"].append(shutdown_event)
    
    # Keep only last 10 events
    dashboard_state["shutdown_events"] = dashboard_state["shutdown_events"][-10:]
    
    return jsonify({"status": "simulated", "event": shutdown_event})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
