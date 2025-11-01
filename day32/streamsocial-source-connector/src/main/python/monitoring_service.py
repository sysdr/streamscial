from flask import Flask, jsonify, render_template_string
from flask_cors import CORS
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
import json
import os
import psutil
import threading
import time
from datetime import datetime

app = Flask(__name__)
CORS(app)


class MonitoringService:
    def __init__(self):
        self.metrics_data = {
            "files_processed": 0,
            "processing_rate": 0,
            "errors_count": 0,
            "current_lag": 0,
            "system_metrics": {},
        }
        self.start_time = time.time()
        self.last_file_count = 0

    def start_background_collection(self):
        def collect():
            while True:
                self.metrics_data["system_metrics"] = {
                    "cpu_percent": psutil.cpu_percent(),
                    "memory_percent": psutil.virtual_memory().percent,
                    "disk_usage": psutil.disk_usage(".").percent,
                    "uptime": time.time() - self.start_time,
                }
                time.sleep(5)

        thread = threading.Thread(target=collect, daemon=True)
        thread.start()


monitoring = MonitoringService()


@app.route("/")
def dashboard():
    return render_template_string(
        """
<!DOCTYPE html>
<html>
<head>
    <title>StreamSocial Source Connector Dashboard</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: #333;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: rgba(255, 255, 255, 0.95);
            border-radius: 20px;
            padding: 30px;
            box-shadow: 0 20px 40px rgba(0,0,0,0.1);
        }
        .header {
            text-align: center;
            margin-bottom: 40px;
        }
        .header h1 {
            color: #2c3e50;
            font-size: 2.5em;
            margin: 0;
            font-weight: 300;
        }
        .header p {
            color: #7f8c8d;
            font-size: 1.2em;
            margin: 10px 0 0 0;
        }
        .metrics-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 25px;
            margin-bottom: 30px;
        }
        .metric-card {
            background: white;
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 10px 25px rgba(0,0,0,0.1);
            border-left: 5px solid;
            transition: transform 0.3s ease;
        }
        .metric-card:hover {
            transform: translateY(-5px);
        }
        .metric-card.primary { border-left-color: #3498db; }
        .metric-card.success { border-left-color: #2ecc71; }
        .metric-card.warning { border-left-color: #f39c12; }
        .metric-card.danger { border-left-color: #e74c3c; }
        
        .metric-value {
            font-size: 2.5em;
            font-weight: bold;
            margin: 10px 0;
        }
        .metric-label {
            color: #7f8c8d;
            font-size: 1.1em;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        .status-indicator {
            display: inline-block;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            margin-right: 10px;
        }
        .status-healthy { background-color: #2ecc71; }
        .status-warning { background-color: #f39c12; }
        .status-error { background-color: #e74c3c; }
        .logs-section {
            background: #2c3e50;
            color: #ecf0f1;
            padding: 20px;
            border-radius: 15px;
            margin-top: 30px;
        }
        .logs-title {
            font-size: 1.3em;
            margin-bottom: 15px;
            display: flex;
            align-items: center;
        }
        .log-content {
            background: #34495e;
            padding: 15px;
            border-radius: 10px;
            font-family: 'Courier New', monospace;
            font-size: 0.9em;
            max-height: 300px;
            overflow-y: auto;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>StreamSocial Source Connector</h1>
            <p>File Upload Ingestion Pipeline - Day 32</p>
        </div>
        
        <div class="metrics-grid">
            <div class="metric-card primary">
                <div class="metric-label">Files Processed</div>
                <div class="metric-value" id="files-processed">0</div>
            </div>
            
            <div class="metric-card success">
                <div class="metric-label">Processing Rate</div>
                <div class="metric-value" id="processing-rate">0/sec</div>
            </div>
            
            <div class="metric-card warning">
                <div class="metric-label">Current Lag</div>
                <div class="metric-value" id="current-lag">0s</div>
            </div>
            
            <div class="metric-card danger">
                <div class="metric-label">Errors</div>
                <div class="metric-value" id="errors-count">0</div>
            </div>
            
            <div class="metric-card primary">
                <div class="metric-label">System Status</div>
                <div class="metric-value">
                    <span class="status-indicator status-healthy"></span>
                    Healthy
                </div>
            </div>
            
            <div class="metric-card success">
                <div class="metric-label">CPU Usage</div>
                <div class="metric-value" id="cpu-usage">0%</div>
            </div>
        </div>
        
        <div class="logs-section">
            <div class="logs-title">
                Recent Activity
            </div>
            <div class="log-content" id="logs">
                Connector starting up...<br>
                Monitoring directory: ./uploads<br>
                Kafka connection established<br>
                Ready to process files
            </div>
        </div>
    </div>
    
    <script>
        function updateMetrics() {
            fetch('/metrics')
                .then(response => response.json())
                .then(data => {
                    document.getElementById('files-processed').textContent = data.files_processed;
                    document.getElementById('processing-rate').textContent = data.processing_rate + '/sec';
                    document.getElementById('current-lag').textContent = data.current_lag + 's';
                    document.getElementById('errors-count').textContent = data.errors_count;
                    document.getElementById('cpu-usage').textContent = data.system_metrics.cpu_percent + '%';
                })
                .catch(error => console.log('Error fetching metrics:', error));
        }
        
        // Update metrics every 2 seconds
        setInterval(updateMetrics, 2000);
        updateMetrics();
    </script>
</body>
</html>
    """
    )


@app.route("/metrics")
def get_metrics():
    try:
        # Try to read Prometheus metrics if connector is running
        try:
            import requests

            # Connect to file-source-connector service using Docker service name
            # Port 8080 is the Prometheus metrics port inside the connector container
            prom_response = requests.get("http://file-source-connector:8080/metrics", timeout=2)
            if prom_response.status_code == 200:
                # Parse Prometheus metrics
                for line in prom_response.text.split("\n"):
                    if line.startswith("files_processed_total "):
                        value = float(line.split()[1])
                        monitoring.metrics_data["files_processed"] = int(value)
                    elif line.startswith("connector_lag_seconds "):
                        value = float(line.split()[1])
                        monitoring.metrics_data["current_lag"] = round(value, 2)
                    elif "connector_errors_total" in line and not line.startswith("#"):
                        parts = line.split()
                        if len(parts) >= 2:
                            monitoring.metrics_data["errors_count"] = int(
                                float(parts[1])
                            )
        except:
            pass  # Connector may not be running yet

        # Read offset file to get processing stats
        files_processed_from_offsets = 0
        if os.path.exists("./data/offsets.json"):
            try:
                with open("./data/offsets.json", "r") as f:
                    offset_data = json.load(f)
                    if offset_data.get("last_processed_file"):
                        files_processed_from_offsets = (
                            monitoring.metrics_data.get("files_processed", 0) + 1
                        )
            except:
                pass

        # Count files from offset directory to get actual count
        try:
            if os.path.exists("./data/offsets.json"):
                # Count how many files have been processed by checking offset file updates
                import glob

                processed_count = 0
                if os.path.exists("./data/offsets.json"):
                    # Try to count processed files from logs or use timestamp tracking
                    stat = os.stat("./data/offsets.json")
                    if stat.st_mtime > monitoring.start_time:
                        processed_count = max(
                            monitoring.metrics_data.get("files_processed", 0), 1
                        )
        except:
            pass

        # Use offset-based count if Prometheus metrics not available
        if monitoring.metrics_data["files_processed"] == 0:
            # Try counting files in uploads directory that have been processed
            try:
                if os.path.exists("./uploads"):
                    # Count files in uploads directory
                    import subprocess

                    result = subprocess.run(
                        ["find", "./uploads", "-type", "f"],
                        capture_output=True,
                        text=True,
                        timeout=5,
                    )
                    if result.returncode == 0 and result.stdout.strip():
                        file_count = len(
                            [f for f in result.stdout.strip().split("\n") if f.strip()]
                        )
                        if file_count > monitoring.last_file_count:
                            monitoring.metrics_data["files_processed"] = file_count
                            monitoring.last_file_count = file_count
                        elif file_count > 0:
                            monitoring.metrics_data["files_processed"] = file_count
            except Exception as e:
                pass  # Ignore errors in file counting

        # Calculate processing rate (simplified - files per second since start)
        uptime = time.time() - monitoring.start_time
        if uptime > 0 and monitoring.metrics_data["files_processed"] > 0:
            monitoring.metrics_data["processing_rate"] = round(
                monitoring.metrics_data["files_processed"] / uptime, 2
            )
        else:
            monitoring.metrics_data["processing_rate"] = 0

        return jsonify(monitoring.metrics_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/health")
def health_check():
    return jsonify(
        {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "uptime": time.time() - monitoring.start_time,
        }
    )


if __name__ == "__main__":
    monitoring.start_background_collection()
    app.run(host="0.0.0.0", port=8080, debug=False)
