import asyncio
import json
from datetime import datetime
from fastapi import FastAPI, WebSocket
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import uvicorn

from .metrics import MetricsCollector

class MonitoringDashboard:
    def __init__(self, metrics_collector: MetricsCollector):
        self.app = FastAPI(title="StreamSocial Engagement Pipeline Monitor")
        self.metrics = metrics_collector
        self.setup_routes()
    
    def setup_routes(self):
        @self.app.get("/")
        async def dashboard():
            return HTMLResponse(self.get_dashboard_html())
        
        @self.app.get("/api/metrics")
        async def get_metrics():
            return self.metrics.get_metrics_summary()
        
        @self.app.websocket("/ws/metrics")
        async def websocket_endpoint(websocket: WebSocket):
            await websocket.accept()
            try:
                while True:
                    metrics = self.metrics.get_metrics_summary()
                    await websocket.send_text(json.dumps(metrics))
                    await asyncio.sleep(2)
            except Exception as e:
                print(f"WebSocket error: {e}")
    
    def get_dashboard_html(self) -> str:
        return """
<!DOCTYPE html>
<html>
<head>
    <title>StreamSocial Engagement Pipeline Monitor</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { background: #2c3e50; color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
        .metrics-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
        .metric-card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .metric-title { font-size: 18px; font-weight: bold; color: #2c3e50; margin-bottom: 10px; }
        .metric-value { font-size: 24px; font-weight: bold; color: #3498db; }
        .metric-unit { font-size: 14px; color: #7f8c8d; }
        .status-indicator { display: inline-block; width: 12px; height: 12px; border-radius: 50%; margin-right: 8px; }
        .status-healthy { background-color: #27ae60; }
        .status-warning { background-color: #f39c12; }
        .status-error { background-color: #e74c3c; }
        .log-container { background: #2c3e50; color: #ecf0f1; padding: 20px; border-radius: 8px; margin-top: 20px; max-height: 300px; overflow-y: auto; font-family: monospace; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 StreamSocial Engagement Pipeline Monitor</h1>
            <p>Real-time monitoring of Kafka commit strategies and reliability</p>
        </div>
        
        <div class="metrics-grid">
            <div class="metric-card">
                <div class="metric-title">
                    <span id="status-indicator" class="status-indicator status-healthy"></span>
                    Pipeline Status
                </div>
                <div class="metric-value" id="pipeline-status">HEALTHY</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-title">Total Processed</div>
                <div class="metric-value" id="total-processed">0</div>
                <div class="metric-unit">engagements</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-title">Throughput</div>
                <div class="metric-value" id="throughput">0.0</div>
                <div class="metric-unit">messages/second</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-title">Error Rate</div>
                <div class="metric-value" id="error-rate">0.0</div>
                <div class="metric-unit">%</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-title">Avg Processing Time</div>
                <div class="metric-value" id="processing-time">0.0</div>
                <div class="metric-unit">ms</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-title">Commit Lag</div>
                <div class="metric-value" id="commit-lag">0</div>
                <div class="metric-unit">messages</div>
            </div>
        </div>
        
        <div class="log-container">
            <div style="margin-bottom: 10px; font-weight: bold;">Live System Logs</div>
            <div id="logs"></div>
        </div>
    </div>

    <script>
        const ws = new WebSocket('ws://localhost:8000/ws/metrics');
        const logs = document.getElementById('logs');
        
        function addLog(message) {
            const timestamp = new Date().toLocaleTimeString();
            logs.innerHTML += `[${timestamp}] ${message}<br>`;
            logs.scrollTop = logs.scrollHeight;
        }
        
        function updateStatus(errorRate) {
            const indicator = document.getElementById('status-indicator');
            const status = document.getElementById('pipeline-status');
            
            if (errorRate < 1) {
                indicator.className = 'status-indicator status-healthy';
                status.textContent = 'HEALTHY';
            } else if (errorRate < 5) {
                indicator.className = 'status-indicator status-warning';
                status.textContent = 'WARNING';
            } else {
                indicator.className = 'status-indicator status-error';
                status.textContent = 'ERROR';
            }
        }
        
        ws.onmessage = function(event) {
            const metrics = JSON.parse(event.data);
            
            document.getElementById('total-processed').textContent = 
                metrics.counters.engagements_processed || 0;
            document.getElementById('throughput').textContent = 
                metrics.throughput_mps.toFixed(2);
            document.getElementById('error-rate').textContent = 
                metrics.error_rate_percent.toFixed(2);
            document.getElementById('processing-time').textContent = 
                metrics.avg_processing_time_ms.toFixed(2);
            document.getElementById('commit-lag').textContent = 
                metrics.commit_lag_count;
            
            updateStatus(metrics.error_rate_percent);
            
            // Add log entries for significant events
            if (metrics.counters.processing_errors > 0) {
                addLog(`Processing errors detected: ${metrics.counters.processing_errors}`);
            }
            if (metrics.counters.commit_errors > 0) {
                addLog(`Commit errors detected: ${metrics.counters.commit_errors}`);
            }
        };
        
        ws.onopen = function() {
            addLog('Connected to StreamSocial monitoring system');
        };
        
        ws.onclose = function() {
            addLog('Disconnected from monitoring system');
        };
        
        // Initial log
        addLog('StreamSocial Engagement Pipeline Monitor started');
    </script>
</body>
</html>
        """
    
    async def start(self, host: str = "127.0.0.1", port: int = 8000):
        config = uvicorn.Config(self.app, host=host, port=port, log_level="info")
        server = uvicorn.Server(config)
        await server.serve()
