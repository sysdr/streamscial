import json
import time
import asyncio
from typing import Dict
from fastapi import FastAPI, WebSocket
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import uvicorn

app = FastAPI(title="StreamSocial Error Monitoring Dashboard")

# Metrics storage
metrics_storage = {
    'error_counts': {'skip': 0, 'retry': 0, 'dlq': 0, 'fatal': 0},
    'processed_count': 0,
    'dlq_depth': 0,
    'consumer_lag': 0,
    'error_patterns': {},
    'last_updated': time.time()
}

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Main dashboard page"""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>StreamSocial Error Monitoring</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
            .container { max-width: 1200px; margin: 0 auto; }
            .header { text-align: center; margin-bottom: 30px; }
            .metrics-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; }
            .metric-card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            .metric-value { font-size: 2em; font-weight: bold; color: #2196F3; }
            .metric-label { color: #666; margin-top: 5px; }
            .error-card { border-left: 4px solid #f44336; }
            .success-card { border-left: 4px solid #4CAF50; }
            .warning-card { border-left: 4px solid #FF9800; }
            .status { padding: 10px; border-radius: 4px; margin: 10px 0; }
            .status.healthy { background: #e8f5e8; color: #2e7d2e; }
            .status.warning { background: #fff3e0; color: #ef6c00; }
            .status.critical { background: #ffebee; color: #c62828; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🚨 StreamSocial Error Monitoring</h1>
                <p>Real-time error handling and DLQ monitoring</p>
            </div>
            
            <div id="status" class="status healthy">System Status: Healthy</div>
            
            <div class="metrics-grid">
                <div class="metric-card success-card">
                    <div class="metric-value" id="processed-count">0</div>
                    <div class="metric-label">Messages Processed</div>
                </div>
                
                <div class="metric-card warning-card">
                    <div class="metric-value" id="skip-count">0</div>
                    <div class="metric-label">Skipped Messages</div>
                </div>
                
                <div class="metric-card warning-card">
                    <div class="metric-value" id="retry-count">0</div>
                    <div class="metric-label">Retry Attempts</div>
                </div>
                
                <div class="metric-card error-card">
                    <div class="metric-value" id="dlq-count">0</div>
                    <div class="metric-label">DLQ Messages</div>
                </div>
                
                <div class="metric-card error-card">
                    <div class="metric-value" id="dlq-depth">0</div>
                    <div class="metric-label">DLQ Depth</div>
                </div>
                
                <div class="metric-card">
                    <div class="metric-value" id="success-rate">100%</div>
                    <div class="metric-label">Success Rate</div>
                </div>
            </div>
            
            <div class="metric-card" style="margin-top: 20px;">
                <h3>Error Patterns</h3>
                <div id="error-patterns">No error patterns detected</div>
            </div>
        </div>
        
        <script>
            const ws = new WebSocket('ws://localhost:8000/ws');
            
            ws.onmessage = function(event) {
                const data = JSON.parse(event.data);
                updateMetrics(data);
            };
            
            function updateMetrics(metrics) {
                document.getElementById('processed-count').textContent = metrics.processed_count || 0;
                document.getElementById('skip-count').textContent = metrics.error_counts.skip || 0;
                document.getElementById('retry-count').textContent = metrics.error_counts.retry || 0;
                document.getElementById('dlq-count').textContent = metrics.error_counts.dlq || 0;
                document.getElementById('dlq-depth').textContent = metrics.dlq_depth || 0;
                
                const successRate = ((metrics.processed_count || 0) / 
                    ((metrics.processed_count || 0) + Object.values(metrics.error_counts || {}).reduce((a,b) => a+b, 0))) * 100;
                document.getElementById('success-rate').textContent = successRate.toFixed(1) + '%';
                
                // Update status
                const statusEl = document.getElementById('status');
                const errorRate = Object.values(metrics.error_counts || {}).reduce((a,b) => a+b, 0) / 
                    Math.max((metrics.processed_count || 0), 1);
                
                if (errorRate < 0.01) {
                    statusEl.className = 'status healthy';
                    statusEl.textContent = 'System Status: Healthy';
                } else if (errorRate < 0.05) {
                    statusEl.className = 'status warning';
                    statusEl.textContent = 'System Status: Warning - Elevated Error Rate';
                } else {
                    statusEl.className = 'status critical';
                    statusEl.textContent = 'System Status: Critical - High Error Rate';
                }
            }
            
            // Update every 2 seconds
            setInterval(() => {
                fetch('/metrics').then(r => r.json()).then(updateMetrics);
            }, 2000);
        </script>
    </body>
    </html>
    """

@app.get("/metrics")
async def get_metrics():
    """Get current metrics"""
    return metrics_storage

@app.post("/metrics")
async def update_metrics(metrics: Dict):
    """Update metrics from consumer"""
    metrics_storage.update(metrics)
    metrics_storage['last_updated'] = time.time()
    return {"status": "updated"}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket for real-time updates"""
    await websocket.accept()
    
    try:
        while True:
            await websocket.send_text(json.dumps(metrics_storage))
            await asyncio.sleep(2)
    except Exception:
        pass

def start_dashboard():
    """Start the monitoring dashboard"""
    uvicorn.run(app, host="0.0.0.0", port=8000)

if __name__ == "__main__":
    start_dashboard()
