"""
Production Monitoring Dashboard
Real-time visibility into system health across all regions
"""
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import asyncio
import json
import time
import psutil
import random
from datetime import datetime
from typing import Dict, List

app = FastAPI(title="StreamSocial Production Dashboard")

# Global metrics store
class MetricsCollector:
    def __init__(self):
        self.regions = {
            "us-east-1": {
                "role": "primary",
                "status": "healthy",
                "traffic_percent": 100,
                "rps": 0,
                "error_rate": 0,
                "latency_p99": 0,
                "cpu": 0,
                "memory": 0
            },
            "us-west-1": {
                "role": "standby",
                "status": "healthy",
                "traffic_percent": 0,
                "rps": 0,
                "error_rate": 0,
                "latency_p99": 0,
                "cpu": 0,
                "memory": 0
            },
            "eu-central-1": {
                "role": "read_replica",
                "status": "healthy",
                "traffic_percent": 0,
                "rps": 0,
                "error_rate": 0,
                "latency_p99": 0,
                "cpu": 0,
                "memory": 0
            }
        }
        self.deployment_status = {
            "current_version": "v2.5.0",
            "blue_traffic": 30,  # Blue environment handling 30% of traffic
            "green_traffic": 70,  # Green environment handling 70% of traffic
            "deployment_state": "STABLE"
        }
        self.alerts = []
    
    def collect_metrics(self):
        """Collect current system metrics"""
        cpu_percent = psutil.cpu_percent(interval=0.1)
        memory = psutil.virtual_memory()
        
        # Simulate metrics for each region
        for region_name, region in self.regions.items():
            if region["role"] == "primary":
                base_rps = random.randint(140000, 160000)
                region["rps"] = base_rps
                region["error_rate"] = round(random.uniform(0.01, 0.05), 3)
                region["latency_p99"] = random.randint(160, 200)
                region["cpu"] = round(cpu_percent * random.uniform(0.9, 1.1), 1)
                region["memory"] = round(memory.percent * random.uniform(0.9, 1.1), 1)
                region["traffic_percent"] = 100
            elif region["role"] == "standby":
                # Standby region has replication traffic and monitoring
                region["rps"] = random.randint(5000, 8000)  # Replication RPS
                region["error_rate"] = round(random.uniform(0.005, 0.015), 3)  # Low error rate
                region["latency_p99"] = random.randint(50, 80)  # Replication latency
                region["cpu"] = round(cpu_percent * random.uniform(0.4, 0.6), 1)
                region["memory"] = round(memory.percent * random.uniform(0.5, 0.7), 1)
                region["traffic_percent"] = random.randint(5, 10)  # Standby ready for failover (5-10% traffic)
            else:  # read_replica
                # Read replica has read traffic
                region["rps"] = random.randint(15000, 25000)  # Higher read traffic
                region["error_rate"] = round(random.uniform(0.01, 0.03), 3)
                region["latency_p99"] = random.randint(15, 30)  # Lower for EU users
                region["cpu"] = round(cpu_percent * random.uniform(0.5, 0.7), 1)
                region["memory"] = round(memory.percent * random.uniform(0.5, 0.7), 1)
                region["traffic_percent"] = random.randint(15, 25)  # Read traffic (15-25% of total)
        
        # Check for alerts
        self._check_alerts()
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "regions": self.regions,
            "deployment": self.deployment_status,
            "alerts": self.alerts[-10:]  # Last 10 alerts
        }
    
    def _check_alerts(self):
        """Check for alerting conditions"""
        # Limit alerts list size to prevent memory growth
        if len(self.alerts) > 50:
            self.alerts = self.alerts[-30:]
        
        for region_name, region in self.regions.items():
            if region["error_rate"] > 0.1:
                self.alerts.append({
                    "severity": "critical",
                    "region": region_name,
                    "message": f"Error rate {region['error_rate']}% exceeds threshold",
                    "timestamp": datetime.utcnow().isoformat()
                })
            
            if region["latency_p99"] > 500:
                self.alerts.append({
                    "severity": "warning",
                    "region": region_name,
                    "message": f"P99 latency {region['latency_p99']}ms above threshold",
                    "timestamp": datetime.utcnow().isoformat()
                })
            
            if region["cpu"] > 85:
                self.alerts.append({
                    "severity": "warning",
                    "region": region_name,
                    "message": f"CPU usage {region['cpu']}% high",
                    "timestamp": datetime.utcnow().isoformat()
                })
            
            # Add informational alerts for high traffic or performance
            if region["rps"] > 140000 and region_name == "us-east-1":
                # Add alert more frequently for visibility
                if random.random() < 0.3:  # 30% chance
                    self.alerts.append({
                        "severity": "info",
                        "region": region_name,
                        "message": f"High traffic detected: {region['rps']:,} RPS",
                        "timestamp": datetime.utcnow().isoformat()
                    })
            
            # Add info alert for replication in standby
            if region["role"] == "standby" and region["rps"] > 0:
                if random.random() < 0.4:  # 40% chance when replication is active
                    self.alerts.append({
                        "severity": "info",
                        "region": region_name,
                        "message": f"Replication active: {region['rps']:,} RPS, latency {region['latency_p99']}ms",
                        "timestamp": datetime.utcnow().isoformat()
                    })
            
            # Add info alert for read replica performance
            if region["role"] == "read_replica" and region["rps"] > 15000:
                if random.random() < 0.35:  # 35% chance
                    self.alerts.append({
                        "severity": "info",
                        "region": region_name,
                        "message": f"Read replica handling {region['rps']:,} RPS with {region['latency_p99']}ms latency",
                        "timestamp": datetime.utcnow().isoformat()
                    })
        
        # Ensure at least some alerts are present for demonstration
        # Add a system status alert if we have very few alerts
        if len(self.alerts) < 2:
            # Add a general system health alert
            if random.random() < 0.5:  # 50% chance to add system alert
                self.alerts.append({
                    "severity": "info",
                    "region": "system",
                    "message": f"All regions operational. Primary handling {self.regions['us-east-1']['rps']:,} RPS",
                    "timestamp": datetime.utcnow().isoformat()
                })

collector = MetricsCollector()

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Serve production dashboard"""
    return """
<!DOCTYPE html>
<html>
<head>
    <title>StreamSocial Production Dashboard</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: #fff;
            padding: 20px;
        }
        
        .header {
            text-align: center;
            margin-bottom: 30px;
            background: rgba(255, 255, 255, 0.1);
            padding: 20px;
            border-radius: 15px;
            backdrop-filter: blur(10px);
        }
        
        .header h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }
        
        .status-badge {
            display: inline-block;
            padding: 8px 20px;
            background: #4caf50;
            border-radius: 20px;
            font-weight: bold;
            font-size: 0.9em;
        }
        
        .container {
            max-width: 1600px;
            margin: 0 auto;
        }
        
        .regions-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(450px, 1fr));
            gap: 25px;
            margin-bottom: 30px;
        }
        
        .region-card {
            background: rgba(255, 255, 255, 0.15);
            border-radius: 15px;
            padding: 25px;
            backdrop-filter: blur(10px);
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.2);
            transition: transform 0.3s ease;
        }
        
        .region-card:hover {
            transform: translateY(-5px);
        }
        
        .region-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
            padding-bottom: 15px;
            border-bottom: 2px solid rgba(255, 255, 255, 0.2);
        }
        
        .region-name {
            font-size: 1.5em;
            font-weight: bold;
        }
        
        .region-role {
            padding: 5px 15px;
            background: rgba(255, 255, 255, 0.2);
            border-radius: 15px;
            font-size: 0.85em;
        }
        
        .metrics-grid {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 15px;
        }
        
        .metric {
            background: rgba(0, 0, 0, 0.2);
            padding: 15px;
            border-radius: 10px;
        }
        
        .metric-label {
            font-size: 0.85em;
            opacity: 0.8;
            margin-bottom: 5px;
        }
        
        .metric-value {
            font-size: 1.8em;
            font-weight: bold;
        }
        
        .metric-unit {
            font-size: 0.6em;
            opacity: 0.7;
        }
        
        .deployment-section {
            background: rgba(255, 255, 255, 0.15);
            border-radius: 15px;
            padding: 25px;
            margin-bottom: 30px;
            backdrop-filter: blur(10px);
        }
        
        .deployment-header {
            font-size: 1.5em;
            margin-bottom: 20px;
            border-bottom: 2px solid rgba(255, 255, 255, 0.2);
            padding-bottom: 10px;
        }
        
        .deployment-info {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
        }
        
        .alerts-section {
            background: rgba(255, 255, 255, 0.15);
            border-radius: 15px;
            padding: 25px;
            backdrop-filter: blur(10px);
        }
        
        .alert {
            background: rgba(244, 67, 54, 0.3);
            border-left: 4px solid #f44336;
            padding: 12px;
            margin-bottom: 10px;
            border-radius: 5px;
        }
        
        .alert.warning {
            background: rgba(255, 152, 0, 0.3);
            border-left-color: #ff9800;
        }
        
        .alert.info {
            background: rgba(33, 150, 243, 0.3);
            border-left-color: #2196f3;
        }
        
        .alert-time {
            font-size: 0.85em;
            opacity: 0.8;
        }
        
        .healthy {
            color: #4caf50;
        }
        
        .warning {
            color: #ff9800;
        }
        
        .critical {
            color: #f44336;
        }
        
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.6; }
        }
        
        .live-indicator {
            display: inline-block;
            width: 10px;
            height: 10px;
            background: #4caf50;
            border-radius: 50%;
            margin-right: 8px;
            animation: pulse 2s infinite;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>🚀 StreamSocial Production Dashboard</h1>
        <div class="status-badge">
            <span class="live-indicator"></span>
            <span id="system-status">ALL SYSTEMS OPERATIONAL</span>
        </div>
        <div style="margin-top: 10px; font-size: 0.9em; opacity: 0.9;">
            Last Updated: <span id="last-update">--</span>
        </div>
    </div>
    
    <div class="container">
        <div class="deployment-section">
            <div class="deployment-header">📦 Deployment Status</div>
            <div class="deployment-info">
                <div class="metric">
                    <div class="metric-label">Current Version</div>
                    <div class="metric-value" id="current-version">--</div>
                </div>
                <div class="metric">
                    <div class="metric-label">Deployment State</div>
                    <div class="metric-value" id="deployment-state">--</div>
                </div>
                <div class="metric">
                    <div class="metric-label">Blue Environment</div>
                    <div class="metric-value">
                        <span id="blue-traffic">--</span>
                        <span class="metric-unit">% traffic</span>
                    </div>
                </div>
                <div class="metric">
                    <div class="metric-label">Green Environment</div>
                    <div class="metric-value">
                        <span id="green-traffic">--</span>
                        <span class="metric-unit">% traffic</span>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="regions-grid" id="regions-container">
            <!-- Regions will be inserted here -->
        </div>
        
        <div class="alerts-section">
            <div class="deployment-header">⚠️ Recent Alerts</div>
            <div id="alerts-container">
                <div style="text-align: center; opacity: 0.6; padding: 20px;">
                    No alerts - System healthy ✓
                </div>
            </div>
        </div>
    </div>
    
    <script>
        const ws = new WebSocket('ws://localhost:8000/ws');
        
        ws.onmessage = function(event) {
            const data = JSON.parse(event.data);
            updateDashboard(data);
        };
        
        function updateDashboard(data) {
            // Update timestamp
            document.getElementById('last-update').textContent = 
                new Date(data.timestamp).toLocaleTimeString();
            
            // Update deployment info
            document.getElementById('current-version').textContent = 
                data.deployment.current_version;
            document.getElementById('deployment-state').textContent = 
                data.deployment.deployment_state;
            document.getElementById('blue-traffic').textContent = 
                data.deployment.blue_traffic;
            document.getElementById('green-traffic').textContent = 
                data.deployment.green_traffic;
            
            // Update regions
            const regionsContainer = document.getElementById('regions-container');
            regionsContainer.innerHTML = '';
            
            for (const [regionName, region] of Object.entries(data.regions)) {
                const card = createRegionCard(regionName, region);
                regionsContainer.innerHTML += card;
            }
            
            // Update alerts
            const alertsContainer = document.getElementById('alerts-container');
            if (data.alerts && data.alerts.length > 0) {
                alertsContainer.innerHTML = data.alerts.map(alert => `
                    <div class="alert ${alert.severity}">
                        <strong>[${alert.severity.toUpperCase()}]</strong> 
                        ${alert.region}: ${alert.message}
                        <div class="alert-time">${new Date(alert.timestamp).toLocaleTimeString()}</div>
                    </div>
                `).join('');
            } else {
                alertsContainer.innerHTML = '<div style="text-align: center; opacity: 0.6; padding: 20px;">No alerts - System healthy ✓</div>';
            }
        }
        
        function createRegionCard(name, region) {
            const statusClass = region.error_rate > 0.1 ? 'critical' : 
                               region.cpu > 80 ? 'warning' : 'healthy';
            
            return `
                <div class="region-card">
                    <div class="region-header">
                        <div class="region-name">${name}</div>
                        <div class="region-role">${region.role.replace('_', ' ')}</div>
                    </div>
                    <div class="metrics-grid">
                        <div class="metric">
                            <div class="metric-label">Traffic</div>
                            <div class="metric-value ${statusClass}">
                                ${region.traffic_percent}<span class="metric-unit">%</span>
                            </div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">Throughput</div>
                            <div class="metric-value">
                                ${(region.rps / 1000).toFixed(1)}<span class="metric-unit">K RPS</span>
                            </div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">Error Rate</div>
                            <div class="metric-value ${region.error_rate > 0.1 ? 'critical' : 'healthy'}">
                                ${region.error_rate}<span class="metric-unit">%</span>
                            </div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">P99 Latency</div>
                            <div class="metric-value ${region.latency_p99 > 300 ? 'warning' : 'healthy'}">
                                ${region.latency_p99}<span class="metric-unit">ms</span>
                            </div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">CPU Usage</div>
                            <div class="metric-value ${region.cpu > 80 ? 'warning' : 'healthy'}">
                                ${region.cpu}<span class="metric-unit">%</span>
                            </div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">Memory Usage</div>
                            <div class="metric-value ${region.memory > 80 ? 'warning' : 'healthy'}">
                                ${region.memory}<span class="metric-unit">%</span>
                            </div>
                        </div>
                    </div>
                </div>
            `;
        }
    </script>
</body>
</html>
"""

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time metrics"""
    await websocket.accept()
    
    try:
        while True:
            metrics = collector.collect_metrics()
            await websocket.send_text(json.dumps(metrics))
            await asyncio.sleep(1)  # Update every second
    except Exception as e:
        logger.error(f"WebSocket error: {e}")

@app.get("/api/metrics")
async def get_metrics():
    """REST API endpoint for metrics"""
    return collector.collect_metrics()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
