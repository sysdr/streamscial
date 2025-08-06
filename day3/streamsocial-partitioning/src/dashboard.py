"""StreamSocial Partition Monitoring Dashboard"""

from fastapi import FastAPI, WebSocket, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import json
import asyncio
import os
from typing import Dict, Any
import logging
from jinja2 import Template

logger = logging.getLogger(__name__)

app = FastAPI(title="StreamSocial Partition Monitor", version="1.0.0")

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Serve the partition monitoring dashboard"""
    html_template = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>StreamSocial - Kafka Partition Monitor</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            color: #333;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
        }
        
        .header {
            text-align: center;
            color: white;
            margin-bottom: 30px;
        }
        
        .header h1 {
            font-size: 2.5rem;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }
        
        .header p {
            font-size: 1.2rem;
            opacity: 0.9;
        }
        
        .metrics-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        
        .metric-card {
            background: white;
            border-radius: 15px;
            padding: 25px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }
        
        .metric-card:hover {
            transform: translateY(-5px);
            box-shadow: 0 15px 40px rgba(0,0,0,0.15);
        }
        
        .metric-title {
            font-size: 1.1rem;
            color: #666;
            margin-bottom: 10px;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        
        .metric-value {
            font-size: 2.5rem;
            font-weight: bold;
            color: #4CAF50;
            margin-bottom: 5px;
        }
        
        .metric-subtitle {
            color: #888;
            font-size: 0.9rem;
        }
        
        .health-indicator {
            display: inline-block;
            padding: 8px 16px;
            border-radius: 20px;
            font-weight: bold;
            text-transform: uppercase;
            font-size: 0.8rem;
        }
        
        .health-healthy { background: #4CAF50; color: white; }
        .health-warning { background: #FF9800; color: white; }
        .health-unhealthy { background: #f44336; color: white; }
        
        .partition-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(80px, 1fr));
            gap: 10px;
            margin: 20px 0;
        }
        
        .partition-cell {
            background: white;
            border-radius: 8px;
            padding: 10px;
            text-align: center;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            transition: all 0.3s ease;
        }
        
        .partition-cell:hover {
            transform: scale(1.05);
            box-shadow: 0 5px 20px rgba(0,0,0,0.2);
        }
        
        .partition-normal { border-left: 4px solid #4CAF50; }
        .partition-hot { border-left: 4px solid #f44336; animation: pulse 2s infinite; }
        .partition-cold { border-left: 4px solid #2196F3; }
        
        @keyframes pulse {
            0% { box-shadow: 0 0 0 0 rgba(244, 67, 54, 0.4); }
            70% { box-shadow: 0 0 0 10px rgba(244, 67, 54, 0); }
            100% { box-shadow: 0 0 0 0 rgba(244, 67, 54, 0); }
        }
        
        .partition-id {
            font-weight: bold;
            color: #333;
            font-size: 0.9rem;
        }
        
        .partition-throughput {
            color: #666;
            font-size: 0.7rem;
            margin-top: 5px;
        }
        
        .topic-section {
            background: white;
            border-radius: 15px;
            padding: 25px;
            margin-bottom: 20px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
        }
        
        .topic-title {
            font-size: 1.5rem;
            color: #333;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 2px solid #eee;
        }
        
        .status-indicator {
            display: inline-block;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            margin-right: 8px;
            vertical-align: middle;
        }
        
        .status-active { background: #4CAF50; animation: blink 2s infinite; }
        .status-idle { background: #FFC107; }
        .status-error { background: #f44336; }
        
        @keyframes blink {
            0%, 50% { opacity: 1; }
            51%, 100% { opacity: 0.3; }
        }
        
        .refresh-info {
            text-align: center;
            color: white;
            margin-top: 20px;
            opacity: 0.8;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 StreamSocial</h1>
            <p>Kafka Partition Monitoring Dashboard</p>
        </div>
        
        <div class="metrics-grid">
            <div class="metric-card">
                <div class="metric-title">Total Partitions</div>
                <div class="metric-value" id="total-partitions">Loading...</div>
                <div class="metric-subtitle">Across all topics</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-title">Active Partitions</div>
                <div class="metric-value" id="active-partitions">Loading...</div>
                <div class="metric-subtitle">Currently processing</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-title">Cluster Health</div>
                <div class="metric-value">
                    <span class="health-indicator" id="health-status">Loading...</span>
                </div>
                <div class="metric-subtitle">Overall system status</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-title">Avg Throughput</div>
                <div class="metric-value" id="avg-throughput">Loading...</div>
                <div class="metric-subtitle">Messages per second</div>
            </div>
        </div>
        
        <div class="topic-section">
            <div class="topic-title">
                <span class="status-indicator status-active"></span>
                User Actions Topic (1000 partitions)
            </div>
            <div class="partition-grid" id="user-actions-grid">
                Loading partition data...
            </div>
        </div>
        
        <div class="topic-section">
            <div class="topic-title">
                <span class="status-indicator status-active"></span>
                Content Interactions Topic (500 partitions)
            </div>
            <div class="partition-grid" id="content-interactions-grid">
                Loading partition data...
            </div>
        </div>
        
        <div class="refresh-info">
            <p>📊 Dashboard updates every 10 seconds</p>
            <p>🔥 Red partitions are hot spots | 🔵 Blue partitions are cold spots</p>
        </div>
    </div>
    
    <script>
        async function updateDashboard() {
            try {
                const response = await fetch('/api/metrics');
                const data = await response.json();
                
                // Update overview metrics
                document.getElementById('total-partitions').textContent = data.total_partitions || 0;
                document.getElementById('active-partitions').textContent = data.active_partitions || 0;
                document.getElementById('avg-throughput').textContent = (data.avg_throughput || 0).toFixed(1);
                
                // Update health status
                const healthElement = document.getElementById('health-status');
                const health = data.overall_health || 'unknown';
                healthElement.textContent = health.toUpperCase();
                healthElement.className = `health-indicator health-${health}`;
                
                // Update partition grids
                updatePartitionGrid('user-actions-grid', data.user_actions_partitions || [], data.hotspots || [], data.cold_spots || []);
                updatePartitionGrid('content-interactions-grid', data.content_interactions_partitions || [], data.hotspots || [], data.cold_spots || []);
                
            } catch (error) {
                console.error('Failed to update dashboard:', error);
            }
        }
        
        function updatePartitionGrid(gridId, partitions, hotspots, coldSpots) {
            const grid = document.getElementById(gridId);
            grid.innerHTML = '';
            
            partitions.forEach(partition => {
                const cell = document.createElement('div');
                cell.className = 'partition-cell';
                
                if (hotspots.includes(partition.id)) {
                    cell.classList.add('partition-hot');
                } else if (coldSpots.includes(partition.id)) {
                    cell.classList.add('partition-cold');
                } else {
                    cell.classList.add('partition-normal');
                }
                
                cell.innerHTML = `
                    <div class="partition-id">${partition.id}</div>
                    <div class="partition-throughput">${(partition.throughput || 0).toFixed(1)}/s</div>
                `;
                
                grid.appendChild(cell);
            });
        }
        
        // Update dashboard every 10 seconds
        updateDashboard();
        setInterval(updateDashboard, 10000);
    </script>
</body>
</html>
    """
    return HTMLResponse(content=html_template)

@app.get("/api/metrics")
async def get_metrics():
    """API endpoint for partition metrics"""
    try:
        # Load latest metrics
        metrics_file = 'monitoring/latest_metrics.json'
        if os.path.exists(metrics_file):
            try:
                with open(metrics_file, 'r') as f:
                    data = json.load(f)
                
                cluster_health = data.get('cluster_health', {})
                partition_metrics = data.get('partition_metrics', [])
                
                # Organize partitions by topic
                user_actions_partitions = []
                content_interactions_partitions = []
                
                for metric in partition_metrics:
                    partition_info = {
                        'id': metric['partition_id'],
                        'throughput': metric['throughput_rps'],
                        'lag': metric['lag']
                    }
                    
                    if metric['topic_name'] == 'user-actions':
                        user_actions_partitions.append(partition_info)
                    elif metric['topic_name'] == 'content-interactions':
                        content_interactions_partitions.append(partition_info)
                
                return {
                    'total_partitions': cluster_health.get('total_partitions', 0),
                    'active_partitions': cluster_health.get('active_partitions', 0),
                    'avg_throughput': cluster_health.get('avg_throughput', 0),
                    'overall_health': cluster_health.get('overall_health', 'unknown'),
                    'hotspots': cluster_health.get('hotspots', []),
                    'cold_spots': cluster_health.get('cold_spots', []),
                    'user_actions_partitions': user_actions_partitions,
                    'content_interactions_partitions': content_interactions_partitions
                }
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Failed to read metrics file: {e}")
                # Fall through to mock data
        
        # Return mock data for demo
        return {
            'total_partitions': 1500,
            'active_partitions': 1200,
            'avg_throughput': 125.7,
            'overall_health': 'healthy',
            'hotspots': [45, 234, 567, 789, 1234],
            'cold_spots': [12, 789, 1456],
            'user_actions_partitions': [{'id': i, 'throughput': 50 + (i % 100), 'lag': i % 10} for i in range(50)],
            'content_interactions_partitions': [{'id': i, 'throughput': 75 + (i % 80), 'lag': i % 8} for i in range(30)]
        }
    
    except Exception as e:
        logger.error(f"Failed to get metrics: {e}")
        # Return basic fallback data
        return {
            'total_partitions': 1000,
            'active_partitions': 800,
            'avg_throughput': 100.0,
            'overall_health': 'unknown',
            'hotspots': [],
            'cold_spots': [],
            'user_actions_partitions': [{'id': i, 'throughput': 50, 'lag': 0} for i in range(10)],
            'content_interactions_partitions': [{'id': i, 'throughput': 50, 'lag': 0} for i in range(10)]
        }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
