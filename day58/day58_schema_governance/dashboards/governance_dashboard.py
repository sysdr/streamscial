"""
Schema Governance Dashboard - Real-time visualization
Modern UI showing schema evolution and health metrics
"""

from flask import Flask, render_template, jsonify
import json
import time

app = Flask(__name__)

# Store metrics in memory (would be Redis/DB in production)
dashboard_metrics = {
    'registry': {},
    'producer': {},
    'consumer': {},
    'system': {},
    'health_score': 100,
    'last_update': time.time()
}


@app.route('/')
def index():
    """Serve dashboard UI"""
    return render_template('governance.html')


@app.route('/api/metrics')
def get_metrics():
    """API endpoint for metrics"""
    return jsonify(dashboard_metrics)


@app.route('/api/update_metrics', methods=['POST'])
def update_metrics():
    """Update metrics (called by monitoring service)"""
    global dashboard_metrics
    from flask import request
    dashboard_metrics = request.json
    dashboard_metrics['last_update'] = time.time()
    return jsonify({'status': 'ok'})


# Create HTML template
html_template = '''
<!DOCTYPE html>
<html>
<head>
    <title>StreamSocial Schema Governance</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #1a202c 0%, #2d3748 100%);
            color: #2d3748;
            padding: 20px;
            min-height: 100vh;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
        }
        
        .header {
            background: white;
            padding: 30px;
            border-radius: 16px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
            margin-bottom: 30px;
        }
        
        .header h1 {
            color: #1a202c;
            font-size: 2.5em;
            margin-bottom: 10px;
        }
        
        .header .subtitle {
            color: #718096;
            font-size: 1.1em;
        }
        
        .health-score {
            display: inline-block;
            background: linear-gradient(135deg, #27ae60 0%, #2ecc71 100%);
            color: white;
            padding: 10px 30px;
            border-radius: 50px;
            font-size: 1.3em;
            font-weight: bold;
            float: right;
            box-shadow: 0 5px 15px rgba(39,174,96,0.3);
        }
        
        .metrics-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        
        .metric-card {
            background: white;
            padding: 25px;
            border-radius: 12px;
            box-shadow: 0 5px 20px rgba(0,0,0,0.08);
            transition: transform 0.3s ease;
        }
        
        .metric-card:hover {
            transform: translateY(-5px);
            box-shadow: 0 8px 30px rgba(0,0,0,0.12);
        }
        
        .metric-card h3 {
            color: #1a202c;
            margin-bottom: 20px;
            font-size: 1.3em;
            display: flex;
            align-items: center;
        }
        
        .metric-card h3::before {
            content: '';
            width: 8px;
            height: 30px;
            background: linear-gradient(135deg, #27ae60 0%, #2ecc71 100%);
            margin-right: 12px;
            border-radius: 4px;
        }
        
        .metric-value {
            font-size: 2.5em;
            font-weight: bold;
            color: #2d3748;
            margin: 10px 0;
        }
        
        .metric-label {
            color: #718096;
            font-size: 0.95em;
            margin-top: 5px;
        }
        
        .version-bar {
            background: #f7fafc;
            height: 40px;
            border-radius: 8px;
            overflow: hidden;
            margin: 10px 0;
            display: flex;
        }
        
        .version-segment {
            height: 100%;
            display: flex;
            align-items: center;
            justify-content: center;
            color: white;
            font-weight: 600;
            transition: all 0.5s ease;
        }
        
        .v1 { background: #e74c3c; }
        .v2 { background: #f39c12; }
        .v3 { background: #27ae60; }
        
        .chart-container {
            background: white;
            padding: 25px;
            border-radius: 12px;
            box-shadow: 0 5px 20px rgba(0,0,0,0.08);
            margin-bottom: 20px;
        }
        
        .chart-container h3 {
            color: #1a202c;
            margin-bottom: 20px;
            font-size: 1.3em;
        }
        
        .stats-row {
            display: flex;
            justify-content: space-between;
            margin: 15px 0;
            padding: 15px;
            background: #f7fafc;
            border-radius: 8px;
        }
        
        .stats-row .label {
            color: #718096;
            font-weight: 500;
        }
        
        .stats-row .value {
            color: #2d3748;
            font-weight: bold;
        }
        
        .status-badge {
            display: inline-block;
            padding: 5px 15px;
            border-radius: 20px;
            font-size: 0.85em;
            font-weight: 600;
        }
        
        .status-success {
            background: #d4edda;
            color: #155724;
        }
        
        .status-warning {
            background: #fff3cd;
            color: #856404;
        }
        
        .status-error {
            background: #f8d7da;
            color: #721c24;
        }
        
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }
        
        .live-indicator {
            display: inline-block;
            width: 10px;
            height: 10px;
            background: #27ae60;
            border-radius: 50%;
            margin-left: 10px;
            animation: pulse 2s ease-in-out infinite;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div class="health-score" id="healthScore">Health: 100/100</div>
            <h1>🛡️ StreamSocial Schema Governance</h1>
            <p class="subtitle">Real-time Schema Evolution Monitoring <span class="live-indicator"></span></p>
        </div>
        
        <div class="metrics-grid">
            <div class="metric-card">
                <h3>Registry Status</h3>
                <div class="metric-value" id="totalSchemas">0</div>
                <div class="metric-label">Total Schemas Registered</div>
                <div class="stats-row">
                    <span class="label">Subjects:</span>
                    <span class="value" id="subjects">0</span>
                </div>
                <div class="stats-row">
                    <span class="label">Compatibility Checks:</span>
                    <span class="value" id="compatChecks">0</span>
                </div>
                <div class="stats-row">
                    <span class="label">Failures:</span>
                    <span class="value" id="failures">0</span>
                </div>
            </div>
            
            <div class="metric-card">
                <h3>Producer Metrics</h3>
                <div class="metric-value" id="totalSent">0</div>
                <div class="metric-label">Events Produced</div>
                <div id="producerVersions"></div>
            </div>
            
            <div class="metric-card">
                <h3>Consumer Metrics</h3>
                <div class="metric-value" id="totalConsumed">0</div>
                <div class="metric-label">Events Consumed</div>
                <div class="stats-row">
                    <span class="label">Compatibility Errors:</span>
                    <span class="value" id="consumerErrors">0</span>
                </div>
                <div class="stats-row">
                    <span class="label">Avg Processing:</span>
                    <span class="value" id="avgProcessing">0ms</span>
                </div>
            </div>
            
            <div class="metric-card">
                <h3>System Resources</h3>
                <div class="stats-row">
                    <span class="label">CPU Usage:</span>
                    <span class="value" id="cpu">0%</span>
                </div>
                <div class="stats-row">
                    <span class="label">Memory Usage:</span>
                    <span class="value" id="memory">0%</span>
                </div>
                <div class="stats-row">
                    <span class="label">Uptime:</span>
                    <span class="value" id="uptime">0s</span>
                </div>
            </div>
        </div>
        
        <div class="chart-container">
            <h3>Schema Version Adoption</h3>
            <canvas id="adoptionChart" height="80"></canvas>
        </div>
        
        <div class="chart-container">
            <h3>Migration Progress</h3>
            <div class="version-bar" id="migrationBar"></div>
        </div>
    </div>
    
    <script>
        let adoptionChart;
        
        function updateDashboard() {
            fetch('/api/metrics')
                .then(response => response.json())
                .then(data => {
                    // Health Score
                    const healthScore = data.health_score || 100;
                    document.getElementById('healthScore').textContent = `Health: ${healthScore}/100`;
                    document.getElementById('healthScore').style.background = 
                        healthScore >= 90 ? 'linear-gradient(135deg, #27ae60 0%, #2ecc71 100%)' :
                        healthScore >= 70 ? 'linear-gradient(135deg, #f39c12 0%, #e67e22 100%)' :
                        'linear-gradient(135deg, #e74c3c 0%, #c0392b 100%)';
                    
                    // Registry
                    if (data.registry && data.registry.registry) {
                        document.getElementById('totalSchemas').textContent = data.registry.registry.total_schemas || 0;
                        document.getElementById('subjects').textContent = data.registry.registry.total_subjects || 0;
                        document.getElementById('compatChecks').textContent = data.registry.registry.compatibility_checks || 0;
                        document.getElementById('failures').textContent = data.registry.registry.failures || 0;
                    }
                    
                    // Producer
                    if (data.producer) {
                        document.getElementById('totalSent').textContent = data.producer.total_sent || 0;
                        
                        if (data.producer.adoption) {
                            let versionsHtml = '';
                            for (const [version, vdata] of Object.entries(data.producer.adoption)) {
                                versionsHtml += `
                                    <div class="stats-row">
                                        <span class="label">${version}:</span>
                                        <span class="value">${vdata.count} (${vdata.percentage.toFixed(1)}%)</span>
                                    </div>
                                `;
                            }
                            document.getElementById('producerVersions').innerHTML = versionsHtml;
                            
                            // Update migration bar
                            let barHtml = '';
                            if (data.producer.adoption.v1) {
                                barHtml += `<div class="version-segment v1" style="width: ${data.producer.adoption.v1.percentage}%">v1: ${data.producer.adoption.v1.percentage.toFixed(1)}%</div>`;
                            }
                            if (data.producer.adoption.v2) {
                                barHtml += `<div class="version-segment v2" style="width: ${data.producer.adoption.v2.percentage}%">v2: ${data.producer.adoption.v2.percentage.toFixed(1)}%</div>`;
                            }
                            if (data.producer.adoption.v3) {
                                barHtml += `<div class="version-segment v3" style="width: ${data.producer.adoption.v3.percentage}%">v3: ${data.producer.adoption.v3.percentage.toFixed(1)}%</div>`;
                            }
                            document.getElementById('migrationBar').innerHTML = barHtml;
                        }
                    }
                    
                    // Consumer
                    if (data.consumer) {
                        document.getElementById('totalConsumed').textContent = data.consumer.total_consumed || 0;
                        document.getElementById('consumerErrors').textContent = data.consumer.compatibility_errors || 0;
                        document.getElementById('avgProcessing').textContent = 
                            (data.consumer.avg_processing_ms || 0).toFixed(2) + 'ms';
                    }
                    
                    // System
                    if (data.system) {
                        document.getElementById('cpu').textContent = (data.system.cpu_percent || 0).toFixed(1) + '%';
                        document.getElementById('memory').textContent = (data.system.memory_percent || 0).toFixed(1) + '%';
                        document.getElementById('uptime').textContent = (data.system.uptime_seconds || 0).toFixed(1) + 's';
                    }
                    
                    // Update chart
                    updateChart(data);
                });
        }
        
        function updateChart(data) {
            const ctx = document.getElementById('adoptionChart');
            
            if (!adoptionChart) {
                adoptionChart = new Chart(ctx, {
                    type: 'bar',
                    data: {
                        labels: ['v1', 'v2', 'v3'],
                        datasets: [{
                            label: 'Events Produced',
                            data: [0, 0, 0],
                            backgroundColor: ['#e74c3c', '#f39c12', '#27ae60']
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: true,
                        scales: {
                            y: {
                                beginAtZero: true
                            }
                        }
                    }
                });
            }
            
            if (data.producer && data.producer.adoption) {
                adoptionChart.data.datasets[0].data = [
                    data.producer.adoption.v1?.count || 0,
                    data.producer.adoption.v2?.count || 0,
                    data.producer.adoption.v3?.count || 0
                ];
                adoptionChart.update();
            }
        }
        
        // Update every 2 seconds
        setInterval(updateDashboard, 2000);
        updateDashboard();
    </script>
</body>
</html>
'''

# Create templates directory and save HTML
import os
# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
templates_dir = os.path.join(script_dir, 'templates')
os.makedirs(templates_dir, exist_ok=True)
template_path = os.path.join(templates_dir, 'governance.html')
with open(template_path, 'w') as f:
    f.write(html_template)
# Set Flask template folder
app.template_folder = templates_dir


def start_dashboard():
    """Start dashboard server"""
    app.run(host='0.0.0.0', port=5058, debug=False)


if __name__ == '__main__':
    start_dashboard()
