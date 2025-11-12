"""
Real-time connector monitoring dashboard
"""
import json
import time
import threading
from flask import Flask, render_template_string, jsonify
from datetime import datetime

app = Flask(__name__)

# Global state
dashboard_state = {
    'tasks': {},
    'last_update': None,
}

HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>SocialStream Connector Dashboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            background: linear-gradient(135deg, #34d399 0%, #1d4ed8 100%);
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
            border-radius: 15px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.1);
            margin-bottom: 30px;
        }
        h1 {
            color: #2d3748;
            font-size: 32px;
            margin-bottom: 10px;
        }
        .subtitle {
            color: #718096;
            font-size: 16px;
        }
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .stat-card {
            background: white;
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.1);
        }
        .stat-label {
            color: #718096;
            font-size: 14px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 10px;
        }
        .stat-value {
            color: #2d3748;
            font-size: 36px;
            font-weight: bold;
        }
        .stat-trend {
            color: #48bb78;
            font-size: 14px;
            margin-top: 8px;
        }
        .tasks-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
            gap: 20px;
        }
        .task-card {
            background: white;
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.1);
        }
        .task-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
            padding-bottom: 15px;
            border-bottom: 2px solid #e2e8f0;
        }
        .task-title {
            font-size: 18px;
            font-weight: 600;
            color: #2d3748;
        }
        .status-badge {
            padding: 6px 12px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: 600;
            text-transform: uppercase;
        }
        .status-running {
            background: #c6f6d5;
            color: #22543d;
        }
        .metric-row {
            display: flex;
            justify-content: space-between;
            padding: 12px 0;
            border-bottom: 1px solid #f7fafc;
        }
        .metric-label {
            color: #718096;
            font-size: 14px;
        }
        .metric-value {
            color: #2d3748;
            font-weight: 600;
            font-size: 14px;
        }
        .platform-twitter { border-left: 4px solid #1da1f2; }
        .platform-linkedin { border-left: 4px solid #0077b5; }
        .rate-limit-bar {
            width: 100%;
            height: 8px;
            background: #e2e8f0;
            border-radius: 4px;
            overflow: hidden;
            margin-top: 10px;
        }
        .rate-limit-fill {
            height: 100%;
            background: linear-gradient(90deg, #48bb78, #38a169);
            transition: width 0.3s ease;
        }
        .timestamp {
            text-align: center;
            color: white;
            font-size: 14px;
            margin-top: 20px;
            opacity: 0.9;
        }
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }
        .live-indicator {
            display: inline-block;
            width: 8px;
            height: 8px;
            background: #48bb78;
            border-radius: 50%;
            margin-right: 8px;
            animation: pulse 2s infinite;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1><span class="live-indicator"></span>SocialStream Connector Dashboard</h1>
            <div class="subtitle">Real-time monitoring of social media connector tasks</div>
        </div>
        
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-label">Total Records Produced</div>
                <div class="stat-value" id="total-records">0</div>
                <div class="stat-trend">↑ Live streaming</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">API Calls Made</div>
                <div class="stat-value" id="total-api-calls">0</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Active Tasks</div>
                <div class="stat-value" id="active-tasks">0</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Rate Limit Waits</div>
                <div class="stat-value" id="rate-limit-waits">0</div>
            </div>
        </div>
        
        <div class="tasks-grid" id="tasks-container">
        </div>
        
        <div class="timestamp">
            Last updated: <span id="last-update">Never</span>
        </div>
    </div>
    
    <script>
        function updateDashboard() {
            fetch('/api/metrics')
                .then(response => response.json())
                .then(data => {
                    // Update summary stats
                    document.getElementById('total-records').textContent = 
                        data.summary.total_records.toLocaleString();
                    document.getElementById('total-api-calls').textContent = 
                        data.summary.total_api_calls.toLocaleString();
                    document.getElementById('active-tasks').textContent = 
                        data.summary.active_tasks;
                    document.getElementById('rate-limit-waits').textContent = 
                        data.summary.rate_limit_waits;
                    
                    // Update tasks
                    const tasksContainer = document.getElementById('tasks-container');
                    tasksContainer.innerHTML = '';
                    
                    data.tasks.forEach(task => {
                        const platformClass = `platform-${task.platform}`;
                        const tokenPercent = Math.min(100, (task.available_tokens / 100) * 100);
                        
                        const taskCard = `
                            <div class="task-card ${platformClass}">
                                <div class="task-header">
                                    <div class="task-title">
                                        Task ${task.task_id} - ${task.platform.toUpperCase()}
                                    </div>
                                    <span class="status-badge status-running">RUNNING</span>
                                </div>
                                <div class="metric-row">
                                    <span class="metric-label">Account ID</span>
                                    <span class="metric-value">${task.account_id}</span>
                                </div>
                                <div class="metric-row">
                                    <span class="metric-label">Records Produced</span>
                                    <span class="metric-value">${task.metrics.records_produced.toLocaleString()}</span>
                                </div>
                                <div class="metric-row">
                                    <span class="metric-label">API Calls</span>
                                    <span class="metric-value">${task.metrics.api_calls}</span>
                                </div>
                                <div class="metric-row">
                                    <span class="metric-label">Current Offset</span>
                                    <span class="metric-value">${task.offset || 'None'}</span>
                                </div>
                                <div class="metric-row">
                                    <span class="metric-label">Rate Limit Tokens</span>
                                    <span class="metric-value">${task.available_tokens.toFixed(1)}</span>
                                </div>
                                <div class="rate-limit-bar">
                                    <div class="rate-limit-fill" style="width: ${tokenPercent}%"></div>
                                </div>
                            </div>
                        `;
                        tasksContainer.innerHTML += taskCard;
                    });
                    
                    // Update timestamp
                    document.getElementById('last-update').textContent = 
                        new Date().toLocaleTimeString();
                });
        }
        
        // Update every 2 seconds
        updateDashboard();
        setInterval(updateDashboard, 2000);
    </script>
</body>
</html>
'''

@app.route('/')
def dashboard():
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/metrics')
def metrics():
    tasks = list(dashboard_state['tasks'].values())
    
    # Calculate summary
    summary = {
        'total_records': sum(t['metrics']['records_produced'] for t in tasks),
        'total_api_calls': sum(t['metrics']['api_calls'] for t in tasks),
        'active_tasks': len(tasks),
        'rate_limit_waits': sum(t['metrics']['rate_limit_waits'] for t in tasks),
    }
    
    return jsonify({
        'summary': summary,
        'tasks': tasks,
        'timestamp': datetime.now().isoformat(),
    })

def update_task_metrics(task_id: int, metrics: dict):
    """Update metrics for a task"""
    dashboard_state['tasks'][task_id] = metrics
    dashboard_state['last_update'] = datetime.now()

def start_dashboard(port: int = 5000):
    """Start dashboard server"""
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
