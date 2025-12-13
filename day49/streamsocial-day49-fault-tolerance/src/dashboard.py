from flask import Flask, jsonify, render_template_string
from flask_cors import CORS
from prometheus_client import Counter, Gauge, generate_latest
import psutil
import json
import time
from rocksdict import Rdict

app = Flask(__name__)
CORS(app)

# Prometheus metrics
events_processed = Counter('events_processed_total', 'Total events processed')
state_updates = Counter('state_updates_total', 'Total state updates')
recovery_time = Gauge('recovery_time_ms', 'Last recovery time in milliseconds')

HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>StreamSocial - Fault Tolerance Monitor</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif;
            background: #f5f7fa;
            color: #1a202c;
            padding: 20px;
            min-height: 100vh;
        }
        .container {
            max-width: 1400px;
            margin: 0 auto;
        }
        h1 {
            color: #1a202c;
            text-align: center;
            margin-bottom: 40px;
            font-size: 2.5em;
            font-weight: 600;
            letter-spacing: -0.5px;
        }
        .metrics-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .metric-card {
            background: #ffffff;
            border-radius: 8px;
            padding: 24px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.08), 0 1px 2px rgba(0,0,0,0.06);
            transition: all 0.2s ease;
            border: 1px solid #e2e8f0;
        }
        .metric-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 6px rgba(0,0,0,0.1), 0 2px 4px rgba(0,0,0,0.08);
            border-color: #cbd5e0;
        }
        .metric-label {
            font-size: 0.85em;
            color: #64748b;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 12px;
            font-weight: 500;
        }
        .metric-value {
            font-size: 2.25em;
            font-weight: 700;
            color: #0f172a;
            line-height: 1.2;
        }
        .status-indicator {
            display: inline-block;
            width: 10px;
            height: 10px;
            border-radius: 50%;
            margin-right: 8px;
            animation: pulse 2s infinite;
        }
        .status-running { background-color: #10b981; }
        .status-restoring { background-color: #f59e0b; }
        .status-error { background-color: #ef4444; }
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.6; }
        }
        .leaderboard {
            background: #ffffff;
            border-radius: 8px;
            padding: 28px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.08), 0 1px 2px rgba(0,0,0,0.06);
            margin-bottom: 30px;
            border: 1px solid #e2e8f0;
        }
        .leaderboard h2 {
            margin-bottom: 24px;
            color: #0f172a;
            font-size: 1.5em;
            font-weight: 600;
        }
        .user-row {
            display: grid;
            grid-template-columns: 50px 2fr 1fr 1fr 1fr 1fr;
            padding: 16px;
            border-bottom: 1px solid #e2e8f0;
            align-items: center;
            transition: background-color 0.15s ease;
        }
        .user-row:last-child {
            border-bottom: none;
        }
        .user-row:hover {
            background: #f8fafc;
        }
        .user-rank {
            font-weight: 600;
            font-size: 1.1em;
            color: #14b8a6;
        }
        .score-bar {
            height: 6px;
            background: linear-gradient(90deg, #14b8a6, #06b6d4);
            border-radius: 3px;
            margin-top: 6px;
        }
        .chart-container {
            background: #ffffff;
            border-radius: 8px;
            padding: 28px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.08), 0 1px 2px rgba(0,0,0,0.06);
            height: 400px;
            border: 1px solid #e2e8f0;
        }
        .action-buttons {
            display: flex;
            gap: 15px;
            justify-content: center;
            margin: 30px 0;
        }
        .btn {
            padding: 12px 30px;
            border: none;
            border-radius: 6px;
            font-size: 0.95em;
            cursor: pointer;
            transition: all 0.2s ease;
            box-shadow: 0 1px 2px rgba(0,0,0,0.1);
            font-weight: 500;
        }
        .btn-primary {
            background: #14b8a6;
            color: white;
        }
        .btn-primary:hover {
            background: #0d9488;
            transform: translateY(-1px);
            box-shadow: 0 2px 4px rgba(0,0,0,0.12);
        }
        .btn-danger {
            background: #ef4444;
            color: white;
        }
        .btn-danger:hover {
            background: #dc2626;
            transform: translateY(-1px);
            box-shadow: 0 2px 4px rgba(0,0,0,0.12);
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🛡️ StreamSocial Fault Tolerance Monitor</h1>
        
        <div class="metrics-grid">
            <div class="metric-card">
                <div class="metric-label">
                    <span class="status-indicator status-running"></span>
                    System Status
                </div>
                <div class="metric-value" id="status">RUNNING</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-label">Events Processed</div>
                <div class="metric-value" id="events">0</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-label">State Updates</div>
                <div class="metric-value" id="updates">0</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-label">Recovery Time</div>
                <div class="metric-value" id="recovery">0ms</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-label">Throughput</div>
                <div class="metric-value" id="throughput">0/sec</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-label">Active Users</div>
                <div class="metric-value" id="users">0</div>
            </div>
        </div>
        
        <div class="leaderboard">
            <h2>📊 Top Engagement Scores</h2>
            <div class="user-row" style="font-weight: 600; border-bottom: 2px solid #cbd5e0; color: #475569;">
                <div>Rank</div>
                <div>User ID</div>
                <div>Score</div>
                <div>Likes</div>
                <div>Comments</div>
                <div>Shares</div>
            </div>
            <div id="leaderboard"></div>
        </div>
        
        <div class="chart-container">
            <canvas id="throughputChart"></canvas>
        </div>
    </div>
    
    <script>
        const ctx = document.getElementById('throughputChart').getContext('2d');
        const chart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: 'Events/sec',
                    data: [],
                    borderColor: '#14b8a6',
                    backgroundColor: 'rgba(20, 184, 166, 0.1)',
                    tension: 0.4,
                    borderWidth: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: { beginAtZero: true }
                }
            }
        });
        
        let lastEventCount = 0;
        
        async function updateMetrics() {
            try {
                const response = await fetch('/api/metrics');
                const data = await response.json();
                
                document.getElementById('status').textContent = data.is_restoring ? 'RESTORING' : 'RUNNING';
                document.getElementById('events').textContent = data.events_processed.toLocaleString();
                document.getElementById('updates').textContent = data.state_updates.toLocaleString();
                document.getElementById('recovery').textContent = data.recovery_time_ms + 'ms';
                document.getElementById('users').textContent = data.active_users;
                
                const throughput = Math.max(0, data.events_processed - lastEventCount);
                document.getElementById('throughput').textContent = throughput + '/sec';
                lastEventCount = data.events_processed;
                
                // Update chart
                const now = new Date().toLocaleTimeString();
                chart.data.labels.push(now);
                chart.data.datasets[0].data.push(throughput);
                if (chart.data.labels.length > 30) {
                    chart.data.labels.shift();
                    chart.data.datasets[0].data.shift();
                }
                chart.update();
                
            } catch (error) {
                console.error('Failed to fetch metrics:', error);
            }
        }
        
        async function updateLeaderboard() {
            try {
                const response = await fetch('/api/leaderboard');
                const users = await response.json();
                
                const leaderboard = document.getElementById('leaderboard');
                leaderboard.innerHTML = users.map((user, index) => `
                    <div class="user-row">
                        <div class="user-rank">#${index + 1}</div>
                        <div>
                            ${user.user_id}
                            <div class="score-bar" style="width: ${Math.min(100, user.score)}%"></div>
                        </div>
                        <div>${user.score.toFixed(1)}</div>
                        <div>${user.likes}</div>
                        <div>${user.comments}</div>
                        <div>${user.shares}</div>
                    </div>
                `).join('');
                
            } catch (error) {
                console.error('Failed to fetch leaderboard:', error);
            }
        }
        
        setInterval(updateMetrics, 1000);
        setInterval(updateLeaderboard, 2000);
        updateMetrics();
        updateLeaderboard();
    </script>
</body>
</html>
'''

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/metrics')
def get_metrics():
    try:
        state_store = Rdict('/tmp/kafka-streams-state/engagement_scores.db')
        keys = list(state_store.keys())
        active_users = len(keys)
        
        # Calculate total events processed from state store
        total_events = 0
        total_updates = 0
        for key in keys:
            try:
                # Handle both bytes and string keys
                if isinstance(key, bytes):
                    user_id = key.decode('utf-8')
                    state_data = state_store[key]
                else:
                    user_id = key
                    state_data = state_store[key]
                
                # Handle both bytes and string values
                if isinstance(state_data, bytes):
                    state = json.loads(state_data.decode('utf-8'))
                else:
                    state = json.loads(state_data) if isinstance(state_data, str) else state_data
                
                # Sum up all interactions (likes + comments + shares = events processed)
                total_events += state.get('likes', 0) + state.get('comments', 0) + state.get('shares', 0)
                total_updates += 1
            except Exception as e:
                continue
        
        state_store.close()
    except Exception as e:
        active_users = 0
        total_events = 0
        total_updates = 0
    
    # Read metrics from processor (in production, use shared metrics)
    return jsonify({
        'events_processed': total_events,
        'state_updates': total_updates,
        'recovery_time_ms': int(recovery_time._value.get()),
        'is_restoring': False,
        'active_users': active_users
    })

@app.route('/api/leaderboard')
def get_leaderboard():
    try:
        state_store = Rdict('/tmp/kafka-streams-state/engagement_scores.db')
        scores = []
        
        for key in state_store.keys():
            try:
                # Handle both bytes and string keys
                if isinstance(key, bytes):
                    user_id = key.decode('utf-8')
                    state_data = state_store[key]
                else:
                    user_id = key
                    state_data = state_store[key]
                
                # Handle both bytes and string values
                if isinstance(state_data, bytes):
                    state = json.loads(state_data.decode('utf-8'))
                else:
                    state = json.loads(state_data) if isinstance(state_data, str) else state_data
                
                scores.append({
                    'user_id': user_id,
                    'score': state.get('score', 0),
                    'likes': state.get('likes', 0),
                    'comments': state.get('comments', 0),
                    'shares': state.get('shares', 0)
                })
            except Exception as e:
                continue
        
        state_store.close()
        scores.sort(key=lambda x: x['score'], reverse=True)
        return jsonify(scores[:10])
        
    except Exception as e:
        import traceback
        print(f"Error in leaderboard: {e}")
        print(traceback.format_exc())
        return jsonify([])

@app.route('/metrics')
def prometheus_metrics():
    return generate_latest()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
