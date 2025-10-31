from flask import Flask, render_template, jsonify
import requests
import json
import time
import struct
import socket
from datetime import datetime
import threading
from collections import defaultdict
import plotly.graph_objs as go
import plotly.utils

app = Flask(__name__, template_folder='../ui/templates', static_folder='../ui/static')

class ConnectClusterMonitor:
    def __init__(self):
        # Use container IPs directly when running in Docker
        import os
        if os.path.exists('/.dockerenv'):
            # Running in Docker - scan network for worker containers
            worker_ips = []
            
            # Get network base from our own IP
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.connect(("8.8.8.8", 80))
                our_ip = s.getsockname()[0]
                s.close()
                network_base = '.'.join(our_ip.split('.')[:3])
                
                # Scan network for containers listening on port 8083
                for last_octet in range(2, 15):
                    test_ip = f'{network_base}.{last_octet}'
                    if test_ip == our_ip:
                        continue
                    try:
                        test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        test_socket.settimeout(0.3)
                        result = test_socket.connect_ex((test_ip, 8083))
                        test_socket.close()
                        if result == 0:  # Connection successful
                            if test_ip not in worker_ips:
                                worker_ips.append(test_ip)
                                if len(worker_ips) >= 3:
                                    break
                    except:
                        continue
            except:
                pass
            
            # Fallback to known IPs if scan didn't find enough
            if len(worker_ips) < 3:
                # Try to get network from gateway
                try:
                    with open('/proc/net/route') as f:
                        for line in f:
                            fields = line.strip().split()
                            if len(fields) >= 3 and fields[1] == '00000000':
                                gateway_hex = fields[2]
                                gateway_ip = socket.inet_ntoa(struct.pack('<L', int(gateway_hex, 16)))
                                network_base = '.'.join(gateway_ip.split('.')[:3])
                                
                                # Try known worker IPs in this network
                                for ip_suffix in ['5', '6', '7']:
                                    fallback_ip = f'{network_base}.{ip_suffix}'
                                    if fallback_ip not in worker_ips:
                                        worker_ips.append(fallback_ip)
                                    if len(worker_ips) >= 3:
                                        break
                                break
                except:
                    worker_ips = ['172.18.0.5', '172.18.0.6', '172.18.0.7']
            
            # Ensure we have 3 IPs
            while len(worker_ips) < 3:
                worker_ips.append(f'172.18.0.{5 + len(worker_ips)}')
            
            worker_ips = worker_ips[:3]
            
            # Use container IPs with internal port 8083
            self.worker_urls = [
                f'http://{worker_ips[0]}:8083',
                f'http://{worker_ips[1]}:8083',
                f'http://{worker_ips[2]}:8083'
            ]
        else:
            # Running on host
            self.worker_urls = [
                'http://localhost:8083',
                'http://localhost:8084', 
                'http://localhost:8085'
            ]
        self.metrics = {
            'workers': {},
            'connectors': {},
            'tasks': {},
            'timeline': []
        }
        self.monitoring = True
        
    def start_monitoring(self):
        def monitor_loop():
            while self.monitoring:
                self.collect_metrics()
                time.sleep(5)
        
        thread = threading.Thread(target=monitor_loop, daemon=True)
        thread.start()
    
    def collect_metrics(self):
        timestamp = datetime.now()
        
        for i, url in enumerate(self.worker_urls):
            worker_id = f"worker-{i+1}"
            try:
                # Check worker health - use /connectors endpoint (root returns 404)
                response = requests.get(f"{url}/connectors", timeout=2)
                # 200 = healthy, 404/other errors = worker might be starting
                # Connection refused = unreachable
                if response.status_code == 200:
                    status = 'healthy'
                    connectors = response.json()
                    # Get connector statuses
                    for connector in connectors:
                        try:
                            status_resp = requests.get(f"{url}/connectors/{connector}/status", timeout=2)
                            if status_resp.status_code == 200:
                                self.metrics['connectors'][connector] = status_resp.json()
                        except:
                            pass
                elif response.status_code == 404:
                    # Service is up but no connectors endpoint (unlikely, but treat as healthy)
                    status = 'healthy'
                else:
                    status = 'unhealthy'
                
                self.metrics['workers'][worker_id] = {
                    'status': status,
                    'url': url,
                    'last_seen': timestamp.isoformat()
                }
                            
            except Exception as e:
                self.metrics['workers'][worker_id] = {
                    'status': 'unreachable',
                    'url': url,
                    'error': str(e),
                    'last_seen': timestamp.isoformat()
                }
        
        # Add timeline entry
        self.metrics['timeline'].append({
            'timestamp': timestamp.isoformat(),
            'active_workers': len([w for w in self.metrics['workers'].values() if w['status'] == 'healthy']),
            'total_connectors': len(self.metrics['connectors'])
        })
        
        # Keep only last 100 entries
        if len(self.metrics['timeline']) > 100:
            self.metrics['timeline'] = self.metrics['timeline'][-100:]

# Global monitor instance
monitor = ConnectClusterMonitor()

@app.route('/')
def dashboard():
    return render_template('dashboard.html')

@app.route('/api/metrics')
def get_metrics():
    return jsonify(monitor.metrics)

@app.route('/api/cluster-health')
def cluster_health():
    healthy_workers = len([w for w in monitor.metrics['workers'].values() if w['status'] == 'healthy'])
    total_workers = len(monitor.metrics['workers'])
    
    return jsonify({
        'health_percentage': (healthy_workers / max(total_workers, 1)) * 100,
        'healthy_workers': healthy_workers,
        'total_workers': total_workers,
        'connectors_count': len(monitor.metrics['connectors'])
    })

@app.route('/api/timeline-chart')
def timeline_chart():
    timeline = monitor.metrics['timeline']
    if not timeline:
        return jsonify({})
    
    timestamps = [entry['timestamp'] for entry in timeline]
    workers = [entry['active_workers'] for entry in timeline]
    connectors = [entry['total_connectors'] for entry in timeline]
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=timestamps, 
        y=workers, 
        name='Active Workers', 
        line=dict(color='#60a5fa', width=2),
        mode='lines+markers',
        marker=dict(size=4, color='#60a5fa')
    ))
    fig.add_trace(go.Scatter(
        x=timestamps, 
        y=connectors, 
        name='Total Connectors', 
        line=dict(color='#10b981', width=2),
        mode='lines+markers',
        marker=dict(size=4, color='#10b981')
    ))
    
    fig.update_layout(
        title=dict(
            text='Connect Cluster Timeline',
            font=dict(size=18, color='#f1f5f9')
        ),
        xaxis=dict(
            title='Time',
            titlefont=dict(color='#94a3b8'),
            tickfont=dict(color='#94a3b8'),
            gridcolor='rgba(148, 163, 184, 0.1)',
            linecolor='rgba(148, 163, 184, 0.2)'
        ),
        yaxis=dict(
            title='Count',
            titlefont=dict(color='#94a3b8'),
            tickfont=dict(color='#94a3b8'),
            gridcolor='rgba(148, 163, 184, 0.1)',
            linecolor='rgba(148, 163, 184, 0.2)'
        ),
        plot_bgcolor='rgba(15, 23, 42, 0.5)',
        paper_bgcolor='rgba(30, 41, 59, 0.8)',
        font=dict(family='Inter, sans-serif'),
        height=400,
        margin=dict(l=50, r=20, t=50, b=50),
        legend=dict(
            bgcolor='rgba(30, 41, 59, 0.8)',
            bordercolor='rgba(148, 163, 184, 0.2)',
            borderwidth=1,
            font=dict(color='#e2e8f0')
        )
    )
    
    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

if __name__ == '__main__':
    monitor.start_monitoring()
    app.run(host='0.0.0.0', port=5000, debug=True)
