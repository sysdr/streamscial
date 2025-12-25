"""Real-time TLS security monitoring dashboard"""
from flask import Flask, render_template, jsonify
import subprocess
import json
import re
import os
from datetime import datetime
import threading
import time

app = Flask(__name__)

# Get the project root directory (parent of monitoring directory)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CERTS_DIR = os.path.join(PROJECT_ROOT, 'certs')

class SecurityMonitor:
    def __init__(self):
        self.metrics = {
            'tls_connections': 0,
            'handshake_failures': 0,
            'certificate_expiry_days': 0,
            'encrypted_throughput': 0,
            'cipher_suites': {},
            'connection_timeline': []
        }
        self.running = False
    
    def check_certificate_expiry(self):
        """Check certificate expiration"""
        try:
            cert_path = os.path.join(CERTS_DIR, 'broker-cert.pem')
            result = subprocess.run(
                ['openssl', 'x509', '-in', cert_path, '-noout', '-enddate'],
                capture_output=True, text=True, check=True
            )
            expiry_str = result.stdout.strip().split('=')[1]
            expiry_date = datetime.strptime(expiry_str, '%b %d %H:%M:%S %Y %Z')
            days_remaining = (expiry_date - datetime.now()).days
            self.metrics['certificate_expiry_days'] = days_remaining
        except Exception as e:
            print(f"Error checking certificate: {e}")
    
    def monitor_connections(self):
        """Monitor TLS connections"""
        while self.running:
            try:
                # Simulate connection monitoring
                self.metrics['tls_connections'] += 1
                self.metrics['encrypted_throughput'] = self.metrics['tls_connections'] * 1024
                
                # Track connection timeline
                self.metrics['connection_timeline'].append({
                    'timestamp': datetime.now().isoformat(),
                    'connections': self.metrics['tls_connections']
                })
                
                # Keep last 50 datapoints
                if len(self.metrics['connection_timeline']) > 50:
                    self.metrics['connection_timeline'].pop(0)
                
            except Exception as e:
                print(f"Monitoring error: {e}")
            
            time.sleep(2)
    
    def start(self):
        """Start monitoring"""
        self.running = True
        self.check_certificate_expiry()
        monitor_thread = threading.Thread(target=self.monitor_connections, daemon=True)
        monitor_thread.start()
    
    def stop(self):
        """Stop monitoring"""
        self.running = False

monitor = SecurityMonitor()

@app.route('/')
def dashboard():
    """Serve dashboard"""
    return render_template('dashboard.html')

@app.route('/api/metrics')
def get_metrics():
    """Return current metrics"""
    return jsonify(monitor.metrics)

if __name__ == '__main__':
    monitor.start()
    print("Security Dashboard running at http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=False)
