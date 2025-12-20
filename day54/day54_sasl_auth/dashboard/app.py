"""Web dashboard for SASL authentication monitoring."""

from flask import Flask, render_template, jsonify
from flask_cors import CORS
import sys
import time
import random
import threading
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / 'src'))

from credential_manager import CredentialManager
from auth_monitor import AuthenticationMonitor

app = Flask(__name__)
CORS(app)

# Initialize managers
credential_mgr = CredentialManager()
auth_monitor = AuthenticationMonitor()

# Background monitoring thread
monitoring_active = True

def background_monitor():
    """Background thread that monitors authentication events."""
    # Wait a bit for Flask to start
    time.sleep(2)
    
    # Ensure we have some test users
    users = credential_mgr.get_all_users()
    if not users:
        # Create default test users if none exist
        try:
            credential_mgr.create_user("producer_service", "prod_secret_2024", "SCRAM-SHA-512")
            credential_mgr.create_user("analytics_service", "analytics_secret_2024", "SCRAM-SHA-512")
            credential_mgr.create_user("legacy_partner", "partner_secret_2024", "PLAIN")
            credential_mgr.create_user("consumer_service", "consumer_secret_2024", "PLAIN")
            users = credential_mgr.get_all_users()
        except Exception as e:
            print(f"Warning: Could not create test users: {e}")
    
    # Known test credentials (matching demo.py)
    known_credentials = {
        "producer_service": ("prod_secret_2024", "SCRAM-SHA-512"),
        "analytics_service": ("analytics_secret_2024", "SCRAM-SHA-512"),
        "legacy_partner": ("partner_secret_2024", "PLAIN"),
        "consumer_service": ("consumer_secret_2024", "PLAIN"),
    }
    
    # Build test users list with known passwords
    test_users = {}
    for user in users:
        username = user['username']
        mechanism = user['mechanism']
        if username in known_credentials:
            test_users[username] = known_credentials[username]
        else:
            # Try to infer password from username pattern
            if 'producer' in username.lower():
                test_users[username] = ('prod_secret_2024', mechanism)
            elif 'analytics' in username.lower():
                test_users[username] = ('analytics_secret_2024', mechanism)
            elif 'partner' in username.lower():
                test_users[username] = ('partner_secret_2024', mechanism)
            elif 'consumer' in username.lower():
                test_users[username] = ('consumer_secret_2024', mechanism)
    
    # Also try real Kafka authentication if available
    try:
        from sasl_producer import SALSProducer
        from sasl_consumer import SALSConsumer
        kafka_available = True
    except ImportError:
        kafka_available = False
    
    print("🔍 Background authentication monitor started")
    
    attempt_count = 0
    while monitoring_active:
        try:
            attempt_count += 1
            
            # Try real Kafka authentication every 5th attempt
            if kafka_available and attempt_count % 5 == 0 and test_users:
                username = random.choice(list(test_users.keys()))
                password, mechanism = test_users[username]
                
                try:
                    # Try to create a producer/consumer to test real Kafka auth
                    start = time.time()
                    if mechanism == "SCRAM-SHA-512":
                        producer = SALSProducer('localhost:9092', username, password, mechanism)
                        producer.close()
                        result = True
                    else:
                        consumer = SALSConsumer('localhost:9092', 'monitor-group', username, password, mechanism)
                        consumer.close()
                        result = True
                    latency = (time.time() - start) * 1000
                    auth_monitor.record_auth_attempt(username, mechanism, result, latency)
                except Exception as e:
                    # Authentication failed
                    latency = (time.time() - start) * 1000
                    auth_monitor.record_auth_attempt(username, mechanism, False, latency)
            
            # Regular credential validation tests
            if test_users:
                username = random.choice(list(test_users.keys()))
                password, mechanism = test_users[username]
                
                # 85% success rate, 15% failure (wrong password)
                should_succeed = random.random() > 0.15
                test_password = password if should_succeed else "wrong_password"
                
                # Perform authentication
                start = time.time()
                result = credential_mgr.validate_credentials(username, test_password)
                latency = (time.time() - start) * 1000
                
                # Record in monitor
                auth_monitor.record_auth_attempt(username, mechanism, result, latency)
            
            # Sleep for 2-5 seconds before next attempt
            time.sleep(random.uniform(2, 5))
            
        except Exception as e:
            print(f"Error in background monitor: {e}")
            time.sleep(5)

# Start background monitoring thread
monitor_thread = threading.Thread(target=background_monitor, daemon=True)
monitor_thread.start()


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/stats')
def get_stats():
    """Get authentication statistics."""
    return jsonify(auth_monitor.get_stats())


@app.route('/api/users')
def get_users():
    """Get all users."""
    return jsonify(credential_mgr.get_all_users())


@app.route('/api/audit-log')
def get_audit_log():
    """Get recent audit log entries."""
    return jsonify(credential_mgr.get_audit_log(limit=50))


@app.route('/api/rotation-needed')
def get_rotation_needed():
    """Get users needing password rotation."""
    return jsonify(credential_mgr.get_users_needing_rotation())


@app.route('/api/failure-rates')
def get_failure_rates():
    """Get users with high auth failure rates."""
    return jsonify(auth_monitor.get_failure_rate_by_user())


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8054, debug=True)
