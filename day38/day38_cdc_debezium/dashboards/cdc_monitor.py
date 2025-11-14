"""Real-time CDC monitoring dashboard"""

import os
from datetime import datetime

from dotenv import load_dotenv
from flask import Flask, render_template, jsonify
from flask_cors import CORS
import psycopg2
import requests

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5433"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "streamsocial")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin123")
CONNECT_REST_URL = os.getenv("CONNECT_REST_URL", "http://localhost:8083")
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "5001"))

app = Flask(__name__)
CORS(app)


class CDCMonitor:
    def __init__(self):
        self.db_conn = None
        self.connect_db()
    
    def connect_db(self):
        """Connect to PostgreSQL"""
        try:
            self.db_conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            self.db_conn.autocommit = True
        except Exception as e:
            print(f"DB connection error: {e}")
            self.db_conn = None
    
    def ensure_connection(self):
        if not self.db_conn or self.db_conn.closed:
            self.connect_db()
        return self.db_conn
    
    def get_replication_slot_lag(self):
        """Get replication slot lag information"""
        try:
            conn = self.ensure_connection()
            if not conn:
                return {'error': 'Database unavailable'}
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    slot_name,
                    active,
                    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as lag,
                    pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) as lag_bytes
                FROM pg_replication_slots
                WHERE slot_name = 'debezium_user_profiles'
            """)
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                return {
                    'slot_name': result[0],
                    'active': result[1],
                    'lag': result[2],
                    'lag_bytes': result[3]
                }
            return {'error': 'Replication slot not found'}
        except Exception as e:
            return {'error': str(e)}
    
    def get_table_stats(self):
        """Get user_profiles table statistics"""
        try:
            conn = self.ensure_connection()
            if not conn:
                return {'error': 'Database unavailable'}
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_users,
                    COUNT(*) FILTER (WHERE is_verified = TRUE) as verified_users,
                    AVG(follower_count)::int as avg_followers,
                    MAX(updated_at) as last_update
                FROM user_profiles
            """)
            result = cursor.fetchone()
            cursor.close()
            
            return {
                'total_users': result[0],
                'verified_users': result[1],
                'avg_followers': result[2],
                'last_update': result[3].isoformat() if result[3] else None
            }
        except Exception as e:
            return {'error': str(e)}
    
    def get_connector_status(self):
        """Get Debezium connector status from Kafka Connect"""
        try:
            response = requests.get(
                f'{CONNECT_REST_URL}/connectors/user-profiles-connector/status',
                timeout=5
            )
            if response.status_code == 200:
                return response.json()
            return {'error': 'Connector not found or not running'}
        except Exception as e:
            return {'error': str(e)}


monitor = CDCMonitor()


@app.route('/')
def index():
    """Render dashboard"""
    return render_template('cdc_dashboard.html')


@app.route('/api/metrics')
def metrics():
    """Get all CDC metrics"""
    return jsonify({
        'replication_slot': monitor.get_replication_slot_lag(),
        'table_stats': monitor.get_table_stats(),
        'connector_status': monitor.get_connector_status(),
        'timestamp': datetime.now().isoformat()
    })


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=DASHBOARD_PORT, debug=False)
