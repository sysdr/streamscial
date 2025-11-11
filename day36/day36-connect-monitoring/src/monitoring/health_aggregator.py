import time
import yaml
import psycopg2
from psycopg2 import OperationalError
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HealthAggregator:
    def __init__(self, config_path, db_config):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        self.db_config = db_config
        self.db_enabled = False

        if self.db_config:
            try:
                self.setup_database()
                self.db_enabled = True
            except OperationalError as exc:
                logger.warning(
                    "Database unavailable, running in in-memory mode: %s",
                    exc,
                )
        
    def setup_database(self):
        """Initialize database schema"""
        conn = psycopg2.connect(**self.db_config)
        cur = conn.cursor()
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS connector_health (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL,
                connector_name VARCHAR(100) NOT NULL,
                health_score FLOAT NOT NULL,
                status VARCHAR(20) NOT NULL,
                error_rate FLOAT,
                throughput FLOAT,
                lag_seconds FLOAT,
                tasks_running INTEGER,
                tasks_total INTEGER
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL,
                connector_name VARCHAR(100),
                alert_type VARCHAR(50) NOT NULL,
                severity VARCHAR(10) NOT NULL,
                message TEXT NOT NULL,
                resolved BOOLEAN DEFAULT FALSE
            )
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_health_timestamp 
            ON connector_health(timestamp DESC)
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_alerts_timestamp 
            ON alerts(timestamp DESC)
        """)
        
        conn.commit()
        cur.close()
        conn.close()
        
    def calculate_health_score(self, metrics):
        """Calculate health score based on multiple factors"""
        weights = self.config['health_score']
        
        error_score = (1 - metrics['error_rate']) * weights['error_weight']
        task_score = (metrics['tasks_running'] / metrics['tasks_total']) * weights['task_weight']
        
        lag_minutes = metrics['lag_seconds'] / 60
        lag_score = (1 / (1 + lag_minutes/30)) * weights['lag_weight']
        
        total_score = (error_score + task_score + lag_score) * 100
        return min(100, max(0, total_score))
        
    def determine_status(self, health_score, metrics):
        """Determine connector status"""
        if health_score >= 90 and metrics['error_rate'] <= 0.001:
            return 'HEALTHY'
        elif health_score >= 70:
            return 'DEGRADED'
        elif health_score >= 50:
            return 'CRITICAL'
        else:
            return 'FAILING'
            
    def check_alerts(self, connector_name, metrics, health_score):
        """Check and trigger alerts"""
        alerts = []
        
        for alert_config in self.config['alerts']:
            condition = alert_config['condition']
            
            if 'error_rate' in condition and metrics['error_rate'] > 0.05:
                alerts.append({
                    'type': alert_config['name'],
                    'severity': alert_config['severity'],
                    'message': f"High error rate: {metrics['error_rate']:.2%}"
                })
            elif 'failed_tasks' in condition and metrics['tasks_running'] < metrics['tasks_total']:
                alerts.append({
                    'type': alert_config['name'],
                    'severity': alert_config['severity'],
                    'message': f"Tasks failed: {metrics['tasks_total'] - metrics['tasks_running']}"
                })
            elif 'lag_seconds' in condition and metrics['lag_seconds'] > 600:
                alerts.append({
                    'type': alert_config['name'],
                    'severity': alert_config['severity'],
                    'message': f"High lag: {metrics['lag_seconds']}s"
                })
                
        return alerts
        
    def store_health_data(self, connector_name, metrics, health_score, status):
        """Store health data in database"""
        if not self.db_enabled:
            logger.debug("Skipping health data storage; database disabled.")
            return

        conn = psycopg2.connect(**self.db_config)
        cur = conn.cursor()
        
        cur.execute("""
            INSERT INTO connector_health 
            (timestamp, connector_name, health_score, status, error_rate, 
             throughput, lag_seconds, tasks_running, tasks_total)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            datetime.now(),
            connector_name,
            health_score,
            status,
            metrics['error_rate'],
            metrics['throughput'],
            metrics['lag_seconds'],
            metrics['tasks_running'],
            metrics['tasks_total']
        ))
        
        conn.commit()
        cur.close()
        conn.close()
        
    def store_alerts(self, connector_name, alerts):
        """Store alerts in database"""
        if not alerts:
            return
            
        if not self.db_enabled:
            if alerts:
                logger.debug("Skipping alert persistence; database disabled.")
            return

        if not alerts:
            return

        conn = psycopg2.connect(**self.db_config)
        cur = conn.cursor()
        
        for alert in alerts:
            cur.execute("""
                INSERT INTO alerts 
                (timestamp, connector_name, alert_type, severity, message)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                datetime.now(),
                connector_name,
                alert['type'],
                alert['severity'],
                alert['message']
            ))
        
        conn.commit()
        cur.close()
        conn.close()
        
    def get_historical_data(self, connector_name, hours=24):
        """Retrieve historical health data"""
        if not self.db_enabled:
            logger.debug("Historical data requested but database disabled; returning empty result.")
            return []

        conn = psycopg2.connect(**self.db_config)
        cur = conn.cursor()
        
        cutoff = datetime.now() - timedelta(hours=hours)
        cur.execute("""
            SELECT timestamp, health_score, status, error_rate, throughput, lag_seconds
            FROM connector_health
            WHERE connector_name = %s AND timestamp > %s
            ORDER BY timestamp ASC
        """, (connector_name, cutoff))
        
        results = cur.fetchall()
        cur.close()
        conn.close()
        
        return results
