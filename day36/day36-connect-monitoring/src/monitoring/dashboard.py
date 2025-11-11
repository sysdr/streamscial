from flask import Flask, render_template, jsonify
import psycopg2
from psycopg2 import OperationalError
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__, template_folder='../../dashboard')

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'monitoring',
    'user': 'admin',
    'password': 'admin123'
}

DEMO_CONNECTORS = ['instagram-source', 'twitter-source', 'linkedin-source']


def _demo_time_points(points=6, interval_minutes=5):
    now = datetime.now()
    return [
        (now - timedelta(minutes=interval_minutes * (points - idx - 1))).isoformat()
        for idx in range(points)
    ]


def _demo_health_summary():
    connectors = [
        {'name': 'instagram-source', 'health_score': 95.2, 'status': 'HEALTHY'},
        {'name': 'twitter-source', 'health_score': 88.4, 'status': 'DEGRADED'},
        {'name': 'linkedin-source', 'health_score': 76.1, 'status': 'DEGRADED'},
    ]
    total = sum(c['health_score'] for c in connectors)
    return {
        'overall_score': round(total / len(connectors), 1),
        'connectors': connectors,
        'timestamp': datetime.now().isoformat(),
        'mode': 'demo'
    }


def _demo_throughput_series():
    timestamps = _demo_time_points()
    values = {
        'instagram-source': [145, 152, 160, 158, 162, 170],
        'twitter-source': [280, 295, 310, 305, 315, 325],
        'linkedin-source': [75, 80, 82, 84, 88, 90],
    }
    return {
        connector: [
            {'timestamp': ts, 'value': throughput}
            for ts, throughput in zip(timestamps, series)
        ]
        for connector, series in values.items()
    }


def _demo_error_series():
    timestamps = _demo_time_points()
    values = {
        'instagram-source': [0.2, 0.3, 0.25, 0.28, 0.22, 0.2],
        'twitter-source': [0.4, 0.45, 0.5, 0.48, 0.46, 0.44],
        'linkedin-source': [0.6, 0.62, 0.65, 0.7, 0.68, 0.66],
    }
    return {
        connector: [
            {'timestamp': ts, 'value': round(err, 3)}
            for ts, err in zip(timestamps, series)
        ]
        for connector, series in values.items()
    }


def _demo_lag_metrics():
    return [
        {'connector': 'instagram-source', 'lag_seconds': 12.5},
        {'connector': 'twitter-source', 'lag_seconds': 24.8},
        {'connector': 'linkedin-source', 'lag_seconds': 45.2},
    ]


def _demo_alerts():
    now = datetime.now()
    return [
        {
            'timestamp': (now - timedelta(minutes=3)).isoformat(),
            'connector': 'twitter-source',
            'type': 'task_failure',
            'severity': 'P1',
            'message': 'Tasks failed: 1'
        },
        {
            'timestamp': (now - timedelta(minutes=8)).isoformat(),
            'connector': 'linkedin-source',
            'type': 'high_lag',
            'severity': 'P1',
            'message': 'High lag: 75s'
        },
    ]


def _demo_history(connector):
    timestamps = _demo_time_points(points=12, interval_minutes=10)
    base_values = {
        'instagram-source': {
            'health': [98, 97, 96, 95, 95, 94, 95, 96, 97, 97, 96, 95],
            'throughput': [150, 152, 155, 158, 160, 162, 164, 165, 166, 167, 168, 169],
            'errors': [0.15, 0.18, 0.2, 0.22, 0.2, 0.18, 0.17, 0.16, 0.15, 0.17, 0.16, 0.15],
            'lag': [8, 9, 10, 12, 11, 10, 9, 8, 9, 10, 11, 12],
        },
        'twitter-source': {
            'health': [90, 89, 88, 88, 87, 86, 85, 86, 87, 88, 89, 90],
            'throughput': [280, 285, 290, 300, 305, 310, 312, 314, 316, 318, 320, 322],
            'errors': [0.35, 0.38, 0.4, 0.43, 0.45, 0.47, 0.48, 0.46, 0.44, 0.42, 0.41, 0.4],
            'lag': [18, 20, 22, 25, 24, 23, 22, 21, 20, 19, 18, 17],
        },
        'linkedin-source': {
            'health': [82, 81, 80, 79, 78, 77, 76, 75, 74, 73, 72, 71],
            'throughput': [70, 72, 74, 76, 78, 80, 82, 84, 85, 86, 87, 88],
            'errors': [0.55, 0.56, 0.58, 0.6, 0.63, 0.65, 0.66, 0.67, 0.68, 0.7, 0.72, 0.73],
            'lag': [30, 32, 35, 38, 40, 42, 45, 47, 49, 50, 52, 55],
        }
    }
    series = base_values.get(connector, base_values['instagram-source'])
    return [
        {
            'timestamp': ts,
            'health_score': round(series['health'][idx], 1),
            'throughput': round(series['throughput'][idx], 2),
            'error_rate': round(series['errors'][idx], 3),
            'lag_seconds': round(series['lag'][idx], 1),
            'mode': 'demo'
        }
        for idx, ts in enumerate(timestamps)
    ]


def get_db_connection():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except OperationalError as exc:
        logger.warning("Dashboard database connection failed: %s", exc)
        return None


@app.route('/')
def index():
    return render_template('dashboard.html')


@app.route('/favicon.ico')
def favicon():
    return ('', 204)


@app.route('/api/health/summary')
def health_summary():
    """Get overall health summary"""
    conn = get_db_connection()
    if not conn:
        summary = _demo_health_summary()
        return jsonify(summary), 200, {'X-Mode': 'demo'}

    cur = conn.cursor()
    
    # Get latest health for each connector
    cur.execute("""
        WITH latest AS (
            SELECT DISTINCT ON (connector_name) 
                connector_name, health_score, status, timestamp
            FROM connector_health
            ORDER BY connector_name, timestamp DESC
        )
        SELECT connector_name, health_score, status
        FROM latest
    """)
    
    connectors = []
    total_score = 0
    
    for row in cur.fetchall():
        connectors.append({
            'name': row[0],
            'health_score': round(row[1], 1),
            'status': row[2]
        })
        total_score += row[1]
    
    overall_score = total_score / len(connectors) if connectors else 0
    
    cur.close()
    conn.close()
    
    return jsonify({
        'overall_score': round(overall_score, 1),
        'connectors': connectors,
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/metrics/throughput')
def throughput_metrics():
    """Get throughput metrics"""
    conn = get_db_connection()
    if not conn:
        return jsonify(_demo_throughput_series()), 200, {'X-Mode': 'demo'}

    cur = conn.cursor()
    
    cutoff = datetime.now() - timedelta(hours=1)
    cur.execute("""
        SELECT connector_name, timestamp, throughput
        FROM connector_health
        WHERE timestamp > %s
        ORDER BY timestamp ASC
    """, (cutoff,))
    
    data = {}
    for row in cur.fetchall():
        connector = row[0]
        if connector not in data:
            data[connector] = []
        data[connector].append({
            'timestamp': row[1].isoformat(),
            'value': round(row[2], 2)
        })
    
    cur.close()
    conn.close()
    
    return jsonify(data)

@app.route('/api/metrics/errors')
def error_metrics():
    """Get error rate metrics"""
    conn = get_db_connection()
    if not conn:
        return jsonify(_demo_error_series()), 200, {'X-Mode': 'demo'}

    cur = conn.cursor()
    
    cutoff = datetime.now() - timedelta(hours=1)
    cur.execute("""
        SELECT connector_name, timestamp, error_rate
        FROM connector_health
        WHERE timestamp > %s
        ORDER BY timestamp ASC
    """, (cutoff,))
    
    data = {}
    for row in cur.fetchall():
        connector = row[0]
        if connector not in data:
            data[connector] = []
        data[connector].append({
            'timestamp': row[1].isoformat(),
            'value': round(row[2] * 100, 3)  # Convert to percentage
        })
    
    cur.close()
    conn.close()
    
    return jsonify(data)

@app.route('/api/metrics/lag')
def lag_metrics():
    """Get lag metrics"""
    conn = get_db_connection()
    if not conn:
        return jsonify(_demo_lag_metrics()), 200, {'X-Mode': 'demo'}

    cur = conn.cursor()
    
    cur.execute("""
        SELECT DISTINCT ON (connector_name)
            connector_name, lag_seconds
        FROM connector_health
        ORDER BY connector_name, timestamp DESC
    """)
    
    data = []
    for row in cur.fetchall():
        data.append({
            'connector': row[0],
            'lag_seconds': round(row[1], 1)
        })
    
    cur.close()
    conn.close()
    
    return jsonify(data)

@app.route('/api/alerts/active')
def active_alerts():
    """Get active alerts"""
    conn = get_db_connection()
    if not conn:
        return jsonify(_demo_alerts()), 200, {'X-Mode': 'demo'}

    cur = conn.cursor()
    
    cur.execute("""
        SELECT timestamp, connector_name, alert_type, severity, message
        FROM alerts
        WHERE resolved = FALSE
        ORDER BY timestamp DESC
        LIMIT 20
    """)
    
    alerts = []
    for row in cur.fetchall():
        alerts.append({
            'timestamp': row[0].isoformat(),
            'connector': row[1] or 'System',
            'type': row[2],
            'severity': row[3],
            'message': row[4]
        })
    
    cur.close()
    conn.close()
    
    return jsonify(alerts)

@app.route('/api/history/<connector>')
def connector_history(connector):
    """Get historical data for a connector"""
    conn = get_db_connection()
    if not conn:
        return jsonify(_demo_history(connector)), 200, {'X-Mode': 'demo'}

    cur = conn.cursor()
    
    cutoff = datetime.now() - timedelta(hours=24)
    cur.execute("""
        SELECT timestamp, health_score, throughput, error_rate, lag_seconds
        FROM connector_health
        WHERE connector_name = %s AND timestamp > %s
        ORDER BY timestamp ASC
    """, (connector, cutoff))
    
    data = []
    for row in cur.fetchall():
        data.append({
            'timestamp': row[0].isoformat(),
            'health_score': round(row[1], 1),
            'throughput': round(row[2], 2),
            'error_rate': round(row[3] * 100, 3),
            'lag_seconds': round(row[4], 1)
        })
    
    cur.close()
    conn.close()
    
    return jsonify(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
