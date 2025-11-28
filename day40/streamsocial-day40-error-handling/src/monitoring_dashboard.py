"""Real-time monitoring dashboard for error handling metrics"""
from flask import Flask, render_template, jsonify
from flask_cors import CORS
from confluent_kafka.admin import AdminClient
from confluent_kafka import Consumer, KafkaError
import redis
import json
import time
import os
from datetime import datetime

# Get the root directory (parent of src/)
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
template_dir = os.path.join(root_dir, 'templates')

app = Flask(__name__, template_folder=template_dir)
CORS(app)

# Kafka config
BOOTSTRAP_SERVERS = 'localhost:9092'
DLQ_TOPIC = 'moderation-dlq'

# Redis client - with connection error handling
try:
    redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True, socket_connect_timeout=2)
    redis_client.ping()  # Test connection
except Exception as e:
    print(f"Warning: Redis connection failed: {e}")
    redis_client = None

def get_topic_lag(topic):
    """Get consumer lag for a topic"""
    try:
        admin_client = AdminClient({'bootstrap.servers': BOOTSTRAP_SERVERS})
        metadata = admin_client.list_topics(topic=topic, timeout=5)
        
        if topic in metadata.topics:
            partitions = metadata.topics[topic].partitions
            return len(partitions)
    except Exception as e:
        print(f"Error getting topic info: {e}")
    return 0

def get_dlq_records():
    """Get recent DLQ records"""
    records = []
    consumer = None
    
    try:
        import uuid
        # Use unique consumer group ID each time to avoid offset issues
        unique_group_id = f'dashboard-viewer-{int(time.time() * 1000)}'
        
        consumer = Consumer({
            'bootstrap.servers': BOOTSTRAP_SERVERS,
            'group.id': unique_group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'session.timeout.ms': 6000
        })
        
        # Subscribe to topic
        consumer.subscribe([DLQ_TOPIC])
        
        # Poll to get assignment - wait longer
        assignment_wait = 0
        while not consumer.assignment() and assignment_wait < 30:
            consumer.poll(timeout=0.2)
            assignment_wait += 1
        
        # Read all available messages with longer timeout
        message_count = 0
        max_messages = 100
        no_message_count = 0
        
        while message_count < max_messages and no_message_count < 10:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                no_message_count += 1
                if no_message_count >= 3:
                    break
                continue
            no_message_count = 0
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                continue
            
            try:
                headers = {}
                if msg.headers():
                    headers = {h[0]: h[1].decode('utf-8') if h[1] else '' for h in msg.headers()}
                
                value = {}
                if msg.value():
                    value = json.loads(msg.value().decode('utf-8'))
                
                timestamp_str = 'N/A'
                if msg.timestamp():
                    try:
                        timestamp_str = datetime.fromtimestamp(msg.timestamp()[1]/1000).strftime('%Y-%m-%d %H:%M:%S')
                    except (ValueError, TypeError, IndexError):
                        timestamp_str = 'Invalid timestamp'
                
                records.append({
                    'key': msg.key().decode('utf-8') if msg.key() else 'null',
                    'timestamp': timestamp_str,
                    'error_type': headers.get('__connect.errors.class.name', 'Unknown'),
                    'error_message': headers.get('__connect.errors.exception.message', '')[:100],
                    'content_preview': value.get('content', '')[:50] if isinstance(value, dict) else str(value)[:50]
                })
                message_count += 1
            except (json.JSONDecodeError, UnicodeDecodeError, AttributeError) as e:
                print(f"Error parsing message: {e}")
                continue
    except Exception as e:
        print(f"Error reading DLQ: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if consumer:
            try:
                consumer.close()
            except:
                pass
    
    # Return last 20 records, newest first
    return records[-20:][::-1] if len(records) > 20 else records[::-1]

def get_retry_statistics():
    """Get retry statistics from Redis"""
    if redis_client is None:
        return {
            'active_retries': 0,
            'permanent_failures': 0,
            'error_breakdown': {}
        }
    
    try:
        retry_keys = redis_client.keys('retry:*') or []
        permanent_keys = redis_client.keys('permanent_failure:*') or []
        
        error_types = {}
        for key in retry_keys:
            try:
                error_type = redis_client.hget(key, 'error_type')
                if error_type:
                    error_types[error_type] = error_types.get(error_type, 0) + 1
            except Exception as e:
                print(f"Error reading key {key}: {e}")
                continue
        
        return {
            'active_retries': len(retry_keys),
            'permanent_failures': len(permanent_keys),
            'error_breakdown': error_types
        }
    except Exception as e:
        print(f"Error getting retry statistics: {e}")
        return {
            'active_retries': 0,
            'permanent_failures': 0,
            'error_breakdown': {}
        }

@app.errorhandler(500)
def internal_error(error):
    """Handle internal server errors"""
    print(f"Internal Server Error: {error}")
    return jsonify({'error': 'Internal server error occurred'}), 500

@app.errorhandler(Exception)
def handle_exception(e):
    """Handle all unhandled exceptions"""
    print(f"Unhandled exception: {e}")
    return jsonify({'error': str(e)}), 500

@app.route('/')
def index():
    """Main dashboard page"""
    try:
        return render_template('dashboard.html')
    except Exception as e:
        print(f"Error rendering template: {e}")
        return f"Error loading dashboard: {str(e)}", 500

@app.route('/api/metrics')
def get_metrics():
    """API endpoint for real-time metrics"""
    try:
        partitions = get_topic_lag(DLQ_TOPIC)
        retry_stats = get_retry_statistics()
        
        metrics = {
            'timestamp': int(time.time() * 1000),
            'dlq_partitions': partitions,
            'active_retries': retry_stats['active_retries'],
            'permanent_failures': retry_stats['permanent_failures'],
            'error_breakdown': retry_stats['error_breakdown']
        }
        
        return jsonify(metrics)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/dlq/records')
def get_dlq_records_api():
    """API endpoint for DLQ records"""
    try:
        records = get_dlq_records()
        return jsonify({'records': records})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/permanent-failures')
def get_permanent_failures():
    """API endpoint for permanent failures"""
    try:
        if redis_client is None:
            return jsonify({'failures': []})
        
        keys = redis_client.keys('permanent_failure:*') or []
        failures = []
        
        for key in keys[:20]:  # Limit to 20
            try:
                key_parts = key.split(':')
                record_id = key_parts[1] if len(key_parts) > 1 else key
                data = redis_client.hgetall(key) or {}
                
                # Handle failed_at timestamp safely
                failed_at_str = 'N/A'
                try:
                    failed_at_val = data.get('failed_at', '0')
                    if failed_at_val and failed_at_val != '0':
                        failed_at_str = datetime.fromtimestamp(int(failed_at_val)/1000).strftime('%Y-%m-%d %H:%M:%S')
                except (ValueError, TypeError, OSError):
                    failed_at_str = 'Invalid timestamp'
                
                # Handle content JSON parsing safely
                content = {}
                try:
                    content_str = data.get('content', '{}')
                    if content_str:
                        content = json.loads(content_str)
                except (json.JSONDecodeError, TypeError):
                    content = {'raw': str(data.get('content', ''))[:100]}
                
                failures.append({
                    'record_id': record_id,
                    'error_type': data.get('error_type', 'Unknown'),
                    'failed_at': failed_at_str,
                    'retry_attempts': data.get('retry_attempts', '0'),
                    'content': content
                })
            except Exception as e:
                print(f"Error processing permanent failure key {key}: {e}")
                continue
        
        return jsonify({'failures': failures})
    except Exception as e:
        print(f"Error in get_permanent_failures: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    print("\n" + "="*50)
    print("StreamSocial Error Handling Dashboard")
    print("="*50)
    print("Dashboard URL: http://localhost:5000")
    print("API Endpoints:")
    print("  - /api/metrics")
    print("  - /api/dlq/records")
    print("  - /api/permanent-failures")
    print("="*50 + "\n")
    
    app.run(host='0.0.0.0', port=5000, debug=False)
