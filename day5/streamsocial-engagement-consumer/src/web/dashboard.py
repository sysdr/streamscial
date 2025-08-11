from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO, emit
import json
import time
import threading
import redis
from src.utils.cache import CacheManager
from src.utils.metrics import MetricsCollector
from config.consumer_config import ConsumerConfig

app = Flask(__name__)
app.config['SECRET_KEY'] = 'streamsocial_secret_key_2025'
socketio = SocketIO(app, cors_allowed_origins="*")

cache_manager = CacheManager()
redis_client = redis.Redis(
    host=ConsumerConfig.REDIS_HOST,
    port=ConsumerConfig.REDIS_PORT,
    db=ConsumerConfig.REDIS_DB,
    decode_responses=True
)

@app.route('/')
def dashboard():
    return render_template('dashboard.html')

@app.route('/api/engagement-stats')
def get_engagement_stats():
    """Get all engagement statistics"""
    stats = cache_manager.get_all_engagement_stats()
    return jsonify(stats)

@app.route('/api/metrics')
def get_metrics():
    """Get consumer metrics from Redis"""
    try:
        # Try to get metrics from Redis first
        redis_metrics = redis_client.get('metrics:message_processing')
        if redis_metrics:
            metrics_data = json.loads(redis_metrics)
            processed_count = metrics_data.get('processed_count', 0)
            error_count = metrics_data.get('error_count', 0)
            processing_times = metrics_data.get('processing_times', [])
            
            avg_processing_time = sum(processing_times) / len(processing_times) if processing_times else 0
            success_rate = (processed_count - error_count) / max(processed_count, 1) if processed_count > 0 else 1.0
            
            return jsonify({
                'uptime_seconds': time.time() - time.time(),  # Dashboard uptime
                'messages_processed': processed_count,
                'messages_failed': error_count,
                'success_rate': success_rate,
                'avg_processing_time_ms': avg_processing_time * 1000,
                'cpu_percent': 0,  # Will be updated by system metrics
                'memory_percent': 0,  # Will be updated by system metrics
                'memory_used_mb': 0  # Will be updated by system metrics
            })
    except Exception as e:
        print(f"Error reading metrics from Redis: {e}")
    
    # Fallback to empty metrics
    return jsonify({
        'uptime_seconds': 0,
        'messages_processed': 0,
        'messages_failed': 0,
        'success_rate': 1.0,
        'avg_processing_time_ms': 0,
        'cpu_percent': 0,
        'memory_percent': 0,
        'memory_used_mb': 0
    })

@socketio.on('connect')
def handle_connect():
    """Handle client connection"""
    print('Client connected')
    emit('status', {'message': 'Connected to StreamSocial Dashboard'})

@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection"""
    print('Client disconnected')

def background_updates():
    """Send periodic updates to connected clients"""
    while True:
        time.sleep(2)  # Update every 2 seconds
        
        try:
            # Get latest engagement stats
            engagement_stats = cache_manager.get_all_engagement_stats()
            
            # Get metrics from Redis
            metrics = get_metrics()
            
            # Emit to all connected clients
            socketio.emit('engagement_update', engagement_stats)
            socketio.emit('metrics_update', metrics.json)
            
        except Exception as e:
            print(f"Error in background updates: {e}")
            time.sleep(5)  # Wait longer on error

# Start background thread for updates
update_thread = threading.Thread(target=background_updates)
update_thread.daemon = True
update_thread.start()

if __name__ == '__main__':
    socketio.run(
        app, 
        host=ConsumerConfig.WEB_HOST, 
        port=ConsumerConfig.WEB_PORT, 
        debug=False
    )
