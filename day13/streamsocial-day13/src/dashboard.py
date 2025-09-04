from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
import json
import time
import threading
from faker import Faker
from src.idempotent_producer import StreamSocialProducer, MetricsCollector
from src.duplicate_detector import DuplicateDetector
from config.kafka_config import KAFKA_CONFIG, CHAOS_CONFIG
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__, template_folder='../templates')
app.config['SECRET_KEY'] = 'streamsocial-secret'
socketio = SocketIO(app, cors_allowed_origins="*")

fake = Faker()

# Global components
producer = None
detector = None
metrics_data = {
    'posts_sent': 0,
    'posts_acknowledged': 0,
    'duplicates_prevented': 0,
    'retries': 0,
    'chaos_events': 0
}

@app.route('/')
def dashboard():
    return render_template('dashboard.html')

@app.route('/api/metrics')
def get_metrics():
    stats = detector.get_stats() if detector else {}
    return jsonify({
        **metrics_data,
        **stats,
        'producer_state': producer.state if producer else 'DISCONNECTED'
    })

@app.route('/api/send_post', methods=['POST'])
def send_post():
    global metrics_data
    
    if not producer or producer.state != 'READY':
        return jsonify({'error': 'Producer not ready'}), 500
        
    try:
        user_id = request.json.get('user_id', fake.user_name())
        content = request.json.get('content', fake.text(max_nb_chars=140))
        
        result = producer.send_post(user_id, content)
        metrics_data['posts_sent'] += 1
        metrics_data['posts_acknowledged'] += 1
        
        # Emit real-time update
        socketio.emit('post_sent', {
            'post_id': result['post_id'],
            'user_id': user_id,
            'content': content,
            'metrics': metrics_data
        })
        
        return jsonify(result)
        
    except Exception as e:
        metrics_data['retries'] += 1
        logger.error(f"Error sending post: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/start_chaos')
def start_chaos():
    """Start chaos testing"""
    if producer:
        producer.chaos = producer.chaos or producer.ChaosMonkey()
        return jsonify({'status': 'Chaos mode activated'})
    return jsonify({'error': 'Producer not available'}), 500

@app.route('/api/simulate_burst', methods=['POST'])
def simulate_burst():
    """Simulate burst of posts with network issues"""
    count = request.json.get('count', 10)
    
    def burst_sender():
        global metrics_data
        for i in range(count):
            try:
                user_id = f"user_{i % 5}"  # 5 different users
                content = f"Burst post #{i+1}: {fake.text(max_nb_chars=100)}"
                
                result = producer.send_post(user_id, content)
                metrics_data['posts_sent'] += 1
                
                socketio.emit('burst_progress', {
                    'current': i + 1,
                    'total': count,
                    'post_id': result['post_id']
                })
                
                time.sleep(0.1)  # Small delay between posts
                
            except Exception as e:
                metrics_data['retries'] += 1
                logger.error(f"Burst send error: {e}")
    
    thread = threading.Thread(target=burst_sender, daemon=True)
    thread.start()
    
    return jsonify({'status': f'Started burst sending {count} posts'})

def init_components():
    """Initialize producer and detector"""
    global producer, detector
    
    try:
        # Initialize producer with chaos enabled
        producer = StreamSocialProducer(KAFKA_CONFIG, enable_chaos=True)
        
        # Initialize duplicate detector
        detector = DuplicateDetector()
        detector.start_monitoring()
        
        logger.info("All components initialized successfully")
        
    except Exception as e:
        logger.error(f"Failed to initialize components: {e}")

if __name__ == '__main__':
    init_components()
    socketio.run(app, debug=True, host='0.0.0.0', port=5000)
