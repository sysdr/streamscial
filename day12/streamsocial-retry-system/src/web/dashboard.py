import json
import time
from flask import Flask, render_template, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Global reference to producer (will be set from main)
retry_producer = None

@app.route('/')
def dashboard():
    """Main dashboard page"""
    return render_template('dashboard.html')

@app.route('/api/metrics')
def get_metrics():
    """API endpoint for current metrics"""
    if retry_producer:
        metrics = retry_producer.get_metrics()
        return jsonify(metrics)
    return jsonify({'error': 'Producer not initialized'})

@app.route('/api/send_test_message', methods=['POST'])
def send_test_message():
    """Send test message to trigger retry logic"""
    if not retry_producer:
        return jsonify({'error': 'Producer not initialized'}), 500
    
    data = request.json
    topic = data.get('topic', 'streamsocial-posts')
    message_type = data.get('type', 'post')
    
    test_message = {
        'user_id': 'test_user_123',
        'type': message_type,
        'content': f'Test {message_type} at {time.strftime("%H:%M:%S")}',
        'test_mode': True
    }
    
    success = retry_producer.send_with_retry(topic, test_message)
    
    return jsonify({
        'success': success,
        'message': 'Message sent successfully' if success else 'Message failed to send',
        'timestamp': time.time()
    })

@app.route('/api/simulate_failure', methods=['POST'])
def simulate_failure():
    """Simulate network failure for testing"""
    # This would typically modify producer configuration
    # For demo purposes, we'll just return success
    return jsonify({'success': True, 'message': 'Failure simulation triggered'})

def set_producer(producer):
    """Set the global producer reference"""
    global retry_producer
    retry_producer = producer

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
