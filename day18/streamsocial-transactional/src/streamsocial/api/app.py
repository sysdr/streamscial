"""StreamSocial API for transactional posting operations."""

import logging
from typing import Dict, Any, List
from datetime import datetime

from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from flask_socketio import SocketIO, emit

from src.streamsocial.services.posting_service import PostingService
from src.kafka_client.consumers.timeline_consumer import (
    StreamSocialTimelineConsumer, 
    handle_timeline_message,
    handle_global_feed_message,
    handle_notification_message,
    handle_analytics_message
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__, template_folder='../../../src/web/templates',
            static_folder='../../../src/web/static')
app.config['SECRET_KEY'] = 'streamsocial-transactional-demo'

CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*")

# Initialize services
posting_service = PostingService()
timeline_consumer = StreamSocialTimelineConsumer("api-consumer")

# Register message handlers
timeline_consumer.register_handler('user-timeline', handle_timeline_message)
timeline_consumer.register_handler('global-feed', handle_global_feed_message) 
timeline_consumer.register_handler('notifications', handle_notification_message)
timeline_consumer.register_handler('analytics', handle_analytics_message)

@app.route('/')
def index():
    """Main dashboard page."""
    return render_template('dashboard.html')

@app.route('/api/users', methods=['GET'])
def get_users():
    """Get all users."""
    try:
        users = posting_service.get_users()
        return jsonify({'success': True, 'data': users})
    except Exception as e:
        logger.error(f"Error getting users: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/posts', methods=['POST'])
def create_post():
    """Create a new post with atomic guarantees."""
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['user_id', 'content']
        for field in required_fields:
            if field not in data:
                return jsonify({'success': False, 'error': f'Missing required field: {field}'}), 400
        
        # Create post
        post_id = posting_service.create_post(
            user_id=data['user_id'],
            content=data['content'],
            content_type=data.get('content_type', 'text'),
            media_urls=data.get('media_urls'),
            hashtags=data.get('hashtags'),
            mentions=data.get('mentions')
        )
        
        if post_id:
            # Emit real-time update
            socketio.emit('new_post', {
                'post_id': post_id,
                'user_id': data['user_id'],
                'content': data['content'],
                'timestamp': datetime.utcnow().isoformat()
            })
            
            return jsonify({'success': True, 'post_id': post_id})
        else:
            return jsonify({'success': False, 'error': 'Failed to create post'}), 500
            
    except Exception as e:
        logger.error(f"Error creating post: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/posts/follower-sync', methods=['POST'])
def create_follower_sync_post():
    """Create post with follower timeline synchronization (assignment feature)."""
    try:
        data = request.get_json()
        
        required_fields = ['user_id', 'content', 'target_followers']
        for field in required_fields:
            if field not in data:
                return jsonify({'success': False, 'error': f'Missing required field: {field}'}), 400
        
        success = posting_service.create_follower_timeline_sync(
            user_id=data['user_id'],
            content=data['content'],
            target_followers=data['target_followers']
        )
        
        if success:
            # Emit real-time update
            socketio.emit('follower_sync_post', {
                'user_id': data['user_id'],
                'content': data['content'],
                'target_followers': data['target_followers'],
                'timestamp': datetime.utcnow().isoformat()
            })
            
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Failed to sync follower timelines'}), 500
            
    except Exception as e:
        logger.error(f"Error in follower sync: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/metrics', methods=['GET'])
def get_metrics():
    """Get system metrics."""
    try:
        service_metrics = posting_service.get_service_metrics()
        consumer_metrics = timeline_consumer.get_consumer_metrics()
        
        return jsonify({
            'success': True,
            'data': {
                'posting_service': service_metrics,
                'timeline_consumer': consumer_metrics,
                'timestamp': datetime.utcnow().isoformat()
            }
        })
    except Exception as e:
        logger.error(f"Error getting metrics: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/consume', methods=['POST'])
def consume_messages():
    """Trigger message consumption for demo purposes."""
    try:
        consumed_count = timeline_consumer.consume_messages(timeout_ms=2000, max_messages=50)
        
        return jsonify({
            'success': True,
            'consumed_messages': consumed_count,
            'timestamp': datetime.utcnow().isoformat()
        })
    except Exception as e:
        logger.error(f"Error consuming messages: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@socketio.on('connect')
def handle_connect():
    """Handle WebSocket connection."""
    logger.info('Client connected')
    emit('status', {'message': 'Connected to StreamSocial Transactional API'})

@socketio.on('disconnect')
def handle_disconnect():
    """Handle WebSocket disconnection."""
    logger.info('Client disconnected')

if __name__ == '__main__':
    try:
        logger.info("Starting StreamSocial Transactional API...")
        socketio.run(app, host='0.0.0.0', port=8080, debug=False, allow_unsafe_werkzeug=True)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        posting_service.close()
        timeline_consumer.close()
