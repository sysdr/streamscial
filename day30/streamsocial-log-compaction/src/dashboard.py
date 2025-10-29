import os
import json
import threading
import time
from datetime import datetime
from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
from src.producer.preference_producer import PreferenceProducer
from src.consumer.preference_consumer import PreferenceConsumer
from src.models.user_preference import UserPreference, NotificationSettings, PrivacySettings
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get the base directory (parent of src)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

app = Flask(
    __name__,
    template_folder=os.path.join(BASE_DIR, 'ui', 'templates'),
    static_folder=os.path.join(BASE_DIR, 'ui', 'static')
)
app.config['SECRET_KEY'] = 'streamsocial-compaction-demo'
socketio = SocketIO(app, cors_allowed_origins="*")

# Global components
producer = None
consumer = None
stats_thread = None

def initialize_kafka_components():
    """Initialize Kafka components in background thread to not block Flask startup"""
    global producer, consumer
    
    def _init():
        global producer, consumer
        try:
            logger.info("Starting Kafka component initialization...")
            topic_name = os.getenv('KAFKA_TOPIC', 'user-preferences')
            group_id = os.getenv('KAFKA_GROUP_ID', 'preference-consumer')
            
            # Create producer FIRST - it's simpler and doesn't block
            logger.info("Creating producer...")
            producer = PreferenceProducer(topic_name)
            logger.info("Producer created successfully")
            
            # Create consumer
            logger.info("Creating consumer...")
            consumer = PreferenceConsumer(topic_name, group_id)
            logger.info("Consumer created successfully")
            
            # Start consumer (this is fast)
            consumer.start_consuming()
            logger.info("Consumer started, Kafka components ready!")
            
            # Rebuild state LATER in separate thread (don't block initialization)
            def rebuild_later():
                time.sleep(5)  # Wait a bit longer for everything to settle
                try:
                    logger.info("Starting optional state rebuild in background...")
                    consumer.rebuild_state_from_beginning()
                    logger.info("State rebuild completed")
                except Exception as e:
                    logger.warning(f"State rebuild failed (non-critical): {e}")
                    # Don't fail initialization if rebuild fails
            
            rebuild_thread = threading.Thread(target=rebuild_later, daemon=True)
            rebuild_thread.start()
            
        except Exception as e:
            logger.error(f"Error initializing Kafka components: {e}", exc_info=True)
            # Set to None on error
            producer = None
            consumer = None
    
    # Start initialization in background thread
    init_thread = threading.Thread(target=_init, daemon=True)
    init_thread.start()

def stats_broadcast():
    """Broadcast stats to all connected clients"""
    # Wait a bit for components to initialize
    time.sleep(5)
    
    while True:
        try:
            if producer and consumer:
                stats_data = {
                    'producer': producer.get_stats(),
                    'consumer': consumer.get_stats(),
                    'timestamp': datetime.utcnow().isoformat(),
                    'user_count': len(consumer.get_all_preferences()) if consumer else 0
                }
                
                # Non-blocking emit
                socketio.emit('stats_update', stats_data)
            else:
                # Send empty stats while initializing
                socketio.emit('stats_update', {
                    'producer': {'message_count': 0, 'topic': 'initializing'},
                    'consumer': {'message_count': 0, 'active_users': 0},
                    'timestamp': datetime.utcnow().isoformat(),
                    'user_count': 0
                })
        except Exception as e:
            logger.error(f"Error broadcasting stats: {e}")
        
        time.sleep(2)

@app.route('/')
def dashboard():
    return render_template('dashboard.html')

@app.route('/favicon.ico')
def favicon():
    return '', 204  # No content instead of 404

@app.route('/api/preferences')
def get_preferences():
    """Get all current user preferences"""
    if not consumer:
        return jsonify({'preferences': {}, 'count': 0, 'status': 'initializing'}), 200
    
    try:
        preferences = consumer.get_all_preferences()
        return jsonify({
            'preferences': {uid: pref.to_dict() for uid, pref in preferences.items()},
            'count': len(preferences)
        })
    except Exception as e:
        logger.error(f"Error getting preferences: {e}")
        return jsonify({'preferences': {}, 'count': 0, 'error': str(e)}), 500

@app.route('/api/preference/<user_id>')
def get_user_preference(user_id):
    """Get preference for specific user"""
    if not consumer:
        return jsonify({'error': 'Consumer not initialized. Please wait a moment and try again.'}), 503
    
    preference = consumer.get_user_preference(user_id)
    if preference:
        return jsonify(preference.to_dict())
    else:
        return jsonify({'error': 'User not found'}), 404

@app.route('/api/preference', methods=['POST'])
def update_preference():
    """Update user preference"""
    if not producer:
        return jsonify({'error': 'Producer not initialized. Please wait a moment and try again.'}), 503
    
    try:
        data = request.get_json()
        
        # Create preference object
        preference = UserPreference(
            user_id=data['user_id'],
            theme=data.get('theme', 'light'),
            language=data.get('language', 'en'),
            timezone=data.get('timezone', 'UTC'),
            version=data.get('version', 1)
        )
        
        # Update notification settings
        if 'notifications' in data:
            preference.notifications = NotificationSettings(**data['notifications'])
        
        # Update privacy settings
        if 'privacy' in data:
            preference.privacy = PrivacySettings(**data['privacy'])
        
        # Send update
        success = producer.send_preference_update(preference)
        
        if success:
            return jsonify({'status': 'success', 'preference': preference.to_dict()})
        else:
            return jsonify({'error': 'Failed to send update'}), 500
            
    except Exception as e:
        logger.error(f"Error updating preference: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/user/<user_id>', methods=['DELETE'])
def delete_user(user_id):
    """Delete user (send tombstone)"""
    if not producer:
        return jsonify({'error': 'Producer not initialized. Please wait a moment and try again.'}), 503
    
    try:
        success = producer.send_user_deletion(user_id)
        
        if success:
            return jsonify({'status': 'success', 'message': f'User {user_id} deleted'})
        else:
            return jsonify({'error': 'Failed to send tombstone'}), 500
            
    except Exception as e:
        logger.error(f"Error deleting user: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/simulate', methods=['POST'])
def start_simulation():
    """Start user activity simulation"""
    if not producer:
        return jsonify({'error': 'Producer not initialized. Please wait a moment and try again.'}), 503
    
    try:
        data = request.get_json()
        num_users = data.get('num_users', 10)
        duration = data.get('duration_seconds', 60)
        
        # Start simulation in background thread
        simulation_thread = threading.Thread(
            target=producer.simulate_user_activity,
            args=(num_users, duration)
        )
        simulation_thread.start()
        
        return jsonify({
            'status': 'started',
            'num_users': num_users,
            'duration_seconds': duration
        })
        
    except Exception as e:
        logger.error(f"Error starting simulation: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/rebuild-state', methods=['POST'])
def rebuild_state():
    """Rebuild consumer state from compacted log"""
    if not consumer:
        return jsonify({'error': 'Consumer not initialized. Please wait a moment and try again.'}), 503
    
    try:
        consumer.rebuild_state_from_beginning()
        return jsonify({'status': 'success', 'message': 'State rebuilt from compacted log'})
        
    except Exception as e:
        logger.error(f"Error rebuilding state: {e}")
        return jsonify({'error': str(e)}), 500

@socketio.on('connect')
def handle_connect():
    logger.info('Client connected')
    emit('connected', {'status': 'Connected to StreamSocial Compaction Demo'})

@socketio.on('disconnect')
def handle_disconnect():
    logger.info('Client disconnected')

if __name__ == '__main__':
    # Start Kafka initialization in background (non-blocking)
    logger.info("Starting Kafka initialization in background thread...")
    initialize_kafka_components()
    
    # Start stats broadcasting
    stats_thread = threading.Thread(target=stats_broadcast, daemon=True)
    stats_thread.start()
    
    # Run Flask app IMMEDIATELY - don't wait for Kafka
    logger.info("Starting Flask server on port 5000...")
    port = int(os.getenv('FLASK_PORT', 5000))
    socketio.run(app, host='0.0.0.0', port=port, debug=False, allow_unsafe_werkzeug=True, use_reloader=False)
