"""Real-time trending analytics dashboard using Flask and WebSockets"""
from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO, emit
from flask_cors import CORS
import requests
import json
import threading
import time
import logging
import queue
from datetime import datetime
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY', 'dev-secret-key-change-in-production')
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

KSQLDB_URL = 'http://localhost:8088'

class TrendingDataManager:
    def __init__(self):
        self.hashtag_data = []
        self.viral_posts = []
        self.active_users = []
        self.running = False
        
    def query_trending_hashtags(self):
        """Query trending hashtags - read directly from Kafka topic for reliability"""
        try:
            # Read directly from Kafka topic for immediate results
            consumer = KafkaConsumer('HASHTAGS_EXPLODED',
                                     bootstrap_servers='localhost:9092',
                                     auto_offset_reset='earliest',
                                     value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                                     consumer_timeout_ms=1000,
                                     enable_auto_commit=False)
            
            hashtag_counts = {}
            hashtag_users = {}
            count = 0
            
            # Read recent messages
            for msg in consumer:
                if count >= 500:  # Limit to recent 500 messages
                    break
                try:
                    data = msg.value
                    if data and 'HASHTAG' in data:
                        hashtag = str(data['HASHTAG'])
                        user_id = data.get('USER_ID', data.get('user_id', 0))
                        if hashtag:
                            hashtag_counts[hashtag] = hashtag_counts.get(hashtag, 0) + 1
                            if hashtag not in hashtag_users:
                                hashtag_users[hashtag] = set()
                            hashtag_users[hashtag].add(user_id)
                        count += 1
                except Exception as e:
                    continue
            
            consumer.close()
            
            # Convert to results
            if hashtag_counts:
                results = [{
                    'hashtag': h,
                    'mentions': count,
                    'unique_users': len(hashtag_users.get(h, set()))
                } for h, count in hashtag_counts.items()]
                results.sort(key=lambda x: x['mentions'], reverse=True)
                return results[:10]
        except Exception as e:
            logger.error(f"Error querying hashtags: {e}")
        return []
    
    def query_viral_posts(self):
        """Query viral posts - calculate engagement from Kafka topics"""
        try:
            # Read all engagement data first (likes and comments) to get post_ids with activity
            likes_count = {}
            comments_count = {}
            active_post_ids = set()
            
            # Read ALL likes to get comprehensive counts
            try:
                likes_consumer = KafkaConsumer('likes',
                                             bootstrap_servers='localhost:9092',
                                             auto_offset_reset='earliest',
                                             value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                                             consumer_timeout_ms=2000,
                                             enable_auto_commit=False,
                                             max_poll_records=500)
                
                count = 0
                for msg in likes_consumer:
                    if count >= 5000:  # Read more likes
                        break
                    try:
                        data = msg.value
                        if data and 'post_id' in data:
                            post_id = data['post_id']
                            likes_count[post_id] = likes_count.get(post_id, 0) + 1
                            active_post_ids.add(post_id)
                            count += 1
                    except:
                        continue
                likes_consumer.close()
            except Exception as e:
                logger.debug(f"Error reading likes: {e}")
            
            # Read ALL comments
            try:
                comments_consumer = KafkaConsumer('comments',
                                                bootstrap_servers='localhost:9092',
                                                auto_offset_reset='earliest',
                                                value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                                                consumer_timeout_ms=2000,
                                                enable_auto_commit=False,
                                                max_poll_records=500)
                
                count = 0
                for msg in comments_consumer:
                    if count >= 5000:  # Read more comments
                        break
                    try:
                        data = msg.value
                        if data and 'post_id' in data:
                            post_id = data['post_id']
                            comments_count[post_id] = comments_count.get(post_id, 0) + 1
                            active_post_ids.add(post_id)
                            count += 1
                    except:
                        continue
                comments_consumer.close()
            except Exception as e:
                logger.debug(f"Error reading comments: {e}")
            
            # Now read posts - focus on posts that have engagement (likes/comments)
            posts_data = {}
            
            # Read posts, prioritizing those with engagement
            try:
                posts_consumer = KafkaConsumer('posts',
                                             bootstrap_servers='localhost:9092',
                                             auto_offset_reset='earliest',
                                             value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                                             consumer_timeout_ms=2000,
                                             enable_auto_commit=False,
                                             max_poll_records=500)
                
                count = 0
                # First pass: collect posts that have engagement
                for msg in posts_consumer:
                    if count >= 10000:  # Read more posts to find matching ones
                        break
                    try:
                        data = msg.value
                        if data and 'post_id' in data:
                            post_id = data['post_id']
                            # Only store posts that have engagement OR read enough posts
                            if post_id in active_post_ids or count < 1000:
                                posts_data[post_id] = {
                                    'post_id': post_id,
                                    'user_id': data.get('user_id', 0),
                                    'content': data.get('content', '')[:100]  # Truncate for display
                                }
                            count += 1
                    except:
                        continue
                posts_consumer.close()
            except Exception as e:
                logger.debug(f"Error reading posts: {e}")
            
            # Calculate engagement scores and filter viral posts
            results = []
            # Check all posts that have engagement data
            all_post_ids = set(posts_data.keys()) | active_post_ids
            
            for post_id in all_post_ids:
                likes = likes_count.get(post_id, 0)
                comments = comments_count.get(post_id, 0)
                engagement_score = likes + (comments * 3)
                
                # Only include posts with engagement score > 5 (lower threshold to show more posts)
                if engagement_score > 5:
                    # Get post info if available, otherwise create minimal entry
                    if post_id in posts_data:
                        post_info = posts_data[post_id]
                    else:
                        post_info = {'user_id': 0, 'content': 'Post content unavailable'}
                    
                    results.append({
                        'post_id': post_id,
                        'user_id': post_info['user_id'],
                        'content': post_info['content'],
                        'likes': likes,
                        'comments': comments,
                        'score': engagement_score
                    })
            
            # Sort by engagement score and return top 10
            if results:
                results.sort(key=lambda x: x['score'], reverse=True)
                return results[:10]
        except Exception as e:
            logger.error(f"Error querying viral posts: {e}")
        return []
    
    def update_loop(self):
        """Continuously update trending data"""
        logger.info("Starting trending data update loop...")
        while self.running:
            try:
                # Query data
                hashtags = self.query_trending_hashtags()
                viral = self.query_viral_posts()
                
                if hashtags:
                    self.hashtag_data = hashtags
                    socketio.emit('hashtag_update', {'data': hashtags})
                
                if viral:
                    self.viral_posts = viral
                    socketio.emit('viral_update', {'data': viral})
                
                # Emit metrics
                socketio.emit('metrics_update', {
                    'trending_tags': len(self.hashtag_data),
                    'viral_posts': len(self.viral_posts),
                    'timestamp': datetime.now().isoformat()
                })
                
            except Exception as e:
                logger.error(f"Update loop error: {e}")
            
            time.sleep(3)  # Update every 3 seconds
    
    def start(self):
        """Start background update thread"""
        self.running = True
        thread = threading.Thread(target=self.update_loop, daemon=True)
        thread.start()
    
    def stop(self):
        """Stop background updates"""
        self.running = False

trending_manager = TrendingDataManager()

@app.route('/')
def index():
    """Serve dashboard HTML"""
    return render_template('index.html')

@app.route('/api/trending/hashtags')
def api_trending_hashtags():
    """REST API endpoint for trending hashtags"""
    return jsonify(trending_manager.hashtag_data)

@app.route('/api/viral/posts')
def api_viral_posts():
    """REST API endpoint for viral posts"""
    return jsonify(trending_manager.viral_posts)

@app.route('/api/health')
def api_health():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'ksqldb': 'connected' if trending_manager.hashtag_data else 'pending',
        'timestamp': datetime.now().isoformat()
    })

@socketio.on('connect')
def handle_connect():
    """Handle WebSocket connection"""
    logger.info("Client connected")
    emit('connection_response', {'status': 'connected'})
    
    # Send current data
    emit('hashtag_update', {'data': trending_manager.hashtag_data})
    emit('viral_update', {'data': trending_manager.viral_posts})

@socketio.on('disconnect')
def handle_disconnect():
    """Handle WebSocket disconnection"""
    logger.info("Client disconnected")

if __name__ == '__main__':
    trending_manager.start()
    socketio.run(app, host='0.0.0.0', port=5000, debug=False, allow_unsafe_werkzeug=True)
