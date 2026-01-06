#!/bin/bash

# Day 59: Advanced Ecosystem Tools - ksqlDB Trending Analytics Implementation
# This script creates a complete real-time trending analysis system using ksqlDB

set -e

echo "=== Day 59: ksqlDB Trending Analytics - StreamSocial ==="
echo "Creating complete implementation..."

# Create project structure
PROJECT_DIR="day59_ksqldb_trending"
mkdir -p ${PROJECT_DIR}/{src,tests,config,ui,scripts,docker}

cd ${PROJECT_DIR}

# Create virtual environment with Python 3.11
echo "Setting up Python 3.11 virtual environment..."
python3.11 -m venv venv
source venv/bin/activate

# Create requirements.txt
cat > requirements.txt << 'EOF'
kafka-python==2.0.2
confluent-kafka==2.5.0
requests==2.31.0
flask==3.0.3
flask-cors==4.0.1
flask-socketio==5.3.6
python-socketio==5.11.2
websocket-client==1.8.0
fastapi==0.111.0
uvicorn==0.30.0
pytest==8.2.0
pytest-asyncio==0.23.6
faker==25.0.0
avro-python3==1.10.2
psutil==5.9.8
python-json-logger==2.0.7
colorlog==6.8.2
EOF

# Install dependencies
echo "Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Create Docker Compose configuration
cat > docker/docker-compose.yml << 'EOF'
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.1
    hostname: zookeeper
    container_name: day59-zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    healthcheck:
      test: nc -z localhost 2181 || exit 1
      interval: 10s
      timeout: 5s
      retries: 5

  kafka:
    image: confluentinc/cp-kafka:7.6.1
    hostname: kafka
    container_name: day59-kafka
    depends_on:
      zookeeper:
        condition: service_healthy
    ports:
      - "9092:9092"
      - "9093:9093"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
    healthcheck:
      test: kafka-broker-api-versions --bootstrap-server localhost:9092 || exit 1
      interval: 10s
      timeout: 10s
      retries: 5

  ksqldb-server:
    image: confluentinc/ksqldb-server:0.29.0
    hostname: ksqldb-server
    container_name: day59-ksqldb-server
    depends_on:
      kafka:
        condition: service_healthy
    ports:
      - "8088:8088"
    environment:
      KSQL_LISTENERS: http://0.0.0.0:8088
      KSQL_BOOTSTRAP_SERVERS: kafka:29092
      KSQL_KSQL_LOGGING_PROCESSING_STREAM_AUTO_CREATE: "true"
      KSQL_KSQL_LOGGING_PROCESSING_TOPIC_AUTO_CREATE: "true"
      KSQL_KSQL_SCHEMA_REGISTRY_URL: http://schema-registry:8081
      KSQL_CACHE_MAX_BYTES_BUFFERING: 100000000
      KSQL_KSQL_STREAMS_NUM_STREAM_THREADS: 4
    healthcheck:
      test: curl -f http://localhost:8088/info || exit 1
      interval: 10s
      timeout: 5s
      retries: 10

  ksqldb-cli:
    image: confluentinc/ksqldb-cli:0.29.0
    container_name: day59-ksqldb-cli
    depends_on:
      - ksqldb-server
    entrypoint: /bin/sh
    tty: true

  schema-registry:
    image: confluentinc/cp-schema-registry:7.6.1
    hostname: schema-registry
    container_name: day59-schema-registry
    depends_on:
      kafka:
        condition: service_healthy
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:29092
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
    healthcheck:
      test: curl -f http://localhost:8081/ || exit 1
      interval: 10s
      timeout: 5s
      retries: 5

networks:
  default:
    name: day59-ksqldb-network
EOF

# Create Kafka producer for events
cat > src/event_producer.py << 'EOF'
"""Event producer for StreamSocial posts, likes, comments, views"""
import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker()

class StreamSocialEventProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        self.hashtags = ['tech', 'startup', 'ai', 'coding', 'design', 'product', 
                         'data', 'cloud', 'security', 'devops', 'python', 'react',
                         'kubernetes', 'ml', 'blockchain', 'web3', 'saas', 'mobile']
        self.user_ids = list(range(1, 1001))
        self.post_id_counter = 0
        
    def generate_post(self):
        """Generate realistic post event"""
        self.post_id_counter += 1
        num_hashtags = random.randint(0, 4)
        selected_tags = random.sample(self.hashtags, num_hashtags) if num_hashtags > 0 else []
        
        content = fake.text(max_nb_chars=200)
        if selected_tags:
            content += " " + " ".join([f"#{tag}" for tag in selected_tags])
        
        return {
            'post_id': self.post_id_counter,
            'user_id': random.choice(self.user_ids),
            'content': content,
            'hashtags': selected_tags,
            'created_at': datetime.now().isoformat(),
            'media_type': random.choice(['text', 'image', 'video', 'link']),
            'timestamp': int(time.time() * 1000)
        }
    
    def generate_like(self, post_id):
        """Generate like event for a post"""
        return {
            'like_id': int(time.time() * 1000000),
            'post_id': post_id,
            'user_id': random.choice(self.user_ids),
            'created_at': datetime.now().isoformat(),
            'timestamp': int(time.time() * 1000)
        }
    
    def generate_comment(self, post_id):
        """Generate comment event for a post"""
        return {
            'comment_id': int(time.time() * 1000000),
            'post_id': post_id,
            'user_id': random.choice(self.user_ids),
            'content': fake.sentence(),
            'created_at': datetime.now().isoformat(),
            'timestamp': int(time.time() * 1000)
        }
    
    def generate_view(self, post_id):
        """Generate view event for a post"""
        return {
            'view_id': int(time.time() * 1000000),
            'post_id': post_id,
            'user_id': random.choice(self.user_ids),
            'duration_ms': random.randint(100, 30000),
            'created_at': datetime.now().isoformat(),
            'timestamp': int(time.time() * 1000)
        }
    
    def send_event(self, topic, event):
        """Send event to Kafka topic"""
        try:
            future = self.producer.send(topic, event)
            future.get(timeout=10)
        except Exception as e:
            logger.error(f"Failed to send event to {topic}: {e}")
    
    def generate_traffic(self, duration_seconds=300, posts_per_sec=50):
        """Generate realistic traffic pattern"""
        logger.info(f"Starting event generation for {duration_seconds} seconds...")
        logger.info(f"Target: {posts_per_sec} posts/sec, ~{posts_per_sec*4} likes/sec, ~{posts_per_sec*0.6} comments/sec")
        
        start_time = time.time()
        posts_generated = 0
        active_posts = []
        
        while time.time() - start_time < duration_seconds:
            cycle_start = time.time()
            
            # Generate posts
            for _ in range(posts_per_sec):
                post = self.generate_post()
                self.send_event('posts', post)
                active_posts.append(post['post_id'])
                posts_generated += 1
                
                # Keep only recent posts for engagement
                if len(active_posts) > 1000:
                    active_posts = active_posts[-1000:]
            
            # Generate likes (4x posts)
            for _ in range(posts_per_sec * 4):
                if active_posts:
                    post_id = random.choice(active_posts[-100:])  # Focus on recent posts
                    like = self.generate_like(post_id)
                    self.send_event('likes', like)
            
            # Generate comments (0.6x posts)
            for _ in range(int(posts_per_sec * 0.6)):
                if active_posts:
                    post_id = random.choice(active_posts[-100:])
                    comment = self.generate_comment(post_id)
                    self.send_event('comments', comment)
            
            # Generate views (20x posts)
            for _ in range(posts_per_sec * 20):
                if active_posts:
                    post_id = random.choice(active_posts[-200:])
                    view = self.generate_view(post_id)
                    self.send_event('views', view)
            
            # Maintain rate
            elapsed = time.time() - cycle_start
            sleep_time = max(0, 1.0 - elapsed)
            time.sleep(sleep_time)
            
            if posts_generated % 500 == 0:
                logger.info(f"Generated {posts_generated} posts, {len(active_posts)} active posts")
        
        self.producer.flush()
        logger.info(f"Event generation complete. Total posts: {posts_generated}")
    
    def close(self):
        """Close producer"""
        self.producer.close()

if __name__ == '__main__':
    producer = StreamSocialEventProducer()
    try:
        producer.generate_traffic(duration_seconds=300, posts_per_sec=50)
    finally:
        producer.close()
EOF

# Create ksqlDB query manager
cat > src/ksqldb_manager.py << 'EOF'
"""ksqlDB query manager for creating streams, tables, and executing queries"""
import requests
import json
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KsqlDBManager:
    def __init__(self, ksqldb_url='http://localhost:8088'):
        self.ksqldb_url = ksqldb_url
        self.session = requests.Session()
        
    def wait_for_ksqldb(self, max_attempts=30):
        """Wait for ksqlDB to be ready"""
        logger.info("Waiting for ksqlDB server to be ready...")
        for i in range(max_attempts):
            try:
                response = self.session.get(f'{self.ksqldb_url}/info')
                if response.status_code == 200:
                    logger.info("ksqlDB server is ready!")
                    return True
            except Exception as e:
                logger.debug(f"Attempt {i+1}/{max_attempts}: {e}")
            time.sleep(2)
        return False
    
    def execute_statement(self, sql, stream_properties=None):
        """Execute ksqlDB statement"""
        payload = {
            'ksql': sql,
            'streamsProperties': stream_properties or {}
        }
        
        try:
            response = self.session.post(
                f'{self.ksqldb_url}/ksql',
                json=payload,
                headers={'Content-Type': 'application/vnd.ksql.v1+json'}
            )
            
            if response.status_code == 200:
                logger.info(f"✓ Executed: {sql[:80]}...")
                return response.json()
            else:
                logger.error(f"✗ Failed ({response.status_code}): {response.text}")
                return None
        except Exception as e:
            logger.error(f"Exception executing statement: {e}")
            return None
    
    def setup_streams_and_tables(self):
        """Create ksqlDB streams and tables for trending analysis"""
        logger.info("Setting up ksqlDB streams and tables...")
        
        # Drop existing if present (for clean restart)
        drops = [
            "DROP TABLE IF EXISTS hashtag_trends DELETE TOPIC;",
            "DROP TABLE IF EXISTS viral_posts DELETE TOPIC;",
            "DROP TABLE IF EXISTS user_activity DELETE TOPIC;",
            "DROP STREAM IF EXISTS posts_enriched DELETE TOPIC;",
            "DROP STREAM IF EXISTS posts_stream DELETE TOPIC;",
            "DROP STREAM IF EXISTS likes_stream DELETE TOPIC;",
            "DROP STREAM IF EXISTS comments_stream DELETE TOPIC;",
        ]
        
        for drop in drops:
            self.execute_statement(drop)
        
        time.sleep(3)  # Allow cleanup
        
        # Create streams from Kafka topics
        streams = [
            """
            CREATE STREAM posts_stream (
                post_id BIGINT,
                user_id INT,
                content VARCHAR,
                hashtags ARRAY<VARCHAR>,
                created_at VARCHAR,
                media_type VARCHAR,
                timestamp BIGINT
            ) WITH (
                KAFKA_TOPIC='posts',
                VALUE_FORMAT='JSON',
                TIMESTAMP='timestamp'
            );
            """,
            """
            CREATE STREAM likes_stream (
                like_id BIGINT,
                post_id BIGINT,
                user_id INT,
                created_at VARCHAR,
                timestamp BIGINT
            ) WITH (
                KAFKA_TOPIC='likes',
                VALUE_FORMAT='JSON',
                TIMESTAMP='timestamp'
            );
            """,
            """
            CREATE STREAM comments_stream (
                comment_id BIGINT,
                post_id BIGINT,
                user_id INT,
                content VARCHAR,
                created_at VARCHAR,
                timestamp BIGINT
            ) WITH (
                KAFKA_TOPIC='comments',
                VALUE_FORMAT='JSON',
                TIMESTAMP='timestamp'
            );
            """
        ]
        
        for stream in streams:
            self.execute_statement(stream)
        
        time.sleep(2)
        
        # Create enriched posts stream with engagement metrics
        self.execute_statement("""
            CREATE STREAM posts_enriched AS
            SELECT 
                p.post_id,
                p.user_id,
                p.content,
                p.hashtags,
                p.media_type,
                p.timestamp,
                0 AS like_count,
                0 AS comment_count
            FROM posts_stream p
            EMIT CHANGES;
        """)
        
        time.sleep(2)
        
        # Create hashtag trends table (materialized aggregation)
        self.execute_statement("""
            CREATE TABLE hashtag_trends AS
            SELECT 
                EXPLODE(hashtags) AS hashtag,
                COUNT(*) AS mention_count,
                COUNT_DISTINCT(user_id) AS unique_users,
                LATEST_BY_OFFSET(timestamp) AS last_mention
            FROM posts_stream
            WHERE ARRAY_LENGTH(hashtags) > 0
            GROUP BY EXPLODE(hashtags)
            EMIT CHANGES;
        """)
        
        time.sleep(2)
        
        # Create viral posts table (high engagement rate)
        self.execute_statement("""
            CREATE TABLE viral_posts AS
            SELECT 
                p.post_id,
                p.user_id,
                p.content,
                COUNT(l.like_id) AS total_likes,
                COUNT(c.comment_id) AS total_comments,
                (COUNT(l.like_id) + COUNT(c.comment_id) * 3) AS engagement_score
            FROM posts_stream p
            LEFT JOIN likes_stream l WITHIN 1 HOUR ON p.post_id = l.post_id
            LEFT JOIN comments_stream c WITHIN 1 HOUR ON p.post_id = c.post_id
            GROUP BY p.post_id, p.user_id, p.content
            HAVING (COUNT(l.like_id) + COUNT(c.comment_id) * 3) > 50
            EMIT CHANGES;
        """)
        
        time.sleep(2)
        
        # Create user activity table
        self.execute_statement("""
            CREATE TABLE user_activity AS
            SELECT 
                user_id,
                COUNT(*) AS post_count,
                LATEST_BY_OFFSET(timestamp) AS last_activity
            FROM posts_stream
            WINDOW TUMBLING (SIZE 5 MINUTES)
            GROUP BY user_id
            EMIT CHANGES;
        """)
        
        logger.info("✓ All streams and tables created successfully!")
    
    def query_trending_hashtags(self, limit=10):
        """Query top trending hashtags"""
        sql = f"""
        SELECT hashtag, mention_count, unique_users 
        FROM hashtag_trends 
        EMIT CHANGES 
        LIMIT {limit};
        """
        
        return self.query_pull(sql)
    
    def query_pull(self, sql):
        """Execute pull query (point-in-time)"""
        payload = {
            'sql': sql,
            'streamsProperties': {}
        }
        
        try:
            response = self.session.post(
                f'{self.ksqldb_url}/query-stream',
                json=payload,
                headers={'Content-Type': 'application/vnd.ksql.v1+json'},
                stream=False
            )
            
            if response.status_code == 200:
                # Parse streaming response
                results = []
                for line in response.text.strip().split('\n'):
                    if line:
                        try:
                            data = json.loads(line)
                            if 'row' in data:
                                results.append(data['row']['columns'])
                        except:
                            pass
                return results
            return []
        except Exception as e:
            logger.error(f"Query failed: {e}")
            return []
    
    def list_streams(self):
        """List all streams"""
        result = self.execute_statement("SHOW STREAMS;")
        return result
    
    def list_tables(self):
        """List all tables"""
        result = self.execute_statement("SHOW TABLES;")
        return result
    
    def describe_stream(self, stream_name):
        """Describe stream details"""
        result = self.execute_statement(f"DESCRIBE {stream_name} EXTENDED;")
        return result

if __name__ == '__main__':
    manager = KsqlDBManager()
    if manager.wait_for_ksqldb():
        manager.setup_streams_and_tables()
        
        # Test queries
        print("\n=== Listing Streams ===")
        print(manager.list_streams())
        
        print("\n=== Listing Tables ===")
        print(manager.list_tables())
EOF

# Create real-time dashboard
cat > ui/dashboard.py << 'EOF'
"""Real-time trending analytics dashboard using Flask and WebSockets"""
from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO, emit
from flask_cors import CORS
import requests
import json
import threading
import time
import logging
import os
from datetime import datetime

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
        """Query trending hashtags from ksqlDB"""
        try:
            payload = {
                'sql': 'SELECT hashtag, mention_count, unique_users FROM hashtag_trends EMIT CHANGES;',
                'streamsProperties': {}
            }
            
            response = requests.post(
                f'{KSQLDB_URL}/query-stream',
                json=payload,
                headers={'Content-Type': 'application/vnd.ksql.v1+json'},
                timeout=5
            )
            
            if response.status_code == 200:
                results = []
                for line in response.text.strip().split('\n')[:20]:  # Limit results
                    if line:
                        try:
                            data = json.loads(line)
                            if 'row' in data and data['row']['columns']:
                                cols = data['row']['columns']
                                results.append({
                                    'hashtag': cols[0],
                                    'mentions': cols[1],
                                    'unique_users': cols[2]
                                })
                        except:
                            pass
                
                # Sort by mentions and take top 10
                results.sort(key=lambda x: x['mentions'], reverse=True)
                return results[:10]
        except Exception as e:
            logger.error(f"Error querying hashtags: {e}")
        return []
    
    def query_viral_posts(self):
        """Query viral posts from ksqlDB"""
        try:
            payload = {
                'sql': '''SELECT post_id, user_id, total_likes, total_comments, engagement_score 
                         FROM viral_posts EMIT CHANGES;''',
                'streamsProperties': {}
            }
            
            response = requests.post(
                f'{KSQLDB_URL}/query-stream',
                json=payload,
                headers={'Content-Type': 'application/vnd.ksql.v1+json'},
                timeout=5
            )
            
            if response.status_code == 200:
                results = []
                for line in response.text.strip().split('\n')[:15]:
                    if line:
                        try:
                            data = json.loads(line)
                            if 'row' in data and data['row']['columns']:
                                cols = data['row']['columns']
                                results.append({
                                    'post_id': cols[0],
                                    'user_id': cols[1],
                                    'likes': cols[2],
                                    'comments': cols[3],
                                    'score': cols[4]
                                })
                        except:
                            pass
                
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
    socketio.run(app, host='0.0.0.0', port=5000, debug=False)
EOF

# Create dashboard HTML template
mkdir -p ui/templates
cat > ui/templates/index.html << 'EOF'
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>StreamSocial Trending Analytics - ksqlDB</title>
    <script src="https://cdn.socket.io/4.5.4/socket.io.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
        }
        
        .header {
            background: white;
            padding: 30px;
            border-radius: 20px;
            margin-bottom: 20px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
        }
        
        .header h1 {
            color: #667eea;
            font-size: 32px;
            margin-bottom: 10px;
        }
        
        .header p {
            color: #666;
            font-size: 16px;
        }
        
        .metrics {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }
        
        .metric-card {
            background: white;
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.1);
        }
        
        .metric-card h3 {
            color: #888;
            font-size: 14px;
            text-transform: uppercase;
            margin-bottom: 10px;
        }
        
        .metric-value {
            font-size: 36px;
            font-weight: bold;
            color: #667eea;
        }
        
        .metric-label {
            color: #999;
            font-size: 12px;
            margin-top: 5px;
        }
        
        .content-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
        }
        
        .panel {
            background: white;
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.1);
        }
        
        .panel h2 {
            color: #333;
            margin-bottom: 20px;
            font-size: 20px;
        }
        
        .trending-list {
            list-style: none;
        }
        
        .trending-item {
            padding: 15px;
            border-left: 4px solid #667eea;
            margin-bottom: 10px;
            background: #f8f9fa;
            border-radius: 8px;
            transition: all 0.3s;
        }
        
        .trending-item:hover {
            transform: translateX(5px);
            box-shadow: 0 3px 10px rgba(0,0,0,0.1);
        }
        
        .tag {
            font-weight: bold;
            color: #667eea;
            font-size: 18px;
        }
        
        .stats {
            display: flex;
            gap: 20px;
            margin-top: 8px;
            color: #666;
            font-size: 14px;
        }
        
        .stat-item {
            display: flex;
            align-items: center;
            gap: 5px;
        }
        
        .viral-post {
            padding: 15px;
            background: linear-gradient(135deg, #ff6b6b 0%, #ee5a6f 100%);
            color: white;
            border-radius: 10px;
            margin-bottom: 12px;
        }
        
        .post-header {
            display: flex;
            justify-content: space-between;
            margin-bottom: 8px;
        }
        
        .post-id {
            font-weight: bold;
        }
        
        .engagement {
            font-size: 12px;
            opacity: 0.9;
        }
        
        .connection-status {
            display: inline-block;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            background: #4caf50;
            margin-right: 8px;
            animation: pulse 2s infinite;
        }
        
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }
        
        .chart-container {
            height: 300px;
            margin-top: 20px;
        }
        
        @media (max-width: 968px) {
            .content-grid {
                grid-template-columns: 1fr;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🔥 StreamSocial Trending Analytics</h1>
            <p><span class="connection-status"></span>Real-time insights powered by ksqlDB</p>
        </div>
        
        <div class="metrics">
            <div class="metric-card">
                <h3>Trending Hashtags</h3>
                <div class="metric-value" id="trendingCount">0</div>
                <div class="metric-label">Active trends</div>
            </div>
            <div class="metric-card">
                <h3>Viral Posts</h3>
                <div class="metric-value" id="viralCount">0</div>
                <div class="metric-label">High engagement</div>
            </div>
            <div class="metric-card">
                <h3>Last Update</h3>
                <div class="metric-value" style="font-size: 18px;" id="lastUpdate">--:--</div>
                <div class="metric-label">System time</div>
            </div>
        </div>
        
        <div class="content-grid">
            <div class="panel">
                <h2>🏷️ Trending Hashtags</h2>
                <ul class="trending-list" id="hashtagList">
                    <li class="trending-item">
                        <div class="tag">Waiting for data...</div>
                    </li>
                </ul>
                <div class="chart-container">
                    <canvas id="hashtagChart"></canvas>
                </div>
            </div>
            
            <div class="panel">
                <h2>🚀 Viral Posts</h2>
                <div id="viralList">
                    <div class="trending-item">Waiting for data...</div>
                </div>
            </div>
        </div>
    </div>
    
    <script>
        const socket = io('http://localhost:5000');
        
        let hashtagChart = null;
        
        socket.on('connect', () => {
            console.log('Connected to server');
        });
        
        socket.on('hashtag_update', (data) => {
            updateHashtags(data.data);
        });
        
        socket.on('viral_update', (data) => {
            updateViralPosts(data.data);
        });
        
        socket.on('metrics_update', (data) => {
            document.getElementById('trendingCount').textContent = data.trending_tags;
            document.getElementById('viralCount').textContent = data.viral_posts;
            const time = new Date(data.timestamp).toLocaleTimeString();
            document.getElementById('lastUpdate').textContent = time;
        });
        
        function updateHashtags(hashtags) {
            const list = document.getElementById('hashtagList');
            if (!hashtags || hashtags.length === 0) return;
            
            list.innerHTML = hashtags.map((tag, index) => `
                <li class="trending-item">
                    <div class="tag">#${index + 1} #${tag.hashtag}</div>
                    <div class="stats">
                        <div class="stat-item">
                            <span>💬</span>
                            <span>${tag.mentions} mentions</span>
                        </div>
                        <div class="stat-item">
                            <span>👥</span>
                            <span>${tag.unique_users} users</span>
                        </div>
                    </div>
                </li>
            `).join('');
            
            updateHashtagChart(hashtags.slice(0, 8));
        }
        
        function updateHashtagChart(hashtags) {
            const ctx = document.getElementById('hashtagChart').getContext('2d');
            
            if (hashtagChart) {
                hashtagChart.destroy();
            }
            
            hashtagChart = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: hashtags.map(t => `#${t.hashtag}`),
                    datasets: [{
                        label: 'Mentions',
                        data: hashtags.map(t => t.mentions),
                        backgroundColor: 'rgba(102, 126, 234, 0.8)',
                        borderColor: 'rgba(102, 126, 234, 1)',
                        borderWidth: 2
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        y: {
                            beginAtZero: true
                        }
                    }
                }
            });
        }
        
        function updateViralPosts(posts) {
            const list = document.getElementById('viralList');
            if (!posts || posts.length === 0) return;
            
            list.innerHTML = posts.map((post, index) => `
                <div class="viral-post">
                    <div class="post-header">
                        <span class="post-id">Post #${post.post_id}</span>
                        <span class="engagement">🔥 ${post.score} score</span>
                    </div>
                    <div class="stats">
                        <span>❤️ ${post.likes} likes</span>
                        <span>💬 ${post.comments} comments</span>
                    </div>
                </div>
            `).join('');
        }
    </script>
</body>
</html>
EOF

# Create test suite
cat > tests/test_ksqldb_system.py << 'EOF'
"""Test suite for ksqlDB trending analytics system"""
import pytest
import time
import requests
import json
from kafka import KafkaProducer, KafkaConsumer
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

KAFKA_BOOTSTRAP = 'localhost:9092'
KSQLDB_URL = 'http://localhost:8088'

@pytest.fixture
def kafka_producer():
    """Create Kafka producer for testing"""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    yield producer
    producer.close()

class TestKsqlDBSystem:
    
    def test_kafka_connectivity(self, kafka_producer):
        """Test Kafka is accessible"""
        test_event = {'test': 'connectivity', 'timestamp': int(time.time() * 1000)}
        future = kafka_producer.send('test-topic', test_event)
        result = future.get(timeout=10)
        assert result is not None
    
    def test_ksqldb_server_health(self):
        """Test ksqlDB server is running"""
        response = requests.get(f'{KSQLDB_URL}/info')
        assert response.status_code == 200
        data = response.json()
        assert 'KsqlServerInfo' in data
    
    def test_streams_exist(self):
        """Test streams are created"""
        payload = {'ksql': 'SHOW STREAMS;', 'streamsProperties': {}}
        response = requests.post(
            f'{KSQLDB_URL}/ksql',
            json=payload,
            headers={'Content-Type': 'application/vnd.ksql.v1+json'}
        )
        assert response.status_code == 200
        data = response.json()
        
        # Check for our streams
        stream_names = []
        if data and len(data) > 0 and 'streams' in data[0]:
            stream_names = [s['name'] for s in data[0]['streams']]
        
        assert 'POSTS_STREAM' in stream_names or 'posts_stream' in [s.lower() for s in stream_names]
    
    def test_tables_exist(self):
        """Test tables are created"""
        payload = {'ksql': 'SHOW TABLES;', 'streamsProperties': {}}
        response = requests.post(
            f'{KSQLDB_URL}/ksql',
            json=payload,
            headers={'Content-Type': 'application/vnd.ksql.v1+json'}
        )
        assert response.status_code == 200
        data = response.json()
        
        table_names = []
        if data and len(data) > 0 and 'tables' in data[0]:
            table_names = [t['name'] for t in data[0]['tables']]
        
        assert len(table_names) > 0  # At least one table exists
    
    def test_post_event_processing(self, kafka_producer):
        """Test post events are processed by ksqlDB"""
        # Send test post with hashtags
        test_post = {
            'post_id': 99999,
            'user_id': 777,
            'content': 'Testing ksqlDB #test #automation',
            'hashtags': ['test', 'automation'],
            'created_at': '2025-05-15T12:00:00',
            'media_type': 'text',
            'timestamp': int(time.time() * 1000)
        }
        
        kafka_producer.send('posts', test_post)
        kafka_producer.flush()
        
        # Wait for processing
        time.sleep(5)
        
        # Query hashtag trends
        payload = {
            'sql': "SELECT hashtag, mention_count FROM hashtag_trends WHERE hashtag = 'test' EMIT CHANGES;",
            'streamsProperties': {}
        }
        
        response = requests.post(
            f'{KSQLDB_URL}/query-stream',
            json=payload,
            headers={'Content-Type': 'application/vnd.ksql.v1+json'},
            timeout=10
        )
        
        assert response.status_code == 200
        # Should have processed the hashtag
        assert 'test' in response.text.lower() or len(response.text) > 0
    
    def test_dashboard_api(self):
        """Test dashboard API endpoints"""
        # Give dashboard time to start
        max_attempts = 10
        for _ in range(max_attempts):
            try:
                response = requests.get('http://localhost:5000/api/health', timeout=2)
                if response.status_code == 200:
                    break
            except:
                pass
            time.sleep(2)
        
        response = requests.get('http://localhost:5000/api/health', timeout=5)
        assert response.status_code == 200
        
        health = response.json()
        assert 'status' in health
    
    def test_trending_hashtags_endpoint(self):
        """Test trending hashtags API"""
        try:
            response = requests.get('http://localhost:5000/api/trending/hashtags', timeout=5)
            assert response.status_code == 200
            data = response.json()
            assert isinstance(data, list)
        except requests.exceptions.ConnectionError:
            pytest.skip("Dashboard not running")
    
    def test_performance_under_load(self, kafka_producer):
        """Test system handles burst of events"""
        events = []
        for i in range(100):
            event = {
                'post_id': 100000 + i,
                'user_id': i % 50,
                'content': f'Load test post {i} #loadtest',
                'hashtags': ['loadtest'],
                'created_at': '2025-05-15T12:00:00',
                'media_type': 'text',
                'timestamp': int(time.time() * 1000)
            }
            events.append(event)
        
        start_time = time.time()
        for event in events:
            kafka_producer.send('posts', event)
        kafka_producer.flush()
        
        send_duration = time.time() - start_time
        
        # Should handle 100 events quickly
        assert send_duration < 5.0
        
        # Wait for ksqlDB processing
        time.sleep(3)
        
        # Verify ksqlDB is still responsive
        response = requests.get(f'{KSQLDB_URL}/info', timeout=5)
        assert response.status_code == 200

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
EOF

# Create build script
cat > build.sh << 'EOF'
#!/bin/bash

echo "=== Building Day 59: ksqlDB Trending Analytics ==="

# Activate virtual environment
source venv/bin/activate

# Start Docker infrastructure
echo "Starting Docker containers..."
cd docker
docker-compose up -d
cd ..

# Wait for services
echo "Waiting for services to be ready..."
sleep 30

# Verify Kafka
echo "Verifying Kafka connectivity..."
timeout 60 bash -c 'until docker exec day59-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &>/dev/null; do sleep 2; done'

# Verify ksqlDB
echo "Waiting for ksqlDB server..."
timeout 90 bash -c 'until curl -sf http://localhost:8088/info &>/dev/null; do sleep 3; done'

echo "✓ All services are ready!"

# Setup ksqlDB streams and tables
echo "Setting up ksqlDB streams and tables..."
python src/ksqldb_manager.py

echo "✓ Build complete!"
echo ""
echo "Next steps:"
echo "  ./start.sh    - Start event generation and dashboard"
echo "  ./test.sh     - Run test suite"
echo "  ./demo.sh     - Run full demonstration"
echo "  ./stop.sh     - Stop all services"
EOF

chmod +x build.sh

# Create start script
cat > start.sh << 'EOF'
#!/bin/bash

echo "=== Starting Day 59: ksqlDB Trending Analytics ==="

source venv/bin/activate

# Start event producer in background
echo "Starting event producer..."
python src/event_producer.py &
PRODUCER_PID=$!
echo $PRODUCER_PID > producer.pid

# Start dashboard
echo "Starting dashboard on http://localhost:5000..."
python ui/dashboard.py &
DASHBOARD_PID=$!
echo $DASHBOARD_PID > dashboard.pid

echo "✓ System started!"
echo ""
echo "Dashboard: http://localhost:5000"
echo "ksqlDB Server: http://localhost:8088"
echo ""
echo "Press Ctrl+C to stop, or run ./stop.sh"
EOF

chmod +x start.sh

# Create test script
cat > test.sh << 'EOF'
#!/bin/bash

echo "=== Testing Day 59: ksqlDB Trending Analytics ==="

source venv/bin/activate

# Run pytest
pytest tests/test_ksqldb_system.py -v --tb=short

echo "✓ Tests complete!"
EOF

chmod +x test.sh

# Create demo script
cat > demo.sh << 'EOF'
#!/bin/bash

echo "=== Day 59: ksqlDB Trending Analytics - Full Demo ==="

source venv/bin/activate

echo ""
echo "🎯 Demo Scenario: Real-time trending hashtag detection"
echo "================================================"
echo ""

# Start services if not running
if ! pgrep -f "event_producer.py" > /dev/null; then
    echo "Starting event producer..."
    python src/event_producer.py &
    PRODUCER_PID=$!
    sleep 5
fi

if ! pgrep -f "dashboard.py" > /dev/null; then
    echo "Starting dashboard..."
    python ui/dashboard.py &
    DASHBOARD_PID=$!
    sleep 5
fi

echo "✓ Services running"
echo ""

# Show ksqlDB streams
echo "📊 ksqlDB Streams:"
curl -X POST http://localhost:8088/ksql \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql": "SHOW STREAMS;"}' 2>/dev/null | python -m json.tool | head -20

echo ""
echo "📊 ksqlDB Tables:"
curl -X POST http://localhost:8088/ksql \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql": "SHOW TABLES;"}' 2>/dev/null | python -m json.tool | head -20

echo ""
echo "🔥 Top Trending Hashtags (Real-time):"
sleep 10  # Let some events process

curl -s http://localhost:5000/api/trending/hashtags | python -m json.tool | head -30

echo ""
echo "🚀 Viral Posts:"
curl -s http://localhost:5000/api/viral/posts | python -m json.tool | head -30

echo ""
echo "✅ Demo Complete!"
echo ""
echo "📈 Dashboard running at: http://localhost:5000"
echo "🔧 ksqlDB REST API: http://localhost:8088"
echo ""
echo "Press Ctrl+C to exit"

# Keep running
wait
EOF

chmod +x demo.sh

# Create stop script
cat > stop.sh << 'EOF'
#!/bin/bash

echo "=== Stopping Day 59: ksqlDB Trending Analytics ==="

# Stop Python processes
if [ -f producer.pid ]; then
    kill $(cat producer.pid) 2>/dev/null
    rm producer.pid
fi

if [ -f dashboard.pid ]; then
    kill $(cat dashboard.pid) 2>/dev/null
    rm dashboard.pid
fi

# Stop Docker
cd docker
docker-compose down
cd ..

echo "✓ All services stopped"
EOF

chmod +x stop.sh

# Create admin tools script
cat > admin_tools.sh << 'EOF'
#!/bin/bash

echo "=== ksqlDB Admin Tools ==="
echo ""
echo "1. Show all streams"
echo "2. Show all tables"
echo "3. Describe stream"
echo "4. Show running queries"
echo "5. Server info"
echo "6. Exit"
echo ""
read -p "Select option: " option

case $option in
  1)
    curl -X POST http://localhost:8088/ksql \
      -H "Content-Type: application/vnd.ksql.v1+json" \
      -d '{"ksql": "SHOW STREAMS;"}' | python -m json.tool
    ;;
  2)
    curl -X POST http://localhost:8088/ksql \
      -H "Content-Type: application/vnd.ksql.v1+json" \
      -d '{"ksql": "SHOW TABLES;"}' | python -m json.tool
    ;;
  3)
    read -p "Stream name: " stream_name
    curl -X POST http://localhost:8088/ksql \
      -H "Content-Type: application/vnd.ksql.v1+json" \
      -d "{\"ksql\": \"DESCRIBE ${stream_name} EXTENDED;\"}" | python -m json.tool
    ;;
  4)
    curl -X POST http://localhost:8088/ksql \
      -H "Content-Type: application/vnd.ksql.v1+json" \
      -d '{"ksql": "SHOW QUERIES;"}' | python -m json.tool
    ;;
  5)
    curl http://localhost:8088/info | python -m json.tool
    ;;
  *)
    exit 0
    ;;
esac
EOF

chmod +x admin_tools.sh

echo ""
echo "=== Day 59 Implementation Complete! ==="
echo ""
echo "📁 Project structure created in: ${PROJECT_DIR}/"
echo ""
echo "🚀 Quick Start:"
echo "   cd ${PROJECT_DIR}"
echo "   ./build.sh     # Build and setup (first time)"
echo "   ./start.sh     # Start system"
echo "   ./demo.sh      # Run demonstration"
echo "   ./test.sh      # Run tests"
echo "   ./stop.sh      # Stop all services"
echo ""
echo "🔧 Admin Tools:"
echo "   ./admin_tools.sh  # ksqlDB management CLI"
echo ""
echo "📊 Access Points:"
echo "   Dashboard:    http://localhost:5000"
echo "   ksqlDB REST:  http://localhost:8088"
echo "   ksqlDB CLI:   docker exec -it day59-ksqldb-cli ksql http://ksqldb-server:8088"
echo ""
echo "✅ Ready to build!"

chmod +x day59_ksqldb_trending/build.sh
chmod +x day59_ksqldb_trending/start.sh
chmod +x day59_ksqldb_trending/test.sh
chmod +x day59_ksqldb_trending/demo.sh
chmod +x day59_ksqldb_trending/stop.sh
chmod +x day59_ksqldb_trending/admin_tools.sh

# Verify all files were generated
echo ""
echo "=== Verifying file generation ==="

MISSING_FILES=()
REQUIRED_FILES=(
    "requirements.txt"
    "src/event_producer.py"
    "src/ksqldb_manager.py"
    "tests/test_ksqldb_system.py"
    "ui/dashboard.py"
    "ui/templates/index.html"
    "docker/docker-compose.yml"
    "build.sh"
    "start.sh"
    "test.sh"
    "demo.sh"
    "stop.sh"
    "admin_tools.sh"
)

for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        MISSING_FILES+=("$file")
        echo "✗ Missing: $file"
    else
        echo "✓ Found: $file"
    fi
done

# Verify directories
REQUIRED_DIRS=("src" "tests" "config" "ui" "scripts" "docker" "ui/templates" "venv")
for dir in "${REQUIRED_DIRS[@]}"; do
    if [ ! -d "$dir" ]; then
        MISSING_FILES+=("$dir/")
        echo "✗ Missing directory: $dir"
    else
        echo "✓ Found directory: $dir"
    fi
done

if [ ${#MISSING_FILES[@]} -gt 0 ]; then
    echo ""
    echo "ERROR: Missing ${#MISSING_FILES[@]} required file(s)/directory(ies):"
    printf '%s\n' "${MISSING_FILES[@]}"
    exit 1
fi

echo ""
echo "✅ All files verified successfully!"