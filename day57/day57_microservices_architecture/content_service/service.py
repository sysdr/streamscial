"""Content service business logic"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from shared.database import DatabaseConnection
from shared.event_schemas import EventFactory
from shared.kafka_client import EventProducer, EventConsumer
import uuid
import logging
import threading
import time

logger = logging.getLogger(__name__)

class ContentService:
    def __init__(self):
        self.db = DatabaseConnection('contentdb', port=5433)
        self.db.connect()
        self.producer = EventProducer()
        self.user_cache = {}  # Cache user data from UserRegistered events
        self.setup_database()
        self.start_outbox_publisher()
        self.start_event_consumers()
    
    def setup_database(self):
        """Initialize database schema"""
        schema = """
            CREATE TABLE IF NOT EXISTS posts (
                post_id VARCHAR(50) PRIMARY KEY,
                user_id VARCHAR(50) NOT NULL,
                content TEXT NOT NULL,
                likes_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT NOW()
            );
            
            CREATE TABLE IF NOT EXISTS likes (
                post_id VARCHAR(50),
                user_id VARCHAR(50),
                created_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (post_id, user_id)
            );
            
            CREATE TABLE IF NOT EXISTS outbox (
                id SERIAL PRIMARY KEY,
                event_id VARCHAR(50) UNIQUE NOT NULL,
                event_type VARCHAR(100) NOT NULL,
                aggregate_id VARCHAR(50) NOT NULL,
                event_data JSONB NOT NULL,
                topic VARCHAR(100) NOT NULL,
                published BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW(),
                published_at TIMESTAMP
            );
            
            CREATE INDEX IF NOT EXISTS idx_outbox_published ON outbox(published, created_at);
            CREATE INDEX IF NOT EXISTS idx_posts_user ON posts(user_id);
        """
        for stmt in schema.split(';'):
            if stmt.strip():
                self.db.execute(stmt.strip())
        self.db.conn.commit()
        logger.info("Content service database initialized")
    
    def create_post(self, user_id: str, content: str) -> dict:
        """Create new post with outbox pattern"""
        # Check if user exists in cache (eventual consistency)
        if user_id not in self.user_cache:
            logger.warning(f"User {user_id} not in cache, eventual consistency delay")
        
        post_id = str(uuid.uuid4())
        
        event = EventFactory.post_created(post_id, user_id, content)
        event_dict = {
            'event_type': event.event_type,
            'event_id': event.event_id,
            'aggregate_id': event.aggregate_id,
            'timestamp': event.timestamp,
            'data': event.data,
            'version': event.version
        }
        
        business_query = """
            INSERT INTO posts (post_id, user_id, content) 
            VALUES (%s, %s, %s)
        """
        
        self.db.execute_with_outbox(
            business_query,
            (post_id, user_id, content),
            event_dict,
            'content-events'
        )
        
        logger.info(f"Post created: {post_id}")
        return {'post_id': post_id, 'user_id': user_id, 'content': content}
    
    def like_post(self, post_id: str, user_id: str) -> dict:
        """Like a post"""
        event = EventFactory.post_liked(post_id, user_id)
        event_dict = {
            'event_type': event.event_type,
            'event_id': event.event_id,
            'aggregate_id': event.aggregate_id,
            'timestamp': event.timestamp,
            'data': event.data,
            'version': event.version
        }
        
        business_query = """
            INSERT INTO likes (post_id, user_id) VALUES (%s, %s)
            ON CONFLICT (post_id, user_id) DO NOTHING;
            UPDATE posts SET likes_count = likes_count + 1 WHERE post_id = %s;
        """
        
        self.db.execute_with_outbox(
            business_query,
            (post_id, user_id, post_id),
            event_dict,
            'content-events'
        )
        
        logger.info(f"Post liked: {post_id} by {user_id}")
        return {'post_id': post_id, 'user_id': user_id}
    
    def get_posts(self, limit: int = 10) -> list:
        """Get recent posts"""
        result = self.db.execute(
            "SELECT * FROM posts ORDER BY created_at DESC LIMIT %s",
            (limit,)
        )
        return result or []
    
    def handle_user_registered(self, event: dict):
        """Handle UserRegistered event - cache user data"""
        data = event.get('data', {})
        user_id = data.get('user_id')
        if user_id:
            self.user_cache[user_id] = {
                'email': data.get('email'),
                'username': data.get('username')
            }
            logger.info(f"Cached user: {user_id}")
    
    def start_event_consumers(self):
        """Start consuming events from other services"""
        consumer = EventConsumer('localhost:9092', 'content-service-group', ['user-events'])
        consumer.start(self.handle_user_registered)
        logger.info("Content service event consumers started")
    
    def start_outbox_publisher(self):
        """Background thread to publish events from outbox"""
        def publish_loop():
            while True:
                try:
                    events = self.db.get_unpublished_events()
                    for event in events:
                        self.producer.publish(
                            event['topic'],
                            event['event_data'],
                            key=event['aggregate_id']
                        )
                        self.db.mark_event_published(event['event_id'])
                        logger.info(f"Published event from outbox: {event['event_id']}")
                    time.sleep(1)
                except Exception as e:
                    logger.error(f"Outbox publisher error: {e}")
                    time.sleep(5)
        
        thread = threading.Thread(target=publish_loop, daemon=True)
        thread.start()
        logger.info("Outbox publisher started")
