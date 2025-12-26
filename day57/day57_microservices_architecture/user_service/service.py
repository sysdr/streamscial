"""User service business logic"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from shared.database import DatabaseConnection
from shared.event_schemas import EventFactory
from shared.kafka_client import EventProducer
import uuid
from datetime import datetime
import logging
import threading
import time

logger = logging.getLogger(__name__)

class UserService:
    def __init__(self):
        self.db = DatabaseConnection('userdb')
        self.db.connect()
        self.producer = EventProducer()
        self.setup_database()
        self.start_outbox_publisher()
    
    def setup_database(self):
        """Initialize database schema"""
        schema = """
            CREATE TABLE IF NOT EXISTS users (
                user_id VARCHAR(50) PRIMARY KEY,
                email VARCHAR(255) UNIQUE NOT NULL,
                username VARCHAR(100) UNIQUE NOT NULL,
                bio TEXT,
                created_at TIMESTAMP DEFAULT NOW()
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
        """
        for stmt in schema.split(';'):
            if stmt.strip():
                self.db.execute(stmt.strip())
        self.db.conn.commit()
        logger.info("User service database initialized")
    
    def register_user(self, email: str, username: str) -> dict:
        """Register new user with outbox pattern"""
        user_id = str(uuid.uuid4())
        
        # Create event
        event = EventFactory.user_registered(user_id, email, username)
        event_dict = {
            'event_type': event.event_type,
            'event_id': event.event_id,
            'aggregate_id': event.aggregate_id,
            'timestamp': event.timestamp,
            'data': event.data,
            'version': event.version
        }
        
        # Execute with outbox pattern
        business_query = """
            INSERT INTO users (user_id, email, username) 
            VALUES (%s, %s, %s)
        """
        
        self.db.execute_with_outbox(
            business_query,
            (user_id, email, username),
            event_dict,
            'user-events'
        )
        
        logger.info(f"User registered: {user_id}")
        return {'user_id': user_id, 'email': email, 'username': username}
    
    def get_user(self, user_id: str) -> dict:
        """Get user by ID"""
        result = self.db.execute(
            "SELECT * FROM users WHERE user_id = %s",
            (user_id,)
        )
        return result[0] if result else None
    
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
