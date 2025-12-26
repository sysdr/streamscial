"""Notification service business logic"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from shared.database import DatabaseConnection
from shared.kafka_client import EventConsumer
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)

class NotificationService:
    def __init__(self):
        self.db = DatabaseConnection('notificationdb', port=5434)
        self.db.connect()
        self.notifications_sent = 0
        self.notifications_by_type = defaultdict(int)
        self.setup_database()
        self.start_event_consumers()
    
    def setup_database(self):
        """Initialize database schema"""
        schema = """
            CREATE TABLE IF NOT EXISTS notifications (
                notification_id SERIAL PRIMARY KEY,
                user_id VARCHAR(50) NOT NULL,
                event_type VARCHAR(100) NOT NULL,
                message TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS idx_notifications_user ON notifications(user_id);
        """
        for stmt in schema.split(';'):
            if stmt.strip():
                self.db.execute(stmt.strip())
        self.db.conn.commit()
        logger.info("Notification service database initialized")
    
    def send_notification(self, user_id: str, event_type: str, message: str):
        """Send notification (store in DB, in real system would send push/email)"""
        query = """
            INSERT INTO notifications (user_id, event_type, message)
            VALUES (%s, %s, %s)
        """
        self.db.execute(query, (user_id, event_type, message))
        self.db.conn.commit()
        self.notifications_sent += 1
        self.notifications_by_type[event_type] += 1
        logger.info(f"Notification sent to {user_id}: {message}")
    
    def handle_event(self, event: dict):
        """Handle any domain event and send appropriate notification"""
        event_type = event.get('event_type')
        data = event.get('data', {})
        
        if event_type == 'UserRegistered':
            user_id = data.get('user_id')
            username = data.get('username')
            self.send_notification(
                user_id,
                event_type,
                f"Welcome to StreamSocial, {username}! Start sharing your thoughts."
            )
        
        elif event_type == 'PostCreated':
            user_id = data.get('user_id')
            post_id = data.get('post_id')
            self.send_notification(
                user_id,
                event_type,
                f"Your post {post_id[:8]}... is now live!"
            )
        
        elif event_type == 'PostLiked':
            # In real system, would notify post author
            # Here we notify the liker for demo purposes
            user_id = data.get('user_id')
            post_id = data.get('post_id')
            self.send_notification(
                user_id,
                event_type,
                f"You liked post {post_id[:8]}..."
            )
    
    def get_user_notifications(self, user_id: str, limit: int = 10) -> list:
        """Get notifications for a user"""
        result = self.db.execute(
            "SELECT * FROM notifications WHERE user_id = %s ORDER BY created_at DESC LIMIT %s",
            (user_id, limit)
        )
        return result or []
    
    def start_event_consumers(self):
        """Start consuming events from all topics"""
        consumer = EventConsumer(
            'localhost:9092',
            'notification-service-group',
            ['user-events', 'content-events']
        )
        consumer.start(self.handle_event)
        logger.info("Notification service consumers started")
