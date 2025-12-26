"""Database utilities with outbox pattern"""
import psycopg2
from psycopg2.extras import RealDictCursor
import json
from typing import Optional, List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class DatabaseConnection:
    def __init__(self, dbname: str, user: str = 'postgres', password: str = 'postgres', 
                 host: str = 'localhost', port: int = 5432):
        self.conn_params = {
            'dbname': dbname,
            'user': user,
            'password': password,
            'host': host,
            'port': port
        }
        self.conn = None
    
    def connect(self):
        self.conn = psycopg2.connect(**self.conn_params)
        self.conn.autocommit = False
        return self.conn
    
    def execute(self, query: str, params: tuple = None) -> Optional[List[Dict]]:
        if not self.conn:
            self.connect()
        
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            if cur.description:
                return [dict(row) for row in cur.fetchall()]
            return None
    
    def execute_with_outbox(self, business_query: str, business_params: tuple,
                           event_data: Dict[str, Any], event_topic: str) -> bool:
        """Execute business logic and write event to outbox in single transaction"""
        if not self.conn:
            self.connect()
        
        try:
            with self.conn.cursor() as cur:
                # Execute business logic
                cur.execute(business_query, business_params)
                
                # Write to outbox
                outbox_query = """
                    INSERT INTO outbox (event_id, event_type, aggregate_id, event_data, topic, created_at)
                    VALUES (%(event_id)s, %(event_type)s, %(aggregate_id)s, %(event_data)s, %(topic)s, NOW())
                """
                cur.execute(outbox_query, {
                    'event_id': event_data['event_id'],
                    'event_type': event_data['event_type'],
                    'aggregate_id': event_data['aggregate_id'],
                    'event_data': json.dumps(event_data),
                    'topic': event_topic
                })
                
                self.conn.commit()
                logger.info(f"Transaction committed with outbox event: {event_data['event_type']}")
                return True
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Transaction failed: {e}")
            raise
    
    def get_unpublished_events(self) -> List[Dict]:
        """Get events from outbox that haven't been published"""
        query = "SELECT * FROM outbox WHERE published = FALSE ORDER BY created_at LIMIT 100"
        return self.execute(query) or []
    
    def mark_event_published(self, event_id: str):
        """Mark event as published"""
        query = "UPDATE outbox SET published = TRUE, published_at = NOW() WHERE event_id = %s"
        self.execute(query, (event_id,))
        self.conn.commit()
    
    def close(self):
        if self.conn:
            self.conn.close()
