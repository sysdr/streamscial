"""
Database manager for hashtag analytics
Handles PostgreSQL operations with optimized upserts
"""
import logging
import time
from typing import List, Optional
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine, text, MetaData, Table, Column, String, BigInteger, DateTime, Numeric
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert

from ..models.hashtag_model import HashtagRecord

class DatabaseManager:
    """Manages database operations for hashtag analytics"""
    
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.engine = create_engine(database_url, pool_size=10, max_overflow=20)
        self.Session = sessionmaker(bind=self.engine)
        self.logger = logging.getLogger(__name__)
        
    def initialize_schema(self):
        """Create database schema if not exists"""
        schema_sql = """
        CREATE TABLE IF NOT EXISTS trending_hashtags (
            hashtag VARCHAR(255) PRIMARY KEY,
            count BIGINT NOT NULL DEFAULT 0,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            trend_score DECIMAL(10,4) DEFAULT 0,
            window_start TIMESTAMP,
            window_end TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_trending_hashtags_count 
        ON trending_hashtags(count DESC);
        
        CREATE INDEX IF NOT EXISTS idx_trending_hashtags_updated 
        ON trending_hashtags(last_updated DESC);
        
        CREATE TABLE IF NOT EXISTS hashtag_leaderboard (
            rank INTEGER PRIMARY KEY,
            hashtag VARCHAR(255) NOT NULL,
            count BIGINT NOT NULL,
            trend_score DECIMAL(10,4) NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(schema_sql))
            conn.commit()
            
        self.logger.info("Database schema initialized")
        
    def upsert_hashtags(self, records: List[HashtagRecord]):
        """Perform optimized upsert operation for hashtag records"""
        if not records:
            return
            
        start_time = time.time()
        
        # Prepare upsert data
        upsert_data = []
        for record in records:
            upsert_data.append({
                'hashtag': record.hashtag,
                'count': record.count,
                'last_updated': record.window_end,
                'window_start': record.window_start,
                'window_end': record.window_end,
                'trend_score': self._calculate_trend_score(record)
            })
        
        # Execute batch upsert using psycopg2 for better performance
        upsert_sql = """
        INSERT INTO trending_hashtags 
        (hashtag, count, last_updated, window_start, window_end, trend_score)
        VALUES (%(hashtag)s, %(count)s, %(last_updated)s, %(window_start)s, %(window_end)s, %(trend_score)s)
        ON CONFLICT (hashtag) 
        DO UPDATE SET 
            count = trending_hashtags.count + EXCLUDED.count,
            last_updated = EXCLUDED.last_updated,
            window_end = EXCLUDED.window_end,
            trend_score = EXCLUDED.trend_score
        """
        
        # Use raw connection for psycopg2 batch operations
        raw_conn = self.engine.raw_connection()
        try:
            with raw_conn.cursor() as cursor:
                psycopg2.extras.execute_batch(cursor, upsert_sql, upsert_data)
            raw_conn.commit()
        finally:
            raw_conn.close()
            
        execution_time = time.time() - start_time
        self.logger.info(f"Upserted {len(records)} records in {execution_time:.2f}s")
        
    def _calculate_trend_score(self, record: HashtagRecord) -> float:
        """Calculate trend score based on count and recency"""
        # Simple trend score: count * time_factor
        time_factor = 1.0  # Could be based on recency
        return float(record.count * time_factor)
        
    def get_top_hashtags(self, limit: int = 50) -> List[dict]:
        """Get top trending hashtags"""
        query_sql = """
        SELECT hashtag, count, trend_score, last_updated
        FROM trending_hashtags
        ORDER BY trend_score DESC, count DESC
        LIMIT :limit
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(query_sql), {'limit': limit})
            return [dict(row._mapping) for row in result]
            
    def get_hashtag_stats(self) -> dict:
        """Get overall hashtag statistics"""
        stats_sql = """
        SELECT 
            COUNT(*) as total_hashtags,
            SUM(count) as total_mentions,
            AVG(count) as avg_mentions,
            MAX(count) as max_mentions,
            MIN(last_updated) as oldest_update,
            MAX(last_updated) as latest_update
        FROM trending_hashtags
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(stats_sql)).first()
            return dict(result._mapping) if result else {}
            
    def cleanup_old_data(self, days_to_keep: int = 7):
        """Cleanup old hashtag data"""
        cleanup_sql = """
        DELETE FROM trending_hashtags 
        WHERE last_updated < NOW() - INTERVAL ':days days'
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(cleanup_sql), {'days': days_to_keep})
            conn.commit()
            
        self.logger.info(f"Cleaned up old data, removed {result.rowcount} records")
