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
            "DROP TABLE IF EXISTS viral_posts DELETE TOPIC;",
            "DROP TABLE IF EXISTS post_likes DELETE TOPIC;",
            "DROP TABLE IF EXISTS post_comments DELETE TOPIC;",
            "DROP TABLE IF EXISTS hashtag_trends DELETE TOPIC;",
            "DROP TABLE IF EXISTS user_activity DELETE TOPIC;",
            "DROP STREAM IF EXISTS all_engagement DELETE TOPIC;",
            "DROP STREAM IF EXISTS engagement_events DELETE TOPIC;",
            "DROP STREAM IF EXISTS post_engagement DELETE TOPIC;",
            "DROP STREAM IF EXISTS post_engagement_raw DELETE TOPIC;",
            "DROP STREAM IF EXISTS hashtags_exploded DELETE TOPIC;",
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
                TIMESTAMP='timestamp',
                PARTITIONS=3,
                REPLICAS=1
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
                TIMESTAMP='timestamp',
                PARTITIONS=3,
                REPLICAS=1
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
                TIMESTAMP='timestamp',
                PARTITIONS=3,
                REPLICAS=1
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
        
        # First create a stream with exploded hashtags
        self.execute_statement("""
            CREATE STREAM hashtags_exploded AS
            SELECT 
                EXPLODE(hashtags) AS hashtag,
                post_id,
                user_id,
                timestamp
            FROM posts_stream
            WHERE ARRAY_LENGTH(hashtags) > 0
            EMIT CHANGES;
        """)
        
        time.sleep(2)
        
        # Create hashtag trends table (materialized aggregation)
        self.execute_statement("""
            CREATE TABLE hashtag_trends AS
            SELECT 
                hashtag,
                COUNT(*) AS mention_count,
                COUNT_DISTINCT(user_id) AS unique_users,
                LATEST_BY_OFFSET(timestamp) AS last_mention
            FROM hashtags_exploded
            GROUP BY hashtag
            EMIT CHANGES;
        """)
        
        time.sleep(2)
        
        # Create viral posts table - simplified approach using windowed aggregation
        # This will track posts that accumulate engagement over time
        self.execute_statement("""
            CREATE TABLE viral_posts AS
            SELECT 
                p.post_id,
                LATEST_BY_OFFSET(p.user_id) AS user_id,
                LATEST_BY_OFFSET(p.content) AS content,
                COUNT(l.like_id) AS total_likes,
                COUNT(c.comment_id) AS total_comments,
                (COUNT(l.like_id) + COUNT(c.comment_id) * 3) AS engagement_score
            FROM posts_stream p
            LEFT JOIN likes_stream l WITHIN 1 HOUR ON p.post_id = l.post_id
            LEFT JOIN comments_stream c WITHIN 1 HOUR ON p.post_id = c.post_id
            WINDOW TUMBLING (SIZE 1 HOUR)
            GROUP BY p.post_id
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
