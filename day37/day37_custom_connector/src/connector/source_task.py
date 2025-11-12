"""
Source Task - Polls external APIs and produces to Kafka
"""
import time
import logging
import json
from typing import Dict, Any, List, Optional
from confluent_kafka import Producer
from src.clients.social_clients import create_client
from src.utils.rate_limiter import TokenBucketRateLimiter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SourceRecord:
    """Represents a record to be written to Kafka"""
    
    def __init__(self, source_partition: Dict, source_offset: Dict, 
                 topic: str, key: Any, value: Any):
        self.source_partition = source_partition
        self.source_offset = source_offset
        self.topic = topic
        self.key = key
        self.value = value


class SocialStreamSourceTask:
    """Source task that polls social platform APIs"""
    
    def __init__(self, task_id: int):
        self.task_id = task_id
        self.config = None
        self.client = None
        self.producer = None
        self.rate_limiter = TokenBucketRateLimiter()
        self.offset = None
        self.running = False
        self.metrics = {
            'records_produced': 0,
            'api_calls': 0,
            'rate_limit_waits': 0,
            'errors': 0,
        }
        
    def start(self, config: Dict[str, Any]):
        """Initialize task"""
        logger.info(f"Task {self.task_id} starting...")
        
        self.config = config
        self.running = True
        
        # Create API client
        self.client = create_client(
            platform=config['platform'],
            api_base=config['api_base'],
            access_token=config['credentials']['access_token']
        )
        
        # Configure rate limiter
        # Twitter: 900 requests per 15 min = 900 req / 900 sec
        # LinkedIn: 100 requests per hour = 100 req / 3600 sec
        window_seconds = 900 if config['platform'] == 'twitter' else 3600
        self.rate_limiter.configure(
            platform=config['platform'],
            max_requests=config['rate_limit'],
            window_seconds=window_seconds
        )
        
        # Create Kafka producer
        kafka_config = config['kafka']
        producer_config = {
            'bootstrap.servers': kafka_config['bootstrap_servers'],
            'client.id': f"social-connector-task-{self.task_id}",
        }
        producer_config.update(kafka_config.get('producer_config', {}))
        self.producer = Producer(producer_config)
        
        # Load offset (in production, this comes from Connect framework)
        self.offset = self._load_offset()
        
        logger.info(f"Task {self.task_id} started for {config['platform']} "
                   f"account {config['account_id']}")
        
    def _load_offset(self) -> Optional[str]:
        """Load last processed offset"""
        # In production, Connect framework manages offsets
        # For demo, start from beginning
        return None
        
    def poll(self) -> List[SourceRecord]:
        """
        Poll external API for new data
        Called repeatedly by Connect framework
        """
        if not self.running:
            return []
            
        records = []
        
        try:
            # Apply rate limiting
            wait_time = self.rate_limiter.acquire(
                platform=self.config['platform'],
                tokens_needed=1
            )
            
            if wait_time > 0:
                self.metrics['rate_limit_waits'] += 1
                logger.warning(f"Task {self.task_id} rate limited, "
                             f"waiting {wait_time:.2f}s")
                time.sleep(wait_time)
                
            # Fetch posts from API
            self.metrics['api_calls'] += 1
            posts = self.client.fetch_posts(
                account_id=self.config['account_id'],
                since_id=self.offset
            )
            
            # Transform to SourceRecords
            for post in posts:
                source_partition = {
                    'platform': self.config['platform'],
                    'account_id': self.config['account_id'],
                }
                
                source_offset = {
                    'post_id': post['id'],
                    'created_at': post['created_at'],
                }
                
                record = SourceRecord(
                    source_partition=source_partition,
                    source_offset=source_offset,
                    topic=self.config['kafka']['topic'],
                    key=post['account_id'],
                    value=json.dumps(post)
                )
                records.append(record)
                
                # Update offset to latest
                self.offset = post['id']
                
            self.metrics['records_produced'] += len(records)
            
            if records:
                logger.info(f"Task {self.task_id} produced {len(records)} records")
                
        except Exception as e:
            self.metrics['errors'] += 1
            logger.error(f"Task {self.task_id} error: {e}")
            
        return records
        
    def commit_record(self, record: SourceRecord):
        """Write record to Kafka"""
        try:
            self.producer.produce(
                topic=record.topic,
                key=record.key,
                value=record.value,
                callback=self._delivery_callback
            )
            self.producer.poll(0)  # Trigger callbacks
            
        except Exception as e:
            logger.error(f"Failed to produce record: {e}")
            raise
            
    def _delivery_callback(self, err, msg):
        """Kafka delivery callback"""
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")
            
    def stop(self):
        """Graceful shutdown"""
        logger.info(f"Task {self.task_id} stopping...")
        self.running = False
        
        if self.producer:
            self.producer.flush()
            
        logger.info(f"Task {self.task_id} stopped. Metrics: {self.metrics}")
        
    def get_metrics(self) -> Dict[str, Any]:
        """Return task metrics"""
        return {
            'task_id': self.task_id,
            'platform': self.config['platform'] if self.config else None,
            'account_id': self.config['account_id'] if self.config else None,
            'metrics': self.metrics,
            'offset': self.offset,
            'available_tokens': self.rate_limiter.get_available_tokens(
                self.config['platform'] if self.config else ''
            )
        }
