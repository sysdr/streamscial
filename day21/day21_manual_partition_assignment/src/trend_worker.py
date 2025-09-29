import logging
import json
import time
import threading
from collections import defaultdict, deque
from typing import Dict, Set, List
from kafka import KafkaConsumer, KafkaProducer
from kafka.consumer.fetcher import ConsumerRecord
from kafka.errors import KafkaTimeoutError, KafkaError
import redis
import re
from datetime import datetime, timedelta
from config.kafka_config import KafkaConfig
from src.partition_manager import PartitionAssignmentManager

logger = logging.getLogger(__name__)

class TrendWorker:
    def __init__(self, worker_id: int):
        self.worker_id = worker_id
        self.worker_name = f"trend-worker-{worker_id}"
        
        # Kafka setup - simplified configuration
        self.consumer = KafkaConsumer(
            bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
            group_id=KafkaConfig.CONSUMER_GROUP_ID,  # Use same group for all workers
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            enable_auto_commit=True,  # Enable auto commit
            auto_offset_reset='latest',
            consumer_timeout_ms=1000,  # Short timeout
            request_timeout_ms=30000,   # Longer request timeout
            session_timeout_ms=10000,  # Shorter session timeout
            heartbeat_interval_ms=3000, # Shorter heartbeat
            max_poll_records=10,       # Process in small batches
            api_version=(0, 10, 1)
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=KafkaConfig.REQUEST_TIMEOUT_MS,
            retries=3,
            retry_backoff_ms=100,
            api_version=(0, 10, 1)  # Specify API version for compatibility
        )
        
        # State management
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0)
        self.partition_manager = PartitionAssignmentManager()
        
        # Trend tracking state
        self.hashtag_counts = defaultdict(int)
        self.hashtag_timeline = defaultdict(deque)
        self.user_hashtag_interactions = defaultdict(set)
        
        # Thread control
        self.running = False
        self.checkpoint_thread = None
        
    def start(self):
        """Start the trend worker"""
        logger.info(f"Starting {self.worker_name}")
        
        # Subscribe to the social posts topic
        self.consumer.subscribe([KafkaConfig.SOCIAL_POSTS_TOPIC])
        
        # Register worker for monitoring
        self.partition_manager.register_worker(self.worker_name)
        
        # Load state from checkpoint
        self.load_state()
        
        # Start checkpoint thread
        self.running = True
        self.checkpoint_thread = threading.Thread(target=self._checkpoint_loop)
        self.checkpoint_thread.start()
        
        # Main processing loop
        self._processing_loop()
    
    def stop(self):
        """Stop the trend worker"""
        logger.info(f"Stopping {self.worker_name}")
        self.running = False
        
        if self.checkpoint_thread:
            self.checkpoint_thread.join()
            
        self.save_state()
        self.partition_manager.unregister_worker(self.worker_name)
        self.consumer.close()
        self.producer.close()
    
    def _processing_loop(self):
        """Main message processing loop"""
        logger.info(f"{self.worker_name} starting processing loop")
        
        while self.running:
            try:
                # Poll for messages
                message_batch = self.consumer.poll(timeout_ms=1000)
                
                if message_batch:
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            self._process_message(message)
                
                # Publish trending results every 5 seconds
                if int(time.time()) % 5 == 0:
                    self._publish_trends()
                
            except KafkaTimeoutError as e:
                # Timeout is expected when no messages are available
                continue
            except KafkaError as e:
                logger.error(f"Kafka error in processing loop: {e}")
                time.sleep(2)
            except Exception as e:
                logger.error(f"Unexpected error in processing loop: {e}")
                time.sleep(1)
    
    def _process_message(self, message: ConsumerRecord):
        """Process a single social media message"""
        try:
            post_data = message.value
            post_text = post_data.get('text', '')
            user_id = post_data.get('user_id', '')
            timestamp = post_data.get('timestamp', time.time())
            
            # Extract hashtags
            hashtags = self._extract_hashtags(post_text)
            
            for hashtag in hashtags:
                # Update counts
                self.hashtag_counts[hashtag] += 1
                
                # Track timeline for velocity calculation
                self.hashtag_timeline[hashtag].append(timestamp)
                
                # Track user interactions
                self.user_hashtag_interactions[hashtag].add(user_id)
                
                # Clean old timeline data
                self._cleanup_timeline(hashtag, timestamp)
                
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def _extract_hashtags(self, text: str) -> List[str]:
        """Extract hashtags from post text"""
        hashtag_pattern = r'#\w+'
        hashtags = re.findall(hashtag_pattern, text.lower())
        return [tag[1:] for tag in hashtags]  # Remove # symbol
    
    def _cleanup_timeline(self, hashtag: str, current_timestamp: float):
        """Remove old entries from hashtag timeline"""
        cutoff_time = current_timestamp - KafkaConfig.TREND_WINDOW_SIZE
        
        timeline = self.hashtag_timeline[hashtag]
        while timeline and timeline[0] < cutoff_time:
            timeline.popleft()
    
    def _calculate_trends(self) -> List[Dict]:
        """Calculate trending hashtags"""
        trends = []
        current_time = time.time()
        
        for hashtag, timeline in self.hashtag_timeline.items():
            if len(timeline) < KafkaConfig.MIN_HASHTAG_COUNT:
                continue
                
            # Calculate velocity (mentions per minute)
            velocity = (len(timeline) / KafkaConfig.TREND_WINDOW_SIZE) * 60
            
            # Calculate engagement (unique users)
            unique_users = len(self.user_hashtag_interactions[hashtag])
            
            # Calculate trend score
            trend_score = velocity * (1 + unique_users * 0.1)
            
            trends.append({
                'hashtag': hashtag,
                'count': self.hashtag_counts[hashtag],
                'velocity': velocity,
                'unique_users': unique_users,
                'trend_score': trend_score,
                'worker_id': self.worker_id,
                'timestamp': current_time
            })
        
        # Sort by trend score
        trends.sort(key=lambda x: x['trend_score'], reverse=True)
        return trends[:10]  # Top 10 trends
    
    def _publish_trends(self):
        """Publish trending results to Kafka"""
        try:
            trends = self._calculate_trends()
            
            if trends:
                future = self.producer.send(
                    KafkaConfig.TRENDING_RESULTS_TOPIC,
                    {
                        'worker_name': self.worker_name,
                        'trends': trends,
                        'timestamp': time.time()
                    }
                )
                # Don't wait for the result to avoid blocking
                # The producer will handle retries automatically
        except KafkaError as e:
            logger.error(f"Error publishing trends: {e}")
        except Exception as e:
            logger.error(f"Unexpected error publishing trends: {e}")
    
    def save_state(self):
        """Save worker state to Redis"""
        state = {
            'hashtag_counts': dict(self.hashtag_counts),
            'hashtag_timeline': {
                k: list(v) for k, v in self.hashtag_timeline.items()
            },
            'user_hashtag_interactions': {
                k: list(v) for k, v in self.user_hashtag_interactions.items()
            }
        }
        
        self.redis_client.set(
            f'worker_state:{self.worker_name}',
            json.dumps(state),
            ex=3600
        )
        
        logger.info(f"State saved for {self.worker_name}")
    
    def load_state(self):
        """Load worker state from Redis"""
        state_data = self.redis_client.get(f'worker_state:{self.worker_name}')
        
        if state_data:
            state = json.loads(state_data)
            
            # Restore hashtag counts
            self.hashtag_counts = defaultdict(int, state.get('hashtag_counts', {}))
            
            # Restore timeline data
            timeline_data = state.get('hashtag_timeline', {})
            for hashtag, timestamps in timeline_data.items():
                self.hashtag_timeline[hashtag] = deque(timestamps)
            
            # Restore user interactions
            interaction_data = state.get('user_hashtag_interactions', {})
            for hashtag, users in interaction_data.items():
                self.user_hashtag_interactions[hashtag] = set(users)
                
            logger.info(f"State loaded for {self.worker_name}")
        else:
            logger.info(f"No previous state found for {self.worker_name}")
    
    def _checkpoint_loop(self):
        """Periodic state checkpointing"""
        while self.running:
            time.sleep(KafkaConfig.STATE_CHECKPOINT_INTERVAL)
            if self.running:
                self.save_state()
