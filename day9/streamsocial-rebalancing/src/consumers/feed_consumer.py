import json
import time
import logging
import threading
from typing import Dict, Any
from kafka import KafkaConsumer, TopicPartition
import redis
from prometheus_client import Counter, Histogram, Gauge

from src.listeners.rebalance_listener import StreamSocialRebalanceListener
from config.kafka_config import *

logger = logging.getLogger(__name__)

# Simple Prometheus metrics without complex labels for now
messages_processed = Counter('kafka_messages_processed_total', 'Total messages processed')
processing_latency = Histogram('kafka_message_processing_seconds', 'Message processing time in seconds')
consumer_lag = Gauge('kafka_consumer_lag', 'Consumer lag')

class FeedGeneratorConsumer:
    def __init__(self, consumer_id: str):
        self.consumer_id = consumer_id
        self.consumer = None
        self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        self.rebalance_listener = StreamSocialRebalanceListener(consumer_id, self.redis_client)
        self.running = False
        self.stats = {
            'messages_processed': 0,
            'feeds_generated': 0,
            'cache_hits': 0,
            'cache_misses': 0
        }
        
    def start(self):
        """Start the consumer"""
        try:
            self.consumer = KafkaConsumer(
                KAFKA_TOPICS['user_interactions'],
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=CONSUMER_GROUP_ID,
                auto_offset_reset='latest',
                enable_auto_commit=False,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                key_deserializer=lambda x: x.decode('utf-8') if x else None,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                max_poll_interval_ms=300000
            )
            
            # Subscribe with custom rebalance listener
            self.consumer.subscribe(
                [KAFKA_TOPICS['user_interactions']], 
                listener=self.rebalance_listener
            )
            
            self.running = True
            logger.info(f"Consumer {self.consumer_id} started successfully")
            
            # Start processing loop
            self._process_messages()
            
        except Exception as e:
            logger.error(f"Error starting consumer {self.consumer_id}: {e}")
            raise
            
    def _process_messages(self):
        """Main message processing loop"""
        
        while self.running:
            try:
                message_batch = self.consumer.poll(timeout_ms=1000)
                
                if not message_batch:
                    continue
                    
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        start_time = time.time()
                        
                        try:
                            self._process_user_interaction(message, topic_partition.partition)
                            self.stats['messages_processed'] += 1
                            messages_processed.inc()
                            
                        except Exception as e:
                            logger.error(f"Error processing message: {e}")
                            
                        finally:
                            processing_latency.observe(time.time() - start_time)
                
                # Commit offsets after successful processing
                self.consumer.commit()
                
            except Exception as e:
                logger.error(f"Error in message processing loop: {e}")
                time.sleep(1)
                
    def _process_user_interaction(self, message, partition: int):
        """Process individual user interaction"""
        interaction_data = message.value
        user_id = interaction_data.get('user_id')
        action_type = interaction_data.get('action_type')
        
        # Get partition-specific cache
        partition_cache = self.rebalance_listener.get_partition_cache(partition)
        
        # Check cache for user preferences
        cache_key = f"user_{user_id}_prefs"
        if cache_key in partition_cache:
            user_prefs = partition_cache[cache_key]
            self.stats['cache_hits'] += 1
        else:
            # Simulate loading user preferences
            user_prefs = self._load_user_preferences(user_id)
            partition_cache[cache_key] = user_prefs
            self.stats['cache_misses'] += 1
            
        # Generate personalized feed
        feed_items = self._generate_feed(user_id, action_type, user_prefs, partition_cache)
        
        # Update cache with new data
        self.rebalance_listener.update_partition_cache(partition, partition_cache)
        
        # Update metrics
        current_lag = self._calculate_partition_lag(partition)
        consumer_lag.set(current_lag)
        
        self.stats['feeds_generated'] += 1
        
        logger.debug(f"Generated feed for user {user_id}: {len(feed_items)} items")
        
    def _load_user_preferences(self, user_id: str) -> Dict[str, Any]:
        """Simulate loading user preferences"""
        return {
            'interests': ['tech', 'sports', 'entertainment'],
            'engagement_history': {},
            'followed_users': [],
            'blocked_topics': []
        }
        
    def _generate_feed(self, user_id: str, action_type: str, 
                      user_prefs: Dict[str, Any], cache: Dict[str, Any]) -> list:
        """Generate personalized feed items"""
        # Simulate ML-based feed generation
        feed_items = []
        
        for i in range(10):  # Generate 10 feed items
            feed_items.append({
                'id': f"feed_{user_id}_{int(time.time())}_{i}",
                'content_type': 'post',
                'score': 0.85 + (i * 0.01),
                'timestamp': time.time()
            })
            
        return feed_items
        
    def _calculate_partition_lag(self, partition: int) -> int:
        """Calculate consumer lag for partition"""
        try:
            # Get partition metadata
            topic_partition = TopicPartition(KAFKA_TOPICS['user_interactions'], partition)
            committed = self.consumer.committed(topic_partition)
            
            if committed is None:
                return 0
                
            # Get latest offset
            self.consumer.seek_to_end(topic_partition)
            latest_offset = self.consumer.position(topic_partition)
            
            return max(0, latest_offset - committed)
            
        except Exception as e:
            logger.error(f"Error calculating lag for partition {partition}: {e}")
            return 0
            
    def stop(self):
        """Stop the consumer gracefully"""
        self.running = False
        if self.consumer:
            self.consumer.close()
        logger.info(f"Consumer {self.consumer_id} stopped")
        
    def get_stats(self) -> Dict[str, Any]:
        """Get consumer statistics"""
        return {
            'consumer_id': self.consumer_id,
            'stats': self.stats,
            'assigned_partitions': len(self.rebalance_listener.feed_cache),
            'cache_size': sum(len(cache) for cache in self.rebalance_listener.feed_cache.values())
        }
