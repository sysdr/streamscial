import json
import time
import logging
from typing import List, Set
from kafka import TopicPartition
from kafka.consumer.subscription_state import ConsumerRebalanceListener
import redis
from prometheus_client import Counter, Histogram, Gauge

logger = logging.getLogger(__name__)

# Metrics
rebalance_events = Counter('kafka_rebalance_events_total', 'Total rebalance events')
rebalance_duration = Histogram('kafka_rebalance_duration_seconds', 'Rebalance duration in seconds')
active_partitions = Gauge('kafka_active_partitions', 'Number of active partitions')

class StreamSocialRebalanceListener(ConsumerRebalanceListener):
    def __init__(self, consumer_id: str, redis_client: redis.Redis):
        self.consumer_id = consumer_id
        self.redis_client = redis_client
        self.feed_cache = {}
        self.user_preferences = {}
        
    def on_partitions_revoked(self, revoked: List[TopicPartition]):
        """Called before rebalancing starts"""
        start_time = time.time()
        rebalance_events.inc()
        
        logger.info(f"Consumer {self.consumer_id}: Partitions revoked: {revoked}")
        
        try:
            # Save feed generation cache to Redis
            for partition in revoked:
                cache_key = f"feed_cache_{self.consumer_id}_{partition.partition}"
                if partition.partition in self.feed_cache:
                    cache_data = json.dumps(self.feed_cache[partition.partition])
                    self.redis_client.setex(cache_key, 300, cache_data)
                    logger.info(f"Saved cache for partition {partition.partition}")
                
                # Save user preferences
                pref_key = f"user_prefs_{self.consumer_id}_{partition.partition}"
                if partition.partition in self.user_preferences:
                    prefs_data = json.dumps(self.user_preferences[partition.partition])
                    self.redis_client.setex(pref_key, 300, prefs_data)
                    
            # Clear local caches
            self.feed_cache.clear()
            self.user_preferences.clear()
            
        except Exception as e:
            logger.error(f"Error saving state during rebalance: {e}")
        
        duration = time.time() - start_time
        rebalance_duration.observe(duration)
        
    def on_partitions_assigned(self, assigned: List[TopicPartition]):
        """Called after rebalancing completes"""
        start_time = time.time()
        rebalance_events.inc()
        
        logger.info(f"Consumer {self.consumer_id}: Partitions assigned: {assigned}")
        
        try:
            # Restore cache for newly assigned partitions
            for partition in assigned:
                cache_key = f"feed_cache_{self.consumer_id}_{partition.partition}"
                cached_data = self.redis_client.get(cache_key)
                
                if cached_data:
                    self.feed_cache[partition.partition] = json.loads(cached_data)
                    logger.info(f"Restored cache for partition {partition.partition}")
                else:
                    # Initialize empty cache for new partition
                    self.feed_cache[partition.partition] = {
                        'recent_feeds': [],
                        'ml_weights': {},
                        'trending_topics': []
                    }
                
                # Restore user preferences
                pref_key = f"user_prefs_{self.consumer_id}_{partition.partition}"
                prefs_data = self.redis_client.get(pref_key)
                
                if prefs_data:
                    self.user_preferences[partition.partition] = json.loads(prefs_data)
                else:
                    self.user_preferences[partition.partition] = {}
                    
        except Exception as e:
            logger.error(f"Error restoring state during rebalance: {e}")
            # Graceful degradation - initialize empty caches
            for partition in assigned:
                self.feed_cache[partition.partition] = {
                    'recent_feeds': [],
                    'ml_weights': {},
                    'trending_topics': []
                }
                self.user_preferences[partition.partition] = {}
        
        active_partitions.set(len(assigned))
        duration = time.time() - start_time
        rebalance_duration.observe(duration)
        
    def get_partition_cache(self, partition: int) -> dict:
        """Get cache for specific partition"""
        return self.feed_cache.get(partition, {})
        
    def update_partition_cache(self, partition: int, cache_data: dict):
        """Update cache for specific partition"""
        if partition not in self.feed_cache:
            self.feed_cache[partition] = {}
        self.feed_cache[partition].update(cache_data)
