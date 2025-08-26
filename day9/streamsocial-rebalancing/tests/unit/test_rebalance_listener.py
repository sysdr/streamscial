import pytest
import json
import redis
from unittest.mock import Mock, patch, MagicMock
from kafka import TopicPartition

from src.listeners.rebalance_listener import StreamSocialRebalanceListener

class TestStreamSocialRebalanceListener:
    def setup_method(self):
        self.redis_mock = Mock(spec=redis.Redis)
        self.consumer_id = "test-consumer-1"
        self.listener = StreamSocialRebalanceListener(self.consumer_id, self.redis_mock)
        
    def test_on_partitions_revoked(self):
        """Test partition revocation saves state to Redis"""
        # Setup
        partition = TopicPartition("test-topic", 0)
        self.listener.feed_cache[0] = {"test": "data"}
        self.listener.user_preferences[0] = {"user1": "prefs"}
        
        # Execute
        self.listener.on_partitions_revoked([partition])
        
        # Verify
        assert self.redis_mock.setex.call_count == 2
        assert len(self.listener.feed_cache) == 0
        assert len(self.listener.user_preferences) == 0
        
    def test_on_partitions_assigned_with_cache(self):
        """Test partition assignment restores from Redis"""
        # Setup
        partition = TopicPartition("test-topic", 0)
        cached_data = {"test": "data"}
        self.redis_mock.get.return_value = json.dumps(cached_data)
        
        # Execute
        self.listener.on_partitions_assigned([partition])
        
        # Verify
        assert 0 in self.listener.feed_cache
        assert self.listener.feed_cache[0] == cached_data
        
    def test_on_partitions_assigned_without_cache(self):
        """Test partition assignment initializes empty cache"""
        # Setup
        partition = TopicPartition("test-topic", 0)
        self.redis_mock.get.return_value = None
        
        # Execute
        self.listener.on_partitions_assigned([partition])
        
        # Verify
        assert 0 in self.listener.feed_cache
        assert "recent_feeds" in self.listener.feed_cache[0]
        assert "ml_weights" in self.listener.feed_cache[0]
        
    def test_get_partition_cache(self):
        """Test getting partition cache"""
        # Setup
        test_cache = {"test": "data"}
        self.listener.feed_cache[0] = test_cache
        
        # Execute & Verify
        assert self.listener.get_partition_cache(0) == test_cache
        assert self.listener.get_partition_cache(999) == {}
        
    def test_update_partition_cache(self):
        """Test updating partition cache"""
        # Setup
        self.listener.feed_cache[0] = {"existing": "data"}
        new_data = {"new": "value"}
        
        # Execute
        self.listener.update_partition_cache(0, new_data)
        
        # Verify
        assert self.listener.feed_cache[0]["existing"] == "data"
        assert self.listener.feed_cache[0]["new"] == "value"
