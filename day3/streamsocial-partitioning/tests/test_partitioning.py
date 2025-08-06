"""Comprehensive tests for StreamSocial partitioning strategy"""

import unittest
import asyncio
import time
import json
from unittest.mock import patch, MagicMock
from src.partition_strategy import PartitionStrategy, UserAction, ContentInteraction, PartitionCalculator
from src.topic_manager import TopicManager, StreamSocialProducer, StreamSocialConsumer
from monitoring.partition_monitor import PartitionMonitor

class TestPartitionStrategy(unittest.TestCase):
    """Test partition strategy implementation"""
    
    def setUp(self):
        self.strategy = PartitionStrategy()
    
    def test_user_action_partitioning(self):
        """Test user action partition calculation"""
        user_id = "user_12345"
        partition1 = self.strategy.calculate_user_action_partition(user_id)
        partition2 = self.strategy.calculate_user_action_partition(user_id)
        
        # Same user should always go to same partition
        self.assertEqual(partition1, partition2)
        self.assertGreaterEqual(partition1, 0)
        self.assertLess(partition1, 1000)
    
    def test_content_interaction_partitioning(self):
        """Test content interaction partition calculation"""
        content_id = "content_67890"
        partition1 = self.strategy.calculate_content_interaction_partition(content_id)
        partition2 = self.strategy.calculate_content_interaction_partition(content_id)
        
        # Same content should always go to same partition
        self.assertEqual(partition1, partition2)
        self.assertGreaterEqual(partition1, 0)
        self.assertLess(partition1, 500)
    
    def test_partition_distribution(self):
        """Test partition distribution across multiple users"""
        partitions = []
        for i in range(1000):
            user_id = f"user_{i}"
            partition = self.strategy.calculate_user_action_partition(user_id)
            partitions.append(partition)
        
        # Check distribution is reasonably even
        unique_partitions = len(set(partitions))
        self.assertGreater(unique_partitions, 800)  # At least 80% of partitions used
    
    def test_serialization(self):
        """Test message serialization/deserialization"""
        user_action = UserAction(
            user_id="user_123",
            action_type="post",
            content_id="content_456",
            timestamp=time.time(),
            metadata={"category": "technology"}
        )
        
        # Serialize and deserialize
        serialized = self.strategy.serialize_user_action(user_action)
        deserialized = self.strategy.deserialize_user_action(serialized)
        
        self.assertEqual(user_action.user_id, deserialized.user_id)
        self.assertEqual(user_action.action_type, deserialized.action_type)
        self.assertEqual(user_action.content_id, deserialized.content_id)

class TestPartitionCalculator(unittest.TestCase):
    """Test partition count calculation"""
    
    def test_throughput_calculation(self):
        """Test partition count calculation for target throughput"""
        # Test 50M req/s calculation
        partitions = PartitionCalculator.calculate_partitions_for_throughput(50_000_000)
        self.assertEqual(partitions, 1500)  # 50M / 50K * 1.5 safety factor
        
        # Test smaller scale
        partitions = PartitionCalculator.calculate_partitions_for_throughput(1_000_000)
        self.assertEqual(partitions, 30)  # 1M / 50K * 1.5 safety factor
    
    def test_partition_distribution_validation(self):
        """Test partition distribution health check"""
        # Balanced distribution
        balanced_distribution = {i: 100 for i in range(10)}
        result = PartitionCalculator.validate_partition_distribution(balanced_distribution)
        self.assertTrue(result['distribution_health'])
        self.assertEqual(len(result['hotspots']), 0)
        
        # Unbalanced distribution with hotspots
        unbalanced_distribution = {0: 500, 1: 100, 2: 100, 3: 100, 4: 100}
        result = PartitionCalculator.validate_partition_distribution(unbalanced_distribution)
        self.assertFalse(result['distribution_health'])
        self.assertGreater(len(result['hotspots']), 0)

class TestTopicManager(unittest.TestCase):
    """Test topic management functionality"""
    
    def setUp(self):
        self.topic_manager = TopicManager()
    
    @patch('src.topic_manager.KafkaAdminClient')
    def test_topic_creation(self, mock_admin):
        """Test topic creation with correct partition counts"""
        mock_admin_instance = MagicMock()
        mock_admin.return_value = mock_admin_instance
        
        # Mock successful topic creation
        mock_future = MagicMock()
        mock_future.result.return_value = None
        mock_admin_instance.create_topics.return_value = {
            'user-actions': mock_future,
            'content-interactions': mock_future
        }
        
        result = self.topic_manager.create_topics()
        self.assertTrue(result)
        
        # Verify create_topics was called
        mock_admin_instance.create_topics.assert_called_once()

class TestIntegration(unittest.TestCase):
    """Integration tests for the complete system"""
    
    def test_end_to_end_flow(self):
        """Test complete message flow from producer to consumer"""
        strategy = PartitionStrategy()
        
        # Create test user action
        user_action = UserAction(
            user_id="test_user",
            action_type="post",
            content_id="test_content",
            timestamp=time.time(),
            metadata={"test": True}
        )
        
        # Generate partition key and serialize
        partition_key = strategy.create_partition_key_user_action(user_action)
        serialized_message = strategy.serialize_user_action(user_action)
        
        # Verify partition consistency
        partition1 = strategy.calculate_user_action_partition(user_action.user_id)
        partition2 = strategy.calculate_user_action_partition(user_action.user_id)
        self.assertEqual(partition1, partition2)
        
        # Verify serialization roundtrip
        deserialized = strategy.deserialize_user_action(serialized_message)
        self.assertEqual(user_action.user_id, deserialized.user_id)

if __name__ == '__main__':
    unittest.main()
