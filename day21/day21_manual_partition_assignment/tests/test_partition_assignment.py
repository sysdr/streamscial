import pytest
import json
from unittest.mock import Mock, patch
from src.partition_manager import PartitionAssignmentManager
from config.kafka_config import KafkaConfig

class TestPartitionAssignmentManager:
    def setup_method(self):
        self.manager = PartitionAssignmentManager()
        
    def test_calculate_partition_assignment(self):
        """Test partition assignment calculation"""
        assignments = self.manager.calculate_partition_assignment(3)
        
        # Should have 3 workers
        assert len(assignments) == 3
        
        # All partitions should be assigned
        all_partitions = []
        for partitions in assignments.values():
            all_partitions.extend(partitions)
        
        assert len(set(all_partitions)) == KafkaConfig.PARTITION_COUNT
        assert min(all_partitions) == 0
        assert max(all_partitions) == KafkaConfig.PARTITION_COUNT - 1
        
    def test_partition_distribution(self):
        """Test even partition distribution"""
        assignments = self.manager.calculate_partition_assignment(3)
        
        partition_counts = [len(partitions) for partitions in assignments.values()]
        
        # Partitions should be distributed evenly
        assert max(partition_counts) - min(partition_counts) <= 1
        
    @patch('redis.Redis')
    def test_store_and_get_assignment(self, mock_redis):
        """Test storing and retrieving assignments"""
        mock_redis_instance = Mock()
        mock_redis.return_value = mock_redis_instance
        
        assignments = {"worker-0": [0, 1], "worker-1": [2, 3]}
        
        self.manager.redis_client = mock_redis_instance
        self.manager.store_assignment(assignments)
        
        # Should store assignments in Redis
        mock_redis_instance.set.assert_called_once()
        
        # Test retrieval
        mock_redis_instance.get.return_value = json.dumps(assignments)
        result = self.manager.get_assignment("worker-0")
        
        assert result == [0, 1]
