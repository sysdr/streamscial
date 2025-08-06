"""StreamSocial Kafka Partitioning Strategy Implementation"""

import hashlib
import json
import time
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict
import logging

logger = logging.getLogger(__name__)

@dataclass
class UserAction:
    user_id: str
    action_type: str  # post, comment, like, share
    content_id: Optional[str]
    timestamp: float
    metadata: Dict[str, Any]

@dataclass
class ContentInteraction:
    content_id: str
    user_id: str
    interaction_type: str  # view, like, share, click
    timestamp: float
    session_id: str
    metadata: Dict[str, Any]

class PartitionStrategy:
    """Implements optimal partitioning strategies for StreamSocial topics"""
    
    def __init__(self):
        self.user_actions_partitions = 1000
        self.content_interactions_partitions = 500
        
    def calculate_user_action_partition(self, user_id: str) -> int:
        """Hash-based partitioning ensuring user actions stay ordered"""
        hash_value = hashlib.md5(f"user_{user_id}".encode()).hexdigest()
        return int(hash_value, 16) % self.user_actions_partitions
    
    def calculate_content_interaction_partition(self, content_id: str) -> int:
        """Hash-based partitioning for content interactions"""
        hash_value = hashlib.md5(f"content_{content_id}".encode()).hexdigest()
        return int(hash_value, 16) % self.content_interactions_partitions
    
    def create_partition_key_user_action(self, user_action: UserAction) -> str:
        """Generate partition key for user actions"""
        return f"user_{user_action.user_id}"
    
    def create_partition_key_content_interaction(self, interaction: ContentInteraction) -> str:
        """Generate partition key for content interactions"""
        return f"content_{interaction.content_id}"
    
    def serialize_user_action(self, user_action: UserAction) -> bytes:
        """Serialize user action for Kafka"""
        return json.dumps(asdict(user_action)).encode('utf-8')
    
    def serialize_content_interaction(self, interaction: ContentInteraction) -> bytes:
        """Serialize content interaction for Kafka"""
        return json.dumps(asdict(interaction)).encode('utf-8')
    
    def deserialize_user_action(self, data: bytes) -> UserAction:
        """Deserialize user action from Kafka"""
        parsed = json.loads(data.decode('utf-8'))
        return UserAction(**parsed)
    
    def deserialize_content_interaction(self, data: bytes) -> ContentInteraction:
        """Deserialize content interaction from Kafka"""
        parsed = json.loads(data.decode('utf-8'))
        return ContentInteraction(**parsed)

class PartitionCalculator:
    """Calculate optimal partition counts for target throughput"""
    
    @staticmethod
    def calculate_partitions_for_throughput(
        target_rps: int,
        consumer_rps: int = 50000,
        safety_factor: float = 1.5
    ) -> int:
        """Calculate partition count for target requests per second"""
        base_partitions = target_rps / consumer_rps
        optimal_partitions = int(base_partitions * safety_factor)
        return max(optimal_partitions, 1)
    
    @staticmethod
    def validate_partition_distribution(partition_assignments: Dict[int, int]) -> Dict[str, Any]:
        """Validate partition distribution for hotspots"""
        total_messages = sum(partition_assignments.values())
        avg_per_partition = total_messages / len(partition_assignments)
        
        hotspots = []
        cold_spots = []
        
        for partition_id, count in partition_assignments.items():
            if count > avg_per_partition * 1.5:  # 50% above average
                hotspots.append((partition_id, count))
            elif count < avg_per_partition * 0.5:  # 50% below average
                cold_spots.append((partition_id, count))
        
        return {
            'total_messages': total_messages,
            'average_per_partition': avg_per_partition,
            'hotspots': hotspots,
            'cold_spots': cold_spots,
            'distribution_health': len(hotspots) == 0 and len(cold_spots) <= len(partition_assignments) * 0.1
        }
