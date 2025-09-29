import logging
from typing import Dict, List, Set
from kafka import TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from config.kafka_config import KafkaConfig
import redis
import json
import time

logger = logging.getLogger(__name__)

class PartitionAssignmentManager:
    def __init__(self):
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS
        )
        self.redis_client = redis.Redis(
            host='localhost', port=6379, db=0, decode_responses=True
        )
        self.worker_assignments = {}
        
    def initialize_topics(self):
        """Create topics if they don't exist"""
        topics = [
            NewTopic(
                name=KafkaConfig.SOCIAL_POSTS_TOPIC,
                num_partitions=KafkaConfig.PARTITION_COUNT,
                replication_factor=KafkaConfig.REPLICATION_FACTOR
            ),
            NewTopic(
                name=KafkaConfig.TRENDING_RESULTS_TOPIC,
                num_partitions=1,
                replication_factor=KafkaConfig.REPLICATION_FACTOR
            )
        ]
        
        try:
            self.admin_client.create_topics(topics)
            logger.info("Topics created successfully")
        except Exception as e:
            logger.info(f"Topics may already exist: {e}")
    
    def calculate_partition_assignment(self, worker_count: int) -> Dict[str, List[int]]:
        """Calculate which partitions each worker should handle"""
        partitions_per_worker = KafkaConfig.PARTITION_COUNT // worker_count
        remaining_partitions = KafkaConfig.PARTITION_COUNT % worker_count
        
        assignments = {}
        partition_idx = 0
        
        for worker_id in range(worker_count):
            worker_name = f"trend-worker-{worker_id}"
            worker_partitions = []
            
            # Assign base partitions
            for _ in range(partitions_per_worker):
                worker_partitions.append(partition_idx)
                partition_idx += 1
            
            # Distribute remaining partitions
            if worker_id < remaining_partitions:
                worker_partitions.append(partition_idx)
                partition_idx += 1
                
            assignments[worker_name] = worker_partitions
            
        return assignments
    
    def store_assignment(self, assignments: Dict[str, List[int]]):
        """Store partition assignments in Redis"""
        self.redis_client.set(
            'partition_assignments',
            json.dumps(assignments),
            ex=3600  # 1 hour TTL
        )
        
    def get_assignment(self, worker_name: str) -> List[int]:
        """Get partition assignment for a specific worker"""
        assignments_str = self.redis_client.get('partition_assignments')
        if not assignments_str:
            return []
            
        assignments = json.loads(assignments_str)
        return assignments.get(worker_name, [])
    
    def register_worker(self, worker_name: str) -> List[TopicPartition]:
        """Register worker and return assigned partitions"""
        partition_ids = self.get_assignment(worker_name)
        
        topic_partitions = [
            TopicPartition(KafkaConfig.SOCIAL_POSTS_TOPIC, pid)
            for pid in partition_ids
        ]
        
        # Store worker registration
        self.redis_client.sadd('active_workers', worker_name)
        self.redis_client.set(f'worker:{worker_name}:last_seen', int(time.time()))
        
        logger.info(f"Worker {worker_name} assigned partitions: {partition_ids}")
        return topic_partitions
    
    def unregister_worker(self, worker_name: str):
        """Unregister worker"""
        self.redis_client.srem('active_workers', worker_name)
        self.redis_client.delete(f'worker:{worker_name}:last_seen')
        logger.info(f"Worker {worker_name} unregistered")
