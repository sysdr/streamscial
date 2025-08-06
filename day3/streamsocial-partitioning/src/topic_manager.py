"""Kafka Topic Management for StreamSocial Partitioning"""

from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import ConfigResource, ConfigResourceType, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError
import logging
import time
from typing import List, Dict, Any
from config.kafka_config import KAFKA_CONFIG, ADMIN_CONFIG, TOPIC_CONFIG, PRODUCER_CONFIG, CONSUMER_CONFIG

logger = logging.getLogger(__name__)

class TopicManager:
    """Manages Kafka topics with optimal partitioning strategy"""
    
    def __init__(self):
        self.admin_client = KafkaAdminClient(**ADMIN_CONFIG)
        self.topics_created = False
        
    def create_topics(self) -> bool:
        """Create StreamSocial topics with calculated partition counts"""
        if self.topics_created:
            return True
            
        topics_to_create = []
        
        for topic_name, config in TOPIC_CONFIG.items():
            topic = NewTopic(
                name=topic_name,
                num_partitions=config['partitions'],
                replication_factor=config['replication_factor']
            )
            topics_to_create.append(topic)
        
        try:
            result = self.admin_client.create_topics(topics_to_create, validate_only=False)
            
            # Wait for topic creation
            for topic_name, future in result.items():
                try:
                    future.result()
                    logger.info(f"✅ Topic '{topic_name}' created successfully")
                except TopicAlreadyExistsError:
                    logger.info(f"ℹ️  Topic '{topic_name}' already exists")
                except Exception as e:
                    logger.error(f"❌ Failed to create topic '{topic_name}': {e}")
                    return False
            
            self.topics_created = True
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create topics: {e}")
            return False
    
    def get_topic_metadata(self, topic_name: str) -> Dict[str, Any]:
        """Get detailed topic metadata including partition information"""
        try:
            metadata = self.admin_client.describe_topics([topic_name])
            
            if topic_name in metadata:
                topic_info = metadata[topic_name]
                partitions_info = []
                
                for partition in topic_info.partitions:
                    partitions_info.append({
                        'partition_id': partition.partition,
                        'leader': partition.leader,
                        'replicas': partition.replicas,
                        'isr': partition.isr
                    })
                
                return {
                    'topic_name': topic_name,
                    'partition_count': len(topic_info.partitions),
                    'partitions': partitions_info,
                    'is_internal': topic_info.is_internal
                }
            else:
                return {'error': f'Topic {topic_name} not found'}
                
        except Exception as e:
            logger.error(f"Failed to get metadata for topic {topic_name}: {e}")
            return {'error': str(e)}
    
    def list_all_topics(self) -> List[str]:
        """List all available topics"""
        try:
            metadata = self.admin_client.list_topics()
            return list(metadata)
        except Exception as e:
            logger.error(f"Failed to list topics: {e}")
            return []
    
    def delete_topics(self, topic_names: List[str]) -> bool:
        """Delete specified topics"""
        try:
            result = self.admin_client.delete_topics(topic_names)
            
            for topic_name, future in result.items():
                try:
                    future.result()
                    logger.info(f"✅ Topic '{topic_name}' deleted successfully")
                except Exception as e:
                    logger.error(f"❌ Failed to delete topic '{topic_name}': {e}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to delete topics: {e}")
            return False

class StreamSocialProducer:
    """High-performance producer for StreamSocial topics"""
    
    def __init__(self):
        self.producer = KafkaProducer(
            value_serializer=lambda v: v,
            key_serializer=lambda k: k.encode('utf-8'),
            **KAFKA_CONFIG,
            **PRODUCER_CONFIG
        )
        self.message_count = 0
        
    def send_user_action(self, topic: str, key: str, value: bytes) -> bool:
        """Send user action to partitioned topic"""
        try:
            future = self.producer.send(topic, key=key, value=value)
            future.get(timeout=10)
            self.message_count += 1
            return True
        except Exception as e:
            logger.error(f"Failed to send user action: {e}")
            return False
    
    def send_content_interaction(self, topic: str, key: str, value: bytes) -> bool:
        """Send content interaction to partitioned topic"""
        try:
            future = self.producer.send(topic, key=key, value=value)
            future.get(timeout=10)
            self.message_count += 1
            return True
        except Exception as e:
            logger.error(f"Failed to send content interaction: {e}")
            return False
    
    def flush_and_close(self):
        """Flush pending messages and close producer"""
        self.producer.flush()
        self.producer.close()
        logger.info(f"Producer closed. Total messages sent: {self.message_count}")

class StreamSocialConsumer:
    """High-performance consumer for StreamSocial topics"""
    
    def __init__(self, topic: str, group_id: str):
        self.consumer = KafkaConsumer(
            topic,
            group_id=group_id,
            value_deserializer=lambda m: m,
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            **KAFKA_CONFIG,
            **CONSUMER_CONFIG
        )
        self.message_count = 0
        self.partition_stats = {}
    
    def consume_messages(self, max_messages: int = 100) -> List[Dict[str, Any]]:
        """Consume messages and track partition statistics"""
        messages = []
        
        try:
            for message in self.consumer:
                # Track partition statistics
                partition_id = message.partition
                if partition_id not in self.partition_stats:
                    self.partition_stats[partition_id] = 0
                self.partition_stats[partition_id] += 1
                
                messages.append({
                    'partition': message.partition,
                    'offset': message.offset,
                    'key': message.key,
                    'value': message.value,
                    'timestamp': message.timestamp
                })
                
                self.message_count += 1
                
                if len(messages) >= max_messages:
                    break
                    
        except Exception as e:
            logger.error(f"Error consuming messages: {e}")
        
        return messages
    
    def get_partition_stats(self) -> Dict[str, Any]:
        """Get partition consumption statistics"""
        return {
            'total_messages': self.message_count,
            'partition_distribution': self.partition_stats,
            'active_partitions': len(self.partition_stats)
        }
    
    def close(self):
        """Close consumer"""
        self.consumer.close()
        logger.info(f"Consumer closed. Total messages consumed: {self.message_count}")
