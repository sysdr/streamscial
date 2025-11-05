from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin.new_topic import NewTopic
from kafka.errors import TopicAlreadyExistsError
import kafka.errors as Errors
import logging
import os

logger = logging.getLogger(__name__)

class KafkaConfigManager:
    def __init__(self, bootstrap_servers: str = None):
        self.bootstrap_servers = bootstrap_servers or os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers,
            client_id='preference_admin'
        )

    def create_compacted_topic(self, topic_name: str, num_partitions: int = 3, replication_factor: int = 1):
        """Create a topic with log compaction enabled"""
        topic_list = []
        
        # Topic configuration for log compaction
        topic_config = {
            'cleanup.policy': 'compact',
            'min.cleanable.dirty.ratio': '0.5',
            'segment.ms': '7200000',  # 2 hours
            'delete.retention.ms': '86400000',  # 24 hours
            'min.compaction.lag.ms': '60000',  # 1 minute
            'max.compaction.lag.ms': '300000',  # 5 minutes
        }

        topic_list.append(NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            topic_configs=topic_config
        ))

        try:
            # create_topics returns a CreateTopicsResponse object with topic_errors
            result = self.admin_client.create_topics(topic_list, validate_only=False)
            
            # Check for errors in the response - topic_errors is a list of tuples: (topic, error_code, error_message)
            if hasattr(result, 'topic_errors'):
                for error_tuple in result.topic_errors:
                    topic = error_tuple[0]
                    error_code = error_tuple[1] if len(error_tuple) > 1 else 0
                    
                    # Error code 0 = NoError, non-zero = error
                    if error_code == 0:
                        logger.info(f"Topic {topic} created successfully with compaction enabled")
                    else:
                        # Check if it's a "topic already exists" error (error code 36)
                        if error_code == 36 or (len(error_tuple) > 2 and "already exists" in str(error_tuple[2]).lower()):
                            logger.info(f"Topic {topic} already exists")
                        else:
                            error_msg = error_tuple[2] if len(error_tuple) > 2 else f"Error code: {error_code}"
                            logger.warning(f"Topic {topic} creation issue: {error_msg}")
            else:
                # If no topic_errors attribute, assume success
                logger.info(f"Topic {topic_name} creation initiated")
        except Exception as e:
            # Check if it's an "already exists" error in the message
            if "already exists" in str(e).lower() or "TopicExistsException" in str(e) or "36" in str(e):
                logger.info(f"Topic {topic_name} already exists")
            else:
                logger.error(f"Error creating topic {topic_name}: {e}")

    def get_topic_config(self, topic_name: str) -> dict:
        """Get current topic configuration"""
        try:
            resource = ConfigResource(ConfigResourceType.TOPIC, topic_name)
            configs = self.admin_client.describe_configs(config_resources=[resource])
            return configs[resource].result()
        except Exception as e:
            logger.error(f"Error getting topic config: {e}")
            return {}

    def create_producer(self) -> KafkaProducer:
        """Create Kafka producer optimized for compacted topics"""
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            value_serializer=lambda v: v.encode('utf-8') if v else None,
            acks='all',  # Wait for all replicas
            retries=3
        )

    def create_consumer(self, group_id: str, topics: list) -> KafkaConsumer:
        """Create Kafka consumer for compacted topics"""
        return KafkaConsumer(
            *topics,
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            value_deserializer=lambda v: v.decode('utf-8') if v else None,
            enable_auto_commit=False,  # Manual commit for exactly-once
            auto_offset_reset='earliest'  # Read from beginning for state rebuilding
        )
