from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
import json

class LagCalculator:
    def __init__(self, bootstrap_servers):
        self.bootstrap_servers = bootstrap_servers
        self.admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
    
    def get_consumer_lag(self, group_id, topic):
        """Calculate lag for a consumer group"""
        consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': group_id,
            'enable.auto.commit': False
        })
        
        try:
            # Get cluster metadata
            metadata = consumer.list_topics(topic, timeout=10)
            if topic not in metadata.topics:
                return {'error': f'Topic {topic} not found'}
            
            topic_metadata = metadata.topics[topic]
            partitions = [TopicPartition(topic, p) for p in topic_metadata.partitions.keys()]
            
            # Get committed offsets
            committed = consumer.committed(partitions, timeout=10)
            
            # Get high watermarks (latest offsets)
            high_watermarks = {}
            for partition in partitions:
                low, high = consumer.get_watermark_offsets(partition, timeout=10)
                high_watermarks[partition.partition] = high
            
            # Calculate lag per partition
            partition_lags = {}
            total_lag = 0
            
            for partition in committed:
                partition_id = partition.partition
                committed_offset = partition.offset if partition.offset >= 0 else 0
                latest_offset = high_watermarks.get(partition_id, 0)
                lag = max(0, latest_offset - committed_offset)
                
                partition_lags[partition_id] = {
                    'committed_offset': committed_offset,
                    'latest_offset': latest_offset,
                    'lag': lag
                }
                total_lag += lag
            
            return {
                'group_id': group_id,
                'topic': topic,
                'total_lag': total_lag,
                'partition_lags': partition_lags
            }
            
        finally:
            consumer.close()
    
    def get_all_consumer_groups_lag(self):
        """Get lag for all consumer groups"""
        consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': 'lag-monitor-temp',
            'enable.auto.commit': False
        })
        
        try:
            groups = consumer.list_groups(timeout=10)
            lag_data = []
            
            for group in groups:
                # Skip internal groups
                if group.startswith('_'):
                    continue
                
                # Get topics for this group
                topics = consumer.list_topics(timeout=10).topics.keys()
                for topic in topics:
                    if not topic.startswith('_'):
                        lag_info = self.get_consumer_lag(group, topic)
                        if 'error' not in lag_info:
                            lag_data.append(lag_info)
            
            return lag_data
            
        finally:
            consumer.close()
