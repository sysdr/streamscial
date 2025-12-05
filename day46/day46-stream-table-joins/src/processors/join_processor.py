import json
import time
from collections import defaultdict
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import threading
import sys
import os
# Add src directory to path
src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)
from models import EnrichedAction

class StreamTableJoinProcessor:
    def __init__(self, bootstrap_servers='localhost:9092'):
        # KTable: User Profiles (materialized view)
        self.profile_table = {}  # user_id -> profile dict
        self.table_lock = threading.Lock()
        
        # Metrics
        self.metrics = {
            'profiles_loaded': 0,
            'profiles_updated': 0,
            'actions_processed': 0,
            'joins_successful': 0,
            'joins_null': 0,
            'total_latency_ms': 0.0
        }
        self.metrics_lock = threading.Lock()
        
        # Consumers
        self.profile_consumer = KafkaConsumer(
            'user-profiles',
            bootstrap_servers=bootstrap_servers,
            key_deserializer=lambda k: k.decode('utf-8'),
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id='join-processor-profiles',
            enable_auto_commit=True
        )
        
        self.action_consumer = KafkaConsumer(
            'user-actions',
            bootstrap_servers=bootstrap_servers,
            key_deserializer=lambda k: k.decode('utf-8'),
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id='join-processor-actions',
            enable_auto_commit=True
        )
        
        # Producer for enriched output
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=lambda k: k.encode('utf-8'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all'
        )
        
        self._create_output_topic()
        
    def _create_output_topic(self):
        admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
        topic = NewTopic(name='enriched-actions', num_partitions=6, replication_factor=1)
        try:
            admin.create_topics([topic])
            print("Created topic: enriched-actions")
        except TopicAlreadyExistsError:
            print("Topic enriched-actions already exists")
        finally:
            admin.close()
    
    def materialize_ktable(self):
        """Build initial KTable state from changelog"""
        print("Materializing KTable from user-profiles...")
        
        # Poll until we've caught up
        message_count = 0
        empty_polls = 0
        
        while empty_polls < 3:  # 3 empty polls = caught up
            messages = self.profile_consumer.poll(timeout_ms=1000, max_records=100)
            
            if not messages:
                empty_polls += 1
                continue
            
            empty_polls = 0
            for topic_partition, records in messages.items():
                for record in records:
                    with self.table_lock:
                        self.profile_table[record.key] = record.value
                        message_count += 1
                        
                        with self.metrics_lock:
                            if message_count <= 1000:
                                self.metrics['profiles_loaded'] += 1
        
        print(f"KTable materialized: {len(self.profile_table)} user profiles")
        return len(self.profile_table)
    
    def update_ktable_continuously(self):
        """Keep KTable updated with new profile changes"""
        print("Starting continuous KTable updates...")
        
        while True:
            messages = self.profile_consumer.poll(timeout_ms=100, max_records=10)
            
            for topic_partition, records in messages.items():
                for record in records:
                    with self.table_lock:
                        self.profile_table[record.key] = record.value
                        
                        with self.metrics_lock:
                            self.metrics['profiles_updated'] += 1
    
    def process_action_stream(self):
        """Join action stream with profile table"""
        print("Starting action stream processing...")
        
        while True:
            messages = self.action_consumer.poll(timeout_ms=100, max_records=50)
            
            for topic_partition, records in messages.items():
                for record in records:
                    join_start = time.time()
                    
                    # Perform join
                    action = record.value
                    user_id = record.key
                    
                    with self.table_lock:
                        profile = self.profile_table.get(user_id)
                    
                    # Create enriched action
                    if profile:
                        enriched = EnrichedAction(
                            user_id=user_id,
                            action_type=action['action_type'],
                            target_id=action['target_id'],
                            timestamp=action['timestamp'],
                            user_age=profile.get('age'),
                            user_city=profile.get('city'),
                            user_tier=profile.get('tier'),
                            user_interests=profile.get('interests'),
                            enrichment_latency_ms=round((time.time() - join_start) * 1000, 2)
                        )
                        
                        self.producer.send(
                            'enriched-actions',
                            key=user_id,
                            value=enriched.to_dict()
                        )
                        
                        with self.metrics_lock:
                            self.metrics['joins_successful'] += 1
                            self.metrics['total_latency_ms'] += enriched.enrichment_latency_ms
                    else:
                        # Null join - profile not found
                        with self.metrics_lock:
                            self.metrics['joins_null'] += 1
                    
                    with self.metrics_lock:
                        self.metrics['actions_processed'] += 1
    
    def get_metrics(self):
        with self.metrics_lock:
            metrics_copy = self.metrics.copy()
            
            if metrics_copy['joins_successful'] > 0:
                metrics_copy['avg_latency_ms'] = round(
                    metrics_copy['total_latency_ms'] / metrics_copy['joins_successful'], 2
                )
            else:
                metrics_copy['avg_latency_ms'] = 0.0
            
            return metrics_copy
    
    def start(self):
        """Start all processing threads"""
        # First materialize the KTable
        self.materialize_ktable()
        
        # Start KTable update thread
        ktable_thread = threading.Thread(target=self.update_ktable_continuously, daemon=True)
        ktable_thread.start()
        
        # Start action processing thread
        action_thread = threading.Thread(target=self.process_action_stream, daemon=True)
        action_thread.start()
        
        print("Stream-Table Join Processor started")
        
        # Keep main thread alive
        try:
            while True:
                time.sleep(10)
                metrics = self.get_metrics()
                print(f"Metrics: {metrics['actions_processed']} actions, "
                      f"{metrics['joins_successful']} joins, "
                      f"{metrics['avg_latency_ms']}ms avg latency")
        except KeyboardInterrupt:
            print("Shutting down...")
            self.producer.close()

if __name__ == '__main__':
    processor = StreamTableJoinProcessor()
    processor.start()
