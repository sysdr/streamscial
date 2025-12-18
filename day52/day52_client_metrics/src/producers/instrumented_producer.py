import json
import time
import random
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from datetime import datetime
import threading

class InstrumentedProducer:
    def __init__(self, bootstrap_servers, topic, client_id):
        self.topic = topic
        self.client_id = client_id
        
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': client_id,
            'statistics.interval.ms': 5000,
            'acks': 'all',
            'linger.ms': 10,
            'batch.size': 16384
        }
        
        self.producer = Producer(self.config)
        self.metrics = {
            'records_sent': 0,
            'bytes_sent': 0,
            'errors': 0,
            'latencies': [],
            'start_time': time.time()
        }
        self.lock = threading.Lock()
        
    def stats_callback(self, stats_json_str):
        """Callback for producer statistics"""
        stats = json.loads(stats_json_str)
        with self.lock:
            self.producer_stats = stats
    
    def delivery_report(self, err, msg):
        """Delivery callback"""
        if err:
            with self.lock:
                self.metrics['errors'] += 1
        else:
            with self.lock:
                self.metrics['records_sent'] += 1
                self.metrics['bytes_sent'] += len(msg.value())
    
    def produce_message(self, key, value):
        """Produce a single message with latency tracking"""
        start_time = time.time()
        
        # Add timestamp header
        headers = [
            ('timestamp', str(int(time.time() * 1000)).encode('utf-8')),
            ('producer_id', self.client_id.encode('utf-8'))
        ]
        
        self.producer.produce(
            self.topic,
            key=key.encode('utf-8'),
            value=json.dumps(value).encode('utf-8'),
            headers=headers,
            callback=self.delivery_report
        )
        
        latency = (time.time() - start_time) * 1000
        with self.lock:
            self.metrics['latencies'].append(latency)
            if len(self.metrics['latencies']) > 1000:
                self.metrics['latencies'] = self.metrics['latencies'][-1000:]
        
        self.producer.poll(0)
    
    def get_metrics(self):
        """Get current producer metrics"""
        with self.lock:
            elapsed = time.time() - self.metrics['start_time']
            latencies = self.metrics['latencies']
            
            return {
                'client_id': self.client_id,
                'topic': self.topic,
                'records_sent': self.metrics['records_sent'],
                'bytes_sent': self.metrics['bytes_sent'],
                'errors': self.metrics['errors'],
                'throughput_rps': self.metrics['records_sent'] / elapsed if elapsed > 0 else 0,
                'avg_latency_ms': sum(latencies) / len(latencies) if latencies else 0,
                'p99_latency_ms': sorted(latencies)[int(len(latencies) * 0.99)] if latencies else 0
            }
    
    def flush(self):
        self.producer.flush()

def create_topics(bootstrap_servers):
    """Create necessary Kafka topics"""
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
    
    topics = [
        NewTopic('posts', num_partitions=3, replication_factor=1),
        NewTopic('likes', num_partitions=3, replication_factor=1),
        NewTopic('comments', num_partitions=3, replication_factor=1),
        NewTopic('notifications', num_partitions=3, replication_factor=1)
    ]
    
    try:
        fs = admin_client.create_topics(topics)
        for topic, f in fs.items():
            try:
                f.result()
                print(f"Topic {topic} created")
            except Exception as e:
                if 'already exists' not in str(e):
                    print(f"Failed to create topic {topic}: {e}")
    except Exception as e:
        print(f"Topic creation error: {e}")

if __name__ == "__main__":
    create_topics('localhost:9092')
