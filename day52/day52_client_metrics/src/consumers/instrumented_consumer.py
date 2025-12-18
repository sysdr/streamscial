import json
import time
import threading
import random
from confluent_kafka import Consumer, KafkaError, TopicPartition
from datetime import datetime

class InstrumentedConsumer:
    def __init__(self, bootstrap_servers, group_id, topics, client_id):
        self.group_id = group_id
        self.topics = topics
        self.client_id = client_id
        
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'client.id': client_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'statistics.interval.ms': 5000
        }
        
        self.consumer = Consumer(self.config)
        self.consumer.subscribe(topics)
        
        self.metrics = {
            'records_consumed': 0,
            'bytes_consumed': 0,
            'commit_count': 0,
            'processing_latencies': [],
            'e2e_latencies': [],
            'start_time': time.time()
        }
        self.lock = threading.Lock()
        self.running = True
    
    def consume_messages(self):
        """Consume messages and track metrics"""
        while self.running:
            msg = self.consumer.poll(1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    continue
            
            process_start = time.time()
            
            # Calculate end-to-end latency
            e2e_latency = 0
            for header in msg.headers() or []:
                if header[0] == 'timestamp':
                    msg_timestamp = int(header[1].decode('utf-8'))
                    e2e_latency = int(time.time() * 1000) - msg_timestamp
                    break
            
            # Simulate processing
            time.sleep(random.uniform(0.001, 0.005))
            
            processing_latency = (time.time() - process_start) * 1000
            
            with self.lock:
                self.metrics['records_consumed'] += 1
                self.metrics['bytes_consumed'] += len(msg.value())
                self.metrics['processing_latencies'].append(processing_latency)
                if e2e_latency > 0:
                    self.metrics['e2e_latencies'].append(e2e_latency)
                
                # Keep only last 1000 latencies
                if len(self.metrics['processing_latencies']) > 1000:
                    self.metrics['processing_latencies'] = self.metrics['processing_latencies'][-1000:]
                if len(self.metrics['e2e_latencies']) > 1000:
                    self.metrics['e2e_latencies'] = self.metrics['e2e_latencies'][-1000:]
            
            # Commit periodically
            if self.metrics['records_consumed'] % 100 == 0:
                self.consumer.commit(asynchronous=True)
                with self.lock:
                    self.metrics['commit_count'] += 1
    
    def get_metrics(self):
        """Get current consumer metrics"""
        with self.lock:
            elapsed = time.time() - self.metrics['start_time']
            proc_latencies = self.metrics['processing_latencies']
            e2e_latencies = self.metrics['e2e_latencies']
            
            return {
                'client_id': self.client_id,
                'group_id': self.group_id,
                'topics': self.topics,
                'records_consumed': self.metrics['records_consumed'],
                'bytes_consumed': self.metrics['bytes_consumed'],
                'commit_count': self.metrics['commit_count'],
                'throughput_rps': self.metrics['records_consumed'] / elapsed if elapsed > 0 else 0,
                'avg_proc_latency_ms': sum(proc_latencies) / len(proc_latencies) if proc_latencies else 0,
                'avg_e2e_latency_ms': sum(e2e_latencies) / len(e2e_latencies) if e2e_latencies else 0,
                'p99_e2e_latency_ms': sorted(e2e_latencies)[int(len(e2e_latencies) * 0.99)] if len(e2e_latencies) > 10 else 0
            }
    
    def stop(self):
        self.running = False
        self.consumer.close()

if __name__ == "__main__":
    pass
