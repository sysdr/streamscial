"""Viral content producer with structured logging"""
import json
import time
import random
from datetime import datetime
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from shared.structured_logger import setup_structured_logger, LogContext, generate_trace_id

class ViralContentProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.topic = 'viral-content'
        self.logger = setup_structured_logger(
            'viral-content-producer',
            'logs/application/producer.log'
        )
        
        # Initialize producer
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'viral-producer-1',
            'linger.ms': 10,
            'compression.type': 'snappy'
        })
        
        self._create_topic()
        self.logger.info("producer_initialized", 
                        bootstrap_servers=bootstrap_servers,
                        topic=self.topic)
    
    def _create_topic(self):
        """Create topic if not exists"""
        admin = AdminClient({'bootstrap.servers': self.bootstrap_servers})
        topic_metadata = admin.list_topics(timeout=10)
        
        if self.topic not in topic_metadata.topics:
            new_topic = NewTopic(self.topic, num_partitions=3, replication_factor=1)
            fs = admin.create_topics([new_topic])
            for topic, f in fs.items():
                try:
                    f.result()
                    self.logger.info("topic_created", topic=topic)
                except Exception as e:
                    self.logger.error("topic_creation_failed", topic=topic, error=str(e))
    
    def delivery_callback(self, err, msg, trace_id, start_time):
        """Callback for message delivery"""
        latency = (time.time() - start_time) * 1000
        
        if err:
            self.logger.error("message_delivery_failed",
                            trace_id=trace_id,
                            error=str(err),
                            topic=msg.topic(),
                            latency_ms=round(latency, 2))
        else:
            self.logger.info("message_delivered",
                           trace_id=trace_id,
                           topic=msg.topic(),
                           partition=msg.partition(),
                           offset=msg.offset(),
                           latency_ms=round(latency, 2),
                           message_size=len(msg.value()))
    
    def produce_viral_content(self, content_id, views, engagement_rate):
        """Produce viral content event"""
        trace_id = generate_trace_id()
        start_time = time.time()
        
        message = {
            'content_id': content_id,
            'views': views,
            'engagement_rate': engagement_rate,
            'timestamp': datetime.utcnow().isoformat(),
            'trace_id': trace_id
        }
        
        with LogContext(self.logger, trace_id=trace_id, 
                       content_id=content_id, views=views) as log:
            try:
                log.info("producing_message",
                        topic=self.topic,
                        engagement_rate=engagement_rate,
                        start_time=datetime.utcnow().isoformat())
                
                self.producer.produce(
                    self.topic,
                    key=str(content_id).encode('utf-8'),
                    value=json.dumps(message).encode('utf-8'),
                    callback=lambda err, msg: self.delivery_callback(
                        err, msg, trace_id, start_time
                    )
                )
                
                self.producer.poll(0)
                
            except Exception as e:
                log.error("production_error",
                         error=str(e),
                         error_type=type(e).__name__)
                raise
    
    def simulate_viral_traffic(self, duration_seconds=30):
        """Simulate viral content traffic with varying rates"""
        self.logger.info("simulation_started",
                        duration_seconds=duration_seconds)
        
        content_id = 1
        start = time.time()
        messages_produced = 0
        errors = 0
        
        while time.time() - start < duration_seconds:
            try:
                # Simulate varying viral traffic
                views = random.randint(10000, 1000000)
                engagement_rate = random.uniform(0.01, 0.15)
                
                # Occasionally simulate errors (5% chance)
                if random.random() < 0.05:
                    engagement_rate = -1  # Invalid value to trigger error
                    self.logger.warning("simulating_error_condition",
                                      content_id=content_id)
                
                self.produce_viral_content(content_id, views, engagement_rate)
                messages_produced += 1
                content_id += 1
                
                # Vary production rate
                time.sleep(random.uniform(0.01, 0.1))
                
            except Exception as e:
                errors += 1
                self.logger.error("simulation_error",
                                error=str(e),
                                messages_produced=messages_produced,
                                errors=errors)
        
        self.producer.flush()
        
        self.logger.info("simulation_completed",
                        duration_seconds=duration_seconds,
                        messages_produced=messages_produced,
                        errors=errors,
                        avg_rate=round(messages_produced/duration_seconds, 2))
    
    def close(self):
        """Close producer"""
        self.producer.flush()
        self.logger.info("producer_closed")

if __name__ == '__main__':
    producer = ViralContentProducer()
    try:
        producer.simulate_viral_traffic(duration_seconds=60)
    except KeyboardInterrupt:
        print("\nShutting down producer...")
    finally:
        producer.close()
