import json
import threading
import time
from kafka import KafkaConsumer, KafkaProducer
from transformers.smt_transformer import SMTTransformer

class PipelineOrchestrator:
    """
    Orchestrates the SMT transformation pipeline
    Simulates Kafka Connect behavior
    """
    
    def __init__(self, bootstrap_servers=['localhost:9093']):
        self.bootstrap_servers = bootstrap_servers
        self.running = False
        self.threads = []
        self.stats = {
            'ios': {'processed': 0, 'filtered': 0, 'errors': 0},
            'android': {'processed': 0, 'filtered': 0, 'errors': 0},
            'web': {'processed': 0, 'filtered': 0, 'errors': 0}
        }
    
    def create_consumer(self, topic: str):
        return KafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=1000
        )
    
    def create_producer(self):
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    
    def process_stream(self, source_topic: str, platform: str):
        """Process messages from a source topic through SMT pipeline"""
        consumer = self.create_consumer(source_topic)
        producer = self.create_producer()
        transformer = SMTTransformer(platform)
        
        print(f"🔄 Started pipeline for {platform} ({source_topic})")
        
        while self.running:
            try:
                for message in consumer:
                    if not self.running:
                        break
                    
                    raw_data = message.value
                    transformed = transformer.transform(raw_data)
                    
                    if transformed:
                        # Send to normalized topic
                        producer.send('normalized-user-actions', transformed)
                        print(f"  ✅ {platform}: {raw_data} -> {transformed}")
                    else:
                        # Send to DLQ
                        dlq_record = {
                            'original': raw_data,
                            'source': platform,
                            'reason': 'failed_validation',
                            'timestamp': int(time.time() * 1000)
                        }
                        producer.send('dlq-transform-failures', dlq_record)
                        print(f"  ❌ {platform}: Filtered/Failed: {raw_data}")
                
                # Update stats
                stats = transformer.get_stats()
                self.stats[platform] = {
                    'processed': stats['total_processed'],
                    'filtered': stats['total_filtered'],
                    'errors': stats['total_errors']
                }
                
                time.sleep(0.1)
                
            except Exception as e:
                print(f"❌ Pipeline error for {platform}: {e}")
                time.sleep(1)
        
        producer.flush()
        producer.close()
        consumer.close()
        print(f"🛑 Stopped pipeline for {platform}")
    
    def start(self):
        """Start all transformation pipelines"""
        self.running = True
        
        platforms = [
            ('raw-user-actions-ios', 'ios'),
            ('raw-user-actions-android', 'android'),
            ('raw-user-actions-web', 'web')
        ]
        
        for topic, platform in platforms:
            thread = threading.Thread(
                target=self.process_stream,
                args=(topic, platform),
                daemon=True
            )
            thread.start()
            self.threads.append(thread)
        
        print("✅ All transformation pipelines started")
    
    def stop(self):
        """Stop all pipelines"""
        self.running = False
        for thread in self.threads:
            thread.join(timeout=5)
        print("✅ All pipelines stopped")
    
    def get_stats(self):
        """Get pipeline statistics"""
        return self.stats.copy()

if __name__ == '__main__':
    orchestrator = PipelineOrchestrator()
    try:
        orchestrator.start()
        print("\n⏳ Running pipelines for 60 seconds...")
        time.sleep(60)
    finally:
        orchestrator.stop()
        print(f"\n📊 Final Statistics:")
        for platform, stats in orchestrator.get_stats().items():
            print(f"  {platform}: {stats}")
