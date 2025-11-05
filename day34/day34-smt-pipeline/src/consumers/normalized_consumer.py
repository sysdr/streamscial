import json
from kafka import KafkaConsumer
from collections import defaultdict
import time

class NormalizedConsumer:
    def __init__(self, bootstrap_servers=['localhost:9093']):
        self.consumer = KafkaConsumer(
            'normalized-user-actions',
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=5000
        )
        
        self.stats = {
            'total_consumed': 0,
            'by_source': defaultdict(int),
            'by_action': defaultdict(int),
            'messages': []
        }
    
    def consume(self, max_messages=100):
        print(f"📊 Consuming from normalized-user-actions topic...")
        
        count = 0
        for message in self.consumer:
            data = message.value
            
            self.stats['total_consumed'] += 1
            self.stats['by_source'][data.get('source', 'unknown')] += 1
            self.stats['by_action'][data.get('action', 'unknown')] += 1
            self.stats['messages'].append(data)
            
            count += 1
            if count >= max_messages:
                break
        
        return self.stats
    
    def close(self):
        self.consumer.close()

if __name__ == '__main__':
    consumer = NormalizedConsumer()
    try:
        stats = consumer.consume(max_messages=100)
        print(f"\n📊 Consumption Statistics:")
        print(f"  Total messages: {stats['total_consumed']}")
        print(f"  By source: {dict(stats['by_source'])}")
        print(f"  By action: {dict(stats['by_action'])}")
    finally:
        consumer.close()
