from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

class GeoAwareProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8'),
            acks='all',
            retries=3
        )
        
        self.regions = [
            'US_EAST', 'US_WEST', 'CANADA', 'UK', 'GERMANY', 'FRANCE',
            'JAPAN', 'SINGAPORE', 'AUSTRALIA', 'BRAZIL', 'INDIA', 'SOUTH_AFRICA'
        ]
    
    def send_geo_message(self, user_id, region, content):
        key = f"user:{region}:{user_id}"
        message = {
            'user_id': user_id,
            'region': region,
            'content': content,
            'timestamp': datetime.now().isoformat(),
            'message_type': 'post'
        }
        
        future = self.producer.send('streamsocial-posts', key=key, value=message)
        return future
    
    def simulate_global_traffic(self, duration_seconds=60):
        """Simulate global user activity"""
        print(f"🌍 Starting {duration_seconds}s global traffic simulation...")
        
        start_time = time.time()
        message_count = 0
        
        while time.time() - start_time < duration_seconds:
            # Simulate regional traffic patterns
            region = random.choice(self.regions)
            user_id = f"user_{random.randint(1000, 9999)}"
            content = f"Post from {region} at {datetime.now().strftime('%H:%M:%S')}"
            
            try:
                future = self.send_geo_message(user_id, region, content)
                future.get(timeout=1)
                message_count += 1
                
                if message_count % 100 == 0:
                    print(f"📊 Sent {message_count} messages")
                    
            except Exception as e:
                print(f"❌ Error sending message: {e}")
            
            time.sleep(0.1)  # 10 messages per second
        
        print(f"✅ Completed simulation: {message_count} messages sent")
        self.producer.flush()
        self.producer.close()

if __name__ == '__main__':
    producer = GeoAwareProducer()
    producer.simulate_global_traffic(120)
