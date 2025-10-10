import json
import random
import time
import uuid
from kafka import KafkaProducer
from datetime import datetime, timedelta

class TestDataProducer:
    def __init__(self, bootstrap_servers: str):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8')
        )
        
    def generate_valid_post(self) -> dict:
        """Generate valid social media post"""
        content_options = [
            "Just finished an amazing workout! 💪 #fitness #motivation",
            "Beautiful sunset today 🌅 #nature #photography", 
            "Coffee and code - perfect morning combo ☕ #dev #coding",
            "Weekend vibes with friends! 🎉 #weekend #friends",
            "New blog post is live! Check it out #blogging #tech"
        ]
        
        return {
            'user_id': f"user_{random.randint(1000, 9999)}",
            'content': random.choice(content_options),
            'timestamp': datetime.utcnow().isoformat(),
            'location': f"City_{random.randint(1, 100)}",
            'tags': ['social', 'post']
        }
        
    def generate_malformed_post(self) -> dict:
        """Generate malformed post for error testing"""
        error_types = [
            # Missing required fields
            {'content': 'Missing user_id', 'timestamp': datetime.utcnow().isoformat()},
            
            # Invalid data types  
            {'user_id': 123, 'content': 456, 'timestamp': 'invalid_date'},
            
            # Content too long
            {'user_id': 'user_001', 'content': 'x' * 500, 'timestamp': datetime.utcnow().isoformat()},
            
            # Invalid JSON structure
            {'user_id': 'user_002', 'content': None, 'timestamp': None},
        ]
        
        return random.choice(error_types)
        
    def generate_corrupt_message(self) -> bytes:
        """Generate corrupted binary data"""
        corrupt_options = [
            b'\x00\x01\x02invalid json\xff\xfe',  # Binary corruption
            b'{"invalid": json missing brace',      # Malformed JSON
            b'\xc3\x28invalid utf8',               # Invalid UTF-8
            b'',                                   # Empty message
        ]
        return random.choice(corrupt_options)
        
    def start_producing(self, topic: str, messages_per_second: int = 10, 
                       error_rate: float = 0.1):
        """Start producing test messages with configurable error rate"""
        
        print(f"🎯 Starting test data production to {topic}")
        print(f"📊 Rate: {messages_per_second} msg/sec, Error rate: {error_rate*100}%")
        
        message_count = 0
        
        try:
            while True:
                start_time = time.time()
                
                for _ in range(messages_per_second):
                    message_key = f"msg_{message_count}"
                    
                    # Determine message type based on error rate
                    rand = random.random()
                    
                    if rand < error_rate * 0.3:  # 30% of errors are corrupt
                        # Send corrupt binary data
                        self.producer.send(
                            topic, 
                            value=self.generate_corrupt_message(),
                            key=message_key
                        )
                        print(f"📤 Sent corrupt message: {message_key}")
                        
                    elif rand < error_rate:  # Rest of errors are malformed JSON
                        message = self.generate_malformed_post()
                        self.producer.send(topic, value=message, key=message_key)
                        print(f"📤 Sent malformed message: {message_key}")
                        
                    else:  # Valid messages
                        message = self.generate_valid_post()
                        self.producer.send(topic, value=message, key=message_key)
                        print(f"📤 Sent valid message: {message_key}")
                    
                    message_count += 1
                
                # Maintain target rate
                elapsed = time.time() - start_time
                if elapsed < 1.0:
                    time.sleep(1.0 - elapsed)
                    
        except KeyboardInterrupt:
            print(f"\n🛑 Stopped producing after {message_count} messages")
        finally:
            self.producer.close()

if __name__ == "__main__":
    producer = TestDataProducer('localhost:9092')
    producer.start_producing('streamsocial-posts', messages_per_second=5, error_rate=0.2)
