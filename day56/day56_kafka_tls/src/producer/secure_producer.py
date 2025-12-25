"""TLS-enabled Kafka producer for StreamSocial"""
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

class SecureMessageProducer:
    def __init__(self, bootstrap_servers, cert_folder):
        """Initialize secure producer with TLS configuration"""
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            security_protocol='SSL',
            ssl_check_hostname=True,
            ssl_cafile=f'{cert_folder}/ca-cert.pem',
            ssl_certfile=f'{cert_folder}/client-cert.pem',
            ssl_keyfile=f'{cert_folder}/client-key.pem',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1
        )
        self.metrics = {
            'sent': 0,
            'failed': 0,
            'encrypted_bytes': 0
        }
    
    def send_message(self, topic, user_id, message_type, content):
        """Send encrypted message to Kafka"""
        try:
            message = {
                'user_id': user_id,
                'message_type': message_type,
                'content': content,
                'timestamp': datetime.now().isoformat(),
                'encrypted': True
            }
            
            future = self.producer.send(
                topic,
                key=str(user_id),
                value=message
            )
            
            # Wait for confirmation
            record_metadata = future.get(timeout=10)
            
            self.metrics['sent'] += 1
            self.metrics['encrypted_bytes'] += len(json.dumps(message))
            
            return {
                'success': True,
                'partition': record_metadata.partition,
                'offset': record_metadata.offset
            }
            
        except KafkaError as e:
            self.metrics['failed'] += 1
            return {'success': False, 'error': str(e)}
    
    def get_metrics(self):
        """Return producer metrics"""
        return self.metrics
    
    def close(self):
        """Close producer connection"""
        self.producer.flush()
        self.producer.close()

def generate_sample_messages():
    """Generate sample encrypted messages"""
    message_types = ['direct_message', 'payment', 'location_share', 'profile_update']
    users = [f'user_{i}' for i in range(1, 101)]
    
    messages = []
    for _ in range(50):
        msg_type = random.choice(message_types)
        user = random.choice(users)
        
        if msg_type == 'direct_message':
            content = f'Private message: {random.choice(["Meeting at 3pm", "See you soon", "Thanks!"])}'
        elif msg_type == 'payment':
            content = f'Payment ${random.randint(10, 1000)} to merchant_{random.randint(1, 50)}'
        elif msg_type == 'location_share':
            content = f'Location: {random.uniform(-90, 90):.4f}, {random.uniform(-180, 180):.4f}'
        else:
            content = f'Profile updated: {random.choice(["email", "phone", "address"])}'
        
        messages.append((user, msg_type, content))
    
    return messages

if __name__ == '__main__':
    import sys
    
    bootstrap_servers = sys.argv[1] if len(sys.argv) > 1 else 'localhost:9093'
    cert_folder = sys.argv[2] if len(sys.argv) > 2 else './certs'
    
    print(f"Connecting to Kafka at {bootstrap_servers} with TLS encryption...")
    
    producer = SecureMessageProducer(bootstrap_servers, cert_folder)
    
    print("Sending encrypted messages...")
    messages = generate_sample_messages()
    
    for user_id, msg_type, content in messages:
        result = producer.send_message('secure-messages', user_id, msg_type, content)
        if result['success']:
            print(f"✓ Encrypted message sent: {msg_type} from {user_id}")
        else:
            print(f"✗ Failed: {result['error']}")
        time.sleep(0.1)
    
    metrics = producer.get_metrics()
    print(f"\nProducer Metrics:")
    print(f"  Messages sent: {metrics['sent']}")
    print(f"  Messages failed: {metrics['failed']}")
    print(f"  Encrypted bytes: {metrics['encrypted_bytes']:,}")
    
    producer.close()
