"""TLS-enabled Kafka consumer for StreamSocial"""
import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from datetime import datetime

class SecureMessageConsumer:
    def __init__(self, bootstrap_servers, cert_folder, group_id='secure-consumer'):
        """Initialize secure consumer with TLS configuration"""
        self.consumer = KafkaConsumer(
            'secure-messages',
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            security_protocol='SSL',
            ssl_check_hostname=True,
            ssl_cafile=f'{cert_folder}/ca-cert.pem',
            ssl_certfile=f'{cert_folder}/client-cert.pem',
            ssl_keyfile=f'{cert_folder}/client-key.pem',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            max_poll_records=10
        )
        self.metrics = {
            'consumed': 0,
            'decrypted_bytes': 0,
            'by_type': {}
        }
    
    def consume_messages(self, max_messages=None):
        """Consume and decrypt messages"""
        try:
            count = 0
            for message in self.consumer:
                data = message.value
                
                self.metrics['consumed'] += 1
                self.metrics['decrypted_bytes'] += len(json.dumps(data))
                
                msg_type = data.get('message_type', 'unknown')
                self.metrics['by_type'][msg_type] = self.metrics['by_type'].get(msg_type, 0) + 1
                
                print(f"Decrypted: {msg_type} from {data['user_id']}")
                
                count += 1
                if max_messages and count >= max_messages:
                    break
                    
        except KeyboardInterrupt:
            print("\nStopping consumer...")
        except KafkaError as e:
            print(f"Error consuming messages: {e}")
    
    def get_metrics(self):
        """Return consumer metrics"""
        return self.metrics
    
    def close(self):
        """Close consumer connection"""
        self.consumer.close()

if __name__ == '__main__':
    import sys
    
    bootstrap_servers = sys.argv[1] if len(sys.argv) > 1 else 'localhost:9093'
    cert_folder = sys.argv[2] if len(sys.argv) > 2 else './certs'
    
    print(f"Connecting to Kafka at {bootstrap_servers} with TLS encryption...")
    
    consumer = SecureMessageConsumer(bootstrap_servers, cert_folder)
    
    print("Consuming encrypted messages (Ctrl+C to stop)...")
    consumer.consume_messages()
    
    metrics = consumer.get_metrics()
    print(f"\nConsumer Metrics:")
    print(f"  Messages consumed: {metrics['consumed']}")
    print(f"  Decrypted bytes: {metrics['decrypted_bytes']:,}")
    print(f"  By type: {metrics['by_type']}")
    
    consumer.close()
