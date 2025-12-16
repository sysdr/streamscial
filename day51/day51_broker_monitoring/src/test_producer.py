from confluent_kafka import Producer
import json
import time
import random
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_producer():
    config = {
        'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
        'client.id': 'streamsocial-metrics-tester',
        'acks': 'all',
        'compression.type': 'lz4'
    }
    return Producer(config)

def generate_test_data():
    """Generate sample StreamSocial events"""
    event_types = ['post', 'like', 'comment', 'share', 'follow']
    return {
        'event_type': random.choice(event_types),
        'user_id': f'user_{random.randint(1, 10000)}',
        'timestamp': int(time.time() * 1000),
        'content_id': f'content_{random.randint(1, 50000)}',
        'metadata': {
            'platform': random.choice(['web', 'ios', 'android']),
            'region': random.choice(['us-east', 'us-west', 'eu-west', 'ap-south'])
        }
    }

def main():
    producer = create_producer()
    topics = ['user-events', 'content-events', 'engagement-events']
    
    logger.info("Starting test data generation...")
    logger.info("Target: 1000-5000 messages/sec across all brokers")
    
    messages_sent = 0
    start_time = time.time()
    
    try:
        while True:
            # Generate bursts of messages
            batch_size = random.randint(100, 500)
            
            for _ in range(batch_size):
                topic = random.choice(topics)
                data = generate_test_data()
                
                producer.produce(
                    topic=topic,
                    value=json.dumps(data).encode('utf-8'),
                    key=data['user_id'].encode('utf-8')
                )
                
                messages_sent += 1
            
            producer.flush()
            
            # Calculate rate
            elapsed = time.time() - start_time
            if elapsed > 0:
                rate = messages_sent / elapsed
                logger.info(f"Sent {messages_sent} messages ({rate:.0f} msg/s)")
            
            # Sleep briefly
            time.sleep(random.uniform(0.1, 0.3))
            
    except KeyboardInterrupt:
        logger.info("Stopping producer...")
        producer.flush()
        logger.info(f"Total messages sent: {messages_sent}")

if __name__ == '__main__':
    main()
