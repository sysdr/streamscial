"""Script to populate dashboard with sample data for demonstration"""
import json
import time
import random
from datetime import datetime, timedelta
import redis
from confluent_kafka import Producer

# Redis connection
try:
    redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True, socket_connect_timeout=2)
    redis_client.ping()
    print("✓ Connected to Redis")
except Exception as e:
    print(f"✗ Redis connection failed: {e}")
    print("Please ensure Redis is running (docker-compose up -d)")
    exit(1)

# Kafka producer
BOOTSTRAP_SERVERS = 'localhost:9092'
DLQ_TOPIC = 'moderation-dlq'

try:
    producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})
    print("✓ Connected to Kafka")
except Exception as e:
    print(f"✗ Kafka connection failed: {e}")
    print("Please ensure Kafka is running (docker-compose up -d)")
    exit(1)

# Error types for variety
ERROR_TYPES = [
    'TimeoutError',
    'ServiceUnavailable',
    'UnicodeDecodeError',
    'RateLimitExceeded',
    'ConnectionError',
    'InvalidContentError'
]

def populate_retry_data(count=8):
    """Populate Redis with active retry records"""
    print(f"\n📝 Creating {count} active retry records...")
    
    for i in range(count):
        record_key = f"post-retry-{i+1:03d}"
        error_type = random.choice(ERROR_TYPES)
        attempts = random.randint(1, 5)
        last_attempt = int((time.time() - random.randint(0, 3600)) * 1000)  # Within last hour
        
        key = f"retry:{record_key}"
        redis_client.hset(key, mapping={
            'attempts': str(attempts),
            'last_attempt': str(last_attempt),
            'error_type': error_type
        })
        redis_client.expire(key, 86400)  # 24 hour TTL
        
        print(f"  ✓ Created retry: {record_key} ({error_type}, {attempts} attempts)")

def populate_permanent_failures(count=3):
    """Populate Redis with permanent failure records"""
    print(f"\n📝 Creating {count} permanent failure records...")
    
    sample_contents = [
        {"userId": "user-123", "content": "Invalid UTF-8 content: \x84\x85", "postId": "post-fail-001"},
        {"userId": "user-456", "content": "Malformed JSON structure", "postId": "post-fail-002"},
        {"userId": "user-789", "content": "Content exceeds maximum length limit", "postId": "post-fail-003"},
    ]
    
    for i in range(count):
        record_key = f"post-perm-{i+1:03d}"
        error_type = random.choice(['UnicodeDecodeError', 'InvalidContentError', 'ValidationError'])
        failed_at = int((time.time() - random.randint(3600, 86400)) * 1000)  # Last 24 hours
        retry_attempts = random.randint(8, 10)
        
        content = sample_contents[i] if i < len(sample_contents) else {
            "userId": f"user-{random.randint(100, 999)}",
            "content": f"Failed content {i+1}",
            "postId": record_key
        }
        
        key = f"permanent_failure:{record_key}"
        redis_client.hset(key, mapping={
            'content': json.dumps(content),
            'error_type': error_type,
            'failed_at': str(failed_at),
            'retry_attempts': str(retry_attempts)
        })
        
        print(f"  ✓ Created permanent failure: {record_key} ({error_type}, {retry_attempts} attempts)")

def populate_dlq_records(count=5):
    """Populate Kafka DLQ topic with sample records"""
    print(f"\n📝 Creating {count} DLQ records in Kafka...")
    
    sample_records = [
        {
            "key": "post-dlq-001",
            "content": {"userId": "user-001", "content": "Timeout during moderation", "postId": "post-dlq-001"},
            "error_type": "TimeoutError",
            "error_msg": "ML API request timeout after 5 seconds"
        },
        {
            "key": "post-dlq-002",
            "content": {"userId": "user-002", "content": "Service unavailable", "postId": "post-dlq-002"},
            "error_type": "ServiceUnavailable",
            "error_msg": "ML service temporarily down for maintenance"
        },
        {
            "key": "post-dlq-003",
            "content": {"userId": "user-003", "content": "Rate limit exceeded", "postId": "post-dlq-003"},
            "error_type": "RateLimitExceeded",
            "error_msg": "Too many requests to moderation API"
        },
        {
            "key": "post-dlq-004",
            "content": {"userId": "user-004", "content": "Connection error occurred", "postId": "post-dlq-004"},
            "error_type": "ConnectionError",
            "error_msg": "Failed to establish connection to moderation service"
        },
        {
            "key": "post-dlq-005",
            "content": {"userId": "user-005", "content": "Invalid content format", "postId": "post-dlq-005"},
            "error_type": "InvalidContentError",
            "error_msg": "Content validation failed: invalid format"
        },
    ]
    
    for i, record in enumerate(sample_records[:count]):
        try:
            headers = [
                ('__connect.errors.class.name', record['error_type'].encode('utf-8')),
                ('__connect.errors.exception.message', record['error_msg'].encode('utf-8')),
                ('__connect.errors.topic', 'user-posts'.encode('utf-8')),
                ('__connect.errors.retry.topic', 'user-posts'.encode('utf-8')),
            ]
            
            producer.produce(
                topic=DLQ_TOPIC,
                key=record['key'].encode('utf-8'),
                value=json.dumps(record['content']).encode('utf-8'),
                headers=headers,
                timestamp=int(time.time() * 1000)
            )
            
            print(f"  ✓ Created DLQ record: {record['key']} ({record['error_type']})")
        except Exception as e:
            print(f"  ✗ Error creating DLQ record {i+1}: {e}")
    
    producer.flush()
    print(f"  ✓ Flushed {count} DLQ records to Kafka")

def main():
    print("="*60)
    print("Dashboard Data Population Script")
    print("="*60)
    
    # Populate data
    populate_retry_data(count=8)
    populate_permanent_failures(count=3)
    populate_dlq_records(count=5)
    
    print("\n" + "="*60)
    print("✓ Sample data populated successfully!")
    print("="*60)
    print("\nDashboard should now show:")
    print("  - Active Retries: 8")
    print("  - Permanent Failures: 3")
    print("  - DLQ Records: 5")
    print("  - Error Type Distribution: Various error types")
    print("\nRefresh your dashboard at: http://localhost:5000")
    print("="*60)

if __name__ == '__main__':
    main()

