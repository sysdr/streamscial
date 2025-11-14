"""CDC event consumer for processing user profile changes"""

from kafka import KafkaConsumer
import json
from datetime import datetime
import time

class CDCConsumer:
    def __init__(self):
        self.consumer = None
        self.processed_count = 0
        self.connect_kafka()
        
    def connect_kafka(self):
        """Connect to Kafka with retry logic"""
        max_retries = 30
        for i in range(max_retries):
            try:
                self.consumer = KafkaConsumer(
                    'cdc.user_profiles',
                    bootstrap_servers=['localhost:9092'],
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    group_id='profile-change-processor',
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
                    key_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None
                )
                print("✓ Connected to Kafka as CDC consumer")
                return
            except Exception as e:
                if i < max_retries - 1:
                    print(f"Waiting for Kafka... ({i+1}/{max_retries})")
                    time.sleep(2)
                else:
                    raise Exception(f"Failed to connect to Kafka: {e}")
    
    def process_create_event(self, payload):
        """Handle new user creation"""
        after = payload.get('after', {})
        print(f"\n📝 NEW USER CREATED:")
        print(f"   User ID: {after.get('user_id')}")
        print(f"   Username: {after.get('username')}")
        print(f"   Email: {after.get('email')}")
        print(f"   Location: {after.get('location')}")
    
    def process_update_event(self, payload):
        """Handle user profile updates"""
        before = payload.get('before', {})
        after = payload.get('after', {})
        
        print(f"\n🔄 PROFILE UPDATED:")
        print(f"   User ID: {after.get('user_id')}")
        print(f"   Username: {after.get('username')}")
        
        # Detect what changed
        changes = []
        for key in after.keys():
            if key in ['updated_at', 'created_at']:
                continue
            if before.get(key) != after.get(key):
                changes.append(f"{key}: {before.get(key)} → {after.get(key)}")
        
        if changes:
            print("   Changes:")
            for change in changes[:3]:  # Show first 3 changes
                print(f"      - {change}")
    
    def process_delete_event(self, key, payload):
        """Handle user deletion (tombstone)"""
        if payload is None:
            print(f"\n🗑️  TOMBSTONE EVENT:")
            print(f"   User ID: {key.get('user_id') if key else 'Unknown'}")
            print("   User profile fully deleted (tombstone for log compaction)")
        else:
            before = payload.get('before', {})
            print(f"\n❌ USER DELETED:")
            print(f"   User ID: {before.get('user_id')}")
            print(f"   Username: {before.get('username')}")
    
    def consume_events(self, duration_seconds=120):
        """Consume and process CDC events"""
        print(f"\n🎧 Listening for CDC events for {duration_seconds} seconds...")
        start_time = time.time()
        
        for message in self.consumer:
            try:
                if time.time() - start_time > duration_seconds:
                    break
                
                key = message.key
                value = message.value
                
                # Handle tombstone events
                if value is None:
                    self.process_delete_event(key, None)
                    self.processed_count += 1
                    continue
                
                payload = value.get('payload', {})
                op = payload.get('op')
                
                if op == 'c':  # Create
                    self.process_create_event(payload)
                elif op == 'u':  # Update
                    self.process_update_event(payload)
                elif op == 'd':  # Delete
                    self.process_delete_event(key, payload)
                elif op == 'r':  # Read (initial snapshot)
                    print(f"📸 Snapshot record: User {payload.get('after', {}).get('username')}")
                
                self.processed_count += 1
                
            except Exception as e:
                print(f"❌ Error processing message: {e}")
        
        print(f"\n✓ Processed {self.processed_count} CDC events")
    
    def close(self):
        """Close consumer"""
        if self.consumer:
            self.consumer.close()

if __name__ == "__main__":
    consumer = CDCConsumer()
    try:
        consumer.consume_events(120)
    finally:
        consumer.close()
