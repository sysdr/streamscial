import json
import time
from kafka import KafkaConsumer
from typing import Dict, Any, List
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from registry.schema_client import SchemaRegistryClient

class StreamSocialConsumer:
    def __init__(self, bootstrap_servers: str = "localhost:9092", group_id: str = "profile-processor"):
        self.consumer = KafkaConsumer(
            "user-profiles",
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        self.registry = SchemaRegistryClient()
        self.stats = {
            "v1_messages": 0,
            "v2_messages": 0,
            "v3_messages": 0,
            "total_messages": 0,
            "processing_errors": 0
        }
        self.processed_profiles = []
    
    def process_profile_v1(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        """Process V1 profile - basic fields only"""
        return {
            "user_id": profile["user_id"],
            "username": profile["username"],
            "email": profile["email"],
            "created_at": profile["created_at"],
            "has_avatar": False,
            "is_premium": False,
            "version": "v1"
        }
    
    def process_profile_v2(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        """Process V2 profile - includes avatar"""
        processed = self.process_profile_v1(profile)
        processed.update({
            "avatar_url": profile.get("avatar_url"),
            "has_avatar": profile.get("avatar_url") is not None,
            "version": "v2"
        })
        return processed
    
    def process_profile_v3(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        """Process V3 profile - includes premium features"""
        processed = self.process_profile_v2(profile)
        processed.update({
            "premium_badge": profile.get("premium_badge"),
            "subscription_tier": profile.get("subscription_tier"),
            "is_premium": profile.get("premium_badge") is not None,
            "version": "v3"
        })
        return processed
    
    def consume_profiles(self, timeout_sec: int = 30):
        """Consume and process profiles with schema evolution support"""
        print(f"🎯 Starting profile consumption (timeout: {timeout_sec}s)...")
        
        start_time = time.time()
        
        try:
            while time.time() - start_time < timeout_sec:
                message_batch = self.consumer.poll(timeout_ms=1000)
                
                if not message_batch:
                    continue
                
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        try:
                            self.process_message(message)
                        except Exception as e:
                            print(f"❌ Error processing message: {e}")
                            self.stats["processing_errors"] += 1
                
                # Print progress every 10 messages
                if self.stats["total_messages"] % 10 == 0 and self.stats["total_messages"] > 0:
                    self.print_stats()
        
        except KeyboardInterrupt:
            print("\n⏹️ Consumption stopped by user")
        
        self.print_final_stats()
        return self.processed_profiles
    
    def process_message(self, message):
        """Process individual message with schema version handling"""
        try:
            data = message.value
            schema_version = data.get("schema_version", "v1")
            profile_data = data.get("data", {})
            
            # Route to appropriate processor
            if schema_version == "v1":
                processed = self.process_profile_v1(profile_data)
                self.stats["v1_messages"] += 1
            elif schema_version == "v2":
                processed = self.process_profile_v2(profile_data)
                self.stats["v2_messages"] += 1
            elif schema_version == "v3":
                processed = self.process_profile_v3(profile_data)
                self.stats["v3_messages"] += 1
            else:
                raise ValueError(f"Unknown schema version: {schema_version}")
            
            self.processed_profiles.append(processed)
            self.stats["total_messages"] += 1
            
        except Exception as e:
            print(f"❌ Message processing error: {e}")
            raise
    
    def print_stats(self):
        """Print processing statistics"""
        print(f"📊 Processed: {self.stats['total_messages']} total | "
              f"V1: {self.stats['v1_messages']} | "
              f"V2: {self.stats['v2_messages']} | "
              f"V3: {self.stats['v3_messages']} | "
              f"Errors: {self.stats['processing_errors']}")
    
    def print_final_stats(self):
        """Print final processing statistics"""
        print("\n📈 Final Processing Statistics:")
        print(f"  Total Messages: {self.stats['total_messages']}")
        print(f"  V1 Profiles: {self.stats['v1_messages']}")
        print(f"  V2 Profiles: {self.stats['v2_messages']}")
        print(f"  V3 Profiles: {self.stats['v3_messages']}")
        print(f"  Processing Errors: {self.stats['processing_errors']}")
        
        if self.stats['total_messages'] > 0:
            print(f"\n📊 Schema Version Distribution:")
            print(f"  V1: {self.stats['v1_messages']/self.stats['total_messages']*100:.1f}%")
            print(f"  V2: {self.stats['v2_messages']/self.stats['total_messages']*100:.1f}%")
            print(f"  V3: {self.stats['v3_messages']/self.stats['total_messages']*100:.1f}%")
    
    def close(self):
        self.consumer.close()

if __name__ == "__main__":
    consumer = StreamSocialConsumer()
    profiles = consumer.consume_profiles(30)
    consumer.close()
    
    # Show sample profiles
    print(f"\n📋 Sample processed profiles:")
    for i, profile in enumerate(profiles[:3]):
        print(f"  {i+1}. {profile}")
