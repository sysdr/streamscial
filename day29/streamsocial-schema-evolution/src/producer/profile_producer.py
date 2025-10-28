import json
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer
from typing import Dict, Any
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from registry.schema_client import SchemaRegistryClient

class StreamSocialProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        self.registry = SchemaRegistryClient()
        self.schema_cache = {}
        
    def load_schema(self, schema_file: str) -> str:
        """Load schema from file"""
        if schema_file not in self.schema_cache:
            with open(schema_file, 'r') as f:
                self.schema_cache[schema_file] = f.read()
        return self.schema_cache[schema_file]
    
    def setup_schemas(self):
        """Register all schema versions with different compatibility modes"""
        # Wait for registry
        self.registry.wait_for_registry()
        
        # V1 Schema - Backward compatibility (default)
        v1_schema = self.load_schema("src/schemas/user_profile_v1.json")
        self.registry.set_compatibility("user-profile", "BACKWARD")
        v1_id = self.registry.register_schema("user-profile", v1_schema)
        print(f"📋 Registered V1 schema (ID: {v1_id}) - BACKWARD compatibility")
        
        # V2 Schema - Keep backward compatibility  
        v2_schema = self.load_schema("src/schemas/user_profile_v2.json")
        compatible = self.registry.check_compatibility("user-profile", v2_schema)
        print(f"✅ V2 compatibility check: {compatible}")
        
        if compatible:
            v2_id = self.registry.register_schema("user-profile", v2_schema)
            print(f"📋 Registered V2 schema (ID: {v2_id})")
        
        # V3 Schema - Full compatibility for mobile apps
        self.registry.set_compatibility("user-profile", "FULL")
        v3_schema = self.load_schema("src/schemas/user_profile_v3.json")
        compatible = self.registry.check_compatibility("user-profile", v3_schema)
        print(f"✅ V3 compatibility check: {compatible}")
        
        if compatible:
            v3_id = self.registry.register_schema("user-profile", v3_schema)
            print(f"📋 Registered V3 schema (ID: {v3_id}) - FULL compatibility")
    
    def create_profile_v1(self, user_id: str) -> Dict[str, Any]:
        """Create V1 profile data"""
        return {
            "user_id": user_id,
            "username": f"user_{user_id}",
            "email": f"user{user_id}@streamsocial.com",
            "created_at": int(datetime.now(timezone.utc).timestamp() * 1000)
        }
    
    def create_profile_v2(self, user_id: str) -> Dict[str, Any]:
        """Create V2 profile data with avatar"""
        profile = self.create_profile_v1(user_id)
        profile["avatar_url"] = f"https://avatars.streamsocial.com/{user_id}.jpg"
        return profile
    
    def create_profile_v3(self, user_id: str) -> Dict[str, Any]:
        """Create V3 profile data with premium features"""
        profile = self.create_profile_v2(user_id)
        if random.random() > 0.7:  # 30% premium users
            profile["premium_badge"] = random.choice(["gold", "platinum", "diamond"])
            profile["subscription_tier"] = random.choice(["monthly", "yearly", "lifetime"])
        return profile
    
    def produce_profiles(self, count: int = 100):
        """Produce user profiles with mixed schema versions"""
        print(f"🚀 Producing {count} user profiles...")
        
        for i in range(count):
            user_id = f"{10000 + i}"
            
            # Simulate different mobile app versions
            version_weight = random.random()
            if version_weight < 0.3:  # 30% legacy v1 clients
                profile = self.create_profile_v1(user_id)
                schema_version = "v1"
            elif version_weight < 0.7:  # 40% v2 clients
                profile = self.create_profile_v2(user_id)
                schema_version = "v2"
            else:  # 30% latest v3 clients
                profile = self.create_profile_v3(user_id)
                schema_version = "v3"
            
            # Add schema metadata
            message = {
                "schema_version": schema_version,
                "data": profile
            }
            
            self.producer.send(
                "user-profiles",
                key=user_id,
                value=message
            )
            
            if (i + 1) % 20 == 0:
                print(f"📤 Produced {i + 1}/{count} profiles")
        
        self.producer.flush()
        print("✅ All profiles produced successfully")
    
    def close(self):
        self.producer.close()

if __name__ == "__main__":
    producer = StreamSocialProducer()
    producer.setup_schemas()
    producer.produce_profiles(50)
    producer.close()
