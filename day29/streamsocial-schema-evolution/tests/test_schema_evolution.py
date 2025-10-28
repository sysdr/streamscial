import unittest
import json
import time
import threading
from datetime import datetime
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from registry.schema_client import SchemaRegistryClient
from producer.profile_producer import StreamSocialProducer
from consumer.profile_consumer import StreamSocialConsumer

class TestSchemaEvolution(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment"""
        cls.registry = SchemaRegistryClient()
        cls.producer = StreamSocialProducer()
        
        # Wait for services
        print("⏳ Waiting for services...")
        cls.registry.wait_for_registry(timeout=60)
        time.sleep(5)  # Additional wait for Kafka
        
    def test_01_schema_registration(self):
        """Test schema registration with different compatibility modes"""
        print("\n🧪 Testing schema registration...")
        
        # Setup schemas
        self.producer.setup_schemas()
        
        # Verify subjects exist
        subjects = self.registry.list_subjects()
        self.assertIn("user-profile", subjects)
        
        # Check versions
        versions = self.registry.get_versions("user-profile")
        self.assertGreaterEqual(len(versions), 1)
        print(f"✅ Found {len(versions)} schema versions")
        
    def test_02_backward_compatibility(self):
        """Test backward compatibility - new consumers read old data"""
        print("\n🧪 Testing backward compatibility...")
        
        # V1 profile data (old format)
        v1_data = {
            "user_id": "test_001",
            "username": "test_user",
            "email": "test@example.com",
            "created_at": int(datetime.now().timestamp() * 1000)
        }
        
        # Consumer should handle V1 data gracefully
        consumer = StreamSocialConsumer(group_id="test-backward")
        processed = consumer.process_profile_v1(v1_data)
        
        # Verify processing
        self.assertEqual(processed["user_id"], "test_001")
        self.assertEqual(processed["version"], "v1")
        self.assertFalse(processed["has_avatar"])
        self.assertFalse(processed["is_premium"])
        
        consumer.close()
        print("✅ Backward compatibility test passed")
        
    def test_03_forward_compatibility(self):
        """Test forward compatibility - old consumers read new data"""
        print("\n🧪 Testing forward compatibility...")
        
        # V3 profile data (new format with premium fields)
        v3_data = {
            "user_id": "test_002",
            "username": "premium_user",
            "email": "premium@example.com",
            "created_at": int(datetime.now().timestamp() * 1000),
            "avatar_url": "https://example.com/avatar.jpg",
            "premium_badge": "gold",
            "subscription_tier": "yearly"
        }
        
        # V1 consumer should handle V3 data (ignore new fields)
        consumer = StreamSocialConsumer(group_id="test-forward")
        processed = consumer.process_profile_v1(v3_data)
        
        # Verify V1 processing ignores new fields
        self.assertEqual(processed["user_id"], "test_002")
        self.assertEqual(processed["version"], "v1")
        
        consumer.close()
        print("✅ Forward compatibility test passed")
        
    def test_04_full_compatibility(self):
        """Test full compatibility with all schema versions"""
        print("\n🧪 Testing full compatibility...")
        
        consumer = StreamSocialConsumer(group_id="test-full")
        
        # Test V1 data
        v1_data = {"user_id": "v1_user", "username": "v1", "email": "v1@test.com", 
                   "created_at": 1640995200000}
        v1_processed = consumer.process_profile_v1(v1_data)
        self.assertEqual(v1_processed["version"], "v1")
        
        # Test V2 data
        v2_data = v1_data.copy()
        v2_data["avatar_url"] = "https://example.com/v2.jpg"
        v2_processed = consumer.process_profile_v2(v2_data)
        self.assertEqual(v2_processed["version"], "v2")
        self.assertTrue(v2_processed["has_avatar"])
        
        # Test V3 data
        v3_data = v2_data.copy()
        v3_data.update({"premium_badge": "platinum", "subscription_tier": "monthly"})
        v3_processed = consumer.process_profile_v3(v3_data)
        self.assertEqual(v3_processed["version"], "v3")
        self.assertTrue(v3_processed["is_premium"])
        
        consumer.close()
        print("✅ Full compatibility test passed")
        
    def test_05_schema_compatibility_check(self):
        """Test schema compatibility validation"""
        print("\n🧪 Testing compatibility validation...")
        
        # Load schemas
        with open("src/schemas/user_profile_v2.json") as f:
            v2_schema = f.read()
        with open("src/schemas/user_profile_v3.json") as f:
            v3_schema = f.read()
            
        # Test compatibility checks
        v2_compatible = self.registry.check_compatibility("user-profile", v2_schema)
        v3_compatible = self.registry.check_compatibility("user-profile", v3_schema)
        
        print(f"✅ V2 schema compatibility: {v2_compatible}")
        print(f"✅ V3 schema compatibility: {v3_compatible}")
        
    def test_06_end_to_end_flow(self):
        """Test complete end-to-end schema evolution flow"""
        print("\n🧪 Testing end-to-end flow...")
        
        # Start consumer in background
        consumer = StreamSocialConsumer(group_id="test-e2e")
        profiles = []
        
        def consume_data():
            nonlocal profiles
            profiles = consumer.consume_profiles(timeout_sec=15)
        
        consumer_thread = threading.Thread(target=consume_data)
        consumer_thread.start()
        
        # Wait a moment for consumer to start
        time.sleep(2)
        
        # Produce mixed version profiles
        producer = StreamSocialProducer()
        producer.produce_profiles(count=10)
        producer.close()
        
        # Wait for consumption to complete
        consumer_thread.join(timeout=20)
        consumer.close()
        
        # Verify results
        self.assertGreater(len(profiles), 0, "Should have consumed some profiles")
        
        # Check version distribution
        versions = [p.get("version", "v1") for p in profiles]
        version_counts = {v: versions.count(v) for v in ["v1", "v2", "v3"]}
        
        print(f"✅ Processed profiles: {len(profiles)}")
        print(f"✅ Version distribution: {version_counts}")
        
        # Verify all profiles have required fields
        for profile in profiles:
            self.assertIn("user_id", profile)
            self.assertIn("version", profile)
            self.assertIn("has_avatar", profile)
            self.assertIn("is_premium", profile)

if __name__ == "__main__":
    # Run tests with verbose output
    unittest.main(verbosity=2, buffer=True)
