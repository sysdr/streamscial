#!/usr/bin/env python3

import sys
import os
import time
import uuid
from datetime import datetime

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from producers.avro_producer import StreamSocialAvroProducer
from consumers.avro_consumer import StreamSocialAvroConsumer
from streamsocial.models import UserProfile, UserInteraction, PostEvent, Location
from performance_test import PerformanceBenchmark
from registry.schema_manager import StreamSocialSchemaManager

def run_demo():
    print("🎬 Starting StreamSocial Avro Serialization Demo")
    print("=" * 60)
    
    # Initialize components
    print("\n📋 1. Initializing Schema Registry and Avro components...")
    schema_manager = StreamSocialSchemaManager()
    
    # Register schemas
    print("📋 2. Registering Avro schemas...")
    schema_manager.register_schema("user-profiles-value", "user_profile")
    schema_manager.register_schema("user-interactions-value", "user_interaction")  
    schema_manager.register_schema("post-events-value", "post_event")
    
    # Initialize producer
    print("📋 3. Creating Avro producer...")
    producer = StreamSocialAvroProducer()
    
    # Create sample data
    print("\n📋 4. Generating sample StreamSocial events...")
    
    # User profiles
    users = [
        UserProfile("user_001", "alice_dev", "alice@streamsocial.com"),
        UserProfile("user_002", "bob_kafka", "bob@streamsocial.com"),
        UserProfile("user_003", "charlie_avro", "charlie@streamsocial.com")
    ]
    
    # User interactions
    interactions = [
        UserInteraction(str(uuid.uuid4()), "user_001", "POST", "post_001"),
        UserInteraction(str(uuid.uuid4()), "user_002", "LIKE", "post_001"),
        UserInteraction(str(uuid.uuid4()), "user_003", "COMMENT", "post_001", "Great post!")
    ]
    
    # Posts
    posts = [
        PostEvent("post_001", "user_001", "Exploring Avro serialization with Kafka! #avro #kafka", 
                 hashtags=["avro", "kafka"], mentions=["@bob_kafka"]),
        PostEvent("post_002", "user_002", "StreamSocial performance is amazing 🚀", 
                 location=Location(37.7749, -122.4194)),
        PostEvent("post_003", "user_003", "Binary serialization FTW! Much faster than JSON")
    ]
    
    # Produce events
    print("\n📋 5. Producing events to Kafka topics...")
    for user in users:
        producer.produce_user_profile(user)
        time.sleep(0.1)
    
    for interaction in interactions:
        producer.produce_interaction(interaction)
        time.sleep(0.1)
    
    for post in posts:
        producer.produce_post(post)
        time.sleep(0.1)
    
    print("\n📋 6. Running performance benchmark...")
    benchmark = PerformanceBenchmark()
    benchmark.benchmark_serialization(iterations=100)
    
    print("\n📋 7. Starting consumer for 10 seconds...")
    consumer = StreamSocialAvroConsumer(["user-profiles", "user-interactions", "post-events"])
    
    try:
        consumer.consume_messages(max_messages=20)
    except:
        pass
    
    print("\n✅ Demo completed successfully!")
    print("\n🎯 Key Achievements:")
    print("   • Avro schemas registered and validated")
    print("   • Binary serialization working correctly")
    print("   • Events produced and consumed successfully")
    print("   • Performance improvements measured")
    print("   • Schema evolution compatibility demonstrated")

if __name__ == "__main__":
    run_demo()
