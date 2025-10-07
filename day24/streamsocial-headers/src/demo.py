#!/usr/bin/env python3
import asyncio
import threading
import time
import random
from producers.post_producer import PostProducer, FeatureFlagService
from consumers.post_consumer import RecommendationConsumer, NotificationConsumer
from common.tracing import global_tracer

def run_consumer(consumer_class, max_messages=5):
    """Run consumer in separate thread"""
    consumer = consumer_class()
    consumer.process_posts(max_messages)

def main():
    print("🚀 StreamSocial Headers & Metadata Demo")
    print("=" * 50)
    
    # Clear any existing traces
    global_tracer.clear()
    
    # Initialize services
    producer = PostProducer()
    flag_service = FeatureFlagService()
    
    # Start consumers in background threads
    print("📱 Starting consumers...")
    recommendation_thread = threading.Thread(
        target=run_consumer, 
        args=(RecommendationConsumer, 10),
        daemon=True
    )
    notification_thread = threading.Thread(
        target=run_consumer, 
        args=(NotificationConsumer, 10),
        daemon=True
    )
    
    recommendation_thread.start()
    notification_thread.start()
    
    # Give consumers time to start
    time.sleep(2)
    
    # Simulate user posts with different feature flags
    print("✏️ Publishing posts with headers...")
    
    users = ["alice", "bob", "charlie", "diana", "eve"]
    posts = [
        "Just discovered an amazing coffee shop! ☕",
        "Working on a new project, very excited! 🚀",
        "Beautiful sunset today 🌅",
        "Reading a fantastic book about AI",
        "Weekend hiking adventure! 🥾"
    ]
    
    for i in range(5):
        user = random.choice(users)
        post = random.choice(posts)
        
        # Get feature flags for user
        flags, experiment_id = flag_service.get_user_flags(user, "premium")
        
        print(f"\n📝 Publishing post {i+1}/5 from {user}")
        print(f"   Content: {post}")
        print(f"   Experiment: {experiment_id or 'None'}")
        
        success = producer.publish_post(
            user_id=user,
            content=post,
            feature_flags=flags,
            user_segment="premium",
            experiment_id=experiment_id
        )
        
        if success:
            print(f"   ✅ Published successfully")
        else:
            print(f"   ❌ Failed to publish")
        
        time.sleep(1)  # Space out messages
    
    # Wait for processing to complete
    print(f"\n⏳ Waiting for message processing...")
    time.sleep(3)
    
    # Show trace summary
    traces = global_tracer.get_traces()
    print(f"\n📊 Trace Summary:")
    print(f"   Total spans: {len(traces)}")
    
    services = set(trace.service_name for trace in traces)
    print(f"   Services involved: {', '.join(services)}")
    
    avg_duration = sum(trace.duration_ms() for trace in traces) / len(traces)
    print(f"   Average span duration: {avg_duration:.1f}ms")
    
    # Group by trace ID
    trace_groups = {}
    for trace in traces:
        if trace.trace_id not in trace_groups:
            trace_groups[trace.trace_id] = []
        trace_groups[trace.trace_id].append(trace)
    
    print(f"   Complete traces: {len(trace_groups)}")
    
    print(f"\n🔗 Sample trace flow:")
    if trace_groups:
        sample_trace_id = list(trace_groups.keys())[0]
        sample_spans = sorted(trace_groups[sample_trace_id], key=lambda x: x.start_time)
        
        for span in sample_spans:
            print(f"   {span.service_name} → {span.operation_name} ({span.duration_ms():.1f}ms)")
    
    producer.close()
    
    print(f"\n🎯 Demo completed successfully!")
    print(f"💡 Start the web dashboard to see real-time tracing: python src/web/dashboard.py")

if __name__ == "__main__":
    main()
