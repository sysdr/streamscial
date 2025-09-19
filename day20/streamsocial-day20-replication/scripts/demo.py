#!/usr/bin/env python3
"""
StreamSocial Day 20 Demo: Multi-Region Replication & Disaster Recovery
"""
import time
import threading
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from producers.streamsocial_producer import StreamSocialProducer
from monitors.replication_monitor import ReplicationMonitor
from utils.chaos_engineer import ChaosEngineer

def demo_normal_operations():
    """Demonstrate normal StreamSocial operations"""
    print("\n🌟 DEMO 1: Normal Operations")
    print("=" * 50)
    
    producer = StreamSocialProducer("localhost:9091,localhost:9092,localhost:9093", "us-east")
    
    try:
        # Create realistic social media content
        print("📝 Creating sample posts...")
        posts = []
        for i in range(5):
            post_id = producer.create_post(
                f"influencer_{i:02d}",
                f"Check out this amazing sunset from day {i+1} of my world tour! 🌅 #travel #photography",
                "image"
            )
            posts.append(post_id)
            
            # Add some engagements
            producer.create_engagement(post_id, f"fan_{i*2}", "like")
            producer.create_engagement(post_id, f"fan_{i*2+1}", "share")
            
            time.sleep(0.5)
        
        # Flush messages
        producer.producer.flush(timeout=10)
        
        print(f"✅ Created {len(posts)} posts with engagements")
        print(f"📊 Total messages sent: {producer.messages_sent}")
        
    finally:
        producer.flush_and_close()

def demo_replication_monitoring():
    """Demonstrate real-time replication monitoring"""
    print("\n📡 DEMO 2: Replication Monitoring")
    print("=" * 50)
    
    monitor = ReplicationMonitor("localhost:9091,localhost:9092,localhost:9093")
    
    try:
        monitor.start_monitoring(interval_seconds=2)
        
        print("🔍 Monitoring cluster for 15 seconds...")
        for i in range(15):
            time.sleep(1)
            if i % 5 == 0:
                data = monitor.get_monitoring_data()
                health = data.get('health_score', {})
                print(f"   Health: {health.get('overall_health', 0):.1f}% | "
                      f"Partitions: {health.get('total_partitions', 0)} | "
                      f"Under-replicated: {health.get('under_replicated_partitions', 0)}")
        
        # Final status
        final_data = monitor.get_monitoring_data()
        print(f"\n📈 Final Cluster Status:")
        print(f"   Brokers: {len(final_data.get('cluster_metadata', {}).get('brokers', {}))}")
        print(f"   Topics: {len(final_data.get('cluster_metadata', {}).get('topics', {}))}")
        print(f"   Recent changes: {len(final_data.get('recent_changes', []))}")
        
    finally:
        monitor.stop_monitoring()

def demo_chaos_engineering():
    """Demonstrate chaos engineering scenarios"""
    print("\n💥 DEMO 3: Chaos Engineering")
    print("=" * 50)
    
    chaos = ChaosEngineer()
    monitor = ReplicationMonitor("localhost:9091,localhost:9092,localhost:9093")
    producer = StreamSocialProducer("localhost:9091,localhost:9092,localhost:9093", "chaos-test")
    
    try:
        monitor.start_monitoring(interval_seconds=2)
        time.sleep(2)
        
        # Get baseline health
        baseline = monitor.get_monitoring_data()
        baseline_health = baseline.get('health_score', {}).get('overall_health', 0)
        print(f"🏥 Baseline health: {baseline_health:.1f}%")
        
        # Inject single broker failure
        print(f"\n💣 Injecting single broker failure...")
        failure_id = chaos.inject_failure("single_broker_failure")
        print(f"   Failure ID: {failure_id}")
        
        # Monitor during failure
        print(f"📊 Monitoring during failure (30 seconds)...")
        for i in range(30):
            if i % 10 == 0:
                data = monitor.get_monitoring_data()
                health = data.get('health_score', {}).get('overall_health', 0)
                changes = len(data.get('recent_changes', []))
                print(f"   t+{i}s: Health={health:.1f}%, Changes={changes}")
            
            # Try to send messages during failure
            if i % 5 == 0:
                try:
                    producer.create_post(f"user_chaos_{i}", f"Message during failure at t+{i}s")
                    producer.producer.flush(timeout=2)
                except Exception as e:
                    print(f"   Expected error during failure: {type(e).__name__}")
            
            time.sleep(1)
        
        # Wait for automatic recovery
        print(f"🔄 Waiting for automatic recovery...")
        time.sleep(35)  # Single broker failure recovers after 60s total
        
        # Check post-recovery health
        recovery_data = monitor.get_monitoring_data()
        recovery_health = recovery_data.get('health_score', {}).get('overall_health', 0)
        print(f"🏥 Post-recovery health: {recovery_health:.1f}%")
        
        # Summary
        status = chaos.get_failure_status()
        print(f"\n📋 Chaos Engineering Summary:")
        print(f"   Total failures injected: {status['total_failures']}")
        print(f"   Active failures: {status['active_failures']}")
        print(f"   Health impact: {baseline_health:.1f}% → {recovery_health:.1f}%")
        
    except Exception as e:
        print(f"❌ Chaos demo error: {e}")
    
    finally:
        monitor.stop_monitoring()
        producer.flush_and_close()

def demo_multi_region_simulation():
    """Demonstrate multi-region setup simulation"""
    print("\n🌍 DEMO 4: Multi-Region Simulation")
    print("=" * 50)
    
    regions = ["us-east", "eu-west", "asia-pacific"]
    producers = {}
    
    try:
        # Create producers for each region
        for region in regions:
            producers[region] = StreamSocialProducer(
                "localhost:9091,localhost:9092,localhost:9093", 
                region
            )
        
        print("🗺️ Simulating multi-region traffic...")
        
        # Simulate cross-region posting
        for i in range(3):
            for region in regions:
                producer = producers[region]
                post_id = producer.create_post(
                    f"{region}_user_{i}",
                    f"Posting from {region} - Message {i+1}",
                    "text"
                )
                
                # Cross-region engagements
                for other_region in regions:
                    if other_region != region:
                        other_producer = producers[other_region]
                        other_producer.create_engagement(
                            post_id, 
                            f"{other_region}_fan_{i}",
                            "like"
                        )
                
                time.sleep(0.5)
        
        # Flush all regions
        total_messages = 0
        for region, producer in producers.items():
            producer.producer.flush(timeout=10)
            total_messages += producer.messages_sent
            print(f"   {region}: {producer.messages_sent} messages")
        
        print(f"✅ Multi-region simulation complete: {total_messages} total messages")
        
    finally:
        for producer in producers.values():
            producer.flush_and_close()

def main():
    """Run all demos in sequence"""
    print("🚀 StreamSocial Day 20 Demo: Kafka Replication & ISR Management")
    print("================================================================")
    print("This demo will showcase:")
    print("  1. Normal StreamSocial operations with replication")
    print("  2. Real-time replication monitoring")
    print("  3. Chaos engineering and failure scenarios")
    print("  4. Multi-region traffic simulation")
    print("\n⚠️  Note: Docker containers must be running for this demo to work!")
    print("   Run 'cd docker && docker-compose up -d' if you haven't already.\n")
    
    input("Press Enter to start the demo...")
    
    try:
        demo_normal_operations()
        demo_replication_monitoring()
        demo_chaos_engineering()
        demo_multi_region_simulation()
        
        print("\n🎉 Demo Complete!")
        print("=" * 50)
        print("Key takeaways:")
        print("✅ Kafka replication ensures data durability")
        print("✅ ISR monitoring helps detect issues early")
        print("✅ Leader election provides automatic failover")
        print("✅ Multi-region setup enables disaster recovery")
        print("✅ Chaos engineering validates system resilience")
        
        print(f"\n🔗 Next steps:")
        print(f"   • Start the web dashboard: python dashboard/app.py")
        print(f"   • Run the test suite: python -m pytest tests/ -v")
        print(f"   • Explore chaos scenarios in the web UI")
        
    except KeyboardInterrupt:
        print("\n🛑 Demo interrupted by user")
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        print("Make sure Docker containers are running:")
        print("  cd docker && docker-compose up -d")

if __name__ == "__main__":
    main()
