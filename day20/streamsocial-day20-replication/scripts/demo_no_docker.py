#!/usr/bin/env python3
"""
StreamSocial Day 20 Demo: Multi-Region Replication & Disaster Recovery (No Docker Version)
"""
import time
import threading
import sys
import os
import json
import random
from datetime import datetime

# Mock implementations for demo without Docker
class MockProducer:
    def __init__(self, bootstrap_servers, region):
        self.bootstrap_servers = bootstrap_servers
        self.region = region
        self.messages_sent = 0
        self.messages_failed = 0
        
    def create_post(self, user_id, content, post_type="text"):
        post_id = f"post_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
        self.messages_sent += 1
        print(f"📝 Mock post created: {post_id} by {user_id} in {self.region}")
        return post_id
        
    def create_engagement(self, post_id, user_id, action):
        engagement_id = f"eng_{int(time.time() * 1000)}_{random.randint(100, 999)}"
        self.messages_sent += 1
        print(f"👍 Mock engagement: {action} on {post_id} by {user_id}")
        return engagement_id
        
    def create_analytics_event(self, event_type, data):
        event_id = f"analytics_{int(time.time() * 1000)}_{random.randint(10, 99)}"
        self.messages_sent += 1
        print(f"📊 Mock analytics event: {event_type}")
        return event_id
        
    def flush_and_close(self):
        print(f"🔄 Mock producer flushed. Total messages: {self.messages_sent}")

class MockMonitor:
    def __init__(self, bootstrap_servers):
        self.bootstrap_servers = bootstrap_servers
        self.monitoring = False
        
    def start_monitoring(self, interval_seconds=2):
        self.monitoring = True
        print(f"🔍 Mock monitoring started (interval: {interval_seconds}s)")
        
    def stop_monitoring(self):
        self.monitoring = False
        print("🛑 Mock monitoring stopped")
        
    def get_monitoring_data(self):
        return {
            'timestamp': time.time(),
            'cluster_metadata': {
                'brokers': {
                    '1': {'id': 1, 'host': 'broker-us-1', 'port': 9091, 'rack': 'us-east'},
                    '2': {'id': 2, 'host': 'broker-us-2', 'port': 9092, 'rack': 'us-east'},
                    '3': {'id': 3, 'host': 'broker-eu-1', 'port': 9093, 'rack': 'eu-west'},
                    '4': {'id': 4, 'host': 'broker-eu-2', 'port': 9094, 'rack': 'eu-west'},
                    '5': {'id': 5, 'host': 'broker-ap-1', 'port': 9095, 'rack': 'asia-pacific'}
                },
                'topics': {
                    'critical_posts': {
                        'partitions': 6,
                        'replication_factor': 5,
                        'partition_count': 6
                    },
                    'user_engagement': {
                        'partitions': 12,
                        'replication_factor': 3,
                        'partition_count': 12
                    },
                    'analytics_events': {
                        'partitions': 24,
                        'replication_factor': 3,
                        'partition_count': 24
                    }
                }
            },
            'health_score': {
                'overall_health': random.uniform(85, 100),
                'total_partitions': 42,
                'healthy_partitions': random.randint(35, 42),
                'under_replicated_partitions': random.randint(0, 3),
                'offline_partitions': 0,
                'status': 'healthy'
            },
            'recent_changes': [
                {
                    'type': 'isr_change',
                    'topic': 'critical_posts',
                    'partition': random.randint(0, 5),
                    'timestamp': time.time() - random.randint(1, 60),
                    'isr_size': random.randint(3, 5)
                }
            ],
            'replication_lags': {
                'critical_posts': {
                    'topic': 'critical_posts',
                    'max_lag': random.randint(0, 10),
                    'avg_lag': random.randint(0, 5)
                },
                'user_engagement': {
                    'topic': 'user_engagement',
                    'max_lag': random.randint(0, 5),
                    'avg_lag': random.randint(0, 2)
                }
            }
        }

class MockChaosEngineer:
    def __init__(self):
        self.active_failures = {}
        
    def inject_failure(self, scenario):
        failure_id = f"{scenario}_{int(time.time())}"
        self.active_failures[failure_id] = {
            'scenario': scenario,
            'status': 'active',
            'start_time': time.time()
        }
        print(f"💣 Mock failure injected: {scenario} (ID: {failure_id})")
        return failure_id
        
    def get_failure_status(self):
        return {
            'active_failures': len([f for f in self.active_failures.values() if f['status'] == 'active']),
            'total_failures': len(self.active_failures),
            'failures': self.active_failures
        }

def demo_normal_operations():
    """Demonstrate normal StreamSocial operations"""
    print("\n🌟 DEMO 1: Normal Operations")
    print("=" * 50)
    
    producer = MockProducer("localhost:9091,localhost:9092,localhost:9093", "us-east")
    
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
        
        print(f"✅ Created {len(posts)} posts with engagements")
        print(f"📊 Total messages sent: {producer.messages_sent}")
        
    finally:
        producer.flush_and_close()

def demo_replication_monitoring():
    """Demonstrate real-time replication monitoring"""
    print("\n📡 DEMO 2: Replication Monitoring")
    print("=" * 50)
    
    monitor = MockMonitor("localhost:9091,localhost:9092,localhost:9093")
    
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
    
    chaos = MockChaosEngineer()
    monitor = MockMonitor("localhost:9091,localhost:9092,localhost:9093")
    producer = MockProducer("localhost:9091,localhost:9092,localhost:9093", "chaos-test")
    
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
        print(f"📊 Monitoring during failure (10 seconds)...")
        for i in range(10):
            if i % 5 == 0:
                data = monitor.get_monitoring_data()
                health = data.get('health_score', {}).get('overall_health', 0)
                changes = len(data.get('recent_changes', []))
                print(f"   t+{i}s: Health={health:.1f}%, Changes={changes}")
            
            # Try to send messages during failure
            if i % 3 == 0:
                try:
                    producer.create_post(f"user_chaos_{i}", f"Message during failure at t+{i}s")
                except Exception as e:
                    print(f"   Expected error during failure: {type(e).__name__}")
            
            time.sleep(1)
        
        # Simulate recovery
        print(f"🔄 Simulating recovery...")
        time.sleep(2)
        
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
            producers[region] = MockProducer(
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
            producer.flush_and_close()
            total_messages += producer.messages_sent
            print(f"   {region}: {producer.messages_sent} messages")
        
        print(f"✅ Multi-region simulation complete: {total_messages} total messages")
        
    finally:
        for producer in producers.values():
            producer.flush_and_close()

def main():
    """Run all demos in sequence"""
    print("🚀 StreamSocial Day 20 Demo: Kafka Replication & ISR Management (Mock Version)")
    print("=================================================================================")
    print("This demo will showcase:")
    print("  1. Normal StreamSocial operations with replication")
    print("  2. Real-time replication monitoring")
    print("  3. Chaos engineering and failure scenarios")
    print("  4. Multi-region traffic simulation")
    print("\n⚠️  Note: This is a mock demo that simulates Kafka operations without Docker!")
    print("   For full functionality, ensure Docker is running and use the original demo.\n")
    
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

if __name__ == "__main__":
    main()
