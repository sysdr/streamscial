"""Demo script showcasing StreamSocial partitioning strategy"""

import asyncio
import time
import random
import json
import logging
from typing import List
from src.partition_strategy import PartitionStrategy, UserAction, ContentInteraction, PartitionCalculator
from src.topic_manager import TopicManager, StreamSocialProducer, StreamSocialConsumer
from monitoring.partition_monitor import PartitionMonitor

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StreamSocialDemo:
    """Comprehensive demo of StreamSocial partitioning strategy"""
    
    def __init__(self):
        self.strategy = PartitionStrategy()
        self.topic_manager = TopicManager()
        self.monitor = PartitionMonitor()
        self.demo_data = []
    
    def generate_demo_data(self, num_users: int = 1000, num_actions: int = 10000) -> List[UserAction]:
        """Generate realistic demo data"""
        logger.info(f"🔄 Generating {num_actions} demo actions for {num_users} users...")
        
        actions = []
        action_types = ["post", "comment", "like", "share", "follow"]
        
        for i in range(num_actions):
            user_id = f"user_{random.randint(1, num_users)}"
            content_id = f"content_{random.randint(1, num_actions // 2)}"
            
            action = UserAction(
                user_id=user_id,
                action_type=random.choice(action_types),
                content_id=content_id if random.random() > 0.3 else None,
                timestamp=time.time() + random.randint(-3600, 3600),  # ±1 hour
                metadata={
                    "device": random.choice(["mobile", "desktop", "tablet"]),
                    "location": random.choice(["US", "EU", "ASIA"]),
                    "session_id": f"session_{random.randint(1, num_users // 10)}"
                }
            )
            actions.append(action)
        
        self.demo_data = actions
        logger.info(f"✅ Generated {len(actions)} demo actions")
        return actions
    
    def demonstrate_partitioning_strategy(self):
        """Demonstrate partition calculation and distribution"""
        logger.info("🎯 Demonstrating partitioning strategy...")
        
        if not self.demo_data:
            self.generate_demo_data()
        
        # Calculate partition distribution
        partition_counts = {}
        user_partition_mapping = {}
        
        for action in self.demo_data[:1000]:  # Sample first 1000
            partition = self.strategy.calculate_user_action_partition(action.user_id)
            
            if partition not in partition_counts:
                partition_counts[partition] = 0
            partition_counts[partition] += 1
            
            user_partition_mapping[action.user_id] = partition
        
        # Analyze distribution
        analysis = PartitionCalculator.validate_partition_distribution(partition_counts)
        
        logger.info(f"📊 Partition Distribution Analysis:")
        logger.info(f"   Total messages: {analysis['total_messages']}")
        logger.info(f"   Average per partition: {analysis['average_per_partition']:.2f}")
        logger.info(f"   Partitions used: {len(partition_counts)}")
        logger.info(f"   Distribution health: {'✅ Healthy' if analysis['distribution_health'] else '⚠️  Needs attention'}")
        
        if analysis['hotspots']:
            logger.warning(f"🔥 Hot partitions: {[f'P{p}({c})' for p, c in analysis['hotspots']]}")
        
        return analysis
    
    def demonstrate_throughput_calculation(self):
        """Demonstrate partition count calculation for different throughput targets"""
        logger.info("⚡ Demonstrating throughput-based partition calculation...")
        
        scenarios = [
            {"name": "Startup", "rps": 1_000, "expected_partitions": 1},
            {"name": "Growing", "rps": 100_000, "expected_partitions": 3},
            {"name": "Scale-up", "rps": 1_000_000, "expected_partitions": 30},
            {"name": "StreamSocial Target", "rps": 50_000_000, "expected_partitions": 1500}
        ]
        
        for scenario in scenarios:
            partitions = PartitionCalculator.calculate_partitions_for_throughput(scenario["rps"])
            logger.info(f"   {scenario['name']}: {scenario['rps']:,} req/s → {partitions} partitions")
    
    def demonstrate_ordering_guarantees(self):
        """Demonstrate message ordering within partitions"""
        logger.info("🔄 Demonstrating ordering guarantees...")
        
        # Create actions for the same user at different times
        user_id = "demo_user_123"
        user_actions = []
        
        for i in range(5):
            action = UserAction(
                user_id=user_id,
                action_type=f"action_{i}",
                content_id=None,
                timestamp=time.time() + i,
                metadata={"sequence": i}
            )
            user_actions.append(action)
        
        # Verify all actions go to same partition
        partitions = [self.strategy.calculate_user_action_partition(action.user_id) for action in user_actions]
        
        logger.info(f"   User {user_id} actions:")
        for i, (action, partition) in enumerate(zip(user_actions, partitions)):
            logger.info(f"     Action {i}: {action.action_type} → Partition {partition}")
        
        all_same_partition = len(set(partitions)) == 1
        logger.info(f"   Ordering guarantee: {'✅ Maintained' if all_same_partition else '❌ Broken'}")
        
        return all_same_partition
    
    async def demonstrate_real_time_monitoring(self):
        """Demonstrate real-time partition monitoring"""
        logger.info("📊 Starting real-time partition monitoring demo...")
        
        try:
            # Create topics first
            success = self.topic_manager.create_topics()
            if not success:
                logger.error("Failed to create topics for monitoring demo")
                return
            
            # Start monitoring in background
            monitor_task = asyncio.create_task(
                self.monitor.start_monitoring(['user-actions', 'content-interactions'], interval=5)
            )
            
            # Let it run for a short demo period
            await asyncio.sleep(15)
            
            self.monitor.stop_monitoring()
            monitor_task.cancel()
            
            logger.info("✅ Monitoring demo completed")
            
        except Exception as e:
            logger.error(f"Monitoring demo failed: {e}")
    
    async def run_complete_demo(self):
        """Run the complete partitioning strategy demonstration"""
        logger.info("🚀 Starting StreamSocial Partitioning Strategy Demo")
        logger.info("=" * 60)
        
        try:
            # 1. Generate demo data
            self.generate_demo_data(num_users=500, num_actions=5000)
            
            # 2. Demonstrate partitioning strategy
            self.demonstrate_partitioning_strategy()
            
            # 3. Demonstrate throughput calculations
            self.demonstrate_throughput_calculation()
            
            # 4. Demonstrate ordering guarantees
            self.demonstrate_ordering_guarantees()
            
            # 5. Real-time monitoring demo
            await self.demonstrate_real_time_monitoring()
            
            logger.info("=" * 60)
            logger.info("✅ StreamSocial Partitioning Demo Completed Successfully!")
            logger.info("🎯 Key Learnings:")
            logger.info("   • Hash-based partitioning ensures even distribution")
            logger.info("   • User actions maintain strict ordering per user")
            logger.info("   • Partition count scales with throughput requirements")
            logger.info("   • Real-time monitoring enables proactive optimization")
            
        except Exception as e:
            logger.error(f"Demo failed: {e}")
            raise

if __name__ == "__main__":
    demo = StreamSocialDemo()
    asyncio.run(demo.run_complete_demo())
