import asyncio
import subprocess
import time
import signal
import os
from typing import List, Dict
import structlog
import psutil

logger = structlog.get_logger()

class ConsumerGroupManager:
    def __init__(self, group_id: str = "feed-generation-workers"):
        self.group_id = group_id
        self.consumers: Dict[str, subprocess.Popen] = {}
        self.target_count = 1
        
    async def scale_to(self, target_count: int):
        """Scale consumer group to target instance count"""
        logger.info(f"Scaling consumer group to {target_count} instances")
        
        current_count = len(self.consumers)
        
        if target_count > current_count:
            # Scale up
            for i in range(current_count, target_count):
                await self.start_consumer(f"consumer-{i:03d}")
                await asyncio.sleep(1)  # Stagger startup
                
        elif target_count < current_count:
            # Scale down
            consumers_to_stop = list(self.consumers.keys())[target_count:]
            for consumer_id in consumers_to_stop:
                await self.stop_consumer(consumer_id)
                
        self.target_count = target_count
        logger.info(f"Scaling complete: {len(self.consumers)} active consumers")
    
    async def start_consumer(self, consumer_id: str):
        """Start a single consumer instance"""
        cmd = [
            "python", "-m", "src.consumers.feed_consumer",
            consumer_id, self.group_id
        ]
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid
        )
        
        self.consumers[consumer_id] = process
        logger.info(f"Started consumer {consumer_id} (PID: {process.pid})")
    
    async def stop_consumer(self, consumer_id: str):
        """Stop a single consumer instance"""
        if consumer_id in self.consumers:
            process = self.consumers[consumer_id]
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                process.wait(timeout=10)
            except (subprocess.TimeoutExpired, ProcessLookupError):
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)
            
            del self.consumers[consumer_id]
            logger.info(f"Stopped consumer {consumer_id}")
    
    async def progressive_scaling_demo(self):
        """Demonstrate progressive scaling from 1 to 100 consumers"""
        scaling_schedule = [1, 2, 5, 10, 25, 50, 100]
        
        for target in scaling_schedule:
            logger.info(f"=== Scaling to {target} consumers ===")
            await self.scale_to(target)
            
            # Let it run for observation
            await asyncio.sleep(30)
            
            # Show current state
            alive_consumers = sum(1 for p in self.consumers.values() if p.poll() is None)
            logger.info(f"Active consumers: {alive_consumers}/{target}")
    
    async def shutdown_all(self):
        """Shutdown all consumer instances"""
        logger.info("Shutting down all consumers...")
        
        for consumer_id in list(self.consumers.keys()):
            await self.stop_consumer(consumer_id)
        
        logger.info("All consumers stopped")

if __name__ == "__main__":
    manager = ConsumerGroupManager()
    
    try:
        asyncio.run(manager.progressive_scaling_demo())
    except KeyboardInterrupt:
        logger.info("Demo interrupted")
    finally:
        asyncio.run(manager.shutdown_all())
