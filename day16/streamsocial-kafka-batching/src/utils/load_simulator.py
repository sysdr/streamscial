import asyncio
import random
import time
from typing import List
from src.models.post_event import PostEvent
from src.producer.adaptive_producer import AdaptiveKafkaProducer

class LoadSimulator:
    def __init__(self, producer: AdaptiveKafkaProducer):
        self.producer = producer
        self.regions = ['us-east', 'us-west', 'europe', 'asia', 'latam']
        self.post_types = ['text', 'image', 'video', 'story']
        
    def generate_post_event(self) -> PostEvent:
        return PostEvent(
            user_id=f"user_{random.randint(1, 1000000)}",
            post_id=None,  # Auto-generated
            content=f"Sample post content {random.randint(1, 10000)}",
            timestamp=int(time.time() * 1000),
            region=random.choice(self.regions),
            post_type=random.choice(self.post_types),
            metadata={
                'likes': random.randint(0, 1000),
                'shares': random.randint(0, 100),
                'hashtags': [f"#{random.choice(['tech', 'life', 'fun', 'news'])}"]
            }
        )
        
    async def simulate_traffic_pattern(self, base_rate: int, duration: int, pattern: str = 'steady'):
        """Simulate different traffic patterns"""
        print(f"🔄 Starting {pattern} traffic simulation: {base_rate} msg/s for {duration}s")
        
        start_time = time.time()
        message_count = 0
        
        while time.time() - start_time < duration:
            current_rate = self._calculate_rate(pattern, time.time() - start_time, duration, base_rate)
            
            # Send messages for current second
            messages_this_second = int(current_rate)
            for _ in range(messages_this_second):
                post_event = self.generate_post_event()
                self.producer.send_message('streamsocial.posts', post_event)
                message_count += 1
                
            await asyncio.sleep(1.0)
            
        print(f"✅ Simulation complete: {message_count} messages sent")
        return message_count
        
    def _calculate_rate(self, pattern: str, elapsed: float, duration: float, base_rate: int) -> float:
        if pattern == 'steady':
            return base_rate
        elif pattern == 'spike':
            # Spike to 10x at 25% and 75% of duration
            spike_points = [duration * 0.25, duration * 0.75]
            for spike_time in spike_points:
                if abs(elapsed - spike_time) < 5:  # 5-second spike window
                    return base_rate * 10
            return base_rate
        elif pattern == 'ramp':
            # Linear ramp from base_rate to 5x base_rate
            progress = elapsed / duration
            return base_rate * (1 + 4 * progress)
        elif pattern == 'peak':
            # Bell curve with peak at middle
            progress = elapsed / duration
            multiplier = 1 + 9 * (4 * progress * (1 - progress))  # Bell curve formula
            return base_rate * multiplier
        else:
            return base_rate
            
    async def benchmark_throughput(self, target_rate: int, test_duration: int = 60):
        """Benchmark producer throughput"""
        print(f"🚀 Benchmarking throughput: {target_rate} msg/s for {test_duration}s")
        
        start_time = time.time()
        sent_count = 0
        failed_count = 0
        
        async def send_batch():
            nonlocal sent_count, failed_count
            batch_size = min(1000, target_rate)  # Send in batches of up to 1000
            
            for _ in range(batch_size):
                post_event = self.generate_post_event()
                success = self.producer.send_message('streamsocial.posts', post_event)
                if success:
                    sent_count += 1
                else:
                    failed_count += 1
                    
        while time.time() - start_time < test_duration:
            batch_start = time.time()
            
            # Calculate how many batches needed per second
            batches_per_second = max(1, target_rate // 1000)
            
            tasks = [send_batch() for _ in range(batches_per_second)]
            await asyncio.gather(*tasks)
            
            # Wait for remainder of second
            elapsed = time.time() - batch_start
            if elapsed < 1.0:
                await asyncio.sleep(1.0 - elapsed)
                
        actual_rate = sent_count / test_duration
        success_rate = (sent_count / (sent_count + failed_count)) * 100 if (sent_count + failed_count) > 0 else 0
        
        print(f"📊 Benchmark Results:")
        print(f"   Target Rate: {target_rate} msg/s")
        print(f"   Actual Rate: {actual_rate:.1f} msg/s")
        print(f"   Success Rate: {success_rate:.1f}%")
        print(f"   Messages Sent: {sent_count}")
        print(f"   Messages Failed: {failed_count}")
        
        return {
            'target_rate': target_rate,
            'actual_rate': actual_rate,
            'success_rate': success_rate,
            'sent_count': sent_count,
            'failed_count': failed_count
        }
