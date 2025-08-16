import asyncio
import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from typing import Dict, Any
import structlog

logger = structlog.get_logger()

class ActivityProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: str(x).encode('utf-8') if x else None,
            acks='all',
            retries=3,
            batch_size=16384,
            linger_ms=10
        )
        self.user_count = 10000
        self.activity_types = ['like', 'share', 'comment', 'follow', 'post']
        
    def generate_activity(self, user_id: int) -> Dict[str, Any]:
        return {
            "user_id": user_id,
            "activity_type": random.choice(self.activity_types),
            "target_user_id": random.randint(1, self.user_count),
            "content_id": random.randint(1, 100000),
            "timestamp": datetime.utcnow().isoformat(),
            "metadata": {
                "device": random.choice(["mobile", "web", "tablet"]),
                "location": random.choice(["US", "EU", "ASIA"]),
                "session_id": f"session_{random.randint(1, 1000)}"
            }
        }
    
    async def produce_activities(self, rate_per_second: int = 1000, duration_seconds: int = 300):
        """Produce user activities at specified rate"""
        logger.info(f"Starting activity production: {rate_per_second}/sec for {duration_seconds}s")
        
        start_time = time.time()
        total_sent = 0
        
        while time.time() - start_time < duration_seconds:
            batch_start = time.time()
            
            # Send batch of activities
            for _ in range(rate_per_second):
                user_id = random.randint(1, self.user_count)
                activity = self.generate_activity(user_id)
                
                # Partition by user_id for consistent assignment
                self.producer.send(
                    'user-activities',
                    key=user_id,
                    value=activity
                )
                total_sent += 1
            
            # Rate limiting
            batch_duration = time.time() - batch_start
            if batch_duration < 1.0:
                await asyncio.sleep(1.0 - batch_duration)
                
            if total_sent % 5000 == 0:
                logger.info(f"Produced {total_sent} activities")
        
        self.producer.flush()
        logger.info(f"Production complete: {total_sent} total activities")
    
    def close(self):
        self.producer.close()

if __name__ == "__main__":
    import sys
    rate = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    duration = int(sys.argv[2]) if len(sys.argv) > 2 else 60
    
    producer = ActivityProducer()
    asyncio.run(producer.produce_activities(rate, duration))
    producer.close()
