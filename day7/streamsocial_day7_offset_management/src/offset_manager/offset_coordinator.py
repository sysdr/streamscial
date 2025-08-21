import asyncio
import json
import time
from typing import Dict, List, Optional, Set
from dataclasses import dataclass
import redis.asyncio as aioredis
import asyncpg
import structlog

logger = structlog.get_logger()

@dataclass
class OffsetRange:
    partition: int
    start_offset: int
    end_offset: int
    worker_id: str
    claimed_at: float
    status: str = "claimed"  # claimed, processing, completed, committed, failed

class OffsetCoordinator:
    def __init__(self, redis_client: aioredis.Redis, postgres_pool: asyncpg.Pool):
        self.redis = redis_client
        self.postgres = postgres_pool
        self.active_ranges: Dict[str, OffsetRange] = {}
        self.worker_heartbeats: Dict[str, float] = {}
        self.heartbeat_timeout = 30.0  # seconds
        
    async def initialize(self):
        """Initialize offset tables in PostgreSQL"""
        async with self.postgres.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS offset_checkpoints (
                    partition_id INTEGER,
                    committed_offset BIGINT,
                    updated_at TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (partition_id)
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS engagement_metrics (
                    user_id VARCHAR(255),
                    metric_type VARCHAR(100),
                    metric_value BIGINT,
                    partition_id INTEGER,
                    offset_position BIGINT,
                    processed_at TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (user_id, metric_type)
                )
            """)
        
        logger.info("Offset coordinator initialized")
    
    async def claim_offset_range(self, worker_id: str, partition: int, 
                                start_offset: int, batch_size: int) -> Optional[OffsetRange]:
        """Atomically claim an offset range for processing"""
        range_key = f"offset_range:{partition}:{start_offset}"
        end_offset = start_offset + batch_size - 1
        
        # Check if range already claimed
        existing = await self.redis.get(range_key)
        if existing:
            range_data = json.loads(existing)
            if time.time() - range_data["claimed_at"] < self.heartbeat_timeout:
                return None  # Range still active
        
        # Claim the range
        offset_range = OffsetRange(
            partition=partition,
            start_offset=start_offset,
            end_offset=end_offset,
            worker_id=worker_id,
            claimed_at=time.time()
        )
        
        await self.redis.setex(
            range_key, 
            int(self.heartbeat_timeout), 
            json.dumps(offset_range.__dict__)
        )
        
        self.active_ranges[range_key] = offset_range
        logger.info("Offset range claimed", 
                   worker_id=worker_id, partition=partition, 
                   start_offset=start_offset, end_offset=end_offset)
        
        return offset_range
    
    async def update_range_status(self, range_key: str, status: str):
        """Update the status of an offset range"""
        if range_key in self.active_ranges:
            self.active_ranges[range_key].status = status
            await self.redis.setex(
                range_key,
                int(self.heartbeat_timeout),
                json.dumps(self.active_ranges[range_key].__dict__)
            )
    
    async def commit_offset_range(self, range_key: str, metrics_data: List[Dict]):
        """Atomically commit offset and metrics"""
        if range_key not in self.active_ranges:
            raise ValueError(f"Range {range_key} not found")
        
        offset_range = self.active_ranges[range_key]
        
        async with self.postgres.acquire() as conn:
            async with conn.transaction():
                # Insert engagement metrics
                for metric in metrics_data:
                    await conn.execute("""
                        INSERT INTO engagement_metrics 
                        (user_id, metric_type, metric_value, partition_id, offset_position)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (user_id, metric_type) 
                        DO UPDATE SET 
                            metric_value = engagement_metrics.metric_value + EXCLUDED.metric_value,
                            processed_at = NOW()
                    """, metric["user_id"], metric["metric_type"], 
                        metric["metric_value"], offset_range.partition, 
                        offset_range.end_offset)
                
                # Update committed offset
                await conn.execute("""
                    INSERT INTO offset_checkpoints (partition_id, committed_offset)
                    VALUES ($1, $2)
                    ON CONFLICT (partition_id)
                    DO UPDATE SET 
                        committed_offset = GREATEST(offset_checkpoints.committed_offset, EXCLUDED.committed_offset),
                        updated_at = NOW()
                """, offset_range.partition, offset_range.end_offset)
        
        # Remove from active ranges
        await self.redis.delete(range_key)
        del self.active_ranges[range_key]
        
        logger.info("Offset range committed", 
                   partition=offset_range.partition, 
                   end_offset=offset_range.end_offset,
                   metrics_count=len(metrics_data))
    
    async def get_committed_offset(self, partition: int) -> int:
        """Get the last committed offset for a partition"""
        async with self.postgres.acquire() as conn:
            result = await conn.fetchval(
                "SELECT committed_offset FROM offset_checkpoints WHERE partition_id = $1",
                partition
            )
            return result or 0
    
    async def worker_heartbeat(self, worker_id: str):
        """Record worker heartbeat"""
        self.worker_heartbeats[worker_id] = time.time()
        await self.redis.setex(f"worker_heartbeat:{worker_id}", 30, time.time())
    
    async def cleanup_failed_ranges(self):
        """Clean up ranges from failed workers"""
        current_time = time.time()
        failed_ranges = []
        
        for range_key, offset_range in list(self.active_ranges.items()):
            if current_time - offset_range.claimed_at > self.heartbeat_timeout:
                failed_ranges.append(range_key)
                logger.warning("Detected failed range", 
                             range_key=range_key, 
                             worker_id=offset_range.worker_id)
        
        for range_key in failed_ranges:
            await self.redis.delete(range_key)
            del self.active_ranges[range_key]
            logger.info("Cleaned up failed range", range_key=range_key)
