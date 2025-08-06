"""Real-time partition monitoring for StreamSocial"""

import asyncio
import json
import time
import psutil
from typing import Dict, Any, List
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient
import logging
from dataclasses import dataclass, asdict
from config.kafka_config import KAFKA_CONFIG, ADMIN_CONFIG

logger = logging.getLogger(__name__)

@dataclass
class PartitionMetrics:
    partition_id: int
    topic_name: str
    message_count: int
    lag: int
    throughput_rps: float
    last_updated: float

@dataclass
class ClusterHealth:
    total_partitions: int
    active_partitions: int
    total_messages: int
    avg_throughput: float
    hotspots: List[int]
    cold_spots: List[int]
    overall_health: str

class PartitionMonitor:
    """Real-time monitoring of partition health and performance"""
    
    def __init__(self):
        self.admin_client = KafkaAdminClient(**ADMIN_CONFIG)
        self.metrics_history = {}
        self.monitoring_active = False
        
    def calculate_partition_lag(self, topic: str, partition: int, group_id: str) -> int:
        """Calculate consumer lag for specific partition"""
        try:
            # Get high water mark and current offset
            consumer = KafkaConsumer(
                **KAFKA_CONFIG,
                group_id=group_id,
                enable_auto_commit=False
            )
            
            # Get partition metadata
            partitions = consumer.partitions_for_topic(topic)
            if partition not in partitions:
                return 0
            
            # Calculate lag (simplified for demo)
            high_water_mark = consumer.end_offsets({(topic, partition)})
            committed_offset = consumer.committed((topic, partition))
            
            consumer.close()
            
            if committed_offset is None:
                return list(high_water_mark.values())[0]
            
            return list(high_water_mark.values())[0] - committed_offset
            
        except Exception as e:
            logger.error(f"Failed to calculate lag for partition {partition}: {e}")
            return 0
    
    def collect_partition_metrics(self, topic: str) -> List[PartitionMetrics]:
        """Collect comprehensive partition metrics"""
        metrics = []
        
        try:
            # Get topic metadata
            metadata = self.admin_client.describe_topics([topic])
            
            if topic not in metadata:
                return metrics
            
            topic_info = metadata[topic]
            current_time = time.time()
            
            for partition_info in topic_info.partitions:
                partition_id = partition_info.partition
                
                # Calculate metrics
                lag = self.calculate_partition_lag(topic, partition_id, f"monitor-{topic}")
                
                # Get historical data for throughput calculation
                key = f"{topic}-{partition_id}"
                if key in self.metrics_history:
                    prev_metrics = self.metrics_history[key]
                    time_diff = current_time - prev_metrics['timestamp']
                    message_diff = 0  # Simplified for demo
                    throughput = message_diff / time_diff if time_diff > 0 else 0
                else:
                    throughput = 0
                
                partition_metrics = PartitionMetrics(
                    partition_id=partition_id,
                    topic_name=topic,
                    message_count=0,  # Would get from partition stats
                    lag=lag,
                    throughput_rps=throughput,
                    last_updated=current_time
                )
                
                metrics.append(partition_metrics)
                
                # Update history
                self.metrics_history[key] = {
                    'timestamp': current_time,
                    'message_count': 0,
                    'lag': lag
                }
        
        except Exception as e:
            logger.error(f"Failed to collect metrics for topic {topic}: {e}")
        
        return metrics
    
    def analyze_cluster_health(self, all_metrics: List[PartitionMetrics]) -> ClusterHealth:
        """Analyze overall cluster health from partition metrics"""
        if not all_metrics:
            return ClusterHealth(0, 0, 0, 0, [], [], "unhealthy")
        
        total_partitions = len(all_metrics)
        active_partitions = len([m for m in all_metrics if m.throughput_rps > 0])
        total_messages = sum(m.message_count for m in all_metrics)
        avg_throughput = sum(m.throughput_rps for m in all_metrics) / total_partitions
        
        # Identify hotspots and cold spots
        hotspots = []
        cold_spots = []
        
        for metrics in all_metrics:
            if metrics.throughput_rps > avg_throughput * 2:  # 2x above average
                hotspots.append(metrics.partition_id)
            elif metrics.throughput_rps < avg_throughput * 0.1:  # 10% of average
                cold_spots.append(metrics.partition_id)
        
        # Determine overall health
        hotspot_ratio = len(hotspots) / total_partitions
        if hotspot_ratio > 0.1:  # More than 10% hotspots
            health = "unhealthy"
        elif hotspot_ratio > 0.05:  # More than 5% hotspots
            health = "warning"
        else:
            health = "healthy"
        
        return ClusterHealth(
            total_partitions=total_partitions,
            active_partitions=active_partitions,
            total_messages=total_messages,
            avg_throughput=avg_throughput,
            hotspots=hotspots,
            cold_spots=cold_spots,
            overall_health=health
        )
    
    async def start_monitoring(self, topics: List[str], interval: int = 30):
        """Start continuous partition monitoring"""
        self.monitoring_active = True
        logger.info(f"🔍 Starting partition monitoring for topics: {topics}")
        
        while self.monitoring_active:
            try:
                all_metrics = []
                
                for topic in topics:
                    topic_metrics = self.collect_partition_metrics(topic)
                    all_metrics.extend(topic_metrics)
                
                cluster_health = self.analyze_cluster_health(all_metrics)
                
                # Log health status
                logger.info(f"📊 Cluster Health: {cluster_health.overall_health}")
                logger.info(f"📈 Active Partitions: {cluster_health.active_partitions}/{cluster_health.total_partitions}")
                
                if cluster_health.hotspots:
                    logger.warning(f"🔥 Hot partitions detected: {cluster_health.hotspots}")
                
                # Store metrics for web dashboard
                self.save_metrics_snapshot({
                    'timestamp': time.time(),
                    'cluster_health': asdict(cluster_health),
                    'partition_metrics': [asdict(m) for m in all_metrics]
                })
                
                await asyncio.sleep(interval)
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(5)
    
    def save_metrics_snapshot(self, snapshot: Dict[str, Any]):
        """Save metrics snapshot for dashboard"""
        try:
            with open('monitoring/latest_metrics.json', 'w') as f:
                json.dump(snapshot, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save metrics snapshot: {e}")
    
    def stop_monitoring(self):
        """Stop partition monitoring"""
        self.monitoring_active = False
        logger.info("🛑 Partition monitoring stopped")
