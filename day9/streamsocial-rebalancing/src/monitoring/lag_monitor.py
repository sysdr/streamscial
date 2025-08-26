import time
import logging
import threading
from typing import Dict, List
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType
import json
import requests
from prometheus_client import Gauge

from config.kafka_config import *

logger = logging.getLogger(__name__)

# Simple Prometheus metrics without complex labels for now
partition_lag_gauge = Gauge('kafka_partition_lag_total', 'Total partition lag')
scaling_events = Gauge('autoscaler_scaling_events_total', 'Total scaling events')

class LagMonitor:
    def __init__(self):
        self.admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        self.running = False
        self.current_lag = {}
        self.scaling_cooldown = {}
        
    def start_monitoring(self):
        """Start lag monitoring in background thread"""
        self.running = True
        monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        monitor_thread.start()
        logger.info("Lag monitor started")
        
    def _monitor_loop(self):
        """Main monitoring loop"""
        while self.running:
            try:
                lag_metrics = self._collect_lag_metrics()
                self._evaluate_scaling_decisions(lag_metrics)
                time.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                logger.error(f"Error in lag monitoring: {e}")
                time.sleep(5)
                
    def _collect_lag_metrics(self) -> Dict[int, int]:
        """Collect lag metrics for all partitions"""
        lag_metrics = {}
        
        try:
            # Create temporary consumer to get lag information
            consumer = KafkaConsumer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=CONSUMER_GROUP_ID,
                enable_auto_commit=False
            )
            
            # Get partition information
            partitions = consumer.partitions_for_topic(KAFKA_TOPICS['user_interactions'])
            
            for partition in partitions:
                topic_partition = (KAFKA_TOPICS['user_interactions'], partition)
                
                # Get committed offset for consumer group
                committed_offsets = consumer.committed(topic_partition)
                committed_offset = committed_offsets if committed_offsets else 0
                
                # Get latest offset
                consumer.seek_to_end(topic_partition)
                latest_offset = consumer.position(topic_partition)
                
                # Calculate lag
                lag = max(0, latest_offset - committed_offset)
                lag_metrics[partition] = lag
                
                # Update Prometheus metrics
                partition_lag_gauge.set(lag)
                
            consumer.close()
            self.current_lag = lag_metrics
            
        except Exception as e:
            logger.error(f"Error collecting lag metrics: {e}")
            
        return lag_metrics
        
    def _evaluate_scaling_decisions(self, lag_metrics: Dict[int, int]):
        """Evaluate whether to scale up or down"""
        total_lag = sum(lag_metrics.values())
        max_partition_lag = max(lag_metrics.values()) if lag_metrics else 0
        
        current_time = time.time()
        
        # Scale up decision
        if (max_partition_lag > LAG_THRESHOLD and 
            current_time - self.scaling_cooldown.get('scale_up', 0) > SCALE_UP_COOLDOWN):
            
            current_consumers = self._get_current_consumer_count()
            if current_consumers < MAX_CONSUMERS:
                logger.info(f"Scaling up: max_lag={max_partition_lag}, threshold={LAG_THRESHOLD}")
                self._trigger_scale_up()
                self.scaling_cooldown['scale_up'] = current_time
                scaling_events.inc()
                
        # Scale down decision
        elif (max_partition_lag < LAG_THRESHOLD // 2 and 
              current_time - self.scaling_cooldown.get('scale_down', 0) > SCALE_DOWN_COOLDOWN):
            
            current_consumers = self._get_current_consumer_count()
            if current_consumers > MIN_CONSUMERS:
                logger.info(f"Scaling down: max_lag={max_partition_lag}, threshold={LAG_THRESHOLD//2}")
                self._trigger_scale_down()
                self.scaling_cooldown['scale_down'] = current_time
                scaling_events.inc()
                
    def _get_current_consumer_count(self) -> int:
        """Get current number of active consumers"""
        # In real implementation, this would query Kubernetes or Docker
        # For demo, we'll simulate based on a simple file-based approach
        try:
            with open('/tmp/consumer_count.txt', 'r') as f:
                return int(f.read().strip())
        except FileNotFoundError:
            return MIN_CONSUMERS
            
    def _trigger_scale_up(self):
        """Trigger scaling up consumers"""
        current_count = self._get_current_consumer_count()
        new_count = min(current_count + 2, MAX_CONSUMERS)
        
        with open('/tmp/consumer_count.txt', 'w') as f:
            f.write(str(new_count))
            
        logger.info(f"Scaled up: {current_count} -> {new_count} consumers")
        
    def _trigger_scale_down(self):
        """Trigger scaling down consumers"""
        current_count = self._get_current_consumer_count()
        new_count = max(current_count - 1, MIN_CONSUMERS)
        
        with open('/tmp/consumer_count.txt', 'w') as f:
            f.write(str(new_count))
            
        logger.info(f"Scaled down: {current_count} -> {new_count} consumers")
        
    def get_current_lag(self) -> Dict[int, int]:
        """Get current lag metrics"""
        return self.current_lag.copy()
        
    def stop(self):
        """Stop lag monitoring"""
        self.running = False
        logger.info("Lag monitor stopped")
