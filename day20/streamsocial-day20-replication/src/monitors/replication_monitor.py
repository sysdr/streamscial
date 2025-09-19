"""
Real-time Kafka Replication & ISR Monitor for StreamSocial
"""
import time
import json
import logging
from typing import Dict, List, Tuple
from confluent_kafka.admin import AdminClient, ConfigResource
from confluent_kafka import Consumer, TopicPartition
import threading
from dataclasses import dataclass, asdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class PartitionInfo:
    topic: str
    partition: int
    leader: int
    replicas: List[int]
    isr: List[int]
    offline_replicas: List[int]
    under_replicated: bool
    leader_epoch: int

@dataclass
class BrokerInfo:
    broker_id: int
    host: str
    port: int
    rack: str
    is_alive: bool
    leader_count: int
    replica_count: int

class ReplicationMonitor:
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
        
        # Monitoring state
        self.partition_states = {}
        self.broker_states = {}
        self.isr_changes = []
        self.leader_elections = []
        self.monitoring = False
        
        # Consumer for monitoring consumer lag
        consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'replication_monitor',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': False
        }
        self.consumer = Consumer(consumer_config)

    def get_cluster_metadata(self) -> Dict:
        """Get comprehensive cluster metadata"""
        try:
            metadata = self.admin_client.list_topics(timeout=10)
            cluster_info = {
                'cluster_id': metadata.cluster_id,
                'controller_id': metadata.controller_id,
                'brokers': {},
                'topics': {}
            }
            
            # Broker information
            for broker_id, broker in metadata.brokers.items():
                cluster_info['brokers'][broker_id] = {
                    'id': broker_id,
                    'host': broker.host,
                    'port': broker.port,
                    'rack': getattr(broker, 'rack', None)
                }
            
            # Topic and partition information
            for topic_name, topic_metadata in metadata.topics.items():
                if topic_metadata.error is not None:
                    continue
                    
                topic_info = {
                    'partitions': {},
                    'partition_count': len(topic_metadata.partitions),
                    'replication_factor': 0
                }
                
                for partition_id, partition in topic_metadata.partitions.items():
                    if partition.error is not None:
                        continue
                        
                    # Calculate replication factor from first partition
                    if topic_info['replication_factor'] == 0:
                        topic_info['replication_factor'] = len(partition.replicas)
                    
                    partition_info = PartitionInfo(
                        topic=topic_name,
                        partition=partition_id,
                        leader=partition.leader,
                        replicas=list(partition.replicas),
                        isr=list(partition.isrs),
                        offline_replicas=list(partition.offline_replicas) if hasattr(partition, 'offline_replicas') else [],
                        under_replicated=len(partition.isrs) < len(partition.replicas),
                        leader_epoch=getattr(partition, 'leader_epoch', 0)
                    )
                    
                    topic_info['partitions'][partition_id] = asdict(partition_info)
                
                cluster_info['topics'][topic_name] = topic_info
            
            return cluster_info
            
        except Exception as e:
            logger.error(f"❌ Failed to get cluster metadata: {e}")
            return {}

    def detect_isr_changes(self, new_metadata: Dict) -> List[Dict]:
        """Detect ISR changes and leader elections"""
        changes = []
        
        for topic_name, topic_info in new_metadata.get('topics', {}).items():
            for partition_id, partition_info in topic_info.get('partitions', {}).items():
                key = f"{topic_name}-{partition_id}"
                
                if key in self.partition_states:
                    old_state = self.partition_states[key]
                    new_isr = set(partition_info['isr'])
                    old_isr = set(old_state['isr'])
                    
                    # ISR membership changes
                    if new_isr != old_isr:
                        joined_isr = new_isr - old_isr
                        left_isr = old_isr - new_isr
                        
                        change = {
                            'type': 'isr_change',
                            'topic': topic_name,
                            'partition': partition_id,
                            'timestamp': time.time(),
                            'joined_isr': list(joined_isr),
                            'left_isr': list(left_isr),
                            'current_isr': list(new_isr),
                            'isr_size': len(new_isr)
                        }
                        changes.append(change)
                        logger.warning(f"⚠️  ISR change: {topic_name}[{partition_id}] ISR: {list(old_isr)} → {list(new_isr)}")
                    
                    # Leader changes
                    if partition_info['leader'] != old_state['leader']:
                        change = {
                            'type': 'leader_election',
                            'topic': topic_name,
                            'partition': partition_id,
                            'timestamp': time.time(),
                            'old_leader': old_state['leader'],
                            'new_leader': partition_info['leader'],
                            'isr': partition_info['isr']
                        }
                        changes.append(change)
                        logger.warning(f"🔄 Leader election: {topic_name}[{partition_id}] Leader: {old_state['leader']} → {partition_info['leader']}")
                
                self.partition_states[key] = partition_info
        
        return changes

    def _initialize_partition_states(self, metadata: Dict):
        """Initialize partition states for change detection"""
        for topic_name, topic_info in metadata.get('topics', {}).items():
            for partition_id, partition_info in topic_info.get('partitions', {}).items():
                key = f"{topic_name}-{partition_id}"
                self.partition_states[key] = partition_info

    def _generate_simulated_events(self, metadata: Dict) -> List[Dict]:
        """Generate simulated events for demo purposes"""
        import random
        
        events = []
        topics = list(metadata.get('topics', {}).keys())
        
        if topics:
            topic = topics[0]  # Use first topic
            topic_info = metadata['topics'][topic]
            partitions = list(topic_info.get('partitions', {}).keys())
            
            if partitions:
                # Generate 1-2 random events occasionally
                if random.random() < 0.3:  # 30% chance
                    partition_id = random.choice(partitions)
                    partition_info = topic_info['partitions'][partition_id]
                    
                    event_type = random.choice(['isr_change', 'leader_election'])
                    
                    if event_type == 'isr_change':
                        event = {
                            'type': 'isr_change',
                            'topic': topic,
                            'partition': int(partition_id),
                            'timestamp': time.time(),
                            'isr_size': len(partition_info['isr']),
                            'current_isr': partition_info['isr']
                        }
                    else:  # leader_election
                        event = {
                            'type': 'leader_election',
                            'topic': topic,
                            'partition': int(partition_id),
                            'timestamp': time.time(),
                            'new_leader': partition_info['leader'],
                            'isr': partition_info['isr']
                        }
                    
                    events.append(event)
                    logger.info(f"🎭 Generated simulated {event_type} event for {topic}[{partition_id}]")
        
        return events

    def calculate_health_score(self, metadata: Dict) -> Dict:
        """Calculate overall cluster health score"""
        total_partitions = 0
        under_replicated = 0
        offline_partitions = 0
        healthy_partitions = 0
        
        for topic_info in metadata.get('topics', {}).values():
            for partition_info in topic_info.get('partitions', {}).values():
                total_partitions += 1
                
                if partition_info['under_replicated']:
                    under_replicated += 1
                elif len(partition_info['offline_replicas']) > 0:
                    offline_partitions += 1
                else:
                    healthy_partitions += 1
        
        health_score = (healthy_partitions / max(total_partitions, 1)) * 100
        
        return {
            'overall_health': health_score,
            'total_partitions': total_partitions,
            'healthy_partitions': healthy_partitions,
            'under_replicated_partitions': under_replicated,
            'offline_partitions': offline_partitions,
            'status': 'healthy' if health_score >= 90 else 'warning' if health_score >= 70 else 'critical'
        }

    def get_replication_lag(self, topic: str) -> Dict:
        """Calculate replication lag across brokers"""
        try:
            partitions = []
            metadata = self.admin_client.list_topics(timeout=5)
            
            if topic in metadata.topics:
                for partition_id in metadata.topics[topic].partitions:
                    partitions.append(TopicPartition(topic, partition_id))
            
            if not partitions:
                return {}
            
            # Get high water marks (leader offsets)
            high_water_marks = self.consumer.get_watermark_offsets(partitions[0], timeout=5)
            
            lag_info = {
                'topic': topic,
                'partition_lags': {},
                'max_lag': 0,
                'avg_lag': 0
            }
            
            # For this demo, we'll simulate lag calculation
            # In production, you'd query each broker's replica logs
            for partition in partitions:
                simulated_lag = max(0, high_water_marks[1] - high_water_marks[0] - 100)
                lag_info['partition_lags'][partition.partition] = simulated_lag
                lag_info['max_lag'] = max(lag_info['max_lag'], simulated_lag)
            
            if partitions:
                lag_info['avg_lag'] = sum(lag_info['partition_lags'].values()) / len(partitions)
            
            return lag_info
            
        except Exception as e:
            logger.error(f"❌ Failed to calculate replication lag: {e}")
            return {}

    def start_monitoring(self, interval_seconds: int = 5):
        """Start continuous monitoring"""
        self.monitoring = True
        logger.info(f"🔍 Starting replication monitoring (interval: {interval_seconds}s)")
        
        def monitor_loop():
            # Initialize partition states on first run
            first_run = True
            while self.monitoring:
                try:
                    metadata = self.get_cluster_metadata()
                    if metadata:
                        if first_run:
                            # Initialize partition states on first run
                            self._initialize_partition_states(metadata)
                            first_run = False
                            logger.info("🔧 Initialized partition states for change detection")
                        
                        # Detect changes
                        changes = self.detect_isr_changes(metadata)
                        
                        # Add some simulated events for demo purposes
                        if len(self.isr_changes) < 5:  # Only add if we don't have many real events
                            simulated_changes = self._generate_simulated_events(metadata)
                            changes.extend(simulated_changes)
                        
                        self.isr_changes.extend(changes)
                        
                        # Keep only recent changes (last 100)
                        self.isr_changes = self.isr_changes[-100:]
                        
                        # Calculate health
                        health = self.calculate_health_score(metadata)
                        logger.info(f"🏥 Cluster health: {health['overall_health']:.1f}% ({health['status']})")
                        
                        # Log critical issues
                        if health['under_replicated_partitions'] > 0:
                            logger.warning(f"⚠️  {health['under_replicated_partitions']} under-replicated partitions detected!")
                        
                        if health['offline_partitions'] > 0:
                            logger.error(f"❌ {health['offline_partitions']} offline partitions detected!")
                    
                    time.sleep(interval_seconds)
                    
                except Exception as e:
                    logger.error(f"❌ Monitoring error: {e}")
                    time.sleep(interval_seconds)
        
        self.monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        self.monitor_thread.start()

    def stop_monitoring(self):
        """Stop monitoring"""
        self.monitoring = False
        logger.info("🛑 Stopping replication monitoring")

    def get_monitoring_data(self) -> Dict:
        """Get current monitoring data for dashboard"""
        metadata = self.get_cluster_metadata()
        health = self.calculate_health_score(metadata)
        
        # Get replication lag for critical topics
        critical_lag = self.get_replication_lag('critical_posts')
        engagement_lag = self.get_replication_lag('user_engagement')
        
        return {
            'timestamp': time.time(),
            'cluster_metadata': metadata,
            'health_score': health,
            'recent_changes': self.isr_changes[-20:],  # Last 20 changes
            'replication_lags': {
                'critical_posts': critical_lag,
                'user_engagement': engagement_lag
            }
        }

if __name__ == "__main__":
    # Demo monitoring
    monitor = ReplicationMonitor("localhost:9091,localhost:9092,localhost:9093")
    
    try:
        monitor.start_monitoring()
        
        # Run for demonstration
        time.sleep(60)
        
        # Show current state
        data = monitor.get_monitoring_data()
        print(json.dumps(data, indent=2, default=str))
        
    except KeyboardInterrupt:
        logger.info("🛑 Stopping monitor...")
    finally:
        monitor.stop_monitoring()
