import json
import time
from collections import defaultdict
from typing import Dict, List, Any
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import structlog
from config.kafka_config import kafka_config

logger = structlog.get_logger(__name__)

class TimelineConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            kafka_config.topic_name,
            **kafka_config.consumer_config
        )
        
        # Store timelines per user
        self.user_timelines = defaultdict(list)
        self.consumed_messages = []
        self.partition_stats = defaultdict(int)
        
        logger.info("Timeline consumer initialized")
    
    def consume_messages(self, timeout_ms: int = 5000) -> Dict[str, Any]:
        """Consume messages and organize by user timeline"""
        messages = self.consumer.poll(timeout_ms=timeout_ms)
        consumption_stats = {
            'messages_consumed': 0,
            'users_updated': set(),
            'partitions_read': set(),
            'ordering_violations': 0
        }
        
        for topic_partition, msgs in messages.items():
            partition = topic_partition.partition
            consumption_stats['partitions_read'].add(partition)
            self.partition_stats[partition] += len(msgs)
            
            for message in msgs:
                try:
                    # Parse message
                    user_message = json.loads(message.value)
                    user_id = user_message['user_id']
                    
                    # Add partition and offset info
                    user_message.update({
                        'partition': partition,
                        'offset': message.offset,
                        'consumed_at': time.time()
                    })
                    
                    # Check for ordering violations
                    if self.check_ordering_violation(user_id, user_message):
                        consumption_stats['ordering_violations'] += 1
                    
                    # Add to user timeline
                    self.user_timelines[user_id].append(user_message)
                    self.consumed_messages.append(user_message)
                    
                    consumption_stats['messages_consumed'] += 1
                    consumption_stats['users_updated'].add(user_id)
                    
                    logger.debug(
                        "Message consumed",
                        user_id=user_id,
                        partition=partition,
                        offset=message.offset
                    )
                    
                except Exception as e:
                    logger.error("Error processing message", error=str(e))
        
        consumption_stats['users_updated'] = len(consumption_stats['users_updated'])
        consumption_stats['partitions_read'] = list(consumption_stats['partitions_read'])
        
        return consumption_stats
    
    def check_ordering_violation(self, user_id: str, new_message: Dict) -> bool:
        """Check if new message violates chronological ordering for user"""
        user_messages = self.user_timelines[user_id]
        if not user_messages:
            return False
        
        last_message = user_messages[-1]
        if new_message['timestamp'] < last_message['timestamp']:
            logger.warning(
                "Ordering violation detected",
                user_id=user_id,
                new_timestamp=new_message['timestamp'],
                last_timestamp=last_message['timestamp']
            )
            return True
        
        return False
    
    def get_user_timeline(self, user_id: str, limit: int = 50) -> List[Dict]:
        """Get chronologically ordered timeline for user"""
        user_messages = self.user_timelines.get(user_id, [])
        sorted_messages = sorted(user_messages, key=lambda x: x['timestamp'])
        return sorted_messages[-limit:]
    
    def get_all_timelines(self) -> Dict[str, List[Dict]]:
        """Get all user timelines ordered chronologically"""
        ordered_timelines = {}
        for user_id, messages in self.user_timelines.items():
            ordered_timelines[user_id] = sorted(messages, key=lambda x: x['timestamp'])
        return ordered_timelines
    
    def get_partition_statistics(self) -> Dict[int, int]:
        """Get message count per partition"""
        return dict(self.partition_stats)
    
    def verify_ordering_consistency(self) -> Dict[str, Any]:
        """Verify ordering consistency across all user timelines"""
        verification_results = {
            'total_users': len(self.user_timelines),
            'total_messages': len(self.consumed_messages),
            'ordering_violations': 0,
            'user_violations': []
        }
        
        for user_id, messages in self.user_timelines.items():
            sorted_messages = sorted(messages, key=lambda x: x['timestamp'])
            
            # Check if current order matches chronological order
            current_order = [msg['timestamp'] for msg in messages]
            expected_order = [msg['timestamp'] for msg in sorted_messages]
            
            if current_order != expected_order:
                verification_results['ordering_violations'] += 1
                verification_results['user_violations'].append({
                    'user_id': user_id,
                    'message_count': len(messages),
                    'violations': len([i for i, (a, b) in enumerate(zip(current_order, expected_order)) if a != b])
                })
        
        return verification_results
    
    def close(self):
        """Close consumer connection"""
        self.consumer.close()
        logger.info("Timeline consumer closed")
