import json
import time
import threading
from typing import Dict, Any, Optional, Set
from collections import defaultdict
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
import logging
from src.models.user_preference import UserPreference
from src.utils.kafka_config import KafkaConfigManager

logger = logging.getLogger(__name__)

class PreferenceConsumer:
    def __init__(self, topic_name: str, group_id: str):
        self.topic_name = topic_name
        self.group_id = group_id
        self.config_manager = KafkaConfigManager()
        
        # State management
        self.user_preferences: Dict[str, UserPreference] = {}
        self.deleted_users: Set[str] = set()
        self.message_count = 0
        self.compaction_stats = defaultdict(int)
        
        # Threading
        self.consumer_thread = None
        self.running = False
        self.lock = threading.RLock()
        
        # Create consumer
        self.consumer = self.config_manager.create_consumer(group_id, [topic_name])
        
    def start_consuming(self):
        """Start consuming messages in background thread"""
        if self.consumer_thread and self.consumer_thread.is_alive():
            logger.warning("Consumer already running")
            return
            
        self.running = True
        self.consumer_thread = threading.Thread(target=self._consume_loop)
        self.consumer_thread.start()
        logger.info(f"Started consuming from {self.topic_name}")

    def _consume_loop(self):
        """Main consumer loop"""
        try:
            while self.running:
                message_batch = self.consumer.poll(timeout_ms=1000)
                
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        self._process_message(message)
                        
                        # Manual commit after processing
                        self.consumer.commit()
                        
        except Exception as e:
            logger.error(f"Error in consumer loop: {e}")
        finally:
            logger.info("Consumer loop ended")

    def _process_message(self, message):
        """Process individual message"""
        try:
            with self.lock:
                user_id = message.key
                value = message.value
                
                self.message_count += 1
                
                if value is None:
                    # Tombstone message - user deletion
                    self._handle_user_deletion(user_id)
                    self.compaction_stats['tombstone_messages'] += 1
                    logger.info(f"Processed tombstone for user {user_id}")
                else:
                    # Regular preference update
                    self._handle_preference_update(user_id, value)
                    self.compaction_stats['preference_updates'] += 1
                
                # Track unique users
                if user_id not in self.deleted_users:
                    self.compaction_stats['active_users'] = len(self.user_preferences)
                    
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def _handle_preference_update(self, user_id: str, value: str):
        """Handle preference update message"""
        try:
            preference_data = json.loads(value)
            preference = UserPreference.from_dict(preference_data)
            
            # Check if this is a newer version
            existing = self.user_preferences.get(user_id)
            if existing is None or preference.version >= existing.version:
                self.user_preferences[user_id] = preference
                
                # Remove from deleted users if they were there
                if user_id in self.deleted_users:
                    self.deleted_users.remove(user_id)
                    
                logger.debug(f"Updated preferences for user {user_id} (version {preference.version})")
            else:
                logger.debug(f"Ignored older preference version for user {user_id}")
                self.compaction_stats['ignored_old_versions'] += 1
                
        except Exception as e:
            logger.error(f"Error handling preference update: {e}")

    def _handle_user_deletion(self, user_id: str):
        """Handle user deletion tombstone"""
        if user_id in self.user_preferences:
            del self.user_preferences[user_id]
            
        self.deleted_users.add(user_id)
        logger.debug(f"Deleted user {user_id}")

    def rebuild_state_from_beginning(self):
        """Rebuild state by consuming from the beginning of the log"""
        logger.info("Rebuilding state from compacted log...")
        
        # Reset state
        with self.lock:
            self.user_preferences.clear()
            self.deleted_users.clear()
            self.message_count = 0
            self.compaction_stats.clear()
        
        # Stop the running consumer if it's consuming
        was_running = self.running
        if was_running:
            self.running = False
            # Wait for the consumer loop to finish current poll and exit
            if self.consumer_thread and self.consumer_thread.is_alive():
                self.consumer_thread.join(timeout=3.0)
            # Close the consumer to fully clean up
            try:
                self.consumer.close()
            except:
                pass
        
        # Create a new consumer for rebuilding (without group subscription)
        from kafka import KafkaConsumer
        rebuild_consumer = KafkaConsumer(
            bootstrap_servers=self.config_manager.bootstrap_servers,
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            value_deserializer=lambda v: v.decode('utf-8') if v else None,
            enable_auto_commit=False,
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000  # Timeout for empty polls
        )
        
        try:
            # Wait a moment for metadata
            time.sleep(0.5)
            
            # Get all partitions for the topic - poll once to get metadata
            rebuild_consumer.poll(timeout_ms=1000)
            partitions = rebuild_consumer.partitions_for_topic(self.topic_name)
            if partitions is None or len(partitions) == 0:
                logger.warning(f"Topic {self.topic_name} has no partitions available yet")
                rebuild_consumer.close()
                # Recreate the original consumer if we were running
                if was_running:
                    self.consumer = self.config_manager.create_consumer(self.group_id, [self.topic_name])
                    self.running = True
                    self.consumer_thread = threading.Thread(target=self._consume_loop)
                    self.consumer_thread.start()
                return
            
            # Manually assign all partitions
            topic_partitions = [TopicPartition(self.topic_name, p) for p in partitions]
            rebuild_consumer.assign(topic_partitions)
            rebuild_consumer.seek_to_beginning()
            
            # Consume all messages
            rebuild_count = 0
            start_time = time.time()
            empty_polls = 0
            max_empty_polls = 2  # Break after 2 empty polls (10 seconds)
            
            while True:
                message_batch = rebuild_consumer.poll(timeout_ms=5000)
                if not message_batch:
                    empty_polls += 1
                    if empty_polls >= max_empty_polls:
                        logger.debug("No more messages to rebuild from")
                        break
                    continue
                
                empty_polls = 0  # Reset counter when we get messages
                    
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        self._process_message(message)
                        rebuild_count += 1
            
            elapsed_time = time.time() - start_time
            
            logger.info(
                f"State rebuilt: processed {rebuild_count} messages in {elapsed_time:.2f}s, "
                f"active users: {len(self.user_preferences)}, deleted users: {len(self.deleted_users)}"
            )
        finally:
            rebuild_consumer.close()
        
        # Recreate the original consumer if we were running before
        if was_running:
            try:
                self.consumer = self.config_manager.create_consumer(self.group_id, [self.topic_name])
                self.running = True
                self.consumer_thread = threading.Thread(target=self._consume_loop)
                self.consumer_thread.start()
                logger.info("Restarted consumer after state rebuild")
            except Exception as e:
                logger.error(f"Error restarting consumer: {e}")

    def get_user_preference(self, user_id: str) -> Optional[UserPreference]:
        """Get current preference for a user"""
        with self.lock:
            return self.user_preferences.get(user_id)

    def get_all_preferences(self) -> Dict[str, UserPreference]:
        """Get all current user preferences"""
        with self.lock:
            return self.user_preferences.copy()

    def get_stats(self) -> Dict[str, Any]:
        """Get consumer statistics"""
        with self.lock:
            return {
                'message_count': self.message_count,
                'active_users': len(self.user_preferences),
                'deleted_users_count': len(self.deleted_users),
                'compaction_stats': dict(self.compaction_stats),
                'topic': self.topic_name,
                'group_id': self.group_id,
                'running': self.running
            }

    def stop(self):
        """Stop the consumer"""
        self.running = False
        if self.consumer_thread and self.consumer_thread.is_alive():
            self.consumer_thread.join(timeout=5)
        
        if self.consumer:
            self.consumer.close()
            
        logger.info("Consumer stopped")
