import json
import time
import threading
from datetime import datetime
from typing import Dict, Any
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging
from src.models.user_preference import UserPreference
from src.utils.kafka_config import KafkaConfigManager

logger = logging.getLogger(__name__)

class PreferenceProducer:
    def __init__(self, topic_name: str):
        self.topic_name = topic_name
        self.config_manager = KafkaConfigManager()
        
        # Ensure topic exists with compaction
        self.config_manager.create_compacted_topic(topic_name)
        
        # Create producer
        self.producer = self.config_manager.create_producer()
        self.message_count = 0
        self.last_send_time = time.time()
        
    def send_preference_update(self, user_preference: UserPreference) -> bool:
        """Send user preference update to compacted topic"""
        try:
            key = user_preference.user_id
            value = user_preference.to_json()
            
            # Send message
            future = self.producer.send(
                self.topic_name,
                key=key,
                value=value,
                timestamp_ms=int(datetime.utcnow().timestamp() * 1000)
            )
            
            # Wait for acknowledgment
            record_metadata = future.get(timeout=10)
            
            self.message_count += 1
            logger.info(
                f"Sent preference update for user {key} to "
                f"partition {record_metadata.partition} at offset {record_metadata.offset}"
            )
            
            return True
            
        except KafkaError as e:
            logger.error(f"Failed to send preference update: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending message: {e}")
            return False

    def send_user_deletion(self, user_id: str) -> bool:
        """Send tombstone message for user deletion"""
        try:
            # Tombstone message (null value)
            future = self.producer.send(
                self.topic_name,
                key=user_id,
                value=None,  # Tombstone
                timestamp_ms=int(datetime.utcnow().timestamp() * 1000)
            )
            
            record_metadata = future.get(timeout=10)
            
            logger.info(
                f"Sent tombstone for user {user_id} to "
                f"partition {record_metadata.partition} at offset {record_metadata.offset}"
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to send tombstone: {e}")
            return False

    def simulate_user_activity(self, num_users: int = 10, duration_seconds: int = 60):
        """Simulate user preference changes for testing"""
        import random
        
        users = [f"user_{i:03d}" for i in range(1, num_users + 1)]
        themes = ["light", "dark", "auto"]
        languages = ["en", "es", "fr", "de", "ja"]
        visibility_options = ["public", "friends", "private"]
        
        start_time = time.time()
        
        logger.info(f"Starting simulation with {num_users} users for {duration_seconds} seconds")
        
        while time.time() - start_time < duration_seconds:
            user_id = random.choice(users)
            
            # Create random preference update
            preference = UserPreference(
                user_id=user_id,
                theme=random.choice(themes),
                language=random.choice(languages),
                timezone=random.choice(["UTC", "EST", "PST", "GMT", "JST"]),
                version=random.randint(1, 20)
            )
            
            # Random privacy settings
            preference.privacy.profile_visibility = random.choice(visibility_options)
            preference.privacy.show_activity = random.choice([True, False])
            preference.privacy.allow_tagging = random.choice([True, False])
            
            # Random notification settings
            preference.notifications.email = random.choice([True, False])
            preference.notifications.push = random.choice([True, False])
            preference.notifications.sms = random.choice([True, False])
            
            self.send_preference_update(preference)
            
            # Random delay between updates
            time.sleep(random.uniform(0.5, 3.0))
        
        logger.info(f"Simulation completed. Sent {self.message_count} messages")

    def get_stats(self) -> Dict[str, Any]:
        """Get producer statistics"""
        return {
            'message_count': self.message_count,
            'topic': self.topic_name,
            'uptime_seconds': time.time() - self.last_send_time
        }

    def close(self):
        """Close producer connection"""
        if self.producer:
            self.producer.close()
            logger.info("Producer closed")
