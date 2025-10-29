import pytest
import threading
import time
import uuid
from datetime import datetime
from src.producer.preference_producer import PreferenceProducer
from src.consumer.preference_consumer import PreferenceConsumer
from src.models.user_preference import UserPreference

class TestCompactionFlow:
    @pytest.fixture
    def topic_name(self):
        return f"test-compaction-{uuid.uuid4().hex[:8]}"
    
    @pytest.fixture
    def producer(self, topic_name):
        return PreferenceProducer(topic_name)
    
    @pytest.fixture
    def consumer(self, topic_name):
        consumer = PreferenceConsumer(topic_name, f"test-group-{uuid.uuid4().hex[:8]}")
        consumer.start_consuming()
        time.sleep(2)  # Allow consumer to start
        return consumer
    
    def test_basic_produce_consume(self, producer, consumer):
        """Test basic message production and consumption"""
        user_id = "test_user_001"
        preference = UserPreference(
            user_id=user_id,
            theme="dark",
            language="en",
            version=1
        )
        
        # Send preference
        success = producer.send_preference_update(preference)
        assert success == True
        
        # Wait for message to be consumed
        time.sleep(3)
        
        # Verify consumer received the message
        consumed_pref = consumer.get_user_preference(user_id)
        assert consumed_pref is not None
        assert consumed_pref.user_id == user_id
        assert consumed_pref.theme == "dark"
        assert consumed_pref.version == 1
    
    def test_multiple_updates_same_user(self, producer, consumer):
        """Test that multiple updates for same user result in latest version"""
        user_id = "test_user_002"
        
        # Send multiple preference updates
        for i in range(1, 6):
            preference = UserPreference(
                user_id=user_id,
                theme=f"theme_{i}",
                language="en",
                version=i
            )
            success = producer.send_preference_update(preference)
            assert success == True
            time.sleep(0.5)
        
        # Wait for all messages to be consumed
        time.sleep(3)
        
        # Should only have the latest preference
        consumed_pref = consumer.get_user_preference(user_id)
        assert consumed_pref is not None
        assert consumed_pref.user_id == user_id
        assert consumed_pref.theme == "theme_5"
        assert consumed_pref.version == 5
        
        # Consumer should have processed all messages
        stats = consumer.get_stats()
        assert stats['message_count'] >= 5
        assert stats['active_users'] == 1
    
    def test_tombstone_deletion(self, producer, consumer):
        """Test user deletion with tombstone messages"""
        user_id = "test_user_003"
        
        # First, create a preference
        preference = UserPreference(
            user_id=user_id,
            theme="light",
            version=1
        )
        success = producer.send_preference_update(preference)
        assert success == True
        
        time.sleep(2)
        
        # Verify user exists
        consumed_pref = consumer.get_user_preference(user_id)
        assert consumed_pref is not None
        
        # Now delete the user (send tombstone)
        success = producer.send_user_deletion(user_id)
        assert success == True
        
        time.sleep(2)
        
        # User should be gone from active preferences
        consumed_pref = consumer.get_user_preference(user_id)
        assert consumed_pref is None
        
        # But should be in deleted users
        stats = consumer.get_stats()
        assert stats['deleted_users_count'] >= 1
    
    def test_out_of_order_messages(self, producer, consumer):
        """Test handling of out-of-order messages"""
        user_id = "test_user_004"
        
        # Send newer version first
        newer_preference = UserPreference(
            user_id=user_id,
            theme="dark",
            version=5
        )
        success = producer.send_preference_update(newer_preference)
        assert success == True
        
        time.sleep(1)
        
        # Send older version
        older_preference = UserPreference(
            user_id=user_id,
            theme="light",
            version=3
        )
        success = producer.send_preference_update(older_preference)
        assert success == True
        
        time.sleep(2)
        
        # Should retain the newer version
        consumed_pref = consumer.get_user_preference(user_id)
        assert consumed_pref is not None
        assert consumed_pref.theme == "dark"
        assert consumed_pref.version == 5
    
    def test_state_rebuilding(self, producer, consumer):
        """Test consumer state rebuilding from compacted log"""
        users = [f"rebuild_user_{i:03d}" for i in range(1, 11)]
        
        # Send preferences for multiple users
        for user_id in users:
            preference = UserPreference(
                user_id=user_id,
                theme="light",
                language="en",
                version=1
            )
            success = producer.send_preference_update(preference)
            assert success == True
        
        # Send updates for some users
        for i in range(0, 5):
            user_id = users[i]
            preference = UserPreference(
                user_id=user_id,
                theme="dark",
                version=2
            )
            success = producer.send_preference_update(preference)
            assert success == True
        
        time.sleep(3)
        
        # Get current state
        all_prefs_before = consumer.get_all_preferences()
        assert len(all_prefs_before) == 10
        
        # Rebuild state from beginning
        consumer.rebuild_state_from_beginning()
        
        time.sleep(2)
        
        # Verify state is correct after rebuild
        all_prefs_after = consumer.get_all_preferences()
        assert len(all_prefs_after) == 10
        
        # Check that updates were preserved
        for i in range(0, 5):
            user_id = users[i]
            pref = consumer.get_user_preference(user_id)
            assert pref.theme == "dark"
            assert pref.version == 2
        
        for i in range(5, 10):
            user_id = users[i]
            pref = consumer.get_user_preference(user_id)
            assert pref.theme == "light"
            assert pref.version == 1
    
    def teardown_method(self, method):
        """Clean up after each test"""
        time.sleep(1)

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
