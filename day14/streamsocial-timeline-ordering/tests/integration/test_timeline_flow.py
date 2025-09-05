import pytest
import time
import json
from src.producers.timeline_producer import TimelineProducer, TimelineMessage
from src.consumers.timeline_consumer import TimelineConsumer

class TestTimelineFlow:
    @pytest.fixture
    def producer(self):
        return TimelineProducer()
    
    @pytest.fixture  
    def consumer(self):
        return TimelineConsumer()
    
    def test_end_to_end_message_flow(self, producer, consumer):
        """Test complete message flow from producer to consumer"""
        # Create test messages
        messages = [
            TimelineMessage("alice", "post1", "First post", time.time()),
            TimelineMessage("alice", "post2", "Second post", time.time() + 1),
            TimelineMessage("bob", "post3", "Bob's post", time.time() + 2)
        ]
        
        # Send messages
        for msg in messages:
            result = producer.send_timeline_message(msg)
            assert result == True
        
        # Allow some time for message processing
        time.sleep(2)
        
        # Consume messages
        consumption_stats = consumer.consume_messages(timeout_ms=5000)
        
        # Verify consumption
        assert consumption_stats['messages_consumed'] >= len(messages)
        
        # Check Alice's timeline ordering
        alice_timeline = consumer.get_user_timeline("alice")
        if len(alice_timeline) >= 2:
            # Should be in chronological order
            assert alice_timeline[0]['timestamp'] <= alice_timeline[1]['timestamp']
        
        # Cleanup
        producer.close()
        consumer.close()
    
    def test_partition_consistency(self, producer):
        """Test that same user messages go to same partition"""
        user_id = "consistency_test_user"
        messages = [
            TimelineMessage(user_id, f"post{i}", f"Message {i}", time.time() + i)
            for i in range(5)
        ]
        
        partitions_used = set()
        for msg in messages:
            producer.send_timeline_message(msg)
            partition_key = producer.calculate_partition_key(msg.user_id)
            partition = producer.get_partition_for_key(partition_key)
            partitions_used.add(partition)
        
        # All messages from same user should go to same partition
        assert len(partitions_used) == 1
        
        producer.close()
    
    def test_ordering_verification(self, producer, consumer):
        """Test ordering verification functionality"""
        user_id = "ordering_test_user"
        
        # Send messages with known timestamps
        timestamps = [time.time() + i for i in range(3)]
        messages = [
            TimelineMessage(user_id, f"post{i}", f"Ordered message {i}", timestamps[i])
            for i in range(3)
        ]
        
        for msg in messages:
            producer.send_timeline_message(msg)
        
        time.sleep(2)
        consumer.consume_messages(timeout_ms=3000)
        
        # Verify ordering
        verification = consumer.verify_ordering_consistency()
        
        # Should have no violations for properly ordered messages
        assert verification['ordering_violations'] == 0
        
        producer.close()
        consumer.close()
