import pytest
import json
import time
import threading
import uuid
from unittest.mock import Mock, patch
from kafka import KafkaProducer
from src.consumers.robust_consumer import RobustStreamSocialConsumer
from src.consumers.post_processor import StreamSocialPostProcessor

class TestConsumerIntegration:
    def setup_method(self):
        self.bootstrap_servers = 'localhost:9092'
        self.test_topic = 'test-streamsocial-posts'
        self.post_processor = StreamSocialPostProcessor()
        
    @patch('src.consumers.robust_consumer.DLQManager')
    def test_end_to_end_processing(self, mock_dlq_manager):
        """Test end-to-end message processing with mocked DLQ"""
        # Mock the DLQ manager to avoid Kafka connection issues
        mock_dlq_instance = Mock()
        mock_dlq_manager.return_value = mock_dlq_instance
        
        # Use a unique group ID to avoid offset conflicts
        unique_group_id = f'test-group-{uuid.uuid4().hex[:8]}'
        
        # Test basic Kafka connectivity first
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            producer.close()
        except Exception as e:
            pytest.skip(f"Kafka not available: {e}")
        
        # Setup consumer
        consumer = RobustStreamSocialConsumer(
            topics=[self.test_topic],
            bootstrap_servers=self.bootstrap_servers,
            group_id=unique_group_id,
            message_processor=self.post_processor.process_social_post
        )
        
        # Produce test message
        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        test_message = {
            'user_id': 'test_user_001',
            'content': 'Test post content #testing',
            'timestamp': '2024-01-01T00:00:00Z'
        }
        
        # Send message first
        producer.send(self.test_topic, value=test_message)
        producer.flush()
        
        # Give Kafka time to make the message available
        time.sleep(2)
        
        # Process messages directly using the consumer's polling mechanism
        processed = False
        max_attempts = 10
        
        for attempt in range(max_attempts):
            try:
                # Poll for messages with timeout
                message_batch = consumer.consumer.poll(timeout_ms=1000)
                if message_batch:
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            consumer._process_message_safely(message)
                            processed = True
                            break
                    if processed:
                        break
            except Exception as e:
                print(f"Consumer error on attempt {attempt + 1}: {e}")
            
            time.sleep(0.5)  # Brief pause between attempts
        
        # Give a moment for processing to complete
        time.sleep(1)
        
        # Verify processing
        processed_posts = self.post_processor.get_processed_posts()
        assert len(processed_posts) > 0, f"No posts were processed after {max_attempts} attempts. Available posts: {processed_posts}"
        assert processed_posts[0]['user_id'] == 'test_user_001'
        assert processed_posts[0]['content'] == 'Test post content #testing'
        assert 'testing' in processed_posts[0]['hashtags']
        
        producer.close()
        consumer.consumer.close()
