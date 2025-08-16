import pytest
import time
import docker
import threading
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from src.producer.high_volume_producer import create_high_volume_producer, UserAction

class TestKafkaIntegration:
    @classmethod
    def setup_class(cls):
        """Setup Kafka container for testing"""
        cls.client = docker.from_env()
        
        # Check if Kafka is already running
        try:
            containers = cls.client.containers.list()
            kafka_running = any('kafka' in container.name.lower() for container in containers)
            
            if not kafka_running:
                print("⚠️  Kafka not running - integration tests may fail")
                print("💡 Run: docker-compose up -d to start Kafka")
        except Exception as e:
            print(f"⚠️  Could not check Docker containers: {e}")

    def test_producer_consumer_integration(self):
        """Test end-to-end message flow"""
        # Skip if Kafka not available
        try:
            admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})
            metadata = admin_client.list_topics(timeout=5)
        except Exception:
            pytest.skip("Kafka not available")

        # Create test topic
        topic_name = "test-integration"
        new_topics = [NewTopic(topic_name, num_partitions=3, replication_factor=1)]
        
        try:
            futures = admin_client.create_topics(new_topics)
            for topic, future in futures.items():
                try:
                    future.result()
                    print(f"✅ Created topic: {topic}")
                except Exception as e:
                    if "already exists" not in str(e):
                        raise
        except Exception as e:
            print(f"Topic creation: {e}")

        # Setup producer
        producer = create_high_volume_producer(
            bootstrap_servers="localhost:9092",
            pool_size=2,
            worker_threads=2
        )
        producer.start()

        # Setup consumer
        consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'test-group',
            'auto.offset.reset': 'earliest'
        })
        consumer.subscribe([topic_name])

        try:
            # Send test messages
            test_messages = []
            for i in range(10):
                action = UserAction(
                    user_id=f"user{i}",
                    action_type="post",
                    content_id=f"content{i}",
                    timestamp=int(time.time() * 1000),
                    metadata={"test": True}
                )
                producer.send_async(topic_name, action)
                test_messages.append(action)

            # Wait for messages to be sent
            time.sleep(2)

            # Consume and verify messages
            consumed_messages = []
            start_time = time.time()
            
            while len(consumed_messages) < len(test_messages) and (time.time() - start_time) < 10:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    continue
                    
                consumed_messages.append(msg.value().decode('utf-8'))

            # Verify we received all messages
            assert len(consumed_messages) == len(test_messages)
            print(f"✅ Successfully sent and consumed {len(consumed_messages)} messages")

        finally:
            producer.stop()
            consumer.close()

    def test_high_throughput_scenario(self):
        """Test high throughput scenario"""
        try:
            admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})
            metadata = admin_client.list_topics(timeout=5)
        except Exception:
            pytest.skip("Kafka not available")

        producer = create_high_volume_producer(
            bootstrap_servers="localhost:9092",
            pool_size=5,
            worker_threads=4
        )
        producer.start()

        try:
            # Send 1000 messages quickly
            start_time = time.time()
            
            for i in range(1000):
                action = UserAction(
                    user_id=f"user{i}",
                    action_type="post",
                    content_id=f"content{i}", 
                    timestamp=int(time.time() * 1000),
                    metadata={"batch_test": True}
                )
                producer.send_async("user-actions", action)

            # Wait for completion
            time.sleep(5)
            
            # Check metrics
            metrics = producer.get_metrics()
            duration = time.time() - start_time + 5  # Include wait time
            
            print(f"📊 Metrics after high throughput test:")
            print(f"   Messages sent: {metrics['messages_sent']}")
            print(f"   Success rate: {metrics['success_rate']:.1f}%")
            print(f"   Throughput: {metrics['throughput_msg_sec']:.0f} msg/sec")
            
            assert metrics['messages_sent'] >= 900  # Allow for some failures
            assert metrics['success_rate'] > 90  # At least 90% success rate

        finally:
            producer.stop()

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
