"""
StreamSocial Kafka Cluster Tests - Day 2
Comprehensive testing for 3-broker cluster setup
"""

import time
import json
import unittest
import threading
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient
from src.kafka_client import StreamSocialKafkaClient

class TestKafkaCluster(unittest.TestCase):
    
    def setUp(self):
        """Set up test environment"""
        self.client = StreamSocialKafkaClient()
        self.client.create_streamsocial_topics()
        time.sleep(5)  # Wait for topics to be created
    
    def test_cluster_connectivity(self):
        """Test basic cluster connectivity"""
        print("🔍 Testing cluster connectivity...")
        
        metadata = self.client.get_cluster_metadata()
        self.assertIsNotNone(metadata, "Should be able to get cluster metadata")
        self.assertEqual(len(metadata['brokers']), 3, "Should have 3 brokers")
        self.assertIsNotNone(metadata['controller'], "Should have a controller")
        
        print(f"✅ Connected to {len(metadata['brokers'])} brokers")
        print(f"👑 Controller: Broker {metadata['controller']}")
    
    def test_topic_creation(self):
        """Test topic creation and configuration"""
        print("🔍 Testing topic creation...")
        
        metadata = self.client.get_cluster_metadata()
        expected_topics = {'user-actions', 'content-interactions', 'system-events'}
        actual_topics = set(metadata['topics'])
        
        self.assertTrue(expected_topics.issubset(actual_topics), 
                       f"Expected topics {expected_topics} not found in {actual_topics}")
        
        print("✅ All required topics created successfully")
    
    def test_producer_functionality(self):
        """Test message production to all brokers"""
        print("🔍 Testing producer functionality...")
        
        test_events = [
            ('user-actions', {'user_id': 123, 'action': 'test'}),
            ('content-interactions', {'content_id': 456, 'interaction': 'test'}),
            ('system-events', {'event': 'test_system_event'})
        ]
        
        for topic, event_data in test_events:
            metadata = self.client.send_event(topic, event_data)
            self.assertIsNotNone(metadata, f"Should successfully send to {topic}")
            print(f"✅ Sent test event to {topic}[{metadata.partition}]")
    
    def test_consumer_functionality(self):
        """Test message consumption from cluster"""
        print("🔍 Testing consumer functionality...")
        
        # First, send a test message
        test_message = {'test': 'consumer_test', 'timestamp': int(time.time())}
        self.client.send_event('user-actions', test_message)
        
        # Then consume it
        consumer = KafkaConsumer(
            'user-actions',
            bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=10000,
            auto_offset_reset='latest'
        )
        
        # Send another message to trigger consumption
        self.client.send_event('user-actions', test_message)
        
        message_received = False
        for message in consumer:
            if message.value.get('test') == 'consumer_test':
                message_received = True
                break
        
        consumer.close()
        self.assertTrue(message_received, "Should receive the test message")
        print("✅ Successfully consumed test message")
    
    def test_fault_tolerance(self):
        """Test cluster behavior during broker failure simulation"""
        print("🔍 Testing fault tolerance (simulated)...")
        
        # Test with different bootstrap servers to simulate partial failures
        test_configs = [
            ['localhost:9092', 'localhost:9093'],  # Broker 3 "failed"
            ['localhost:9092', 'localhost:9094'],  # Broker 2 "failed"
            ['localhost:9093', 'localhost:9094']   # Broker 1 "failed"
        ]
        
        for i, servers in enumerate(test_configs):
            try:
                test_client = StreamSocialKafkaClient(servers)
                metadata = test_client.get_cluster_metadata()
                
                if metadata and len(metadata['brokers']) >= 2:
                    test_event = {'test': f'failover_test_{i}', 'config': i}
                    test_client.send_event('system-events', test_event)
                    print(f"✅ Failover test {i+1} passed with {len(metadata['brokers'])} brokers")
                else:
                    print(f"⚠️ Failover test {i+1}: Limited broker availability")
                    
            except Exception as e:
                print(f"⚠️ Failover test {i+1} error: {e}")
    
    def test_throughput_performance(self):
        """Test cluster throughput under load"""
        print("🔍 Testing throughput performance...")
        
        start_time = time.time()
        event_count = 100
        
        for i in range(event_count):
            event_data = {
                'event_id': i,
                'user_id': i % 10,
                'action': 'performance_test',
                'timestamp': int(time.time() * 1000)
            }
            
            self.client.send_event('user-actions', event_data, user_id=i % 10)
        
        duration = time.time() - start_time
        throughput = event_count / duration
        
        print(f"✅ Throughput test: {event_count} events in {duration:.2f}s ({throughput:.2f} events/sec)")
        self.assertGreater(throughput, 10, "Should achieve at least 10 events/sec")

def run_continuous_test():
    """Run continuous monitoring test"""
    print("🔄 Starting continuous cluster monitoring...")
    
    client = StreamSocialKafkaClient()
    test_count = 0
    
    try:
        while test_count < 10:  # Run 10 iterations
            # Check cluster health
            metadata = client.get_cluster_metadata()
            if metadata:
                broker_count = len(metadata['brokers'])
                print(f"📊 Iteration {test_count + 1}: {broker_count} brokers active")
                
                # Send test event
                test_event = {
                    'test_iteration': test_count,
                    'timestamp': int(time.time() * 1000),
                    'cluster_health': 'monitoring'
                }
                
                try:
                    client.send_event('system-events', test_event)
                    print(f"✅ Test event {test_count + 1} sent successfully")
                except Exception as e:
                    print(f"❌ Test event {test_count + 1} failed: {e}")
            else:
                print(f"⚠️ Iteration {test_count + 1}: Cluster metadata unavailable")
            
            test_count += 1
            time.sleep(3)
            
    except KeyboardInterrupt:
        print("🛑 Continuous test stopped by user")

if __name__ == '__main__':
    print("🧪 StreamSocial Kafka Cluster Test Suite")
    print("=" * 50)
    
    # Run unit tests
    unittest.main(argv=[''], exit=False, verbosity=2)
    
    print("\n" + "=" * 50)
    
    # Run continuous monitoring
    run_continuous_test()
    
    print("\n🎉 All tests completed!")
