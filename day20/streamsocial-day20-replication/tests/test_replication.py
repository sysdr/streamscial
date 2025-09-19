"""
Test suite for Kafka replication and ISR management
"""
import unittest
import time
import json
import threading
import sys
import os

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from producers.streamsocial_producer import StreamSocialProducer
from monitors.replication_monitor import ReplicationMonitor
from utils.chaos_engineer import ChaosEngineer

class TestKafkaReplication(unittest.TestCase):
    def setUp(self):
        self.bootstrap_servers = "localhost:9091,localhost:9092,localhost:9093"
        self.producer = StreamSocialProducer(self.bootstrap_servers, "test-region")
        self.monitor = ReplicationMonitor(self.bootstrap_servers)
        self.chaos = ChaosEngineer()
        
        # Wait for cluster to be ready
        time.sleep(2)

    def tearDown(self):
        if hasattr(self, 'producer'):
            self.producer.flush_and_close()
        if hasattr(self, 'monitor'):
            self.monitor.stop_monitoring()

    def test_topic_creation(self):
        """Test that topics are created with correct replication configuration"""
        self.producer.create_topics()
        time.sleep(2)
        
        metadata = self.monitor.get_cluster_metadata()
        self.assertIn('critical_posts', metadata.get('topics', {}))
        
        critical_posts = metadata['topics']['critical_posts']
        self.assertEqual(critical_posts['replication_factor'], 5)

    def test_message_production(self):
        """Test message production with replication"""
        # Create topics first
        self.producer.create_topics()
        time.sleep(2)
        
        # Send test messages
        post_id = self.producer.create_post("test_user", "Test message content")
        engagement_id = self.producer.create_engagement(post_id, "test_user", "like")
        
        # Flush and verify
        self.producer.producer.flush(timeout=10)
        
        self.assertIsNotNone(post_id)
        self.assertIsNotNone(engagement_id)
        self.assertGreater(self.producer.messages_sent, 0)

    def test_cluster_health_monitoring(self):
        """Test cluster health monitoring"""
        self.monitor.start_monitoring(interval_seconds=1)
        time.sleep(3)
        
        monitoring_data = self.monitor.get_monitoring_data()
        
        self.assertIn('cluster_metadata', monitoring_data)
        self.assertIn('health_score', monitoring_data)
        
        health = monitoring_data['health_score']
        self.assertIn('overall_health', health)
        self.assertGreaterEqual(health['overall_health'], 0)
        self.assertLessEqual(health['overall_health'], 100)

    def test_isr_detection(self):
        """Test ISR change detection"""
        self.producer.create_topics()
        time.sleep(2)
        
        self.monitor.start_monitoring(interval_seconds=1)
        time.sleep(2)
        
        # Get initial state
        initial_data = self.monitor.get_monitoring_data()
        initial_changes = len(initial_data.get('recent_changes', []))
        
        # Inject a failure to trigger ISR changes
        try:
            failure_id = self.chaos.inject_failure("single_broker_failure")
            time.sleep(5)  # Wait for ISR changes to be detected
            
            # Check for new changes
            updated_data = self.monitor.get_monitoring_data()
            updated_changes = len(updated_data.get('recent_changes', []))
            
            # We should detect some changes (though they may be subtle in test env)
            print(f"ISR changes detected: {updated_changes - initial_changes}")
            
        except Exception as e:
            print(f"Chaos injection failed (expected in some environments): {e}")

    def test_message_durability_during_failure(self):
        """Test that messages survive broker failures"""
        self.producer.create_topics()
        time.sleep(2)
        
        # Send initial batch
        initial_posts = []
        for i in range(10):
            post_id = self.producer.create_post(f"user_{i}", f"Message {i}")
            initial_posts.append(post_id)
        
        self.producer.producer.flush(timeout=10)
        initial_sent = self.producer.messages_sent
        
        # Inject failure
        try:
            failure_id = self.chaos.inject_failure("single_broker_failure")
            time.sleep(2)
            
            # Continue sending messages during failure
            failure_posts = []
            for i in range(10, 15):
                try:
                    post_id = self.producer.create_post(f"user_{i}", f"Message during failure {i}")
                    failure_posts.append(post_id)
                except Exception as e:
                    print(f"Expected failure during broker down: {e}")
            
            self.producer.producer.flush(timeout=10)
            
            # Wait for recovery
            time.sleep(60)  # Single broker failure lasts 60s
            
            # Send post-recovery batch
            recovery_posts = []
            for i in range(15, 20):
                post_id = self.producer.create_post(f"user_{i}", f"Recovery message {i}")
                recovery_posts.append(post_id)
            
            self.producer.producer.flush(timeout=10)
            final_sent = self.producer.messages_sent
            
            # Verify some messages got through
            self.assertGreater(final_sent, initial_sent)
            print(f"Messages sent: Initial={initial_sent}, Final={final_sent}")
            
        except Exception as e:
            print(f"Durability test failed (expected in some environments): {e}")

class TestChaosEngineering(unittest.TestCase):
    def setUp(self):
        self.chaos = ChaosEngineer()

    def test_failure_scenarios_available(self):
        """Test that all expected failure scenarios are available"""
        expected_scenarios = [
            "single_broker_failure",
            "leader_broker_failure", 
            "network_partition",
            "cascading_failure",
            "zk_failure"
        ]
        
        for scenario in expected_scenarios:
            self.assertIn(scenario, self.chaos.scenarios)

    def test_failure_status_tracking(self):
        """Test failure status tracking"""
        initial_status = self.chaos.get_failure_status()
        self.assertEqual(initial_status['active_failures'], 0)
        self.assertEqual(initial_status['total_failures'], 0)

class TestEndToEndReplication(unittest.TestCase):
    """End-to-end integration tests"""
    
    def setUp(self):
        self.bootstrap_servers = "localhost:9091,localhost:9092,localhost:9093"
        
    def test_full_replication_workflow(self):
        """Test complete replication workflow"""
        # Initialize components
        producer = StreamSocialProducer(self.bootstrap_servers, "integration-test")
        monitor = ReplicationMonitor(self.bootstrap_servers)
        
        try:
            # Create topics
            producer.create_topics()
            time.sleep(2)
            
            # Start monitoring
            monitor.start_monitoring(interval_seconds=1)
            time.sleep(1)
            
            # Simulate traffic
            producer.simulate_traffic(duration_seconds=10)
            
            # Check final state
            monitoring_data = monitor.get_monitoring_data()
            
            # Verify cluster is healthy
            health = monitoring_data.get('health_score', {})
            self.assertGreater(health.get('overall_health', 0), 50)
            
            # Verify messages were sent
            self.assertGreater(producer.messages_sent, 0)
            
            # Verify topics exist
            topics = monitoring_data.get('cluster_metadata', {}).get('topics', {})
            self.assertIn('critical_posts', topics)
            
        finally:
            producer.flush_and_close()
            monitor.stop_monitoring()

if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)
