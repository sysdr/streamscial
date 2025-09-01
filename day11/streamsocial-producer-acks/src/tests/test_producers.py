import unittest
import time
import sys
import os

# Add the src directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from producers.acks_producer import AcksProducerManager
from events.event_types import StreamSocialEvent, EventType

class TestProducerAcks(unittest.TestCase):
    def setUp(self):
        self.producer_manager = AcksProducerManager()
        time.sleep(2)  # Allow producers to initialize
    
    def test_critical_event_routing(self):
        """Test that critical events use the correct producer"""
        event = StreamSocialEvent.create_critical_event(
            EventType.PAYMENT_PROCESSING,
            "test_user_001",
            {"amount": 2999, "currency": "USD"}
        )
        
        result = self.producer_manager.send_event(event)
        self.assertTrue(result)
        
        # Allow time for acknowledgment
        time.sleep(1)
        metrics = self.producer_manager.get_metrics()
        self.assertGreater(metrics['critical']['sent'], 0)
    
    def test_social_event_routing(self):
        """Test that social events use the correct producer"""
        event = StreamSocialEvent.create_social_event(
            EventType.POST_CREATION,
            "test_user_002",
            {"post_id": "post_123", "content_type": "image"}
        )
        
        result = self.producer_manager.send_event(event)
        self.assertTrue(result)
        
        time.sleep(1)
        metrics = self.producer_manager.get_metrics()
        self.assertGreater(metrics['social']['sent'], 0)
    
    def test_analytics_event_routing(self):
        """Test that analytics events use the correct producer"""
        event = StreamSocialEvent.create_analytics_event(
            EventType.PAGE_VIEW,
            "test_user_003",
            {"page": "/feed", "referrer": "https://google.com"}
        )
        
        result = self.producer_manager.send_event(event)
        self.assertTrue(result)
        
        time.sleep(1)
        metrics = self.producer_manager.get_metrics()
        self.assertGreater(metrics['analytics']['sent'], 0)
    
    def test_performance_comparison(self):
        """Test performance differences between acknowledgment strategies"""
        events = []
        
        # Create test events for each category
        for i in range(10):
            events.extend([
                StreamSocialEvent.create_critical_event(
                    EventType.PAYMENT_PROCESSING, f"user_{i}", {"amount": 1000}
                ),
                StreamSocialEvent.create_social_event(
                    EventType.POST_CREATION, f"user_{i}", {"post_id": f"post_{i}"}
                ),
                StreamSocialEvent.create_analytics_event(
                    EventType.PAGE_VIEW, f"user_{i}", {"page": "/feed"}
                )
            ])
        
        start_time = time.time()
        for event in events:
            self.producer_manager.send_event(event)
        
        # Flush to ensure all messages are sent
        self.producer_manager.flush_all()
        end_time = time.time()
        
        total_time = end_time - start_time
        self.assertLess(total_time, 10, "Should complete within 10 seconds")
        
        metrics = self.producer_manager.get_metrics()
        print(f"\nPerformance Test Results (30 events in {total_time:.2f}s):")
        for producer_type, metric in metrics.items():
            avg_latency = sum(metric['latency']) / len(metric['latency']) if metric['latency'] else 0
            print(f"  {producer_type}: {metric['sent']} sent, {metric['acked']} acked, {avg_latency:.2f}ms avg latency")
    
    def tearDown(self):
        self.producer_manager.close_all()

if __name__ == '__main__':
    unittest.main()
