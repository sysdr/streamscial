#!/usr/bin/env python3

from src.utils.metrics import MetricsCollector

def test_metrics():
    """Test the metrics collector"""
    print("🔍 Testing metrics collector...")
    
    collector = MetricsCollector()
    
    print("✅ Metrics collector created")
    
    # Test initial state
    initial_metrics = collector.get_system_metrics()
    print(f"📊 Initial metrics: {initial_metrics}")
    
    # Record some messages
    print("📝 Recording message processing...")
    collector.record_message_processed(0.001, True)   # Success
    collector.record_message_processed(0.002, True)   # Success
    collector.record_message_processed(0.003, False)  # Failure
    
    # Record engagement events
    print("📝 Recording engagement events...")
    collector.record_engagement_event("like")
    collector.record_engagement_event("share")
    collector.record_engagement_event("comment")
    
    # Get updated metrics
    updated_metrics = collector.get_system_metrics()
    print(f"📊 Updated metrics: {updated_metrics}")
    
    # Verify the counts
    assert updated_metrics['messages_processed'] == 3, f"Expected 3, got {updated_metrics['messages_processed']}"
    assert updated_metrics['messages_failed'] == 1, f"Expected 1, got {updated_metrics['messages_failed']}"
    
    print("✅ Metrics collector test passed!")

if __name__ == "__main__":
    test_metrics()
