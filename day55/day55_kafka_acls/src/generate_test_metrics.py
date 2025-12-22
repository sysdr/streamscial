#!/usr/bin/env python3
"""
Generate test metrics for the dashboard
"""
import time
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from monitoring.acl_dashboard import monitor

def generate_test_metrics():
    """Generate some test metrics to populate the dashboard"""
    print("Generating test metrics for dashboard...")
    
    # Simulate some access patterns
    services = ['post-service', 'analytics-service', 'moderation-service']
    operations = ['READ', 'WRITE', 'DELETE', 'DESCRIBE']
    topics = ['posts.created', 'posts.updated', 'posts.deleted', 'analytics.metrics', 'moderation.flags']
    
    # Generate some granted accesses
    for i in range(20):
        service = services[i % len(services)]
        operation = operations[i % len(operations)]
        topic = topics[i % len(topics)]
        monitor.record_access(service, operation, topic, True)
        print(f"✓ Recorded: {service} - {operation} on {topic} (granted)")
        time.sleep(0.1)
    
    # Generate some denied accesses
    for i in range(5):
        service = services[i % len(services)]
        operation = operations[i % len(operations)]
        topic = topics[(i + 2) % len(topics)]
        monitor.record_access(service, operation, topic, False)
        print(f"✗ Recorded: {service} - {operation} on {topic} (denied)")
        time.sleep(0.1)
    
    print("\n✅ Test metrics generated!")
    print(f"Total requests: {monitor.get_metrics()['total_requests']}")
    print(f"Granted: {monitor.get_metrics()['granted']}")
    print(f"Denied: {monitor.get_metrics()['denied']}")
    print("\nRefresh your dashboard at http://localhost:5055 to see the metrics!")

if __name__ == '__main__':
    generate_test_metrics()

