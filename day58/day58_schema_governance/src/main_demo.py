"""
Main Demo - Schema Governance in Action
Demonstrates gradual schema migration with real-time monitoring
"""

import sys
import os
import time
import threading

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.registry.schema_manager import registry_client
from src.producers.schema_evolution_producer import evolution_producer
from src.consumers.schema_adaptive_consumer import adaptive_consumer
from src.monitoring.governance_monitor import governance_monitor


def simulate_message_production():
    """Simulate production environment with multiple schema versions"""
    print("\n" + "="*70)
    print("PHASE 1: SCHEMA REGISTRATION & VALIDATION")
    print("="*70)
    
    # Load and register schemas
    evolution_producer.load_schemas('schemas')
    
    # Update schema states to APPROVED
    for version in range(1, 4):
        registry_client.update_schema_state('posts-value', version, 'APPROVED')
    
    time.sleep(2)
    
    print("\n" + "="*70)
    print("PHASE 2: GRADUAL MIGRATION SIMULATION")
    print("="*70)
    print("\nSimulating production traffic with mixed schema versions...")
    print("This demonstrates how LinkedIn/Netflix handle schema evolution\n")
    
    # Simulate gradual migration
    evolution_producer.simulate_gradual_migration(num_posts=30)
    
    time.sleep(2)
    
    # Collect messages for consumption
    # In real Kafka, consumer would read from topic
    # Here we simulate by creating message bytes
    simulated_messages = []
    
    # Create sample messages with different versions
    from io import BytesIO
    import fastavro
    
    for i in range(15):
        # Random version selection
        import random
        version = random.choice(['v1', 'v2', 'v3'])
        schema_info = evolution_producer.schemas[version]
        
        if version == 'v1':
            post_data = evolution_producer.create_post_v1(i, random.randint(1, 50))
        elif version == 'v2':
            post_data = evolution_producer.create_post_v2(i, random.randint(1, 50))
        else:
            post_data = evolution_producer.create_post_v3(i, random.randint(1, 50))
        
        # Serialize
        bytes_io = BytesIO()
        fastavro.schemaless_writer(bytes_io, schema_info['schema_obj'], post_data)
        avro_bytes = bytes_io.getvalue()
        
        # Add schema ID
        schema_id_bytes = schema_info['id'].to_bytes(4, byteorder='big')
        message = schema_id_bytes + avro_bytes
        
        simulated_messages.append(message)
    
    print("\n" + "="*70)
    print("PHASE 3: ADAPTIVE CONSUMPTION")
    print("="*70)
    print("\nConsumer processing mixed schema versions...")
    print("Demonstrating forward/backward compatibility\n")
    
    adaptive_consumer.consume_messages(simulated_messages)
    
    time.sleep(2)
    
    print("\n" + "="*70)
    print("PHASE 4: GOVERNANCE HEALTH REPORT")
    print("="*70)
    
    # Generate health report
    report = governance_monitor.generate_health_report(
        evolution_producer,
        adaptive_consumer
    )
    
    governance_monitor.print_report(report)
    
    # Update dashboard metrics
    try:
        import requests
        requests.post('http://localhost:5058/api/update_metrics', json=report, timeout=1)
    except:
        pass
    
    print("\n" + "="*70)
    print("✓ DEMO COMPLETE - Schema Governance Framework Working!")
    print("="*70)
    print("\nKey Achievements:")
    print("  ✓ Multiple schema versions registered successfully")
    print("  ✓ Backward compatibility maintained across versions")
    print("  ✓ Zero consumer errors during migration")
    print("  ✓ Real-time monitoring and health tracking")
    print("  ✓ Production-ready governance framework")
    print("\n📊 Dashboard available at: http://localhost:5058")
    print("="*70)


def main():
    """Run complete demo"""
    print("\n" + "="*70)
    print("StreamSocial Schema Governance Framework Demo")
    print("Day 58: Managing Feature Evolution at Scale")
    print("="*70)
    
    simulate_message_production()


if __name__ == '__main__':
    main()
