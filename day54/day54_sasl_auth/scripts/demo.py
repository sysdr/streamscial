"""Demo script showing SASL authentication in action."""

import time
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / 'src'))

from credential_manager import CredentialManager
from auth_monitor import AuthenticationMonitor
from sasl_producer import SALSProducer
from sasl_consumer import SALSConsumer


def main():
    print("\n" + "="*80)
    print("🔐 StreamSocial SASL Authentication Demo")
    print("="*80 + "\n")
    
    # Initialize systems
    print("📋 Phase 1: Setting up credential management...")
    mgr = CredentialManager()
    monitor = AuthenticationMonitor()
    
    # Create users
    print("\n👥 Creating SASL users...")
    users = [
        ("producer_service", "prod_secret_2024", "SCRAM-SHA-512"),
        ("analytics_service", "analytics_secret_2024", "SCRAM-SHA-512"),
        ("legacy_partner", "partner_secret_2024", "PLAIN"),
        ("consumer_service", "consumer_secret_2024", "PLAIN")
    ]
    
    for username, password, mechanism in users:
        try:
            mgr.create_user(username, password, mechanism)
            print(f"   ✅ Created {username} with {mechanism}")
        except ValueError:
            print(f"   ⚠️ User {username} already exists")
    
    # Show user list
    print("\n📊 Registered users:")
    for user in mgr.get_all_users():
        print(f"   • {user['username']:20s} {user['mechanism']:15s} {'Enabled' if user['enabled'] else 'Disabled'}")
    
    # Test authentication
    print("\n🔒 Phase 2: Testing authentication...")
    test_cases = [
        ("producer_service", "prod_secret_2024", True),
        ("producer_service", "wrong_password", False),
        ("analytics_service", "analytics_secret_2024", True),
        ("legacy_partner", "partner_secret_2024", True),
        ("nonexistent_user", "password", False)
    ]
    
    for username, password, should_succeed in test_cases:
        start = time.time()
        result = mgr.validate_credentials(username, password)
        latency = (time.time() - start) * 1000
        
        mechanism = "SCRAM-SHA-512" if username in ["producer_service", "analytics_service"] else "PLAIN"
        monitor.record_auth_attempt(username, mechanism, result, latency)
        
        status = "✅ SUCCESS" if result else "❌ FAILED"
        print(f"   {status} - {username} ({latency:.2f}ms)")
    
    # Show authentication stats
    print("\n📈 Authentication Statistics:")
    stats = monitor.get_stats()
    print(f"   Total attempts: {stats['total_attempts']}")
    print(f"   Successful: {stats['successful_auths']}")
    print(f"   Failed: {stats['failed_auths']}")
    print(f"   Success rate: {stats['success_rate']}%")
    print(f"   SCRAM auths: {stats['scram_auths']}")
    print(f"   PLAIN auths: {stats['plain_auths']}")
    print(f"   Avg latency: {stats['avg_latency_ms']}ms")
    
    # Test Kafka producers/consumers
    print("\n📤 Phase 3: Testing Kafka producers with SASL...")
    try:
        # SCRAM producer
        print("\n   Creating SCRAM-SHA-512 producer...")
        producer_scram = SALSProducer(
            'localhost:9092',
            'producer_service',
            'prod_secret_2024',
            'SCRAM-SHA-512'
        )
        
        producer_scram.produce_batch('streamsocial-posts', count=50)
        scram_stats = producer_scram.get_stats()
        print(f"   ✅ SCRAM Producer: {scram_stats['messages_sent']} messages sent")
        
        producer_scram.close()
        
    except Exception as e:
        print(f"   ⚠️ Kafka producer test skipped: {e}")
    
    print("\n📥 Phase 4: Testing Kafka consumers with SASL...")
    try:
        # PLAIN consumer
        print("\n   Creating PLAIN consumer...")
        consumer_plain = SALSConsumer(
            'localhost:9092',
            'demo-consumer-group',
            'consumer_service',
            'consumer_secret_2024',
            'PLAIN'
        )
        
        consumer_plain.subscribe(['streamsocial-posts'])
        
        def process_message(msg):
            print(f"   📨 Received: {msg['event_type']} from {msg['user_id'][:12]}")
        
        consumer_plain.consume(callback=process_message, duration_seconds=10)
        plain_stats = consumer_plain.get_stats()
        print(f"   ✅ PLAIN Consumer: {plain_stats['messages_consumed']} messages consumed")
        
        consumer_plain.close()
        
    except Exception as e:
        print(f"   ⚠️ Kafka consumer test skipped: {e}")
    
    # Test credential rotation
    print("\n🔄 Phase 5: Testing credential rotation...")
    rotation_user = "producer_service"
    print(f"   Rotating password for {rotation_user}...")
    mgr.rotate_password(rotation_user, "new_prod_secret_2024")
    print(f"   ✅ Password rotated successfully")
    
    # Verify old password fails
    old_result = mgr.validate_credentials(rotation_user, "prod_secret_2024")
    print(f"   Old password: {'❌ Rejected (correct)' if not old_result else '⚠️ Still valid (error)'}")
    
    # Verify new password works
    new_result = mgr.validate_credentials(rotation_user, "new_prod_secret_2024")
    print(f"   New password: {'✅ Accepted (correct)' if new_result else '❌ Rejected (error)'}")
    
    # Show users needing rotation
    print("\n⏰ Users needing password rotation:")
    rotation_needed = mgr.get_users_needing_rotation()
    if rotation_needed:
        for user in rotation_needed:
            print(f"   ⚠️ {user['username']} - {user['days_overdue']} days overdue")
    else:
        print("   ✅ All passwords are current")
    
    # Show high failure rate users
    print("\n⚠️ Users with high authentication failure rates:")
    failure_rates = monitor.get_failure_rate_by_user()
    if failure_rates:
        for rate in failure_rates:
            print(f"   🚨 {rate['username']}: {rate['failure_rate']}% failure rate ({rate['failures']}/{rate['total_attempts']} attempts)")
    else:
        print("   ✅ No users with high failure rates")
    
    # Show audit log
    print("\n📝 Recent audit log entries:")
    audit_log = mgr.get_audit_log(limit=10)
    for entry in audit_log[-5:]:
        print(f"   [{entry['timestamp']}] {entry['action']:15s} - {entry['username']}")
    
    print("\n" + "="*80)
    print("✅ Demo completed successfully!")
    print("="*80)
    print("\n💡 Next steps:")
    print("   1. Open http://localhost:8054 to view the monitoring dashboard")
    print("   2. Run pytest tests/ to execute test suite")
    print("   3. Check audit logs for security analysis")
    print("\n")


if __name__ == '__main__':
    main()
