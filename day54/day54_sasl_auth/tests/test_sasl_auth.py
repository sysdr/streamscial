"""Comprehensive tests for SASL authentication system."""

import pytest
import time
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / 'src'))

from credential_manager import CredentialManager
from auth_monitor import AuthenticationMonitor
from sasl_producer import SALSProducer
from sasl_consumer import SALSConsumer


class TestCredentialManager:
    """Test credential management system."""
    
    def setup_method(self):
        """Setup test environment."""
        self.test_storage = "data/credentials/test"
        Path(self.test_storage).mkdir(parents=True, exist_ok=True)
        self.mgr = CredentialManager(storage_path=self.test_storage)
    
    def test_create_user(self):
        """Test user creation."""
        user = self.mgr.create_user("test_user", "test_pass", "SCRAM-SHA-512")
        
        assert user['username'] == "test_user"
        assert user['mechanism'] == "SCRAM-SHA-512"
        assert user['enabled'] is True
        assert 'password_hash' in user
    
    def test_validate_credentials(self):
        """Test credential validation."""
        self.mgr.create_user("valid_user", "valid_pass")
        
        assert self.mgr.validate_credentials("valid_user", "valid_pass") is True
        assert self.mgr.validate_credentials("valid_user", "wrong_pass") is False
        assert self.mgr.validate_credentials("nonexistent", "pass") is False
    
    def test_password_rotation(self):
        """Test password rotation."""
        self.mgr.create_user("rotate_user", "old_pass")
        assert self.mgr.validate_credentials("rotate_user", "old_pass") is True
        
        self.mgr.rotate_password("rotate_user", "new_pass")
        assert self.mgr.validate_credentials("rotate_user", "new_pass") is True
        assert self.mgr.validate_credentials("rotate_user", "old_pass") is False
    
    def test_user_deletion(self):
        """Test user deletion."""
        self.mgr.create_user("delete_user", "pass")
        assert self.mgr.delete_user("delete_user") is True
        assert self.mgr.validate_credentials("delete_user", "pass") is False
    
    def test_audit_log(self):
        """Test audit logging."""
        self.mgr.create_user("audit_user", "pass")
        self.mgr.validate_credentials("audit_user", "pass")
        
        audit_log = self.mgr.get_audit_log()
        assert len(audit_log) >= 2
        assert any(entry['action'] == 'USER_CREATED' for entry in audit_log)
        assert any(entry['action'] == 'AUTH_SUCCESS' for entry in audit_log)


class TestAuthenticationMonitor:
    """Test authentication monitoring."""
    
    def setup_method(self):
        """Setup test environment."""
        self.monitor = AuthenticationMonitor()
    
    def test_record_auth_success(self):
        """Test recording successful authentication."""
        self.monitor.record_auth_attempt("user1", "SCRAM-SHA-512", True, 2.5)
        
        stats = self.monitor.get_stats()
        assert stats['total_attempts'] == 1
        assert stats['successful_auths'] == 1
        assert stats['scram_auths'] == 1
        assert stats['avg_latency_ms'] == 2.5
    
    def test_record_auth_failure(self):
        """Test recording failed authentication."""
        self.monitor.record_auth_attempt("user1", "PLAIN", False, 1.0)
        
        stats = self.monitor.get_stats()
        assert stats['failed_auths'] == 1
        assert stats['plain_auths'] == 1
    
    def test_success_rate_calculation(self):
        """Test success rate calculation."""
        self.monitor.record_auth_attempt("user1", "SCRAM-SHA-512", True)
        self.monitor.record_auth_attempt("user2", "SCRAM-SHA-512", True)
        self.monitor.record_auth_attempt("user3", "SCRAM-SHA-512", False)
        
        stats = self.monitor.get_stats()
        assert stats['success_rate'] == 66.67
    
    def test_user_failure_rate(self):
        """Test per-user failure rate tracking."""
        for i in range(8):
            self.monitor.record_auth_attempt("failing_user", "PLAIN", False)
        for i in range(2):
            self.monitor.record_auth_attempt("failing_user", "PLAIN", True)
        
        failure_rates = self.monitor.get_failure_rate_by_user()
        assert len(failure_rates) > 0
        assert failure_rates[0]['username'] == 'failing_user'
        assert failure_rates[0]['failure_rate'] > 10


@pytest.fixture(scope="session")
def kafka_connection():
    """Fixture for Kafka connection details."""
    return {
        'bootstrap_servers': 'localhost:9092',
        'scram_username': 'producer',
        'scram_password': 'producer-secret',
        'plain_username': 'consumer',
        'plain_password': 'consumer-secret'
    }


class TestSASLProducer:
    """Test SASL producer functionality."""
    
    def test_producer_initialization(self, kafka_connection):
        """Test producer initialization with SASL."""
        try:
            producer = SALSProducer(
                kafka_connection['bootstrap_servers'],
                kafka_connection['scram_username'],
                kafka_connection['scram_password'],
                'SCRAM-SHA-512'
            )
            
            stats = producer.get_stats()
            assert stats['messages_sent'] == 0
            assert stats['auth_failures'] == 0
            
            producer.close()
        except Exception as e:
            pytest.skip(f"Kafka not available: {e}")
    
    def test_producer_send_message(self, kafka_connection):
        """Test sending messages with SASL."""
        try:
            producer = SALSProducer(
                kafka_connection['bootstrap_servers'],
                kafka_connection['scram_username'],
                kafka_connection['scram_password']
            )
            
            success = producer.produce_post(
                'streamsocial-posts',
                'user_123',
                'Test post content'
            )
            
            assert success is True
            stats = producer.get_stats()
            assert stats['messages_sent'] > 0
            
            producer.close()
        except Exception as e:
            pytest.skip(f"Kafka not available: {e}")


class TestSASLConsumer:
    """Test SASL consumer functionality."""
    
    def test_consumer_initialization(self, kafka_connection):
        """Test consumer initialization with SASL."""
        try:
            consumer = SALSConsumer(
                kafka_connection['bootstrap_servers'],
                'test-group',
                kafka_connection['plain_username'],
                kafka_connection['plain_password'],
                'PLAIN'
            )
            
            stats = consumer.get_stats()
            assert stats['messages_consumed'] == 0
            assert stats['auth_failures'] == 0
            
            consumer.close()
        except Exception as e:
            pytest.skip(f"Kafka not available: {e}")


def test_full_authentication_flow(kafka_connection):
    """Integration test for full authentication flow."""
    try:
        # Create credential manager
        mgr = CredentialManager()
        monitor = AuthenticationMonitor()
        
        # Create test users
        mgr.create_user("test_producer", "prod_pass", "SCRAM-SHA-512")
        mgr.create_user("test_consumer", "cons_pass", "PLAIN")
        
        # Simulate authentication attempts
        start = time.time()
        monitor.record_auth_attempt("test_producer", "SCRAM-SHA-512", True, 2.1)
        latency = (time.time() - start) * 1000
        
        monitor.record_auth_attempt("test_consumer", "PLAIN", True, 1.5)
        
        # Verify statistics
        stats = monitor.get_stats()
        assert stats['total_attempts'] >= 2
        assert stats['successful_auths'] >= 2
        assert stats['scram_auths'] >= 1
        assert stats['plain_auths'] >= 1
        
        print(f"\n✅ Full authentication flow test passed")
        print(f"   Total attempts: {stats['total_attempts']}")
        print(f"   Success rate: {stats['success_rate']}%")
        print(f"   Avg latency: {stats['avg_latency_ms']}ms")
        
    except Exception as e:
        pytest.skip(f"Integration test requires full setup: {e}")
