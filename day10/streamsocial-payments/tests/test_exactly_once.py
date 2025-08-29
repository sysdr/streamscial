import pytest
import asyncio
import json
import uuid
from unittest.mock import Mock, patch
from src.models.payment import PaymentEvent, PaymentStatus
from src.models.user import UserUpdate
from src.models.analytics import AnalyticsEvent
from src.producers.payment_producer import PaymentProducer
from src.consumers.payment_consumer import PaymentConsumer
from src.services.payment_service import PaymentService

class TestExactlyOnceDelivery:
    
    def test_payment_event_creation(self):
        """Test payment event model creation"""
        payment = PaymentEvent(
            user_id="user123",
            amount=9.99,
            payment_method="credit_card",
            subscription_type="premium",
            idempotency_key="unique-key-123"
        )
        
        assert payment.user_id == "user123"
        assert payment.amount == 9.99
        assert payment.status == PaymentStatus.INITIATED
        assert payment.idempotency_key == "unique-key-123"
    
    def test_payment_event_kafka_message(self):
        """Test payment event to Kafka message conversion"""
        payment = PaymentEvent(
            user_id="user123",
            amount=9.99,
            payment_method="credit_card",
            idempotency_key="unique-key-123"
        )
        
        message = payment.to_kafka_message()
        
        assert message["event_type"] == "payment"
        assert message["idempotency_key"] == "unique-key-123"
        assert "payload" in message
        assert "timestamp" in message
    
    def test_idempotent_payment_processing(self):
        """Test that duplicate payments are handled idempotently"""
        payment_service = PaymentService()
        
        # Use a unique idempotency key for this test to avoid conflicts
        unique_key = f"test-unique-key-{uuid.uuid4()}"
        
        payment = PaymentEvent(
            user_id="user123",
            amount=9.99,
            payment_method="credit_card",
            idempotency_key=unique_key
        )
        
        # Mock the Redis connection to avoid external dependencies
        with patch.object(payment_service.idempotency_service, 'redis_client') as mock_redis:
            # Mock Redis to return False for is_processed (no existing payment)
            mock_redis.exists.return_value = 0
            mock_redis.setex.return_value = True
            
            # Mock the external gateway to track calls
            with patch.object(payment_service.external_payment_gateway, 'charge') as mock_charge:
                mock_charge.return_value = {
                    'success': True,
                    'transaction_id': 'txn_123',
                    'amount': 9.99,
                    'currency': 'USD'
                }
                
                # First processing
                result1 = payment_service.process_payment(payment)
                
                # Mock Redis to return True for is_processed (payment already exists)
                mock_redis.exists.return_value = 1
                mock_redis.get.return_value = result1.model_dump_json()
                
                # Second processing with same idempotency key
                result2 = payment_service.process_payment(payment)
                
                # Should return same result without calling gateway twice
                assert result1.success == result2.success
                assert result1.payment_id == result2.payment_id
                assert mock_charge.call_count == 1  # Called only once
    
    def test_payment_failure_handling(self):
        """Test payment failure scenarios"""
        payment_service = PaymentService()
        
        payment = PaymentEvent(
            user_id="user123",
            amount=9999999.99,  # Unrealistic amount to trigger failure
            payment_method="credit_card",
            idempotency_key="fail-key-123"
        )
        
        # Mock gateway to return failure
        with patch.object(payment_service.external_payment_gateway, 'charge') as mock_charge:
            mock_charge.return_value = {
                'success': False,
                'error': 'Insufficient funds',
                'retry_after': 300
            }
            
            result = payment_service.process_payment(payment)
            
            assert not result.success
            assert result.error_message == 'Insufficient funds'
            assert result.retry_after == 300

class TestKafkaTransactions:
    
    @pytest.fixture
    def mock_producer(self):
        with patch('confluent_kafka.Producer') as mock:
            producer_instance = Mock()
            mock.return_value = producer_instance
            yield producer_instance
    
    def test_transaction_success(self, mock_producer):
        """Test successful transaction processing"""
        # Configure mock producer methods to not raise exceptions
        mock_producer.begin_transaction.return_value = None
        mock_producer.commit_transaction.return_value = None
        mock_producer.produce.return_value = None
        mock_producer.flush.return_value = None
        
        # Ensure no exceptions are raised during transaction
        mock_producer.begin_transaction.side_effect = None
        mock_producer.produce.side_effect = None
        mock_producer.commit_transaction.side_effect = None
        
        # Mock the AdminClient to avoid real Kafka connections
        with patch('src.producers.payment_producer.AdminClient') as mock_admin:
            mock_admin.return_value.create_topics.return_value = {}
            
            # Mock the Producer initialization to avoid real connections
            with patch('src.producers.payment_producer.Producer') as mock_producer_class:
                mock_producer_class.return_value = mock_producer
                
                producer = PaymentProducer("localhost:9092")
                # Don't replace the producer since we're mocking the class
        
        payment = PaymentEvent(
            user_id="user123",
            amount=9.99,
            payment_method="credit_card",
            idempotency_key="txn-key-123"
        )
        
        user_update = UserUpdate(
            user_id="user123",
            subscription_type="premium"
        )
        
        analytics = AnalyticsEvent(
            event_type="payment",
            user_id="user123",
            amount=9.99,
            payment_method="credit_card",
            success=True,
            processing_time_ms=50.0
        )
        
        result = producer.send_payment_transaction(payment, user_update, analytics)
        
        assert result is True
        mock_producer.begin_transaction.assert_called_once()
        mock_producer.commit_transaction.assert_called_once()
        assert mock_producer.produce.call_count == 3  # Three messages
    
    def test_transaction_failure_rollback(self, mock_producer):
        """Test transaction rollback on failure"""
        mock_producer.produce.side_effect = Exception("Network error")
        
        producer = PaymentProducer("localhost:9092")
        producer.producer = mock_producer
        
        payment = PaymentEvent(
            user_id="user123",
            amount=9.99,
            payment_method="credit_card",
            idempotency_key="fail-txn-123"
        )
        
        user_update = UserUpdate(user_id="user123")
        analytics = AnalyticsEvent(
            event_type="payment",
            user_id="user123",
            amount=9.99,
            payment_method="credit_card",
            success=False,
            processing_time_ms=0
        )
        
        result = producer.send_payment_transaction(payment, user_update, analytics)
        
        assert result is False
        mock_producer.begin_transaction.assert_called_once()
        mock_producer.abort_transaction.assert_called_once()
        mock_producer.commit_transaction.assert_not_called()

class TestEndToEndWorkflow:
    
    def test_complete_payment_workflow(self):
        """Test complete payment processing workflow"""
        # This test would require running Kafka infrastructure
        # For now, we'll test the integration points
        
        payment = PaymentEvent(
            user_id="user123",
            amount=19.99,
            payment_method="paypal",
            subscription_type="pro",
            idempotency_key="e2e-test-123"
        )
        
        # Verify payment can be serialized/deserialized
        message = payment.to_kafka_message()
        payload_json = json.dumps(message)
        parsed_message = json.loads(payload_json)
        
        assert parsed_message["event_type"] == "payment"
        assert parsed_message["idempotency_key"] == "e2e-test-123"
        
        # Reconstruct payment from message
        reconstructed_payment = PaymentEvent(**parsed_message["payload"])
        assert reconstructed_payment.user_id == "user123"
        assert reconstructed_payment.amount == 19.99
