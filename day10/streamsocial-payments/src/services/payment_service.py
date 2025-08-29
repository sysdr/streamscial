import time
import uuid
from typing import Optional
import structlog
from src.models.payment import PaymentEvent, PaymentResult, PaymentStatus
from src.services.idempotency_service import IdempotencyService

logger = structlog.get_logger(__name__)

class PaymentService:
    def __init__(self):
        self.idempotency_service = IdempotencyService()
        self.external_payment_gateway = MockPaymentGateway()

    def process_payment(self, payment_event: PaymentEvent) -> PaymentResult:
        """Process payment with exactly-once guarantees"""
        start_time = time.time()
        
        try:
            # Check idempotency
            if self.idempotency_service.is_processed(payment_event.idempotency_key):
                existing_result = self.idempotency_service.get_result(payment_event.idempotency_key)
                logger.info("Payment already processed", 
                           payment_id=payment_event.id,
                           idempotency_key=payment_event.idempotency_key)
                return existing_result
            
            # Update status to processing
            payment_event.status = PaymentStatus.PROCESSING
            
            # Process with external gateway
            gateway_result = self.external_payment_gateway.charge(
                amount=payment_event.amount,
                currency=payment_event.currency,
                payment_method=payment_event.payment_method,
                idempotency_key=payment_event.idempotency_key
            )
            
            # Create result
            if gateway_result['success']:
                payment_event.status = PaymentStatus.COMPLETED
                payment_event.external_payment_id = gateway_result['transaction_id']
                
                result = PaymentResult(
                    success=True,
                    payment_id=payment_event.id,
                    external_id=gateway_result['transaction_id']
                )
            else:
                payment_event.status = PaymentStatus.FAILED
                result = PaymentResult(
                    success=False,
                    payment_id=payment_event.id,
                    error_message=gateway_result['error'],
                    retry_after=gateway_result.get('retry_after')
                )
            
            # Store result for idempotency
            self.idempotency_service.store_result(payment_event.idempotency_key, result)
            
            processing_time = (time.time() - start_time) * 1000
            logger.info("Payment processing completed",
                       payment_id=payment_event.id,
                       success=result.success,
                       processing_time_ms=processing_time)
            
            return result
            
        except Exception as e:
            logger.error("Payment processing failed", 
                        payment_id=payment_event.id,
                        error=str(e))
            
            result = PaymentResult(
                success=False,
                payment_id=payment_event.id,
                error_message=str(e)
            )
            
            return result

class MockPaymentGateway:
    """Mock external payment gateway with idempotency support"""
    
    def __init__(self):
        self.processed_payments = {}
    
    def charge(self, amount: float, currency: str, payment_method: str, idempotency_key: str) -> dict:
        """Simulate payment processing with idempotency"""
        
        # Check if already processed
        if idempotency_key in self.processed_payments:
            return self.processed_payments[idempotency_key]
        
        # Simulate processing delay
        time.sleep(0.1)
        
        # Simulate occasional failures (10% failure rate)
        import random
        if random.random() < 0.1:
            result = {
                'success': False,
                'error': 'Insufficient funds',
                'retry_after': 300  # Retry after 5 minutes
            }
        else:
            result = {
                'success': True,
                'transaction_id': str(uuid.uuid4()),
                'amount': amount,
                'currency': currency
            }
        
        # Store for idempotency
        self.processed_payments[idempotency_key] = result
        return result
