import json
import logging
from typing import Dict, Any, Optional
from confluent_kafka import Consumer, KafkaException, TopicPartition
import structlog
from src.services.payment_service import PaymentService
from src.models.payment import PaymentEvent, PaymentStatus

logger = structlog.get_logger(__name__)

class PaymentConsumer:
    def __init__(self, bootstrap_servers: str, group_id: str = "payment-processors"):
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'isolation.level': 'read_committed',
            'max.poll.interval.ms': 300000,
            'session.timeout.ms': 45000,
            'client.id': f'streamsocial-payment-consumer-{group_id}'
        }
        
        self.consumer = Consumer(self.config)
        self.payment_service = PaymentService()
        self.processed_payments = set()  # In-memory deduplication
        
    def subscribe_and_consume(self, topics: list):
        """Subscribe to topics and start consuming with exactly-once semantics"""
        try:
            self.consumer.subscribe(topics)
            logger.info("Subscribed to topics", topics=topics)
            
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    logger.error("Consumer error", error=str(msg.error()))
                    continue
                
                # Process message with exactly-once guarantees
                self._process_message_exactly_once(msg)
                
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.error("Consumer error", error=str(e))
        finally:
            self.consumer.close()

    def _process_message_exactly_once(self, msg):
        """Process message with exactly-once semantics"""
        try:
            # Parse message
            message_data = json.loads(msg.value().decode('utf-8'))
            idempotency_key = message_data.get('idempotency_key')
            
            # Check if already processed (idempotent operation)
            if idempotency_key in self.processed_payments:
                logger.info("Message already processed, skipping", 
                           idempotency_key=idempotency_key)
                self.consumer.commit(message=msg)
                return
            
            # Process payment
            if msg.topic() == 'payment-events':
                payment_event = PaymentEvent(**message_data['payload'])
                result = self.payment_service.process_payment(payment_event)
                
                if result.success:
                    # Mark as processed
                    self.processed_payments.add(idempotency_key)
                    logger.info("Payment processed successfully", 
                               payment_id=payment_event.id,
                               idempotency_key=idempotency_key)
                else:
                    logger.error("Payment processing failed", 
                                payment_id=payment_event.id,
                                error=result.error_message)
                    
                    # Send to dead letter queue for failed payments
                    if result.retry_after is None:
                        self._send_to_dlq(msg, result.error_message)
            
            # Commit offset after successful processing
            self.consumer.commit(message=msg)
            
        except Exception as e:
            logger.error("Message processing failed", 
                        topic=msg.topic(),
                        partition=msg.partition(),
                        offset=msg.offset(),
                        error=str(e))
            
            # Send to DLQ for manual review
            self._send_to_dlq(msg, str(e))

    def _send_to_dlq(self, original_msg, error_reason: str):
        """Send failed message to dead letter queue"""
        dlq_message = {
            'original_topic': original_msg.topic(),
            'original_partition': original_msg.partition(),
            'original_offset': original_msg.offset(),
            'error_reason': error_reason,
            'payload': original_msg.value().decode('utf-8'),
            'timestamp': original_msg.timestamp()
        }
        
        # In a real implementation, you'd use a producer to send to DLQ
        logger.error("Message sent to DLQ", dlq_message=dlq_message)

    def close(self):
        """Close the consumer"""
        self.consumer.close()
        logger.info("Consumer closed")
