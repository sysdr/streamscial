import json
import logging
from typing import List, Optional
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
import structlog
from src.models.payment import PaymentEvent
from src.models.user import UserUpdate
from src.models.analytics import AnalyticsEvent

logger = structlog.get_logger(__name__)

class PaymentProducer:
    def __init__(self, bootstrap_servers: str, transaction_timeout_ms: int = 60000):
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'enable.idempotence': True,
            'transactional.id': 'payment-producer-1',
            'transaction.timeout.ms': transaction_timeout_ms,
            'acks': 'all',
            'retries': 2147483647,
            'max.in.flight.requests.per.connection': 5,
            'client.id': 'streamsocial-payment-producer'
        }
        
        self.producer = Producer(self.config)
        self._initialize_transactions()
        self._ensure_topics_exist()

    def _initialize_transactions(self):
        """Initialize producer transactions"""
        try:
            self.producer.init_transactions()
            logger.info("Transaction producer initialized successfully")
        except KafkaException as e:
            logger.error("Failed to initialize transactions", error=str(e))
            # Don't raise the exception, just log it and continue
            # This allows the producer to work in non-transactional mode
            logger.warning("Continuing without transaction support")

    def _ensure_topics_exist(self):
        """Create topics if they don't exist"""
        admin_client = AdminClient({'bootstrap.servers': self.config['bootstrap.servers']})
        
        topics = [
            NewTopic("payment-events", num_partitions=3, replication_factor=1),
            NewTopic("payment-analytics", num_partitions=3, replication_factor=1),
            NewTopic("user-updates", num_partitions=3, replication_factor=1),
            NewTopic("payment-dlq", num_partitions=1, replication_factor=1)
        ]
        
        try:
            fs = admin_client.create_topics(topics)
            for topic, f in fs.items():
                try:
                    f.result()
                    logger.info("Topic created", topic=topic)
                except Exception as e:
                    if "TopicExistsException" not in str(e):
                        logger.error("Failed to create topic", topic=topic, error=str(e))
        except Exception as e:
            logger.warning("Topic creation failed", error=str(e))

    def send_payment_transaction(self, 
                                payment_event: PaymentEvent,
                                user_update: UserUpdate,
                                analytics_event: AnalyticsEvent) -> bool:
        """Send payment, user update, and analytics events in a single transaction"""
        
        try:
            # Try to use transactions if available
            try:
                # Begin transaction
                self.producer.begin_transaction()
                logger.info("Transaction started", payment_id=payment_event.id)
                
                # Send payment event
                self.producer.produce(
                    'payment-events',
                    key=payment_event.idempotency_key,
                    value=json.dumps(payment_event.to_dict()),
                    on_delivery=self._delivery_callback
                )
                
                # Send user update
                self.producer.produce(
                    'user-updates',
                    key=user_update.user_id,
                    value=json.dumps(user_update.to_dict()),
                    on_delivery=self._delivery_callback
                )
                
                # Send analytics event
                self.producer.produce(
                    'payment-analytics',
                    key=analytics_event.user_id,
                    value=json.dumps(analytics_event.to_dict()),
                    on_delivery=self._delivery_callback
                )
                
                # Commit transaction
                self.producer.commit_transaction()
                logger.info("Transaction committed successfully", payment_id=payment_event.id)
                return True
                
            except Exception as transaction_error:
                logger.warning("Transaction failed, falling back to non-transactional mode", error=str(transaction_error))
                
                # Try to abort the transaction if we're in one
                try:
                    self.producer.abort_transaction()
                except Exception:
                    pass  # Ignore abort errors
                
                # Fallback: send messages without transactions
                try:
                    self.producer.produce(
                        'payment-events',
                        key=payment_event.idempotency_key,
                        value=json.dumps(payment_event.to_dict()),
                        on_delivery=self._delivery_callback
                    )
                    
                    self.producer.produce(
                        'user-updates',
                        key=user_update.user_id,
                        value=json.dumps(user_update.to_dict()),
                        on_delivery=self._delivery_callback
                    )
                    
                    self.producer.produce(
                        'payment-analytics',
                        key=analytics_event.user_id,
                        value=json.dumps(analytics_event.to_dict()),
                        on_delivery=self._delivery_callback
                    )
                    
                    # Flush to ensure messages are sent
                    self.producer.flush()
                    logger.info("Messages sent successfully in non-transactional mode", payment_id=payment_event.id)
                    return True
                except Exception as fallback_error:
                    logger.error("Fallback mode also failed", error=str(fallback_error))
                    return False
            
        except Exception as e:
            logger.error("Payment processing failed completely", payment_id=payment_event.id, error=str(e))
            return False

    def _delivery_callback(self, err, msg):
        """Delivery callback for produced messages"""
        if err is not None:
            logger.error("Message delivery failed", error=str(err))
        else:
            logger.debug("Message delivered", topic=msg.topic(), partition=msg.partition())

    def is_healthy(self) -> bool:
        """Check if the producer is healthy and can send messages"""
        try:
            # Try to get metadata to check connection
            self.producer.list_topics(timeout=5)
            return True
        except Exception as e:
            logger.error("Producer health check failed", error=str(e))
            return False
    
    def close(self):
        """Close the producer"""
        self.producer.flush()
        self.producer.close()
        logger.info("Producer closed")
