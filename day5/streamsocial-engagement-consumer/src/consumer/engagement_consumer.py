import json
import signal
import time
import structlog
from typing import List, Optional
from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord
from config.consumer_config import ConsumerConfig
from src.models.engagement import EngagementEvent
from src.handlers.error_handler import ErrorHandler
from src.utils.metrics import MetricsCollector
from src.utils.cache import CacheManager

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

class EngagementConsumer:
    def __init__(self):
        self.consumer: Optional[KafkaConsumer] = None
        self.error_handler = ErrorHandler(
            max_retries=ConsumerConfig.MAX_RETRIES,
            base_delay=ConsumerConfig.RETRY_BACKOFF_MS / 1000
        )
        self.metrics_collector = MetricsCollector()
        self.cache_manager = CacheManager()
        self.running = False
        self.batch_size = ConsumerConfig.BATCH_SIZE
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        
        logger.info("EngagementConsumer initialized")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info("Received shutdown signal", signal=signum)
        self.stop()
    
    def start(self):
        """Start the consumer"""
        try:
            self.consumer = KafkaConsumer(
                *ConsumerConfig.KAFKA_TOPICS,
                **ConsumerConfig.get_kafka_config()
            )
            
            self.running = True
            logger.info("Consumer started", topics=ConsumerConfig.KAFKA_TOPICS)
            
            # Main processing loop
            self._process_messages()
            
        except Exception as e:
            logger.error("Failed to start consumer", error=str(e))
            raise
    
    def stop(self):
        """Stop the consumer gracefully"""
        logger.info("Stopping consumer...")
        self.running = False
        
        if self.consumer:
            try:
                # Commit any pending offsets
                self.consumer.commit()
                self.consumer.close()
                logger.info("Consumer stopped gracefully")
            except Exception as e:
                logger.error("Error during consumer shutdown", error=str(e))
    
    def _process_messages(self):
        """Main message processing loop - Simplified to avoid offset commit issues"""
        
        while self.running:
            try:
                # Poll for messages
                message_batch = self.consumer.poll(timeout_ms=1000)
                
                if not message_batch:
                    continue
                
                # Process messages immediately from all partitions
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        if not self.running:
                            break
                        
                        try:
                            start_time = time.time()
                            success = self._process_single_message(message)
                            processing_time = time.time() - start_time
                            
                            if success:
                                # Record successful processing metrics
                                self.metrics_collector.record_message_processed(processing_time, True)
                                
                        except Exception as e:
                            logger.error("Error processing message", error=str(e))
                            # Record failed processing metrics
                            self.metrics_collector.record_message_processed(0.001, False)
                    
            except Exception as e:
                logger.error("Error in message processing loop", error=str(e))
                time.sleep(5)  # Brief pause before retrying
    

    
    def _process_single_message(self, message: ConsumerRecord) -> bool:
        """Process a single engagement message"""
        try:
            # Deserialize message
            engagement_event = EngagementEvent.from_kafka_message(message.value)
            
            # Update engagement statistics in cache
            self.cache_manager.update_engagement_stats(
                engagement_event.post_id,
                engagement_event.action_type
            )
            
            # Record metrics
            self.metrics_collector.record_engagement_event(engagement_event.action_type)
            
            logger.debug(
                "Processed engagement event",
                user_id=engagement_event.user_id,
                post_id=engagement_event.post_id,
                action_type=engagement_event.action_type
            )
            
            return True
            
        except Exception as e:
            # Handle error with retry logic
            return self.error_handler.handle_processing_error(message, e)
    
    def get_metrics(self):
        """Get consumer metrics"""
        system_metrics = self.metrics_collector.get_system_metrics()
        dead_letter_count = len(self.error_handler.get_dead_letter_messages())
        
        return {
            **system_metrics,
            'dead_letter_count': dead_letter_count,
            'consumer_running': self.running
        }

def main():
    """Main function to run the consumer"""
    consumer = EngagementConsumer()
    
    try:
        logger.info("Starting StreamSocial Engagement Consumer")
        consumer.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error("Consumer failed", error=str(e))
    finally:
        consumer.stop()

if __name__ == "__main__":
    main()
