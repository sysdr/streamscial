import json
import logging
import time
import threading
import httpx
from typing import Dict, Callable, Optional
from kafka import KafkaConsumer
from error_handling.error_classifier import ErrorClassifier, ErrorType, ErrorContext
from error_handling.dlq.dlq_manager import DLQManager, DLQMessage

class RobustStreamSocialConsumer:
    def __init__(self, topics: list, bootstrap_servers: str, 
                 group_id: str, message_processor: Callable):
        self.topics = topics
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.message_processor = message_processor
        
        self.logger = logging.getLogger(__name__)
        self.error_classifier = ErrorClassifier()
        self.dlq_manager = DLQManager(bootstrap_servers)
        
        # Error tracking
        self.error_counts = {'skip': 0, 'retry': 0, 'dlq': 0, 'fatal': 0}
        self.processed_count = 0
        
        # Metrics reporting
        self.dashboard_url = "http://localhost:8000/metrics"
        self.metrics_thread = None
        self.stop_metrics = False
        
        # Consumer configuration
        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=self._safe_deserializer,
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            enable_auto_commit=False,
            max_poll_records=100,
            auto_offset_reset='earliest'
        )
        
    def _safe_deserializer(self, message_bytes):
        """Safe deserializer that handles malformed JSON"""
        if message_bytes is None:
            return None
            
        try:
            return json.loads(message_bytes.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            # Return raw bytes for error handling
            return {'_error': str(e), '_raw': message_bytes}
    
    def start_consuming(self):
        """Start the robust consumer with error handling"""
        self.logger.info(f"Starting robust consumer for topics: {self.topics}")
        
        # Start metrics reporting thread
        self._start_metrics_reporting()
        
        try:
            for message in self.consumer:
                try:
                    self._process_message_safely(message)
                    self.processed_count += 1
                    
                    # Commit after successful processing
                    self.consumer.commit()
                    
                except Exception as e:
                    self.logger.error(f"Unexpected error in consumer loop: {e}")
                    time.sleep(1)  # Brief pause before continuing
                    
        except KeyboardInterrupt:
            self.logger.info("Consumer stopped by user")
        finally:
            self.stop_metrics = True
            if self.metrics_thread:
                self.metrics_thread.join(timeout=2)
            self.consumer.close()
            
    def _process_message_safely(self, message):
        """Process message with comprehensive error handling"""
        context = {
            'topic': message.topic,
            'partition': message.partition,
            'offset': message.offset,
            'timestamp': message.timestamp,
            'key': message.key,
            'attempt_count': 0
        }
        
        try:
            # Check if deserialization failed
            if isinstance(message.value, dict) and '_error' in message.value:
                raise json.JSONDecodeError(
                    message.value['_error'], 
                    message.value['_raw'], 
                    0
                )
            
            # Process the message
            self.message_processor(message.value, context)
            
        except Exception as e:
            self._handle_processing_error(e, message, context)
    
    def _handle_processing_error(self, error: Exception, message, context: Dict):
        """Handle processing errors with classification and routing"""
        
        # Classify the error
        error_type = self.error_classifier.classify_error(error, context)
        self.error_counts[error_type.value] += 1
        
        self.logger.warning(
            f"Processing error: {error_type.value} - {str(error)} "
            f"(Topic: {context['topic']}, Offset: {context['offset']})"
        )
        
        if error_type == ErrorType.SKIP:
            self._skip_message(error, context)
            
        elif error_type == ErrorType.RETRY:
            self._retry_message(error, message, context)
            
        elif error_type == ErrorType.DLQ:
            self._send_to_dlq(error, message, context)
            
        elif error_type == ErrorType.FATAL:
            self._handle_fatal_error(error, context)
            
    def _skip_message(self, error: Exception, context: Dict):
        """Skip message and log for monitoring"""
        self.logger.info(f"Skipping message: {context['key']} - {str(error)}")
        
    def _retry_message(self, error: Exception, message, context: Dict):
        """Implement retry logic with exponential backoff"""
        max_retries = 3
        base_delay = 1  # seconds
        
        for attempt in range(max_retries):
            try:
                delay = base_delay * (2 ** attempt)
                time.sleep(delay)
                
                # Update context
                context['attempt_count'] = attempt + 1
                
                # Retry processing
                self.message_processor(message.value, context)
                return  # Success
                
            except Exception as retry_error:
                if attempt == max_retries - 1:
                    # Final attempt failed - send to DLQ
                    self.logger.error(f"All retry attempts failed: {retry_error}")
                    self._send_to_dlq(retry_error, message, context)
                else:
                    self.logger.warning(f"Retry {attempt + 1} failed: {retry_error}")
    
    def _send_to_dlq(self, error: Exception, message, context: Dict):
        """Send failed message to dead letter queue"""
        dlq_message = DLQMessage(
            original_topic=context['topic'],
            partition=context['partition'],
            offset=context['offset'],
            key=context['key'] or '',
            value=message.value if isinstance(message.value, bytes) else 
                   json.dumps(message.value).encode('utf-8'),
            timestamp=context['timestamp'],
            error_type=type(error).__name__,
            error_message=str(error),
            attempt_count=context.get('attempt_count', 0),
            headers={}
        )
        
        self.dlq_manager.send_to_dlq(dlq_message)
        
    def _handle_fatal_error(self, error: Exception, context: Dict):
        """Handle fatal errors that require stopping the consumer"""
        self.logger.critical(f"Fatal error encountered: {error}")
        raise error
    
    def get_metrics(self) -> Dict:
        """Get consumer metrics for monitoring"""
        total_errors = sum(self.error_counts.values())
        total_processed = self.processed_count + total_errors
        
        return {
            'processed_count': self.processed_count,
            'error_counts': self.error_counts.copy(),
            'success_rate': self.processed_count / max(total_processed, 1),
            'error_rate': total_errors / max(total_processed, 1),
            'dlq_depth': 0  # Placeholder - could be enhanced to track actual DLQ depth
        }
    
    def _start_metrics_reporting(self):
        """Start background thread to report metrics to dashboard"""
        self.metrics_thread = threading.Thread(target=self._report_metrics_loop, daemon=True)
        self.metrics_thread.start()
        self.logger.info("Started metrics reporting to dashboard")
    
    def _report_metrics_loop(self):
        """Background loop to report metrics every 2 seconds"""
        while not self.stop_metrics:
            try:
                metrics = self.get_metrics()
                with httpx.Client(timeout=5) as client:
                    response = client.post(self.dashboard_url, json=metrics)
                    if response.status_code == 200:
                        self.logger.debug("Metrics reported to dashboard successfully")
                    else:
                        self.logger.warning(f"Failed to report metrics: {response.status_code}")
            except Exception as e:
                self.logger.warning(f"Error reporting metrics: {e}")
            
            time.sleep(2)  # Report every 2 seconds
