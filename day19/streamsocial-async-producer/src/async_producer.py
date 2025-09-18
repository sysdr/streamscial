import json
import time
import uuid
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaProducer
from kafka.errors import KafkaError, RequestTimedOutError, NotLeaderForPartitionError, LeaderNotAvailableError
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ContentModerationProducer:
    def __init__(self, config):
        self.config = config
        self.producer = self._create_producer()
        self.in_flight_messages = {}
        self.metrics = {
            'sent': 0,
            'delivered': 0,
            'failed': 0,
            'retried': 0,
            'in_flight': 0
        }
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            timeout=30,
            expected_exception=KafkaError
        )
        self._metrics_lock = threading.Lock()
        
    def _create_producer(self):
        return KafkaProducer(
            bootstrap_servers=self.config['kafka']['servers'],
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',  # Enable idempotence by using acks='all'
            retries=3,
            max_in_flight_requests_per_connection=5,
            compression_type='gzip',
            batch_size=32768,
            linger_ms=10,
            buffer_memory=67108864
        )
    
    def send_for_moderation(self, user_id, content, post_id=None):
        """Send content for asynchronous moderation"""
        message_id = str(uuid.uuid4())
        post_id = post_id or str(uuid.uuid4())
        
        message = {
            'message_id': message_id,
            'post_id': post_id,
            'user_id': user_id,
            'content': content,
            'timestamp': datetime.utcnow().isoformat(),
            'priority': self._calculate_priority(user_id, content)
        }
        
        # Track in-flight message
        with self._metrics_lock:
            self.in_flight_messages[message_id] = {
                'message': message,
                'timestamp': time.time(),
                'retry_count': 0
            }
            self.metrics['in_flight'] += 1
            self.metrics['sent'] += 1
        
        try:
            # Send with custom callback
            future = self.producer.send(
                'content-moderation',
                key=user_id,
                value=message,
                headers=[('message_type', b'moderation_request')]
            )
            
            # Add callback
            future.add_callback(self._on_send_success, message_id)
            future.add_errback(self._on_send_error, message_id)
            
            logger.info(f"Queued moderation request: {message_id}")
            return {'status': 'queued', 'message_id': message_id}
            
        except Exception as e:
            logger.error(f"Failed to queue message {message_id}: {e}")
            self._cleanup_in_flight(message_id)
            return {'status': 'failed', 'error': str(e)}
    
    def _on_send_success(self, record_metadata, message_id):
        """Callback for successful message delivery"""
        try:
            with self._metrics_lock:
                if message_id in self.in_flight_messages:
                    message_data = self.in_flight_messages[message_id]
                    del self.in_flight_messages[message_id]
                    self.metrics['delivered'] += 1
                    self.metrics['in_flight'] -= 1
                    
                    # Calculate delivery time
                    delivery_time = time.time() - message_data['timestamp']
                    
                    logger.info(
                        f"Message delivered: {message_id} "
                        f"to {record_metadata.topic}:{record_metadata.partition} "
                        f"offset {record_metadata.offset} "
                        f"in {delivery_time:.3f}s"
                    )
                    
                    # Trigger downstream actions
                    self._trigger_timeline_update(message_data['message'])
                    self._update_user_metrics(message_data['message']['user_id'])
                    
        except Exception as e:
            logger.error(f"Error in success callback: {e}")
    
    def _on_send_error(self, exception, message_id):
        """Callback for failed message delivery"""
        try:
            with self._metrics_lock:
                if message_id in self.in_flight_messages:
                    message_data = self.in_flight_messages[message_id]
                    
                    # Classify error
                    if self._is_retriable_error(exception):
                        retry_count = message_data['retry_count']
                        if retry_count < 3:
                            logger.warning(f"Retriable error for {message_id}, retry {retry_count + 1}: {exception}")
                            self._schedule_retry(message_id, exception)
                            return
                    
                    # Permanent failure
                    del self.in_flight_messages[message_id]
                    self.metrics['failed'] += 1
                    self.metrics['in_flight'] -= 1
                    
                    logger.error(f"Permanent failure for {message_id}: {exception}")
                    self._send_to_dead_letter_queue(message_data['message'], str(exception))
                    
        except Exception as e:
            logger.error(f"Error in failure callback: {e}")
    
    def _is_retriable_error(self, exception):
        """Determine if error is worth retrying"""
        retriable_errors = (
            RequestTimedOutError,
            NotLeaderForPartitionError,
            LeaderNotAvailableError
        )
        return isinstance(exception, retriable_errors)
    
    def _schedule_retry(self, message_id, exception):
        """Schedule message retry with exponential backoff"""
        def retry_send():
            try:
                time.sleep(2 ** self.in_flight_messages[message_id]['retry_count'])
                
                with self._metrics_lock:
                    if message_id in self.in_flight_messages:
                        message_data = self.in_flight_messages[message_id]
                        message_data['retry_count'] += 1
                        self.metrics['retried'] += 1
                        
                        # Retry send
                        future = self.producer.send(
                            'content-moderation',
                            key=message_data['message']['user_id'],
                            value=message_data['message']
                        )
                        future.add_callback(self._on_send_success, message_id)
                        future.add_errback(self._on_send_error, message_id)
                        
            except Exception as e:
                logger.error(f"Retry failed for {message_id}: {e}")
                self._cleanup_in_flight(message_id)
        
        threading.Thread(target=retry_send).start()
    
    def _calculate_priority(self, user_id, content):
        """Calculate content priority for moderation queue"""
        priority = 'normal'
        
        # VIP users get high priority
        if self._is_vip_user(user_id):
            priority = 'high'
        
        # Content with potential issues gets high priority
        risk_keywords = ['spam', 'abuse', 'hate', 'violence']
        if any(keyword in content.lower() for keyword in risk_keywords):
            priority = 'high'
        
        return priority
    
    def _is_vip_user(self, user_id):
        """Check if user is VIP (simplified)"""
        # In real system, check user tier from database
        return user_id.startswith('vip_')
    
    def _trigger_timeline_update(self, message):
        """Trigger timeline update for approved content"""
        logger.info(f"Triggering timeline update for post {message['post_id']}")
        # In real system, send to timeline-update topic
    
    def _update_user_metrics(self, user_id):
        """Update user engagement metrics"""
        logger.info(f"Updating metrics for user {user_id}")
        # In real system, update user stats
    
    def _send_to_dead_letter_queue(self, message, error):
        """Send failed message to dead letter queue"""
        dlq_message = {
            'original_message': message,
            'error': error,
            'failed_at': datetime.utcnow().isoformat(),
            'attempts': 3
        }
        
        try:
            self.producer.send('content-moderation-dlq', value=dlq_message)
            logger.info(f"Sent to DLQ: {message['message_id']}")
        except Exception as e:
            logger.error(f"Failed to send to DLQ: {e}")
    
    def _cleanup_in_flight(self, message_id):
        """Clean up in-flight message tracking"""
        with self._metrics_lock:
            if message_id in self.in_flight_messages:
                del self.in_flight_messages[message_id]
                self.metrics['in_flight'] -= 1
    
    def get_metrics(self):
        """Get current producer metrics"""
        with self._metrics_lock:
            return self.metrics.copy()
    
    def flush_and_close(self):
        """Flush pending messages and close producer"""
        self.producer.flush(timeout=30)
        self.producer.close(timeout=10)

class CircuitBreaker:
    def __init__(self, failure_threshold, timeout, expected_exception):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.expected_exception = expected_exception
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
        
    def is_open(self):
        if self.state == 'OPEN':
            if time.time() - self.last_failure_time > self.timeout:
                self.state = 'HALF_OPEN'
                return False
            return True
        return False
    
    def record_success(self):
        self.failure_count = 0
        self.state = 'CLOSED'
        
    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = 'OPEN'
