import threading
import time
import json
import uuid
from typing import Dict, Optional, Callable, Any
from confluent_kafka import Producer, KafkaException, Message
from confluent_kafka.admin import AdminClient, NewTopic
import structlog
import queue
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import psutil

logger = structlog.get_logger()

@dataclass
class ProducerConfig:
    bootstrap_servers: str = "localhost:9092"
    batch_size: int = 65536  # 64KB
    linger_ms: int = 5
    compression_type: str = "lz4"
    max_in_flight_requests: int = 10
    buffer_memory: int = 67108864  # 64MB
    retries: int = 2147483647  # Max retries
    request_timeout_ms: int = 30000
    retry_backoff_ms: int = 100
    max_block_ms: int = 10000

@dataclass
class UserAction:
    user_id: str
    action_type: str  # post, like, share, comment
    content_id: str
    timestamp: int
    metadata: Dict[str, Any]

class ConnectionPool:
    def __init__(self, config: ProducerConfig, pool_size: int = 10):
        self.config = config
        self.pool_size = pool_size
        self.producers = queue.Queue(maxsize=pool_size)
        self.active_connections = 0
        self.lock = threading.Lock()
        self._initialize_pool()

    def _initialize_pool(self):
        """Initialize the connection pool with producer instances"""
        producer_config = {
            'bootstrap.servers': self.config.bootstrap_servers,
            'batch.size': self.config.batch_size,
            'linger.ms': self.config.linger_ms,
            'compression.type': self.config.compression_type,
            'max.in.flight.requests.per.connection': self.config.max_in_flight_requests,
            'retries': self.config.retries,
            'request.timeout.ms': self.config.request_timeout_ms,
            'retry.backoff.ms': self.config.retry_backoff_ms,
            'max.block.ms': self.config.max_block_ms
        }

        for _ in range(self.pool_size):
            producer = Producer(producer_config)
            self.producers.put(producer)
            self.active_connections += 1

    def get_producer(self) -> Producer:
        """Get a producer from the pool"""
        return self.producers.get()

    def return_producer(self, producer: Producer):
        """Return a producer to the pool"""
        self.producers.put(producer)

    def close_all(self):
        """Close all producers in the pool"""
        while not self.producers.empty():
            try:
                producer = self.producers.get_nowait()
                producer.flush()
                # Producer automatically closes when garbage collected
            except queue.Empty:
                break

class ProducerMetrics:
    def __init__(self):
        self.messages_sent = 0
        self.messages_failed = 0
        self.bytes_sent = 0
        self.total_latency = 0
        self.lock = threading.Lock()
        self.start_time = time.time()

    def record_success(self, message_size: int, latency: float):
        with self.lock:
            self.messages_sent += 1
            self.bytes_sent += message_size
            self.total_latency += latency

    def record_failure(self):
        with self.lock:
            self.messages_failed += 1

    def get_stats(self) -> Dict[str, float]:
        with self.lock:
            runtime = time.time() - self.start_time
            total_messages = self.messages_sent + self.messages_failed
            
            return {
                'messages_sent': self.messages_sent,
                'messages_failed': self.messages_failed,
                'bytes_sent': self.bytes_sent,
                'success_rate': (self.messages_sent / total_messages * 100) if total_messages > 0 else 0,
                'throughput_msg_sec': self.messages_sent / runtime if runtime > 0 else 0,
                'throughput_bytes_sec': self.bytes_sent / runtime if runtime > 0 else 0,
                'avg_latency_ms': (self.total_latency / self.messages_sent * 1000) if self.messages_sent > 0 else 0,
                'runtime_seconds': runtime
            }

class HighVolumeProducer:
    def __init__(self, config: ProducerConfig, pool_size: int = 10, worker_threads: int = 8):
        self.config = config
        self.pool = ConnectionPool(config, pool_size)
        self.metrics = ProducerMetrics()
        self.worker_threads = worker_threads
        self.executor = ThreadPoolExecutor(max_workers=worker_threads)
        self.message_queue = queue.Queue(maxsize=100000)
        self.running = False
        self.workers = []

    def _delivery_callback(self, err: Optional[KafkaException], msg: Message, start_time: float):
        """Callback for message delivery confirmation"""
        if err:
            logger.error("Message delivery failed", error=str(err))
            self.metrics.record_failure()
        else:
            latency = time.time() - start_time
            self.metrics.record_success(len(msg.value()), latency)
            logger.debug("Message delivered", 
                        topic=msg.topic(), 
                        partition=msg.partition(), 
                        offset=msg.offset())

    def _worker_thread(self):
        """Worker thread to process messages from queue"""
        while self.running:
            try:
                # Get message from queue with timeout
                message_data = self.message_queue.get(timeout=1)
                if message_data is None:  # Shutdown signal
                    break

                topic, action, partition_key = message_data
                self._send_message_internal(topic, action, partition_key)
                self.message_queue.task_done()

            except queue.Empty:
                continue
            except Exception as e:
                logger.error("Worker thread error", error=str(e))

    def _send_message_internal(self, topic: str, action: UserAction, partition_key: str):
        """Internal method to send message using connection pool"""
        producer = self.pool.get_producer()
        start_time = time.time()
        
        try:
            # Serialize the action
            message_value = json.dumps(asdict(action)).encode('utf-8')
            
            # Send message with callback
            producer.produce(
                topic=topic,
                value=message_value,
                key=partition_key.encode('utf-8'),
                callback=lambda err, msg: self._delivery_callback(err, msg, start_time)
            )
            
            # Trigger delivery report callbacks
            producer.poll(0)
            
        except Exception as e:
            logger.error("Failed to send message", error=str(e))
            self.metrics.record_failure()
        finally:
            self.pool.return_producer(producer)

    def start(self):
        """Start the producer workers"""
        if self.running:
            return

        self.running = True
        logger.info("Starting high volume producer", worker_threads=self.worker_threads)

        # Start worker threads
        for i in range(self.worker_threads):
            worker = threading.Thread(target=self._worker_thread, name=f"ProducerWorker-{i}")
            worker.start()
            self.workers.append(worker)

    def send_async(self, topic: str, action: UserAction, partition_key: Optional[str] = None) -> bool:
        """Send message asynchronously"""
        if not self.running:
            raise RuntimeError("Producer not started")

        if partition_key is None:
            partition_key = action.user_id

        try:
            self.message_queue.put((topic, action, partition_key), timeout=0.1)
            return True
        except queue.Full:
            logger.warning("Message queue full, dropping message")
            self.metrics.record_failure()
            return False

    def send_batch(self, topic: str, actions: list[UserAction], partition_key_func: Callable[[UserAction], str] = None):
        """Send multiple messages in batch"""
        if partition_key_func is None:
            partition_key_func = lambda action: action.user_id

        for action in actions:
            partition_key = partition_key_func(action)
            self.send_async(topic, action, partition_key)

    def get_metrics(self) -> Dict[str, float]:
        """Get current producer metrics"""
        stats = self.metrics.get_stats()
        
        # Add system metrics
        process = psutil.Process()
        stats.update({
            'cpu_percent': process.cpu_percent(),
            'memory_mb': process.memory_info().rss / 1024 / 1024,
            'queue_size': self.message_queue.qsize(),
            'active_connections': self.pool.active_connections
        })
        
        return stats

    def stop(self, timeout: int = 30):
        """Stop the producer and cleanup resources"""
        if not self.running:
            return

        logger.info("Stopping high volume producer")
        self.running = False

        # Send shutdown signals to workers
        for _ in self.workers:
            try:
                self.message_queue.put(None, timeout=1)
            except queue.Full:
                pass

        # Wait for workers to finish
        for worker in self.workers:
            worker.join(timeout=timeout)

        # Flush all producers
        self.pool.close_all()
        
        logger.info("High volume producer stopped")

# Factory function for easy instantiation
def create_high_volume_producer(
    bootstrap_servers: str = "localhost:9092",
    pool_size: int = 10,
    worker_threads: int = 8,
    **config_overrides
) -> HighVolumeProducer:
    """Create a configured high volume producer"""
    config = ProducerConfig(bootstrap_servers=bootstrap_servers, **config_overrides)
    return HighVolumeProducer(config, pool_size, worker_threads)
