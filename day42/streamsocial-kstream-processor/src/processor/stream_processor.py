from kafka import KafkaConsumer, KafkaProducer
import json
import time
import logging
from collections import defaultdict
import threading

from src.models.interaction import InteractionEvent, RankingSignal
from src.utils.scoring import EngagementScorer
from src.utils.validator import InteractionValidator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class InteractionStreamProcessor:
    """
    KStream-style processor for user interactions
    Implements record-by-record stateless transformations
    """
    
    def __init__(self, config):
        self.config = config
        self.kafka_config = config['kafka']
        self.processing_config = config['processing']
        
        # Initialize components
        self.scorer = EngagementScorer(config)
        self.validator = InteractionValidator()
        
        # Metrics
        self.metrics = {
            'processed': 0,
            'filtered': 0,
            'errors': 0,
            'total_latency_ms': 0,
            'events_by_type': defaultdict(int)
        }
        self.metrics_lock = threading.Lock()
        
        # Initialize Kafka
        self._init_kafka()
    
    def _init_kafka(self):
        """Initialize Kafka consumer and producer"""
        self.consumer = KafkaConsumer(
            self.kafka_config['input_topic'],
            bootstrap_servers=self.kafka_config['bootstrap_servers'],
            group_id=self.kafka_config['consumer_group'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            max_poll_records=self.processing_config['batch_size']
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_config['bootstrap_servers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all'
        )
        
        logger.info(f"Kafka initialized - consuming from {self.kafka_config['input_topic']}")
    
    def transform(self, interaction_event):
        """
        Stateless transformation: enrich and score interaction
        This is the core KStream processing logic
        """
        start_time = time.time()
        
        # Validate
        is_valid, reason = self.validator.is_valid(interaction_event)
        if not is_valid:
            logger.debug(f"Filtered invalid interaction: {reason}")
            with self.metrics_lock:
                self.metrics['filtered'] += 1
            return None
        
        # Score interaction
        scoring_result = self.scorer.calculate_score(interaction_event)
        
        # Create ranking signal
        processing_time = (time.time() - start_time) * 1000  # ms
        
        signal = RankingSignal(
            user_id=interaction_event.user_id,
            post_id=interaction_event.post_id,
            interaction_type=interaction_event.interaction_type,
            engagement_score=scoring_result['score'],
            timestamp=interaction_event.timestamp,
            recency_factor=scoring_result['recency_factor'],
            user_reputation=scoring_result['user_reputation'],
            base_weight=scoring_result['base_weight'],
            processing_time_ms=processing_time
        )
        
        # Update metrics
        with self.metrics_lock:
            self.metrics['processed'] += 1
            self.metrics['total_latency_ms'] += processing_time
            self.metrics['events_by_type'][interaction_event.interaction_type] += 1
        
        return signal
    
    def process_stream(self):
        """
        Main processing loop - consume, transform, produce
        Record-by-record processing with batch commits
        """
        logger.info("Starting stream processing...")
        batch_count = 0
        
        try:
            for message in self.consumer:
                try:
                    # Deserialize
                    interaction_data = message.value
                    interaction = InteractionEvent.from_json(interaction_data)
                    
                    # Transform (stateless)
                    signal = self.transform(interaction)
                    
                    # Produce if valid
                    if signal:
                        self.producer.send(
                            self.kafka_config['output_topic'],
                            value=signal.to_json()
                        )
                    
                    batch_count += 1
                    
                    # Commit offsets periodically
                    if batch_count >= self.processing_config['batch_size']:
                        self.consumer.commit()
                        self.producer.flush()
                        batch_count = 0
                        
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    with self.metrics_lock:
                        self.metrics['errors'] += 1
                    
        except KeyboardInterrupt:
            logger.info("Shutting down processor...")
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean shutdown"""
        self.consumer.close()
        self.producer.close()
        logger.info("Processor shutdown complete")
    
    def get_metrics(self):
        """Get current processing metrics"""
        with self.metrics_lock:
            avg_latency = (
                self.metrics['total_latency_ms'] / self.metrics['processed']
                if self.metrics['processed'] > 0 else 0
            )
            
            return {
                'processed': self.metrics['processed'],
                'filtered': self.metrics['filtered'],
                'errors': self.metrics['errors'],
                'avg_latency_ms': round(avg_latency, 2),
                'events_by_type': dict(self.metrics['events_by_type'])
            }
