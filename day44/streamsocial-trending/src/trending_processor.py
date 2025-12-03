from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
import json
import time
import logging
from typing import Dict
import yaml

from src.models.post import Post
from src.extractors.hashtag_extractor import HashtagExtractor
from src.aggregators.window_manager import WindowManager
from src.scorers.trending_scorer import TrendingScorer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TrendingProcessor:
    """Main stream processor for trending hashtag detection"""
    
    def __init__(self, config_path: str):
        # Load configuration
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        kafka_config = self.config['kafka']
        stream_config = self.config['streaming']
        trending_config = self.config['trending']
        
        # Initialize Kafka
        self.consumer = KafkaConsumer(
            kafka_config['topics']['posts'],
            bootstrap_servers=kafka_config['bootstrap_servers'],
            group_id=kafka_config['group_id'],
            auto_offset_reset=kafka_config['auto_offset_reset'],
            enable_auto_commit=False,
            value_deserializer=lambda m: m.decode('utf-8')
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_config['bootstrap_servers'],
            value_serializer=lambda v: v.encode('utf-8')
        )
        
        self.trending_topic = kafka_config['topics']['trending']
        
        # Initialize processing components
        self.extractor = HashtagExtractor()
        
        self.window_manager = WindowManager(
            window_size_minutes=stream_config['window_size_minutes'],
            advance_minutes=stream_config['window_advance_minutes'],
            grace_minutes=stream_config['grace_period_minutes']
        )
        
        self.scorer = TrendingScorer(
            velocity_weight=trending_config['velocity_weight'],
            engagement_weight=trending_config['engagement_weight'],
            recency_weight=trending_config['recency_weight'],
            min_threshold=trending_config['min_mentions_threshold'],
            decay_rate=trending_config['decay_rate']
        )
        
        self.top_k = trending_config['top_k']
        self.suppression_interval = stream_config['suppression_duration_seconds']
        self.last_emission_time = time.time()
        
        # Metrics
        self.processed_posts = 0
        self.emitted_trends = 0
    
    def process(self):
        """Main processing loop"""
        logger.info("Starting trending processor...")
        
        try:
            for message in self.consumer:
                # Parse post
                post = Post.from_json(message.value)
                self.processed_posts += 1
                
                # Extract hashtags
                hashtags = self.extractor.extract(post)
                
                # Add to windows
                for hashtag in hashtags:
                    updates = self.window_manager.add_to_window(hashtag)
                
                # Check if suppression interval has passed
                current_time = time.time()
                if current_time - self.last_emission_time >= self.suppression_interval:
                    self._emit_trending_scores(datetime.now())
                    self.last_emission_time = current_time
                
                # Commit offsets periodically
                if self.processed_posts % 100 == 0:
                    self.consumer.commit()
                    self._log_metrics()
        
        except KeyboardInterrupt:
            logger.info("Shutting down processor...")
        finally:
            self._cleanup()
    
    def _emit_trending_scores(self, current_time: datetime):
        """Calculate and emit trending scores for all active windows"""
        all_windows = self.window_manager.get_all_windows()
        
        # Calculate scores
        scores = []
        for hashtag, window_start, window_count in all_windows:
            score = self.scorer.calculate_score(window_count, current_time)
            if score:
                scores.append(score)
        
        # Rank and emit top K
        top_trending = self.scorer.rank_trending(scores, self.top_k)
        
        for trending_score in top_trending:
            self.producer.send(
                self.trending_topic,
                value=trending_score.to_json()
            )
            self.emitted_trends += 1
        
        self.producer.flush()
        
        # Close expired windows
        closeable = self.window_manager.get_closeable_windows(current_time)
        for hashtag, window_start, _ in closeable:
            self.window_manager.close_window(hashtag, window_start)
    
    def _log_metrics(self):
        """Log processing metrics"""
        logger.info(f"Processed: {self.processed_posts} posts")
        logger.info(f"Extracted: {self.extractor.get_metrics()}")
        logger.info(f"Windows: {self.window_manager.get_metrics()}")
        logger.info(f"Scores: {self.scorer.get_metrics()}")
        logger.info(f"Emitted: {self.emitted_trends} trending updates")
    
    def _cleanup(self):
        """Cleanup resources"""
        self.consumer.close()
        self.producer.close()

if __name__ == "__main__":
    processor = TrendingProcessor("config/application.conf")
    processor.process()
