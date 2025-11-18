import json
from typing import Dict, Any, List, Optional
from datetime import datetime
from confluent_kafka import Consumer, Producer, KafkaError
import structlog
import time

logger = structlog.get_logger()

class FeatureProcessor:
    """Processes raw data into ML feature vectors"""
    
    def __init__(self, bootstrap_servers: str, group_id: str = 'feature-processor'):
        self.consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        self.producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'acks': 'all'
        }
        
        self.user_profiles: Dict[int, Dict[str, Any]] = {}
        self.content_metadata: Dict[int, Dict[str, Any]] = {}
        self.running = False
    
    def extract_user_features(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        """Extract ML features from user profile"""
        interests = profile.get('interests', []) or []
        
        return {
            'user_id': profile.get('user_id'),
            'follower_count_normalized': min(profile.get('follower_count', 0) / 10000, 1.0),
            'following_ratio': self._safe_ratio(
                profile.get('following_count', 0),
                profile.get('follower_count', 1)
            ),
            'account_maturity': min(profile.get('account_age_days', 0) / 365, 1.0),
            'engagement_rate': profile.get('engagement_rate', 0.0),
            'interest_count': len(interests),
            'has_interests': 1 if interests else 0,
            'feature_timestamp': datetime.utcnow().isoformat()
        }
    
    def extract_content_features(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Extract ML features from content metadata"""
        tags = metadata.get('tags', []) or []
        
        return {
            'content_id': metadata.get('content_id'),
            'creator_id': metadata.get('creator_id'),
            'engagement_score_normalized': min(metadata.get('engagement_score', 0) / 100, 1.0),
            'view_count_log': self._log_normalize(metadata.get('view_count', 0)),
            'tag_count': len(tags),
            'category_encoded': self._encode_category(metadata.get('category', 'unknown')),
            'feature_timestamp': datetime.utcnow().isoformat()
        }
    
    def extract_interaction_features(self, 
                                     interaction: Dict[str, Any],
                                     user_features: Optional[Dict[str, Any]],
                                     content_features: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Extract ML features from user interaction with context"""
        
        features = {
            'interaction_id': interaction.get('interaction_id'),
            'user_id': interaction.get('user_id'),
            'content_id': interaction.get('content_id'),
            'interaction_type_encoded': self._encode_interaction(interaction.get('interaction_type', 'view')),
            'duration_normalized': min(interaction.get('duration_seconds', 0) / 300, 1.0),
            'feature_timestamp': datetime.utcnow().isoformat()
        }
        
        # Join with user features
        if user_features:
            features['user_follower_norm'] = user_features.get('follower_count_normalized', 0)
            features['user_engagement'] = user_features.get('engagement_rate', 0)
            features['user_maturity'] = user_features.get('account_maturity', 0)
        
        # Join with content features
        if content_features:
            features['content_engagement'] = content_features.get('engagement_score_normalized', 0)
            features['content_views_log'] = content_features.get('view_count_log', 0)
            features['content_category'] = content_features.get('category_encoded', 0)
        
        return features
    
    def _safe_ratio(self, numerator: float, denominator: float) -> float:
        if denominator == 0:
            return 0.0
        return min(numerator / denominator, 10.0)
    
    def _log_normalize(self, value: int) -> float:
        import math
        if value <= 0:
            return 0.0
        return math.log10(value + 1) / 6  # Normalize to 0-1 for up to 1M views
    
    def _encode_category(self, category: str) -> int:
        categories = {
            'entertainment': 1, 'news': 2, 'sports': 3, 'technology': 4,
            'music': 5, 'gaming': 6, 'education': 7, 'lifestyle': 8,
            'food': 9, 'travel': 10, 'unknown': 0
        }
        return categories.get(category.lower(), 0)
    
    def _encode_interaction(self, interaction_type: str) -> int:
        types = {
            'view': 1, 'like': 2, 'comment': 3, 'share': 4,
            'save': 5, 'click': 6
        }
        return types.get(interaction_type.lower(), 0)
    
    def process_stream(self, 
                       input_topics: List[str],
                       output_topic: str,
                       batch_size: int = 100):
        """Process incoming records and generate features"""
        
        consumer = Consumer(self.consumer_config)
        producer = Producer(self.producer_config)
        
        consumer.subscribe(input_topics)
        self.running = True
        
        batch = []
        
        try:
            while self.running:
                msg = consumer.poll(1.0)
                
                if msg is None:
                    if batch:
                        self._flush_batch(producer, output_topic, batch)
                        batch = []
                        consumer.commit()
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("consumer_error", error=msg.error())
                    continue
                
                try:
                    value = json.loads(msg.value().decode('utf-8'))
                    topic = msg.topic()
                    
                    # Update local cache and generate features
                    if 'user_profiles' in topic:
                        user_id = value.get('user_id')
                        self.user_profiles[user_id] = value
                        features = self.extract_user_features(value)
                        batch.append(('user', features))
                        
                    elif 'content_metadata' in topic:
                        content_id = value.get('content_id')
                        self.content_metadata[content_id] = value
                        features = self.extract_content_features(value)
                        batch.append(('content', features))
                        
                    elif 'user_interactions' in topic:
                        user_id = value.get('user_id')
                        content_id = value.get('content_id')
                        user_feat = self.user_profiles.get(user_id)
                        if user_feat:
                            user_feat = self.extract_user_features(user_feat)
                        content_feat = self.content_metadata.get(content_id)
                        if content_feat:
                            content_feat = self.extract_content_features(content_feat)
                        features = self.extract_interaction_features(value, user_feat, content_feat)
                        batch.append(('interaction', features))
                    
                    if len(batch) >= batch_size:
                        self._flush_batch(producer, output_topic, batch)
                        batch = []
                        consumer.commit()
                        
                except json.JSONDecodeError as e:
                    logger.error("json_decode_error", error=str(e))
                    
        finally:
            if batch:
                self._flush_batch(producer, output_topic, batch)
            consumer.close()
            producer.flush()
    
    def _flush_batch(self, producer: Producer, topic: str, batch: List):
        """Flush batch of features to output topic"""
        for feature_type, features in batch:
            key = f"{feature_type}-{features.get('user_id') or features.get('content_id') or features.get('interaction_id')}"
            producer.produce(
                topic,
                key=key.encode('utf-8'),
                value=json.dumps(features).encode('utf-8')
            )
        producer.flush()
        logger.info("batch_flushed", count=len(batch), topic=topic)
    
    def stop(self):
        self.running = False
