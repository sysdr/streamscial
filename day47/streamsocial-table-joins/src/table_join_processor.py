"""
Table-Table Join Processor for StreamSocial Recommendation Engine
Joins user preferences KTable with content metadata KTable
"""

import json
import time
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from collections import defaultdict
import threading
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class UserPreference:
    user_id: str
    languages: List[str]
    categories: List[str]
    topics: List[str]
    weights: Dict[str, float]
    updated_at: str


@dataclass
class ContentMetadata:
    content_id: str
    language: str
    category: str
    tags: List[str]
    created_at: str
    updated_at: str


@dataclass
class Recommendation:
    user_id: str
    content_id: str
    match_score: float
    matched_attributes: Dict[str, str]
    generated_at: str


class StateStore:
    """Simple in-memory state store simulating RocksDB"""
    
    def __init__(self, name: str):
        self.name = name
        self.store: Dict[str, any] = {}
        self.lock = threading.Lock()
    
    def put(self, key: str, value: any):
        with self.lock:
            self.store[key] = value
    
    def get(self, key: str) -> Optional[any]:
        with self.lock:
            return self.store.get(key)
    
    def delete(self, key: str):
        with self.lock:
            if key in self.store:
                del self.store[key]
    
    def size(self) -> int:
        with self.lock:
            return len(self.store)
    
    def get_all(self) -> Dict[str, any]:
        with self.lock:
            return dict(self.store)


class TableJoinProcessor:
    """
    Implements table-table join between user preferences and content metadata.
    Produces recommendations when either table updates.
    """
    
    def __init__(self, bootstrap_servers: str, group_id: str, dashboard_url: str = 'http://localhost:5000'):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.dashboard_url = dashboard_url
        
        # State stores for both tables
        self.user_prefs_store = StateStore("user-preferences")
        self.content_store = StateStore("content-metadata")
        
        # Metrics
        self.metrics = {
            'preferences_processed': 0,
            'content_processed': 0,
            'recommendations_generated': 0,
            'join_operations': 0,
            'high_score_recommendations': 0
        }
        self.metrics_lock = threading.Lock()
        
        # Producer for recommendations
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            compression_type='gzip'
        )
        
        self.running = False
    
    def compute_match_score(self, preference: UserPreference, content: ContentMetadata) -> Tuple[float, Dict]:
        """
        Compute weighted match score between user preference and content.
        Score components:
        - Language match: 0.3
        - Category match: 0.4
        - Topic/tag overlap: 0.2
        - Recency bonus: 0.1
        """
        score = 0.0
        matched_attrs = {}
        
        # Language match (0.3 weight)
        if content.language in preference.languages:
            language_weight = preference.weights.get('language', 1.0)
            score += 0.3 * language_weight
            matched_attrs['language'] = content.language
        
        # Category match (0.4 weight)
        if content.category in preference.categories:
            category_weight = preference.weights.get('category', 1.0)
            score += 0.4 * category_weight
            matched_attrs['category'] = content.category
        
        # Topic/tag overlap (0.2 weight)
        matching_tags = set(preference.topics) & set(content.tags)
        if matching_tags:
            topic_weight = preference.weights.get('topics', 1.0)
            overlap_ratio = len(matching_tags) / len(preference.topics)
            score += 0.2 * overlap_ratio * topic_weight
            matched_attrs['tags'] = list(matching_tags)
        
        # Recency bonus (0.1 weight)
        try:
            content_age_hours = (datetime.now() - datetime.fromisoformat(content.created_at)).total_seconds() / 3600
            if content_age_hours < 24:
                score += 0.1
                matched_attrs['recency'] = 'new_content'
            elif content_age_hours < 168:  # 7 days
                score += 0.05
                matched_attrs['recency'] = 'recent_content'
        except:
            pass
        
        return round(score, 3), matched_attrs
    
    def process_join(self, user_id: str = None, content_id: str = None):
        """
        Execute join operation. Called when either table updates.
        If user_id provided: join that user with all content
        If content_id provided: join that content with all users
        """
        recommendations = []
        
        with self.metrics_lock:
            self.metrics['join_operations'] += 1
        
        if user_id:
            # User preference updated - join with all content
            preference = self.user_prefs_store.get(user_id)
            if not preference:
                return recommendations
            
            for cid, content in self.content_store.get_all().items():
                score, matched = self.compute_match_score(preference, content)
                if score >= 0.5:  # Threshold for valid recommendations
                    rec = Recommendation(
                        user_id=user_id,
                        content_id=cid,
                        match_score=score,
                        matched_attributes=matched,
                        generated_at=datetime.now().isoformat()
                    )
                    recommendations.append(rec)
                    
                    if score >= 0.7:
                        with self.metrics_lock:
                            self.metrics['high_score_recommendations'] += 1
        
        elif content_id:
            # Content updated - join with all users
            content = self.content_store.get(content_id)
            if not content:
                return recommendations
            
            for uid, preference in self.user_prefs_store.get_all().items():
                score, matched = self.compute_match_score(preference, content)
                if score >= 0.5:
                    rec = Recommendation(
                        user_id=uid,
                        content_id=content_id,
                        match_score=score,
                        matched_attributes=matched,
                        generated_at=datetime.now().isoformat()
                    )
                    recommendations.append(rec)
                    
                    if score >= 0.7:
                        with self.metrics_lock:
                            self.metrics['high_score_recommendations'] += 1
        
        # Emit recommendations
        for rec in recommendations:
            self.producer.send('content-recommendations', value=asdict(rec))
            with self.metrics_lock:
                self.metrics['recommendations_generated'] += 1
        
        if recommendations:
            logger.info(f"Generated {len(recommendations)} recommendations")
        
        return recommendations
    
    def process_user_preference(self, message):
        """Process update to user preferences table"""
        try:
            data = json.loads(message.value.decode('utf-8'))
            
            if data.get('value') is None:
                # Tombstone - delete
                self.user_prefs_store.delete(data['key'])
                logger.info(f"Deleted user preference: {data['key']}")
                return
            
            preference = UserPreference(
                user_id=data['key'],
                languages=data['value']['languages'],
                categories=data['value']['categories'],
                topics=data['value']['topics'],
                weights=data['value'].get('weights', {}),
                updated_at=data['value']['updated_at']
            )
            
            self.user_prefs_store.put(preference.user_id, preference)
            
            with self.metrics_lock:
                self.metrics['preferences_processed'] += 1
            
            logger.info(f"Processed user preference: {preference.user_id}")
            
            # Trigger join with all content
            self.process_join(user_id=preference.user_id)
            
        except Exception as e:
            logger.error(f"Error processing user preference: {e}")
    
    def process_content_metadata(self, message):
        """Process update to content metadata table"""
        try:
            data = json.loads(message.value.decode('utf-8'))
            
            if data.get('value') is None:
                # Tombstone - delete
                self.content_store.delete(data['key'])
                logger.info(f"Deleted content: {data['key']}")
                return
            
            content = ContentMetadata(
                content_id=data['key'],
                language=data['value']['language'],
                category=data['value']['category'],
                tags=data['value']['tags'],
                created_at=data['value']['created_at'],
                updated_at=data['value']['updated_at']
            )
            
            self.content_store.put(content.content_id, content)
            
            with self.metrics_lock:
                self.metrics['content_processed'] += 1
            
            logger.info(f"Processed content metadata: {content.content_id}")
            
            # Trigger join with all users
            self.process_join(content_id=content.content_id)
            
        except Exception as e:
            logger.error(f"Error processing content metadata: {e}")
    
    def start(self):
        """Start consuming from both input topics"""
        self.running = True
        
        # Consumer for user preferences
        prefs_consumer = KafkaConsumer(
            'user-preferences',
            bootstrap_servers=self.bootstrap_servers,
            group_id=f"{self.group_id}-prefs",
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda m: m
        )
        
        # Consumer for content metadata
        content_consumer = KafkaConsumer(
            'content-metadata',
            bootstrap_servers=self.bootstrap_servers,
            group_id=f"{self.group_id}-content",
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda m: m
        )
        
        # Start consumer threads
        prefs_thread = threading.Thread(target=self._consume_preferences, args=(prefs_consumer,), daemon=True)
        content_thread = threading.Thread(target=self._consume_content, args=(content_consumer,), daemon=True)
        metrics_thread = threading.Thread(target=self._report_metrics, daemon=True)
        
        prefs_thread.start()
        content_thread.start()
        metrics_thread.start()
        
        logger.info("Table-table join processor started")
        
        prefs_thread.join()
        content_thread.join()
    
    def _consume_preferences(self, consumer):
        """Consumer thread for user preferences"""
        while self.running:
            messages = consumer.poll(timeout_ms=1000)
            for topic_partition, records in messages.items():
                for message in records:
                    self.process_user_preference(message)
    
    def _consume_content(self, consumer):
        """Consumer thread for content metadata"""
        while self.running:
            messages = consumer.poll(timeout_ms=1000)
            for topic_partition, records in messages.items():
                for message in records:
                    self.process_content_metadata(message)
    
    def _report_metrics(self):
        """Periodically send metrics to dashboard"""
        while self.running:
            try:
                metrics = self.get_metrics()
                response = requests.post(
                    f"{self.dashboard_url}/api/update_metrics",
                    json=metrics,
                    timeout=5
                )
                if response.status_code == 200:
                    logger.debug("Metrics reported successfully")
            except Exception as e:
                logger.error(f"Error reporting metrics: {e}")
            
            time.sleep(2)
    
    def stop(self):
        """Stop the processor"""
        self.running = False
        self.producer.flush()
        self.producer.close()
        logger.info("Table-table join processor stopped")
    
    def get_metrics(self) -> Dict:
        """Get current metrics"""
        with self.metrics_lock:
            metrics_copy = dict(self.metrics)
        
        metrics_copy['user_preferences_count'] = self.user_prefs_store.size()
        metrics_copy['content_metadata_count'] = self.content_store.size()
        
        return metrics_copy


if __name__ == "__main__":
    processor = TableJoinProcessor(
        bootstrap_servers='localhost:9092',
        group_id='streamsocial-table-join'
    )
    
    try:
        processor.start()
    except KeyboardInterrupt:
        processor.stop()
