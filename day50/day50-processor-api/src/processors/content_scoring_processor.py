"""
Content Scoring Processor - Low-level Kafka Streams Processor API
Implements multi-factor content recommendation scoring
"""
import json
import time
import hashlib
from typing import Dict, Any, Optional
from collections import defaultdict
import numpy as np

class ProcessorContext:
    """Context passed to processors with state store access"""
    def __init__(self, state_stores: Dict[str, Any]):
        self.state_stores = state_stores
        self.timestamp = int(time.time() * 1000)
        self.offset = 0
        self.partition = 0
        
    def get_state_store(self, name: str):
        return self.state_stores.get(name)
    
    def forward(self, key: str, value: Any, to: str = None):
        """Forward record to downstream processor or topic"""
        return (key, value)
    
    def commit(self):
        """Commit offsets and flush state stores"""
        pass


class StateStore:
    """In-memory state store with RocksDB-like interface"""
    def __init__(self, name: str, changelog_topic: str = None):
        self.name = name
        self.changelog_topic = changelog_topic
        self.store = {}
        self.metrics = {
            'reads': 0,
            'writes': 0,
            'deletes': 0,
            'size_bytes': 0
        }
        
    def put(self, key: str, value: Any):
        """Store key-value pair"""
        self.store[key] = value
        self.metrics['writes'] += 1
        self.metrics['size_bytes'] = len(json.dumps(self.store))
        
    def get(self, key: str) -> Optional[Any]:
        """Retrieve value by key"""
        self.metrics['reads'] += 1
        return self.store.get(key)
    
    def delete(self, key: str):
        """Remove key from store"""
        if key in self.store:
            del self.store[key]
            self.metrics['deletes'] += 1
            self.metrics['size_bytes'] = len(json.dumps(self.store))
    
    def range(self, from_key: str, to_key: str):
        """Range query (time-based for our use case)"""
        results = []
        for k, v in self.store.items():
            if from_key <= k <= to_key:
                results.append((k, v))
        return results
    
    def all(self):
        """Return all entries"""
        return list(self.store.items())
    
    def size(self) -> int:
        """Number of entries"""
        return len(self.store)
    
    def get_metrics(self) -> Dict:
        """Return store metrics"""
        return self.metrics.copy()


class FeatureExtractionProcessor:
    """Extracts ML features from raw engagement events"""
    
    def __init__(self):
        self.context: Optional[ProcessorContext] = None
        self.feature_store: Optional[StateStore] = None
        self.metrics = defaultdict(int)
        
    def init(self, context: ProcessorContext):
        """Initialize processor with context and state stores"""
        self.context = context
        self.feature_store = context.get_state_store("feature-store")
        if not self.feature_store:
            raise RuntimeError("Feature store not found!")
        print(f"[FeatureExtractionProcessor] Initialized with state store: {self.feature_store.name}")
    
    def process(self, key: str, value: Dict[str, Any]) -> tuple:
        """Extract features from engagement event"""
        try:
            self.metrics['processed'] += 1
            
            event_type = value.get('event_type', 'unknown')
            user_id = value.get('user_id', key)
            content_id = value.get('content_id')
            timestamp = value.get('timestamp', int(time.time()))
            
            # Extract features based on event type
            features = self._extract_features(event_type, value)
            
            # Store features with composite key
            feature_key = f"{user_id}:{content_id}:{timestamp}"
            self.feature_store.put(feature_key, {
                'user_id': user_id,
                'content_id': content_id,
                'features': features,
                'timestamp': timestamp,
                'event_type': event_type
            })
            
            self.metrics['features_extracted'] += 1
            
            # Forward enriched event downstream
            enriched = {
                **value,
                'features': features,
                'feature_key': feature_key
            }
            
            return self.context.forward(key, enriched)
            
        except Exception as e:
            self.metrics['errors'] += 1
            print(f"[FeatureExtractionProcessor] Error: {e}")
            return None
    
    def _extract_features(self, event_type: str, event: Dict) -> Dict[str, float]:
        """Extract numerical features from event"""
        features = {
            'engagement_weight': 0.0,
            'dwell_time': 0.0,
            'interaction_depth': 0.0,
            'virality_signal': 0.0
        }
        
        # Event type weighting
        weights = {
            'view': 0.1,
            'like': 0.3,
            'comment': 0.5,
            'share': 0.8,
            'bookmark': 0.6
        }
        features['engagement_weight'] = weights.get(event_type, 0.1)
        
        # Dwell time (if present)
        features['dwell_time'] = min(event.get('dwell_time', 0) / 300.0, 1.0)
        
        # Interaction depth (clicks, scrolls, etc.)
        features['interaction_depth'] = min(event.get('interactions', 0) / 10.0, 1.0)
        
        # Virality signal (from content metadata)
        features['virality_signal'] = event.get('viral_score', 0.0)
        
        return features
    
    def close(self):
        """Cleanup resources"""
        print(f"[FeatureExtractionProcessor] Closing. Metrics: {dict(self.metrics)}")


class ScoreAggregationProcessor:
    """Aggregates features and computes content scores"""
    
    def __init__(self):
        self.context: Optional[ProcessorContext] = None
        self.feature_store: Optional[StateStore] = None
        self.score_store: Optional[StateStore] = None
        self.metrics = defaultdict(int)
        
        # Scoring weights
        self.weights = {
            'engagement': 0.4,
            'relevance': 0.3,
            'virality': 0.2,
            'freshness': 0.1
        }
        
    def init(self, context: ProcessorContext):
        """Initialize with context and state stores"""
        self.context = context
        self.feature_store = context.get_state_store("feature-store")
        self.score_store = context.get_state_store("score-store")
        print(f"[ScoreAggregationProcessor] Initialized")
    
    def process(self, key: str, value: Dict[str, Any]) -> tuple:
        """Compute multi-factor content score"""
        try:
            self.metrics['processed'] += 1
            
            user_id = value.get('user_id')
            content_id = value.get('content_id')
            features = value.get('features', {})
            
            # Retrieve historical features for this user-content pair
            historical_features = self._get_historical_features(user_id, content_id)
            
            # Compute score components
            engagement_score = self._compute_engagement_score(features, historical_features)
            relevance_score = self._compute_relevance_score(value, user_id)
            virality_score = features.get('virality_signal', 0.0)
            freshness_score = self._compute_freshness_score(value.get('timestamp', 0))
            
            # Weighted aggregation
            total_score = (
                self.weights['engagement'] * engagement_score +
                self.weights['relevance'] * relevance_score +
                self.weights['virality'] * virality_score +
                self.weights['freshness'] * freshness_score
            )
            
            # Store score
            score_key = f"{user_id}:{content_id}"
            score_data = {
                'user_id': user_id,
                'content_id': content_id,
                'total_score': total_score,
                'components': {
                    'engagement': engagement_score,
                    'relevance': relevance_score,
                    'virality': virality_score,
                    'freshness': freshness_score
                },
                'timestamp': int(time.time()),
                'update_count': self.score_store.get(score_key).get('update_count', 0) + 1 if self.score_store.get(score_key) else 1
            }
            
            self.score_store.put(score_key, score_data)
            self.metrics['scores_computed'] += 1
            
            # Forward scored content
            scored = {
                **value,
                'score': total_score,
                'score_components': score_data['components']
            }
            
            return self.context.forward(key, scored)
            
        except Exception as e:
            self.metrics['errors'] += 1
            print(f"[ScoreAggregationProcessor] Error: {e}")
            return None
    
    def _get_historical_features(self, user_id: str, content_id: str) -> list:
        """Retrieve recent features for user-content interaction"""
        # Simplified: get last 10 interactions
        all_features = self.feature_store.all()
        relevant = [
            f[1] for f in all_features 
            if f[1].get('user_id') == user_id and f[1].get('content_id') == content_id
        ]
        return sorted(relevant, key=lambda x: x.get('timestamp', 0), reverse=True)[:10]
    
    def _compute_engagement_score(self, features: Dict, historical: list) -> float:
        """Compute engagement score with time decay"""
        if not historical:
            return features.get('engagement_weight', 0.0)
        
        # Aggregate with exponential decay
        current_time = time.time()
        decay_rate = 0.1  # per day
        
        total_weight = 0.0
        for hist in historical:
            age_days = (current_time - hist.get('timestamp', 0)) / 86400
            decay = np.exp(-decay_rate * age_days)
            weight = hist.get('features', {}).get('engagement_weight', 0.0)
            total_weight += weight * decay
        
        # Combine with current
        return min((total_weight / max(len(historical), 1) + features.get('engagement_weight', 0.0)) / 2, 1.0)
    
    def _compute_relevance_score(self, event: Dict, user_id: str) -> float:
        """Compute relevance based on user preferences (simplified)"""
        # In production, this would query user preference embeddings
        # For now, use category matching
        content_category = event.get('category', '')
        user_hash = int(hashlib.md5(user_id.encode()).hexdigest(), 16)
        preference_score = (user_hash % 100) / 100.0
        return preference_score
    
    def _compute_freshness_score(self, timestamp: int) -> float:
        """Compute freshness with exponential decay"""
        if timestamp == 0:
            return 0.5
        
        age_hours = (time.time() - timestamp) / 3600
        # Decay half-life of 24 hours
        return np.exp(-np.log(2) * age_hours / 24)
    
    def close(self):
        """Cleanup resources"""
        print(f"[ScoreAggregationProcessor] Closing. Metrics: {dict(self.metrics)}")


class RecommendationProcessor:
    """Final processor that ranks and filters recommendations"""
    
    def __init__(self):
        self.context: Optional[ProcessorContext] = None
        self.score_store: Optional[StateStore] = None
        self.recommendation_store: Optional[StateStore] = None
        self.metrics = defaultdict(int)
        self.top_k = 50  # Top 50 recommendations per user
        
    def init(self, context: ProcessorContext):
        """Initialize with context"""
        self.context = context
        self.score_store = context.get_state_store("score-store")
        self.recommendation_store = context.get_state_store("recommendation-store")
        print(f"[RecommendationProcessor] Initialized")
    
    def process(self, key: str, value: Dict[str, Any]) -> tuple:
        """Generate top-K recommendations"""
        try:
            self.metrics['processed'] += 1
            
            user_id = value.get('user_id')
            
            # Get all scores for this user
            user_scores = self._get_user_scores(user_id)
            
            # Rank by total score
            ranked = sorted(user_scores, key=lambda x: x['total_score'], reverse=True)[:self.top_k]
            
            # Store recommendations
            recommendation_data = {
                'user_id': user_id,
                'recommendations': ranked,
                'timestamp': int(time.time()),
                'count': len(ranked)
            }
            
            self.recommendation_store.put(user_id, recommendation_data)
            self.metrics['recommendations_generated'] += 1
            
            # Forward (or don't - this is the terminal processor)
            return (user_id, recommendation_data)
            
        except Exception as e:
            self.metrics['errors'] += 1
            print(f"[RecommendationProcessor] Error: {e}")
            return None
    
    def _get_user_scores(self, user_id: str) -> list:
        """Get all content scores for a user"""
        all_scores = self.score_store.all()
        user_scores = [
            s[1] for s in all_scores
            if s[1].get('user_id') == user_id
        ]
        return user_scores
    
    def close(self):
        """Cleanup resources"""
        print(f"[RecommendationProcessor] Closing. Metrics: {dict(self.metrics)}")
