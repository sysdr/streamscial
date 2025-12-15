"""
Unit tests for processors
"""
import pytest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.processors.content_scoring_processor import (
    FeatureExtractionProcessor,
    ScoreAggregationProcessor,
    RecommendationProcessor,
    ProcessorContext,
    StateStore
)

def test_feature_extraction():
    """Test feature extraction processor"""
    # Create state stores
    feature_store = StateStore('feature-store')
    state_stores = {'feature-store': feature_store}
    context = ProcessorContext(state_stores)
    
    # Initialize processor
    processor = FeatureExtractionProcessor()
    processor.init(context)
    
    # Process event
    event = {
        'event_type': 'like',
        'user_id': 'user_123',
        'content_id': 'content_456',
        'timestamp': 1234567890,
        'dwell_time': 150,
        'interactions': 5,
        'viral_score': 0.8
    }
    
    result = processor.process('user_123', event)
    
    assert result is not None
    key, value = result
    assert 'features' in value
    assert value['features']['engagement_weight'] == 0.3  # like weight
    assert feature_store.size() == 1
    
    print("✓ Feature extraction test passed")

def test_score_aggregation():
    """Test score aggregation processor"""
    # Create state stores
    feature_store = StateStore('feature-store')
    score_store = StateStore('score-store')
    state_stores = {
        'feature-store': feature_store,
        'score-store': score_store
    }
    context = ProcessorContext(state_stores)
    
    # Initialize processor
    processor = ScoreAggregationProcessor()
    processor.init(context)
    
    # Process event with features
    event = {
        'user_id': 'user_123',
        'content_id': 'content_456',
        'timestamp': 1234567890,
        'features': {
            'engagement_weight': 0.5,
            'dwell_time': 0.8,
            'interaction_depth': 0.6,
            'virality_signal': 0.7
        }
    }
    
    result = processor.process('user_123', event)
    
    assert result is not None
    key, value = result
    assert 'score' in value
    assert value['score'] > 0
    assert score_store.size() == 1
    
    print("✓ Score aggregation test passed")

def test_recommendation_generation():
    """Test recommendation processor"""
    # Create state stores
    score_store = StateStore('score-store')
    recommendation_store = StateStore('recommendation-store')
    
    # Pre-populate score store
    for i in range(10):
        score_store.put(f"user_123:content_{i}", {
            'user_id': 'user_123',
            'content_id': f'content_{i}',
            'total_score': 0.5 + (i * 0.05),
            'timestamp': 1234567890
        })
    
    state_stores = {
        'score-store': score_store,
        'recommendation-store': recommendation_store
    }
    context = ProcessorContext(state_stores)
    
    # Initialize processor
    processor = RecommendationProcessor()
    processor.init(context)
    
    # Generate recommendations
    event = {
        'user_id': 'user_123',
        'content_id': 'content_5'
    }
    
    result = processor.process('user_123', event)
    
    assert result is not None
    user_id, recs = result
    assert user_id == 'user_123'
    assert len(recs['recommendations']) <= 50
    assert recommendation_store.size() == 1
    
    print("✓ Recommendation generation test passed")

def test_state_store_operations():
    """Test state store basic operations"""
    store = StateStore('test-store')
    
    # Put
    store.put('key1', {'value': 'data1'})
    assert store.size() == 1
    
    # Get
    value = store.get('key1')
    assert value == {'value': 'data1'}
    
    # Delete
    store.delete('key1')
    assert store.size() == 0
    assert store.get('key1') is None
    
    # Metrics
    metrics = store.get_metrics()
    assert metrics['reads'] == 2  # Two get calls
    assert metrics['writes'] == 1
    assert metrics['deletes'] == 1
    
    print("✓ State store operations test passed")

if __name__ == '__main__':
    test_feature_extraction()
    test_score_aggregation()
    test_recommendation_generation()
    test_state_store_operations()
    print("\n✅ All tests passed!")
