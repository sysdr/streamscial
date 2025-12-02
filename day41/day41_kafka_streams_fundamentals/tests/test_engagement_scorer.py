"""
Tests for Engagement Scoring Topology
"""

import pytest
import json
import time
import tempfile
import shutil
from unittest.mock import patch, MagicMock
from src.engagement_scorer import (
    StateStore, WindowedAggregator, EngagementScoringTopology
)


def test_state_store():
    """Test state store operations"""
    temp_dir = tempfile.mkdtemp()
    try:
        store = StateStore(f'{temp_dir}/test-state-store')
        
        # Test put and get
        store.put('post_1', {'score': 100, 'timestamp': time.time()})
        result = store.get('post_1')
        
        assert result is not None
        assert result['score'] == 100
        
        # Test scan
        store.put('post_2', {'score': 200, 'timestamp': time.time()})
        items = store.scan(limit=10)
        
        assert len(items) >= 2
        assert items[0][1]['score'] >= items[1][1]['score']  # Sorted by score
        
        store.close()
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_windowed_aggregator():
    """Test windowed aggregation"""
    aggregator = WindowedAggregator(window_size_seconds=60)
    
    current_time = time.time()
    
    # Add events
    aggregator.add_event('post_1', 10, current_time)
    aggregator.add_event('post_1', 5, current_time + 30)
    aggregator.add_event('post_1', 3, current_time + 90)
    
    # Get score (should include all events in recent windows)
    score = aggregator.get_score('post_1', current_time + 100)
    assert score == 18  # 10 + 5 + 3
    
    # Test window expiration
    score_after_expiry = aggregator.get_score('post_1', current_time + 300)
    assert score_after_expiry == 0  # All windows expired


@patch('src.engagement_scorer.Consumer')
@patch('src.engagement_scorer.Producer')
def test_filter_processor(mock_producer_class, mock_consumer_class):
    """Test filter processor"""
    # Mock Kafka components
    mock_consumer = MagicMock()
    mock_producer = MagicMock()
    mock_consumer_class.return_value = mock_consumer
    mock_producer_class.return_value = mock_producer
    
    temp_dir = tempfile.mkdtemp()
    try:
        topology = EngagementScoringTopology(
            bootstrap_servers='localhost:9092',
            group_id='test-group',
            state_dir=temp_dir
        )
        
        # Valid event
        valid_event = {
            'content_id': 'post_1',
            'user_id': 'user_1',
            'interaction_type': 'LIKE'
        }
        assert topology.filter_processor(valid_event) is True
        
        # Invalid events
        assert topology.filter_processor({}) is False
        assert topology.filter_processor({'content_id': 'post_1'}) is False
        
        topology.stop()
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


@patch('src.engagement_scorer.Consumer')
@patch('src.engagement_scorer.Producer')
def test_map_processor(mock_producer_class, mock_consumer_class):
    """Test map processor with weights"""
    # Mock Kafka components
    mock_consumer = MagicMock()
    mock_producer = MagicMock()
    mock_consumer_class.return_value = mock_consumer
    mock_producer_class.return_value = mock_producer
    
    temp_dir = tempfile.mkdtemp()
    try:
        topology = EngagementScoringTopology(
            bootstrap_servers='localhost:9092',
            group_id='test-group',
            state_dir=temp_dir
        )
        
        event = {
            'content_id': 'post_1',
            'user_id': 'user_1',
            'interaction_type': 'share',
            'timestamp': time.time()
        }
        
        enriched = topology.map_processor(event)
        
        assert enriched['interaction_type'] == 'SHARE'
        assert enriched['weight'] == 10  # Share weight
        
        topology.stop()
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
