import pytest
import json
import time
from src.engagement_processor import EngagementScoreProcessor

def test_engagement_calculation():
    """Test engagement score calculation"""
    processor = EngagementScoreProcessor(
        bootstrap_servers='localhost:9092',
        group_id='test-processor',
        state_dir='/tmp/test-state'
    )
    
    # Test score calculation
    state = processor.calculate_engagement_score('user_001', 'like', time.time())
    assert state['likes'] == 1
    assert state['score'] > 0
    
    state = processor.calculate_engagement_score('user_001', 'comment', time.time())
    assert state['comments'] == 1
    
    state = processor.calculate_engagement_score('user_001', 'share', time.time())
    assert state['shares'] == 1
    
    print("✓ Engagement calculation test passed")

def test_time_decay():
    """Test time decay mechanism"""
    processor = EngagementScoreProcessor(
        bootstrap_servers='localhost:9092',
        group_id='test-processor',
        state_dir='/tmp/test-state'
    )
    
    current_time = time.time()
    state1 = processor.calculate_engagement_score('user_002', 'like', current_time)
    score1 = state1['score']
    
    # 24 hours later
    future_time = current_time + (24 * 3600)
    state2 = processor.calculate_engagement_score('user_002', 'like', future_time)
    score2 = state2['score']
    
    # Score should decay
    assert score2 < score1 * 2  # Should be less than 2x original due to decay
    
    print("✓ Time decay test passed")

if __name__ == '__main__':
    test_engagement_calculation()
    test_time_decay()
    print("\n✓ All tests passed!")
