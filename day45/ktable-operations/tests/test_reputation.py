import pytest
from datetime import datetime
from src.models import UserAction, ReputationScore, ActionType
from src.reputation_calculator import ReputationCalculator

def test_reputation_calculation():
    """Test reputation point calculation"""
    assert ReputationCalculator.calculate_delta(ActionType.LIKE) == 1
    assert ReputationCalculator.calculate_delta(ActionType.COMMENT) == 2
    assert ReputationCalculator.calculate_delta(ActionType.SHARE) == 5

def test_score_update():
    """Test score update logic"""
    score = ReputationScore(
        user_id="test_user",
        score=10,
        like_count=5,
        comment_count=2,
        share_count=1,
        last_updated=datetime.utcnow()
    )
    
    updated = ReputationCalculator.update_score(score, ActionType.LIKE)
    assert updated.score == 11
    assert updated.like_count == 6

def test_tier_assignment():
    """Test tier assignment based on score"""
    assert ReputationCalculator.get_tier(50) == "bronze"
    assert ReputationCalculator.get_tier(250) == "silver"
    assert ReputationCalculator.get_tier(1000) == "gold"
    assert ReputationCalculator.get_tier(3000) == "platinum"

def test_action_model():
    """Test action model validation"""
    action = UserAction(
        user_id="user_001",
        action_type=ActionType.LIKE,
        post_id="post_123",
        timestamp=datetime.utcnow()
    )
    assert action.user_id == "user_001"
    assert action.action_type == ActionType.LIKE

