import pytest
from datetime import datetime, timedelta
from src.models.post import WindowedCount
from src.scorers.trending_scorer import TrendingScorer

def test_score_calculation():
    """Test trending score calculation"""
    scorer = TrendingScorer(
        velocity_weight=0.5,
        engagement_weight=0.3,
        recency_weight=0.2,
        min_threshold=5,
        decay_rate=0.95
    )
    
    window = WindowedCount(
        hashtag="ai",
        window_start=datetime.now() - timedelta(minutes=30),
        window_end=datetime.now(),
        mention_count=100,
        unique_users=set([f"user{i}" for i in range(50)]),
        total_engagement=500.0
    )
    
    score = scorer.calculate_score(window, datetime.now())
    
    assert score is not None
    assert score.score > 0
    assert score.mention_count == 100
    assert score.unique_users == 50

def test_minimum_threshold():
    """Test that low-mention hashtags are filtered"""
    scorer = TrendingScorer(
        velocity_weight=0.5,
        engagement_weight=0.3,
        recency_weight=0.2,
        min_threshold=10,
        decay_rate=0.95
    )
    
    window = WindowedCount(
        hashtag="rare",
        window_start=datetime.now() - timedelta(minutes=30),
        window_end=datetime.now(),
        mention_count=5,  # Below threshold
        unique_users=set(["user1", "user2"]),
        total_engagement=10.0
    )
    
    score = scorer.calculate_score(window, datetime.now())
    
    assert score is None

def test_ranking():
    """Test top-K ranking"""
    scorer = TrendingScorer(
        velocity_weight=0.5,
        engagement_weight=0.3,
        recency_weight=0.2,
        min_threshold=1,
        decay_rate=0.95
    )
    
    from src.models.post import TrendingScore
    
    scores = [
        TrendingScore("ai", 0.9, 50, 100, 50, 0, "global", datetime.now()),
        TrendingScore("ml", 0.7, 30, 80, 40, 0, "global", datetime.now()),
        TrendingScore("python", 0.5, 20, 60, 30, 0, "global", datetime.now()),
    ]
    
    ranked = scorer.rank_trending(scores, top_k=2)
    
    assert len(ranked) == 2
    assert ranked[0].hashtag == "ai"
    assert ranked[0].rank == 1
    assert ranked[1].hashtag == "ml"
    assert ranked[1].rank == 2
