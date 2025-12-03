from datetime import datetime, timedelta
from typing import List, Dict
from src.models.post import WindowedCount, TrendingScore
import math

class TrendingScorer:
    """Calculates trending scores using velocity-based algorithm"""
    
    def __init__(self, velocity_weight: float, engagement_weight: float, 
                 recency_weight: float, min_threshold: int, decay_rate: float):
        self.velocity_weight = velocity_weight
        self.engagement_weight = engagement_weight
        self.recency_weight = recency_weight
        self.min_threshold = min_threshold
        self.decay_rate = decay_rate
        
        # Historical baselines for velocity calculation
        self.previous_counts: Dict[str, int] = {}
        self.score_count = 0
    
    def calculate_score(self, window: WindowedCount, current_time: datetime) -> TrendingScore:
        """
        Calculate trending score for a windowed count.
        
        Score = (velocity * v_weight) + (engagement * e_weight) + (recency * r_weight)
        """
        hashtag = window.hashtag
        
        # Skip if below minimum threshold
        if window.mention_count < self.min_threshold:
            return None
        
        # Calculate velocity (rate of change)
        previous_count = self.previous_counts.get(hashtag, 0)
        velocity = window.mention_count - previous_count
        velocity_score = self._normalize_velocity(velocity)
        
        # Calculate engagement score
        avg_engagement = window.total_engagement / max(window.mention_count, 1)
        engagement_score = self._normalize_engagement(avg_engagement)
        
        # Calculate recency score (time decay)
        time_diff = (current_time - window.window_end).total_seconds() / 3600  # hours
        recency_score = math.exp(-time_diff * (1 - self.decay_rate))
        
        # Combined weighted score
        total_score = (
            velocity_score * self.velocity_weight +
            engagement_score * self.engagement_weight +
            recency_score * self.recency_weight
        )
        
        # Update historical baseline
        self.previous_counts[hashtag] = window.mention_count
        self.score_count += 1
        
        return TrendingScore(
            hashtag=hashtag,
            score=total_score,
            velocity=velocity,
            mention_count=window.mention_count,
            unique_users=len(window.unique_users),
            rank=0,  # Will be set by ranker
            region=window.hashtag,  # Default to global
            timestamp=current_time
        )
    
    def _normalize_velocity(self, velocity: float) -> float:
        """Normalize velocity using log scale"""
        if velocity <= 0:
            return 0.0
        return math.log1p(velocity) / 10.0  # Log scale, capped
    
    def _normalize_engagement(self, avg_engagement: float) -> float:
        """Normalize engagement score to [0, 1]"""
        return min(avg_engagement / 100.0, 1.0)
    
    def rank_trending(self, scores: List[TrendingScore], top_k: int) -> List[TrendingScore]:
        """
        Rank trending scores and return top K.
        """
        # Sort by score descending
        sorted_scores = sorted(scores, key=lambda x: x.score, reverse=True)
        
        # Assign ranks
        for rank, score in enumerate(sorted_scores[:top_k], start=1):
            score.rank = rank
        
        return sorted_scores[:top_k]
    
    def get_metrics(self) -> dict:
        return {
            'scores_calculated': self.score_count,
            'hashtags_tracked': len(self.previous_counts)
        }
