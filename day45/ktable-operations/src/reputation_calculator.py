from src.models import ActionType, ReputationScore
from datetime import datetime

class ReputationCalculator:
    """Calculates reputation scores based on user actions"""
    
    POINTS = {
        ActionType.LIKE: 1,
        ActionType.COMMENT: 2,
        ActionType.SHARE: 5
    }
    
    @staticmethod
    def calculate_delta(action_type: ActionType) -> int:
        """Calculate reputation point delta for an action"""
        return ReputationCalculator.POINTS.get(action_type, 0)
    
    @staticmethod
    def update_score(current: ReputationScore, action_type: ActionType) -> ReputationScore:
        """Update reputation score with new action"""
        delta = ReputationCalculator.calculate_delta(action_type)
        
        # Update specific counter
        if action_type == ActionType.LIKE:
            current.like_count += 1
        elif action_type == ActionType.COMMENT:
            current.comment_count += 1
        elif action_type == ActionType.SHARE:
            current.share_count += 1
        
        # Update total score
        current.score += delta
        current.last_updated = datetime.utcnow()
        
        return current
    
    @staticmethod
    def get_tier(score: int) -> str:
        """Determine reputation tier based on score"""
        if score >= 2000:
            return "platinum"
        elif score >= 500:
            return "gold"
        elif score >= 100:
            return "silver"
        else:
            return "bronze"

