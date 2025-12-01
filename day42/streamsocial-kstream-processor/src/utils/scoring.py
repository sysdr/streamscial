import time
import math

class EngagementScorer:
    """Calculate engagement scores for interactions"""
    
    def __init__(self, config):
        self.weights = config['weights']
        self.reputation = config['reputation']
    
    def calculate_score(self, interaction, user_reputation_level='regular_user'):
        """
        Calculate engagement score
        score = base_weight × recency_factor × user_reputation
        """
        base_weight = self.weights.get(interaction.interaction_type, 0.1)
        recency_factor = self._calculate_recency_factor(interaction.timestamp)
        user_reputation = self.reputation.get(user_reputation_level, 1.0)
        
        score = base_weight * recency_factor * user_reputation
        
        return {
            'score': round(score, 4),
            'base_weight': base_weight,
            'recency_factor': round(recency_factor, 4),
            'user_reputation': user_reputation
        }
    
    def _calculate_recency_factor(self, event_timestamp):
        """
        Calculate recency factor: 1.0 for first hour, decays to 0.3 over 24 hours
        """
        current_time = time.time()
        age_hours = (current_time - event_timestamp) / 3600.0
        
        if age_hours < 0:
            return 1.0  # Future timestamp, treat as current
        elif age_hours <= 1:
            return 1.0
        elif age_hours >= 24:
            return 0.3
        else:
            # Exponential decay from 1.0 to 0.3 over 23 hours
            decay_rate = -math.log(0.3) / 23.0
            return 1.0 * math.exp(-decay_rate * (age_hours - 1))
