import unittest
import time
from src.utils.scoring import EngagementScorer
from src.models.interaction import InteractionEvent

class TestEngagementScorer(unittest.TestCase):
    
    def setUp(self):
        self.config = {
            'weights': {
                'LIKE': 1.0,
                'SHARE': 3.0,
                'COMMENT': 2.0,
                'VIEW': 0.1
            },
            'reputation': {
                'new_user': 0.5,
                'regular_user': 1.0,
                'established_user': 2.0
            }
        }
        self.scorer = EngagementScorer(self.config)
    
    def test_base_scoring(self):
        event = InteractionEvent(
            user_id="user_123",
            post_id="post_456",
            interaction_type="LIKE",
            timestamp=time.time()
        )
        result = self.scorer.calculate_score(event)
        
        self.assertGreater(result['score'], 0)
        self.assertEqual(result['base_weight'], 1.0)
        self.assertEqual(result['user_reputation'], 1.0)
    
    def test_interaction_type_weights(self):
        timestamp = time.time()
        
        like_event = InteractionEvent("u1", "p1", "LIKE", timestamp)
        share_event = InteractionEvent("u1", "p1", "SHARE", timestamp)
        
        like_score = self.scorer.calculate_score(like_event)
        share_score = self.scorer.calculate_score(share_event)
        
        # Share should be worth 3x like
        self.assertAlmostEqual(share_score['score'] / like_score['score'], 3.0, places=1)
    
    def test_recency_decay(self):
        current = time.time()
        old_timestamp = current - (12 * 3600)  # 12 hours ago
        
        recent_event = InteractionEvent("u1", "p1", "LIKE", current)
        old_event = InteractionEvent("u1", "p1", "LIKE", old_timestamp)
        
        recent_score = self.scorer.calculate_score(recent_event)
        old_score = self.scorer.calculate_score(old_event)
        
        # Recent should score higher
        self.assertGreater(recent_score['score'], old_score['score'])
        self.assertEqual(recent_score['recency_factor'], 1.0)
        self.assertLess(old_score['recency_factor'], 1.0)

if __name__ == '__main__':
    unittest.main()
