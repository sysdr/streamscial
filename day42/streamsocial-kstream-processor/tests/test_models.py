import unittest
import time
from src.models.interaction import InteractionEvent, RankingSignal

class TestInteractionModels(unittest.TestCase):
    
    def test_interaction_event_creation(self):
        event = InteractionEvent(
            user_id="user_123",
            post_id="post_456",
            interaction_type="LIKE",
            timestamp=time.time()
        )
        self.assertEqual(event.user_id, "user_123")
        self.assertTrue(event.is_valid())
    
    def test_interaction_event_validation(self):
        # Invalid type
        event = InteractionEvent(
            user_id="user_123",
            post_id="post_456",
            interaction_type="INVALID",
            timestamp=time.time()
        )
        self.assertFalse(event.is_valid())
        
        # Missing user
        event = InteractionEvent(
            user_id="",
            post_id="post_456",
            interaction_type="LIKE",
            timestamp=time.time()
        )
        self.assertFalse(event.is_valid())
    
    def test_serialization(self):
        event = InteractionEvent(
            user_id="user_123",
            post_id="post_456",
            interaction_type="LIKE",
            timestamp=time.time()
        )
        json_str = event.to_json()
        restored = InteractionEvent.from_json(json_str)
        self.assertEqual(event.user_id, restored.user_id)
        self.assertEqual(event.interaction_type, restored.interaction_type)

if __name__ == '__main__':
    unittest.main()
