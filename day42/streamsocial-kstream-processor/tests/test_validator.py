import unittest
import time
from src.utils.validator import InteractionValidator
from src.models.interaction import InteractionEvent

class TestInteractionValidator(unittest.TestCase):
    
    def setUp(self):
        self.validator = InteractionValidator()
    
    def test_valid_interaction(self):
        event = InteractionEvent(
            user_id="user_123",
            post_id="post_456",
            interaction_type="LIKE",
            timestamp=time.time()
        )
        is_valid, reason = self.validator.is_valid(event)
        self.assertTrue(is_valid)
    
    def test_bot_detection(self):
        event = InteractionEvent(
            user_id="bot_account_123",
            post_id="post_456",
            interaction_type="LIKE",
            timestamp=time.time()
        )
        is_valid, reason = self.validator.is_valid(event)
        self.assertFalse(is_valid)
        self.assertIn("Bot", reason)
    
    def test_invalid_type(self):
        event = InteractionEvent(
            user_id="user_123",
            post_id="post_456",
            interaction_type="INVALID_TYPE",
            timestamp=time.time()
        )
        is_valid, reason = self.validator.is_valid(event)
        self.assertFalse(is_valid)

if __name__ == '__main__':
    unittest.main()
