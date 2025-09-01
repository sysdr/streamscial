import random
import time
import threading
from typing import List
from events.event_types import StreamSocialEvent, EventType
from producers.acks_producer import AcksProducerManager

class StreamSocialEventSimulator:
    def __init__(self, producer_manager: AcksProducerManager):
        self.producer_manager = producer_manager
        self.running = False
        self.thread = None
    
    def _generate_random_event(self) -> StreamSocialEvent:
        """Generate random StreamSocial events for testing"""
        user_id = f"user_{random.randint(1000, 9999)}"
        
        # Weight different event types realistically
        event_weights = {
            # Critical events (5% of traffic)
            EventType.USER_REGISTRATION: 1,
            EventType.PAYMENT_PROCESSING: 2,
            EventType.PREMIUM_SUBSCRIPTION: 2,
            
            # Social events (45% of traffic)
            EventType.POST_CREATION: 10,
            EventType.COMMENT_ADDED: 15,
            EventType.FRIEND_REQUEST: 8,
            EventType.PROFILE_UPDATE: 12,
            
            # Analytics events (50% of traffic)
            EventType.PAGE_VIEW: 25,
            EventType.CLICK_TRACKING: 15,
            EventType.SCROLL_BEHAVIOR: 8,
            EventType.AD_IMPRESSION: 7
        }
        
        event_type = random.choices(
            list(event_weights.keys()),
            weights=list(event_weights.values())
        )[0]
        
        # Generate appropriate data for each event type
        data = self._generate_event_data(event_type, user_id)
        
        if event_type in [EventType.USER_REGISTRATION, EventType.PAYMENT_PROCESSING, EventType.PREMIUM_SUBSCRIPTION]:
            return StreamSocialEvent.create_critical_event(event_type, user_id, data)
        elif event_type in [EventType.POST_CREATION, EventType.COMMENT_ADDED, EventType.FRIEND_REQUEST, EventType.PROFILE_UPDATE]:
            return StreamSocialEvent.create_social_event(event_type, user_id, data)
        else:
            return StreamSocialEvent.create_analytics_event(event_type, user_id, data)
    
    def _generate_event_data(self, event_type: EventType, user_id: str) -> dict:
        """Generate realistic event data based on event type"""
        base_data = {"ip_address": f"192.168.1.{random.randint(1, 254)}"}
        
        if event_type == EventType.USER_REGISTRATION:
            return {**base_data, "email": f"{user_id}@example.com", "plan": "free"}
        elif event_type == EventType.PAYMENT_PROCESSING:
            return {**base_data, "amount": random.randint(999, 9999), "currency": "USD"}
        elif event_type == EventType.POST_CREATION:
            return {**base_data, "post_id": f"post_{random.randint(1000, 9999)}", "content_type": "text"}
        elif event_type == EventType.PAGE_VIEW:
            return {**base_data, "page": f"/feed", "referrer": "https://google.com"}
        else:
            return base_data
    
    def start(self, events_per_second: int = 10):
        """Start generating events at specified rate"""
        self.running = True
        
        def generate_events():
            while self.running:
                event = self._generate_random_event()
                self.producer_manager.send_event(event)
                time.sleep(1.0 / events_per_second)
        
        self.thread = threading.Thread(target=generate_events, daemon=True)
        self.thread.start()
        print(f"Started event simulation at {events_per_second} events/second")
    
    def stop(self):
        """Stop event generation"""
        self.running = False
        if self.thread:
            self.thread.join()
        print("Stopped event simulation")
