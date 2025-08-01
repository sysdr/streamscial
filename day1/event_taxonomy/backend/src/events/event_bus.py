import asyncio
from typing import Dict, List, Callable, Any
from .models import BaseEvent
import json
from datetime import datetime

class EventBus:
    def __init__(self):
        self.subscribers: Dict[str, List[Callable]] = {}
        self.event_store = []
        
    async def publish(self, event: BaseEvent):
        # Store event for replay capability
        self.event_store.append(event.model_dump())
        
        # Notify all subscribers for this event type
        event_type = event.event_type
        if event_type in self.subscribers:
            tasks = []
            for handler in self.subscribers[event_type]:
                tasks.append(asyncio.create_task(handler(event)))
            
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
        
        print(f"✅ Published {event_type} event: {event.event_id}")
        
    def subscribe(self, event_type: str, handler: Callable):
        if event_type not in self.subscribers:
            self.subscribers[event_type] = []
        self.subscribers[event_type].append(handler)
        print(f"📡 Subscribed handler to {event_type}")
        
    def get_events(self, event_type: str = None, limit: int = 100):
        events = self.event_store[-limit:]
        if event_type:
            events = [e for e in events if e['event_type'] == event_type]
        return events
        
    def get_event_stats(self):
        stats = {}
        for event in self.event_store:
            event_type = event['event_type']
            stats[event_type] = stats.get(event_type, 0) + 1
        return stats

# Global event bus instance
event_bus = EventBus()
