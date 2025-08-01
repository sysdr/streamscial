import pytest
from datetime import datetime
from backend.src.events.models import BaseEvent, EventType, PostCreatedEvent
from backend.src.events.event_bus import EventBus

@pytest.mark.asyncio
async def test_event_creation():
    event = PostCreatedEvent(
        user_id="test_user",
        data={"content": "Test post"}
    )
    assert event.event_type == EventType.POST_CREATED
    assert event.user_id == "test_user"
    assert event.data["content"] == "Test post"
    assert event.event_id is not None

@pytest.mark.asyncio
async def test_event_bus_publish():
    bus = EventBus()
    event = BaseEvent(
        event_type=EventType.USER_REGISTERED,
        user_id="test_user",
        data={"email": "test@example.com"}
    )
    
    await bus.publish(event)
    assert len(bus.event_store) == 1
    assert bus.event_store[0]["event_type"] == EventType.USER_REGISTERED

@pytest.mark.asyncio 
async def test_event_subscription():
    bus = EventBus()
    received_events = []
    
    async def handler(event):
        received_events.append(event)
    
    bus.subscribe(EventType.POST_CREATED, handler)
    
    event = PostCreatedEvent(
        user_id="test_user",
        data={"content": "Test"}
    )
    
    await bus.publish(event)
    assert len(received_events) == 1
    assert received_events[0].event_type == EventType.POST_CREATED
