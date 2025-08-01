from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import List, Dict, Any
import json
import asyncio
import uuid

from .events.models import BaseEvent, EventType, PostCreatedEvent, CommentAddedEvent
from .events.event_bus import event_bus
from .handlers.feed_handler import feed_handler
from .handlers.notification_handler import notification_handler

app = FastAPI(title="StreamSocial Event System", version="1.0.0")

# WebSocket connections manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_text(json.dumps(message))
            except:
                pass

manager = ConnectionManager()

# Setup event handlers
event_bus.subscribe(EventType.POST_CREATED, feed_handler.handle_post_created)
event_bus.subscribe(EventType.POST_LIKED, feed_handler.handle_post_liked)
event_bus.subscribe(EventType.FOLLOW_INITIATED, notification_handler.handle_follow_initiated)
event_bus.subscribe(EventType.COMMENT_ADDED, notification_handler.handle_comment_added)

# API Models
class CreatePostRequest(BaseModel):
    user_id: str
    content: str
    media_urls: List[str] = []

class LikePostRequest(BaseModel):
    user_id: str
    post_id: str

class FollowUserRequest(BaseModel):
    follower_id: str
    followed_user_id: str

class AddCommentRequest(BaseModel):
    user_id: str
    post_id: str
    post_owner_id: str
    content: str

# API Endpoints
@app.post("/api/events/post")
async def create_post(request: CreatePostRequest):
    event = PostCreatedEvent(
        user_id=request.user_id,
        data={
            'post_id': str(uuid.uuid4()),
            'content': request.content,
            'media_urls': request.media_urls
        }
    )
    
    await event_bus.publish(event)
    await manager.broadcast({
        'type': 'event_published',
        'event_type': event.event_type,
        'event_id': event.event_id
    })
    
    return {'status': 'success', 'event_id': event.event_id}

@app.post("/api/events/like")
async def like_post(request: LikePostRequest):
    event = BaseEvent(
        event_type=EventType.POST_LIKED,
        user_id=request.user_id,
        data={'post_id': request.post_id}
    )
    
    await event_bus.publish(event)
    await manager.broadcast({
        'type': 'event_published',
        'event_type': event.event_type,
        'event_id': event.event_id
    })
    
    return {'status': 'success', 'event_id': event.event_id}

@app.post("/api/events/follow")
async def follow_user(request: FollowUserRequest):
    event = BaseEvent(
        event_type=EventType.FOLLOW_INITIATED,
        user_id=request.follower_id,
        data={'followed_user_id': request.followed_user_id}
    )
    
    await event_bus.publish(event)
    await manager.broadcast({
        'type': 'event_published',
        'event_type': event.event_type,
        'event_id': event.event_id
    })
    
    return {'status': 'success', 'event_id': event.event_id}

@app.post("/api/events/comment")
async def add_comment(request: AddCommentRequest):
    event = CommentAddedEvent(
        user_id=request.user_id,
        data={
            'post_id': request.post_id,
            'post_owner_id': request.post_owner_id,
            'content': request.content
        }
    )
    
    await event_bus.publish(event)
    await manager.broadcast({
        'type': 'event_published',
        'event_type': event.event_type,
        'event_id': event.event_id
    })
    
    return {'status': 'success', 'event_id': event.event_id}

@app.get("/api/events")
async def get_events(event_type: str = None, limit: int = 50):
    events = event_bus.get_events(event_type, limit)
    return {'events': events}

@app.get("/api/stats")
async def get_stats():
    stats = event_bus.get_event_stats()
    feeds = feed_handler.get_all_feeds()
    notifications = notification_handler.get_all_notifications()
    
    return {
        'event_stats': stats,
        'total_events': len(event_bus.event_store),
        'feed_users': len(feeds),
        'notification_users': len(notifications)
    }

@app.get("/api/feed/{user_id}")
async def get_user_feed(user_id: str):
    feed = feed_handler.get_user_feed(user_id)
    return {'feed': feed}

@app.get("/api/notifications/{user_id}")
async def get_user_notifications(user_id: str):
    notifications = notification_handler.get_notifications(user_id)
    return {'notifications': notifications}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            # Keep connection alive
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# Serve static files
app.mount("/", StaticFiles(directory="../frontend/dist", html=True), name="static")
