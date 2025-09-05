import json
import asyncio
from typing import Dict, Any
from datetime import datetime
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import structlog

from src.producers.timeline_producer import TimelineProducer, TimelineMessage
from src.consumers.timeline_consumer import TimelineConsumer
from src.utils.partition_analyzer import PartitionAnalyzer
from config.app_config import settings

logger = structlog.get_logger(__name__)

app = FastAPI(title=settings.app_name)
templates = Jinja2Templates(directory="src/web/templates")

# Global instances
timeline_producer = None
timeline_consumer = None
partition_analyzer = PartitionAnalyzer()

@app.on_event("startup")
async def startup_event():
    global timeline_producer, timeline_consumer
    timeline_producer = TimelineProducer()
    timeline_consumer = TimelineConsumer()
    logger.info("Dashboard services initialized")

@app.on_event("shutdown")
async def shutdown_event():
    global timeline_producer, timeline_consumer
    if timeline_producer:
        timeline_producer.close()
    if timeline_consumer:
        timeline_consumer.close()
    logger.info("Dashboard services closed")

@app.get("/", response_class=HTMLResponse)
async def dashboard_home(request: Request):
    """Main dashboard page"""
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "title": "StreamSocial Timeline Ordering Dashboard"
    })

@app.post("/api/send-message")
async def send_timeline_message(message_data: Dict[str, Any]):
    """Send a new timeline message"""
    try:
        message = TimelineMessage(
            user_id=message_data['user_id'],
            post_id=message_data.get('post_id', f"post_{datetime.now().timestamp()}"),
            content=message_data['content']
        )
        
        success = timeline_producer.send_timeline_message(message)
        
        return {
            "success": success,
            "message": message.to_dict(),
            "partition_key": timeline_producer.calculate_partition_key(message.user_id),
            "target_partition": timeline_producer.get_partition_for_key(
                timeline_producer.calculate_partition_key(message.user_id)
            )
        }
    except Exception as e:
        logger.error("Error sending message", error=str(e))
        return {"success": False, "error": str(e)}

@app.get("/api/user-timeline/{user_id}")
async def get_user_timeline(user_id: str):
    """Get chronologically ordered timeline for user"""
    # Consume latest messages first
    timeline_consumer.consume_messages(timeout_ms=1000)
    
    timeline = timeline_consumer.get_user_timeline(user_id)
    
    return {
        "user_id": user_id,
        "message_count": len(timeline),
        "timeline": timeline,
        "partition_info": {
            "partition_key": timeline_producer.calculate_partition_key(user_id),
            "target_partition": timeline_producer.get_partition_for_key(
                timeline_producer.calculate_partition_key(user_id)
            )
        }
    }

@app.get("/api/partition-analysis")
async def get_partition_analysis():
    """Get partition distribution analysis"""
    # Get all sent messages
    all_user_ids = list(set(
        msg['message']['user_id'] for msg in timeline_producer.sent_messages
    ))
    
    if not all_user_ids:
        return {"message": "No messages sent yet"}
    
    # Analyze distribution
    keys = [f"user:{user_id}" for user_id in all_user_ids]
    analysis = partition_analyzer.analyze_key_distribution(keys)
    
    # Get consumer partition stats
    partition_stats = timeline_consumer.get_partition_statistics()
    
    return {
        "key_distribution": analysis,
        "consumer_partition_stats": partition_stats,
        "optimization_suggestions": partition_analyzer.suggest_optimization(analysis)
    }

@app.get("/api/ordering-verification")
async def verify_message_ordering():
    """Verify timeline message ordering consistency"""
    # Consume latest messages
    timeline_consumer.consume_messages(timeout_ms=1000)
    
    # Verify ordering
    verification_results = timeline_consumer.verify_ordering_consistency()
    
    return {
        "verification_results": verification_results,
        "timestamp": datetime.now().isoformat()
    }

@app.get("/api/system-stats")
async def get_system_stats():
    """Get overall system statistics"""
    # Consume latest messages
    consumption_stats = timeline_consumer.consume_messages(timeout_ms=1000)
    
    return {
        "producer_stats": {
            "total_sent": len(timeline_producer.sent_messages),
            "unique_users": len(set(
                msg['message']['user_id'] for msg in timeline_producer.sent_messages
            ))
        },
        "consumer_stats": {
            "total_consumed": len(timeline_consumer.consumed_messages),
            "unique_users": len(timeline_consumer.user_timelines),
            "partition_distribution": timeline_consumer.get_partition_statistics()
        },
        "recent_consumption": consumption_stats
    }

@app.websocket("/ws/live-updates")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for live dashboard updates"""
    await websocket.accept()
    
    try:
        while True:
            # Consume new messages
            consumption_stats = timeline_consumer.consume_messages(timeout_ms=1000)
            
            if consumption_stats['messages_consumed'] > 0:
                # Send live update
                update_data = {
                    "type": "message_update",
                    "stats": consumption_stats,
                    "timestamp": datetime.now().isoformat()
                }
                await websocket.send_text(json.dumps(update_data))
            
            await asyncio.sleep(2)
            
    except Exception as e:
        logger.error("WebSocket error", error=str(e))
    finally:
        await websocket.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=settings.host, port=settings.port, reload=settings.debug)
