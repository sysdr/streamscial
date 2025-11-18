from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from starlette.requests import Request
import httpx
import asyncio
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
import structlog
from src.models import UserProfile, UserInteraction, ContentMetadata, get_session
from sqlalchemy import func

logger = structlog.get_logger()

app = FastAPI(title="StreamSocial ML Training Data Sync Monitor")

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# WebSocket connections
active_connections: List[WebSocket] = []

CONNECT_URL = "http://localhost:8083"
KAFKA_BOOTSTRAP = "localhost:9092"

class MetricsCollector:
    def __init__(self):
        self.metrics_history: List[Dict[str, Any]] = []
        self.max_history = 100
    
    async def collect_connector_metrics(self) -> Dict[str, Any]:
        async with httpx.AsyncClient() as client:
            try:
                # Get all connectors
                response = await client.get(f"{CONNECT_URL}/connectors")
                connectors = response.json() if response.status_code == 200 else []
                
                connector_metrics = []
                for name in connectors:
                    status_resp = await client.get(f"{CONNECT_URL}/connectors/{name}/status")
                    if status_resp.status_code == 200:
                        status = status_resp.json()
                        connector_metrics.append({
                            'name': name,
                            'state': status['connector']['state'],
                            'tasks': [
                                {
                                    'id': t['id'],
                                    'state': t['state'],
                                    'worker_id': t.get('worker_id', 'unknown')
                                }
                                for t in status.get('tasks', [])
                            ]
                        })
                
                return {
                    'timestamp': datetime.utcnow().isoformat(),
                    'connectors': connector_metrics,
                    'total_connectors': len(connectors)
                }
            except Exception as e:
                logger.error("metrics_collection_failed", error=str(e))
                return {
                    'timestamp': datetime.utcnow().isoformat(),
                    'connectors': [],
                    'error': str(e)
                }
    
    def add_metrics(self, metrics: Dict[str, Any]):
        self.metrics_history.append(metrics)
        if len(self.metrics_history) > self.max_history:
            self.metrics_history.pop(0)

metrics_collector = MetricsCollector()

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.get("/api/connectors")
async def get_connectors():
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f"{CONNECT_URL}/connectors?expand=status")
            return response.json()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/connector/{name}/status")
async def get_connector_status(name: str):
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f"{CONNECT_URL}/connectors/{name}/status")
            return response.json()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/connector/{name}/restart")
async def restart_connector(name: str):
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(f"{CONNECT_URL}/connectors/{name}/restart")
            return {"status": "restarted" if response.status_code == 204 else "failed"}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/metrics")
async def get_metrics():
    metrics = await metrics_collector.collect_connector_metrics()
    return metrics

@app.get("/api/metrics/history")
async def get_metrics_history():
    return {"history": metrics_collector.metrics_history}

@app.websocket("/ws/metrics")
async def websocket_metrics(websocket: WebSocket):
    await websocket.accept()
    active_connections.append(websocket)
    
    try:
        while True:
            metrics = await metrics_collector.collect_connector_metrics()
            metrics_collector.add_metrics(metrics)
            
            for connection in active_connections:
                try:
                    await connection.send_json(metrics)
                except Exception:
                    pass
            
            await asyncio.sleep(5)
    except WebSocketDisconnect:
        active_connections.remove(websocket)

@app.get("/api/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "components": {
            "api": "running",
            "websocket_connections": len(active_connections)
        }
    }

@app.get("/api/demo/stats")
async def get_demo_stats():
    """Get overall statistics about demo data"""
    session = get_session()
    try:
        user_count = session.query(func.count(UserProfile.user_id)).scalar()
        content_count = session.query(func.count(ContentMetadata.content_id)).scalar()
        interaction_count = session.query(func.count(UserInteraction.interaction_id)).scalar()
        
        avg_followers = session.query(func.avg(UserProfile.follower_count)).scalar() or 0
        avg_engagement = session.query(func.avg(UserProfile.engagement_rate)).scalar() or 0
        avg_views = session.query(func.avg(ContentMetadata.view_count)).scalar() or 0
        
        return {
            "user_profiles": {
                "total": user_count,
                "avg_followers": round(float(avg_followers), 2),
                "avg_engagement_rate": round(float(avg_engagement), 4)
            },
            "content_metadata": {
                "total": content_count,
                "avg_views": round(float(avg_views), 2)
            },
            "user_interactions": {
                "total": interaction_count
            }
        }
    except Exception as e:
        logger.error("demo_stats_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        session.close()

@app.get("/api/demo/users")
async def get_demo_users(limit: int = 10, offset: int = 0):
    """Get sample user profiles"""
    session = get_session()
    try:
        users = session.query(UserProfile).offset(offset).limit(limit).all()
        return {
            "users": [
                {
                    "user_id": u.user_id,
                    "username": u.username,
                    "email": u.email,
                    "interests": u.interests,
                    "follower_count": u.follower_count,
                    "following_count": u.following_count,
                    "account_age_days": u.account_age_days,
                    "engagement_rate": u.engagement_rate,
                    "created_at": u.created_at.isoformat() if u.created_at else None
                }
                for u in users
            ],
            "total": session.query(func.count(UserProfile.user_id)).scalar()
        }
    except Exception as e:
        logger.error("demo_users_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        session.close()

@app.get("/api/demo/content")
async def get_demo_content(limit: int = 10, offset: int = 0):
    """Get sample content metadata"""
    session = get_session()
    try:
        content = session.query(ContentMetadata).offset(offset).limit(limit).all()
        return {
            "content": [
                {
                    "content_id": c.content_id,
                    "creator_id": c.creator_id,
                    "title": c.title,
                    "category": c.category,
                    "tags": c.tags,
                    "engagement_score": c.engagement_score,
                    "view_count": c.view_count,
                    "created_at": c.created_at.isoformat() if c.created_at else None
                }
                for c in content
            ],
            "total": session.query(func.count(ContentMetadata.content_id)).scalar()
        }
    except Exception as e:
        logger.error("demo_content_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        session.close()

@app.get("/api/demo/interactions")
async def get_demo_interactions(limit: int = 10, offset: int = 0, user_id: Optional[int] = None, content_id: Optional[int] = None):
    """Get sample user interactions"""
    session = get_session()
    try:
        query = session.query(UserInteraction)
        if user_id:
            query = query.filter(UserInteraction.user_id == user_id)
        if content_id:
            query = query.filter(UserInteraction.content_id == content_id)
        
        interactions = query.offset(offset).limit(limit).all()
        return {
            "interactions": [
                {
                    "interaction_id": i.interaction_id,
                    "user_id": i.user_id,
                    "content_id": i.content_id,
                    "interaction_type": i.interaction_type,
                    "duration_seconds": i.duration_seconds,
                    "created_at": i.created_at.isoformat() if i.created_at else None
                }
                for i in interactions
            ],
            "total": query.count()
        }
    except Exception as e:
        logger.error("demo_interactions_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        session.close()

@app.get("/api/demo/user/{user_id}")
async def get_user_details(user_id: int):
    """Get detailed user information"""
    session = get_session()
    try:
        user = session.query(UserProfile).filter(UserProfile.user_id == user_id).first()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        # Get user's interactions
        interactions = session.query(UserInteraction).filter(UserInteraction.user_id == user_id).limit(10).all()
        
        return {
            "user": {
                "user_id": user.user_id,
                "username": user.username,
                "email": user.email,
                "interests": user.interests,
                "follower_count": user.follower_count,
                "following_count": user.following_count,
                "account_age_days": user.account_age_days,
                "engagement_rate": user.engagement_rate,
                "created_at": user.created_at.isoformat() if user.created_at else None,
                "updated_at": user.updated_at.isoformat() if user.updated_at else None
            },
            "recent_interactions": [
                {
                    "interaction_id": i.interaction_id,
                    "content_id": i.content_id,
                    "interaction_type": i.interaction_type,
                    "duration_seconds": i.duration_seconds,
                    "created_at": i.created_at.isoformat() if i.created_at else None
                }
                for i in interactions
            ]
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("user_details_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        session.close()

@app.get("/api/demo/content/{content_id}")
async def get_content_details(content_id: int):
    """Get detailed content information"""
    session = get_session()
    try:
        content = session.query(ContentMetadata).filter(ContentMetadata.content_id == content_id).first()
        if not content:
            raise HTTPException(status_code=404, detail="Content not found")
        
        # Get interactions for this content
        interactions = session.query(UserInteraction).filter(UserInteraction.content_id == content_id).limit(10).all()
        
        return {
            "content": {
                "content_id": content.content_id,
                "creator_id": content.creator_id,
                "title": content.title,
                "category": content.category,
                "tags": content.tags,
                "engagement_score": content.engagement_score,
                "view_count": content.view_count,
                "created_at": content.created_at.isoformat() if content.created_at else None,
                "updated_at": content.updated_at.isoformat() if content.updated_at else None
            },
            "recent_interactions": [
                {
                    "interaction_id": i.interaction_id,
                    "user_id": i.user_id,
                    "interaction_type": i.interaction_type,
                    "duration_seconds": i.duration_seconds,
                    "created_at": i.created_at.isoformat() if i.created_at else None
                }
                for i in interactions
            ]
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("content_details_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        session.close()

@app.get("/api/connectors/detailed")
async def get_detailed_connectors():
    """Get detailed information about all connectors with their tasks"""
    async with httpx.AsyncClient() as client:
        try:
            # Get all connectors
            response = await client.get(f"{CONNECT_URL}/connectors")
            if response.status_code != 200:
                return {"connectors": [], "error": "Failed to fetch connectors"}
            
            connectors_list = response.json()
            detailed_connectors = []
            
            for name in connectors_list:
                try:
                    # Get connector config
                    config_resp = await client.get(f"{CONNECT_URL}/connectors/{name}/config")
                    config = config_resp.json() if config_resp.status_code == 200 else {}
                    
                    # Get connector status
                    status_resp = await client.get(f"{CONNECT_URL}/connectors/{name}/status")
                    status = status_resp.json() if status_resp.status_code == 200 else {}
                    
                    connector_info = {
                        "name": name,
                        "state": status.get("connector", {}).get("state", "UNKNOWN"),
                        "worker_id": status.get("connector", {}).get("worker_id", "unknown"),
                        "config": {
                            "connector_class": config.get("connector.class", "N/A"),
                            "topics": config.get("topics", config.get("topic.prefix", "N/A")),
                            "tasks_max": config.get("tasks.max", "N/A"),
                            "poll_interval_ms": config.get("poll.interval.ms", "N/A")
                        },
                        "tasks": []
                    }
                    
                    # Get detailed task information
                    for task in status.get("tasks", []):
                        task_info = {
                            "id": task.get("id", -1),
                            "state": task.get("state", "UNKNOWN"),
                            "worker_id": task.get("worker_id", "unknown"),
                            "trace": task.get("trace", None)
                        }
                        connector_info["tasks"].append(task_info)
                    
                    detailed_connectors.append(connector_info)
                except Exception as e:
                    logger.error(f"Error fetching connector {name}", error=str(e))
                    continue
            
            # Calculate statistics
            total_connectors = len(detailed_connectors)
            running_tasks = sum(len([t for t in c["tasks"] if t["state"] == "RUNNING"]) for c in detailed_connectors)
            failed_tasks = sum(len([t for t in c["tasks"] if t["state"] == "FAILED"]) for c in detailed_connectors)
            paused_tasks = sum(len([t for t in c["tasks"] if t["state"] == "PAUSED"]) for c in detailed_connectors)
            
            return {
                "connectors": detailed_connectors,
                "statistics": {
                    "total_connectors": total_connectors,
                    "running_tasks": running_tasks,
                    "failed_tasks": failed_tasks,
                    "paused_tasks": paused_tasks,
                    "total_tasks": running_tasks + failed_tasks + paused_tasks
                },
                "timestamp": datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error("detailed_connectors_error", error=str(e))
            return {
                "connectors": [],
                "statistics": {
                    "total_connectors": 0,
                    "running_tasks": 0,
                    "failed_tasks": 0,
                    "paused_tasks": 0,
                    "total_tasks": 0
                },
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }

@app.get("/api/connector/{name}/detailed")
async def get_connector_detailed(name: str):
    """Get detailed information about a specific connector"""
    async with httpx.AsyncClient() as client:
        try:
            # Get connector config
            config_resp = await client.get(f"{CONNECT_URL}/connectors/{name}/config")
            if config_resp.status_code != 200:
                raise HTTPException(status_code=404, detail=f"Connector {name} not found")
            
            config = config_resp.json()
            
            # Get connector status
            status_resp = await client.get(f"{CONNECT_URL}/connectors/{name}/status")
            status = status_resp.json() if status_resp.status_code == 200 else {}
            
            return {
                "name": name,
                "state": status.get("connector", {}).get("state", "UNKNOWN"),
                "worker_id": status.get("connector", {}).get("worker_id", "unknown"),
                "config": config,
                "status": status,
                "tasks": status.get("tasks", []),
                "timestamp": datetime.utcnow().isoformat()
            }
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"connector_detailed_error_{name}", error=str(e))
            raise HTTPException(status_code=500, detail=str(e))
