import asyncio
import json
import os
from datetime import datetime, timedelta
from fastapi import FastAPI, WebSocket, Request
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import redis.asyncio as aioredis
import asyncpg
import structlog
from config.app_config import AppConfig

logger = structlog.get_logger()

app = FastAPI(title="StreamSocial Offset Management Dashboard")
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

class DashboardData:
    def __init__(self):
        self.redis = None
        self.postgres_pool = None
        
    async def initialize(self, config: AppConfig):
        self.redis = aioredis.from_url(
            f"redis://{config.redis.host}:{config.redis.port}/{config.redis.db}"
        )
        self.postgres_pool = await asyncpg.create_pool(
            host=config.postgres.host,
            port=config.postgres.port,
            database=config.postgres.database,
            user=config.postgres.username,
            password=config.postgres.password,
            min_size=2, max_size=5
        )
    
    async def get_offset_status(self):
        """Get current offset processing status"""
        async with self.postgres_pool.acquire() as conn:
            # Get partition offsets
            partitions = await conn.fetch(
                "SELECT partition_id, committed_offset, updated_at FROM offset_checkpoints ORDER BY partition_id"
            )
            
            # Get active ranges from Redis
            active_ranges = []
            keys = await self.redis.keys("offset_range:*")
            for key in keys:
                range_data = await self.redis.get(key)
                if range_data:
                    active_ranges.append(json.loads(range_data))
            
            return {
                "partitions": [dict(p) for p in partitions],
                "active_ranges": active_ranges,
                "total_partitions": len(partitions),
                "processing_ranges": len(active_ranges)
            }
    
    async def get_engagement_metrics(self):
        """Get engagement metrics summary"""
        async with self.postgres_pool.acquire() as conn:
            metrics = await conn.fetch("""
                SELECT metric_type, SUM(metric_value) as total_value, COUNT(*) as user_count
                FROM engagement_metrics 
                GROUP BY metric_type
                ORDER BY total_value DESC
            """)
            
            recent_activity = await conn.fetch("""
                SELECT metric_type, COUNT(*) as events, 
                       MAX(processed_at) as last_update
                FROM engagement_metrics 
                WHERE processed_at > NOW() - INTERVAL '1 hour'
                GROUP BY metric_type
                ORDER BY events DESC
            """)
            
            return {
                "summary": [dict(m) for m in metrics],
                "recent_activity": [dict(r) for r in recent_activity]
            }
    
    async def get_worker_status(self):
        """Get worker heartbeat status"""
        keys = await self.redis.keys("worker_heartbeat:*")
        workers = []
        
        for key in keys:
            worker_id = key.decode().split(":")[-1]
            last_heartbeat = await self.redis.get(key)
            if last_heartbeat:
                workers.append({
                    "worker_id": worker_id,
                    "last_heartbeat": float(last_heartbeat),
                    "status": "active" if datetime.now().timestamp() - float(last_heartbeat) < 30 else "inactive"
                })
        
        return workers

dashboard_data = DashboardData()

async def get_status_aggregated():
    """Aggregate status for API and WebSocket"""
    offset_status = await dashboard_data.get_offset_status()
    engagement_metrics = await dashboard_data.get_engagement_metrics()
    worker_status = await dashboard_data.get_worker_status()
    return {
        "timestamp": datetime.now().isoformat(),
        "offsets": offset_status,
        "metrics": engagement_metrics,
        "workers": worker_status
    }

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.get("/api/status")
async def get_status():
    return await get_status_aggregated()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            status_data = await get_status_aggregated()
            await websocket.send_json(status_data)
            await asyncio.sleep(2)
    except Exception as e:
        logger.error("WebSocket error", error=str(e))

@app.on_event("startup")
async def startup_event():
    try:
        config = AppConfig()
        await dashboard_data.initialize(config)
        logger.info("Dashboard initialized successfully")
    except Exception as e:
        logger.error("Failed to initialize dashboard", error=str(e))
        raise
