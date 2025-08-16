from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import json
import asyncio
from kafka import KafkaConsumer
from typing import Dict, List
import structlog

logger = structlog.get_logger()

app = FastAPI(title="StreamSocial Consumer Group Dashboard")
templates = Jinja2Templates(directory="templates")

class MetricsCollector:
    def __init__(self):
        self.consumer_metrics: Dict[str, Dict] = {}
        self.total_messages = 0
        self.total_rate = 0.0
        
    async def collect_metrics(self):
        """Collect metrics from Kafka topic"""
        consumer = KafkaConsumer(
            'consumer-metrics',
            bootstrap_servers='localhost:9092',
            group_id='metrics-dashboard',
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=1000
        )
        
        try:
            for message in consumer:
                metrics = message.value
                consumer_id = metrics['consumer_id']
                self.consumer_metrics[consumer_id] = metrics
                
                # Calculate totals
                self.total_messages = sum(m.get('messages_processed', 0) for m in self.consumer_metrics.values())
                self.total_rate = sum(m.get('processing_rate', 0) for m in self.consumer_metrics.values())
                
                logger.debug("Updated metrics", consumer_count=len(self.consumer_metrics))
                
        except Exception as e:
            logger.error("Metrics collection error", error=str(e))
        finally:
            consumer.close()

metrics_collector = MetricsCollector()

@app.on_event("startup")
async def startup_event():
    # Start metrics collection in background
    asyncio.create_task(metrics_collector.collect_metrics())

@app.get("/")
async def dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.get("/api/metrics")
async def get_metrics():
    return {
        "consumers": metrics_collector.consumer_metrics,
        "summary": {
            "total_consumers": len(metrics_collector.consumer_metrics),
            "total_messages": metrics_collector.total_messages,
            "total_rate": metrics_collector.total_rate,
            "avg_cpu": sum(m.get('cpu_percent', 0) for m in metrics_collector.consumer_metrics.values()) / max(1, len(metrics_collector.consumer_metrics)),
            "avg_memory": sum(m.get('memory_percent', 0) for m in metrics_collector.consumer_metrics.values()) / max(1, len(metrics_collector.consumer_metrics))
        }
    }

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            metrics_data = await get_metrics()
            await websocket.send_text(json.dumps(metrics_data))
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        pass

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
