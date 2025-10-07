import json
import asyncio
from datetime import datetime
from typing import List, Dict, Any
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import uvicorn
import sys
sys.path.append('..')

from common.tracing import global_tracer

app = FastAPI(title="StreamSocial Headers Dashboard")

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

@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>StreamSocial Headers & Tracing Dashboard</title>
        <style>
            body { 
                font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; 
                margin: 0; 
                background: linear-gradient(135deg, #0f0f23 0%, #1a1a2e 50%, #16213e 100%);
                color: #e2e8f0;
                min-height: 100vh;
            }
            .header { 
                background: linear-gradient(135deg, #1e3a8a 0%, #3b82f6 50%, #06b6d4 100%); 
                color: white; 
                padding: 30px 20px; 
                text-align: center;
                box-shadow: 0 4px 20px rgba(0,0,0,0.3);
            }
            .header h1 { margin: 0; font-size: 2.5em; font-weight: 700; }
            .header p { margin: 10px 0 0 0; font-size: 1.1em; opacity: 0.9; }
            .container { max-width: 1400px; margin: 0 auto; padding: 30px 20px; }
            .metrics { 
                display: grid; 
                grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); 
                gap: 25px; 
                margin-bottom: 40px; 
            }
            .metric-card { 
                background: linear-gradient(145deg, #1e293b 0%, #334155 100%); 
                padding: 25px; 
                border-radius: 16px; 
                box-shadow: 0 8px 32px rgba(0,0,0,0.3);
                border: 1px solid rgba(148, 163, 184, 0.1);
                transition: transform 0.3s ease, box-shadow 0.3s ease;
            }
            .metric-card:hover {
                transform: translateY(-4px);
                box-shadow: 0 12px 40px rgba(0,0,0,0.4);
            }
            .metric-value { 
                font-size: 2.8em; 
                font-weight: 800; 
                background: linear-gradient(135deg, #06b6d4 0%, #3b82f6 100%);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                background-clip: text;
                margin-bottom: 8px;
            }
            .metric-label { 
                color: #94a3b8; 
                font-size: 1.1em;
                font-weight: 500;
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }
            .traces-container { 
                background: linear-gradient(145deg, #1e293b 0%, #334155 100%); 
                border-radius: 16px; 
                box-shadow: 0 8px 32px rgba(0,0,0,0.3);
                border: 1px solid rgba(148, 163, 184, 0.1);
                overflow: hidden;
            }
            .traces-header { 
                background: linear-gradient(135deg, #374151 0%, #4b5563 100%); 
                padding: 20px 25px; 
                border-bottom: 1px solid rgba(148, 163, 184, 0.2);
            }
            .traces-header h3 { 
                margin: 0; 
                color: #f1f5f9; 
                font-size: 1.4em; 
                font-weight: 600;
            }
            .trace-item { 
                padding: 20px 25px; 
                border-bottom: 1px solid rgba(148, 163, 184, 0.1); 
                display: grid; 
                grid-template-columns: 200px 180px 1fr 120px; 
                gap: 20px; 
                align-items: center;
                transition: background-color 0.2s ease;
            }
            .trace-item:hover {
                background: rgba(59, 130, 246, 0.05);
            }
            .trace-item:last-child {
                border-bottom: none;
            }
            .trace-id { 
                font-family: 'JetBrains Mono', 'Fira Code', monospace; 
                font-size: 0.9em; 
                background: linear-gradient(135deg, #1e40af 0%, #3b82f6 100%);
                color: white;
                padding: 6px 12px; 
                border-radius: 8px;
                font-weight: 500;
            }
            .service-name { 
                font-weight: 600; 
                color: #f1f5f9;
                font-size: 1.1em;
            }
            .operation { 
                color: #94a3b8; 
                font-style: italic;
                font-size: 1em;
            }
            .duration { 
                font-weight: 700; 
                color: #10b981;
                font-size: 1.1em;
            }
            .status-connected { 
                color: #10b981; 
                font-size: 1.5em;
                text-shadow: 0 0 10px rgba(16, 185, 129, 0.5);
            }
            .status-disconnected { 
                color: #ef4444; 
                font-size: 1.5em;
                text-shadow: 0 0 10px rgba(239, 68, 68, 0.5);
            }
            .service-post-producer { color: #3b82f6; }
            .service-recommendation-consumer { color: #8b5cf6; }
            .service-notification-consumer { color: #06b6d4; }
            .service-analytics-service { color: #f59e0b; }
            .service-cache-service { color: #10b981; }
        </style>
    </head>
    <body>
        <div class="header">
            <h1>🚀 StreamSocial Headers & Tracing Dashboard</h1>
            <p>Real-time monitoring of Kafka message headers and distributed tracing</p>
            <div style="margin-top: 15px; font-size: 0.9em; opacity: 0.8;">
                <span id="connection-status" class="status-disconnected">●</span>
                <span style="margin-left: 8px;">Live Connection</span>
            </div>
        </div>
        
        <div class="container">
            <div class="metrics">
                <div class="metric-card">
                    <div class="metric-value" id="total-traces">0</div>
                    <div class="metric-label">Total Traces</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value" id="active-services">0</div>
                    <div class="metric-label">Active Services</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value" id="avg-duration">0ms</div>
                    <div class="metric-label">Avg Duration</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value" id="error-rate">0%</div>
                    <div class="metric-label">Error Rate</div>
                </div>
            </div>
            
            <div class="traces-container">
                <div class="traces-header">
                    <h3>📊 Recent Traces</h3>
                </div>
                <div id="traces-list">
                    <div style="padding: 40px; text-align: center; color: #666;">
                        Waiting for trace data...
                    </div>
                </div>
            </div>
        </div>

        <script>
            const ws = new WebSocket("ws://localhost:8000/ws");
            
            ws.onopen = function(event) {
                document.getElementById('connection-status').className = 'status-connected';
                document.getElementById('connection-status').textContent = '●';
            };
            
            ws.onclose = function(event) {
                document.getElementById('connection-status').className = 'status-disconnected';
                document.getElementById('connection-status').textContent = '●';
            };
            
            ws.onmessage = function(event) {
                const data = JSON.parse(event.data);
                updateDashboard(data);
            };
            
            function updateDashboard(data) {
                document.getElementById('total-traces').textContent = data.total_traces;
                document.getElementById('active-services').textContent = data.active_services;
                document.getElementById('avg-duration').textContent = data.avg_duration_ms.toFixed(1) + 'ms';
                
                // Calculate error rate
                const errorCount = data.recent_traces.filter(trace => 
                    trace.tags && (trace.tags.error === true || trace.tags.error === 'true')
                ).length;
                const errorRate = data.recent_traces.length > 0 ? (errorCount / data.recent_traces.length * 100) : 0;
                document.getElementById('error-rate').textContent = errorRate.toFixed(1) + '%';
                
                const tracesList = document.getElementById('traces-list');
                tracesList.innerHTML = '';
                
                if (data.recent_traces.length === 0) {
                    tracesList.innerHTML = `
                        <div style="padding: 60px; text-align: center; color: #94a3b8; font-size: 1.1em;">
                            <div style="font-size: 3em; margin-bottom: 20px;">📊</div>
                            <div>Waiting for trace data...</div>
                            <div style="font-size: 0.9em; margin-top: 10px; opacity: 0.7;">Connect to WebSocket to see live traces</div>
                        </div>
                    `;
                    return;
                }
                
                data.recent_traces.forEach(trace => {
                    const traceDiv = document.createElement('div');
                    traceDiv.className = 'trace-item';
                    
                    // Get service color class
                    const serviceClass = getServiceColorClass(trace.service_name);
                    
                    traceDiv.innerHTML = `
                        <div class="trace-id">${trace.trace_id.substring(0, 8)}...</div>
                        <div class="service-name ${serviceClass}">${trace.service_name}</div>
                        <div class="operation">${trace.operation_name}</div>
                        <div class="duration">${trace.duration_ms.toFixed(1)}ms</div>
                    `;
                    tracesList.appendChild(traceDiv);
                });
            }
            
            function getServiceColorClass(serviceName) {
                const serviceMap = {
                    'post-producer': 'service-post-producer',
                    'recommendation-consumer': 'service-recommendation-consumer',
                    'notification-consumer': 'service-notification-consumer',
                    'analytics-service': 'service-analytics-service',
                    'cache-service': 'service-cache-service'
                };
                return serviceMap[serviceName] || '';
            }
        </script>
    </body>
    </html>
    """

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # Create some sample traces for demonstration
        from common.tracing import Span
        import uuid
        import time
        
        # Add sample traces if none exist
        if len(global_tracer.get_traces()) == 0:
            sample_traces = [
                Span(
                    trace_id=str(uuid.uuid4()),
                    span_id=str(uuid.uuid4()),
                    operation_name="publish_post",
                    service_name="post-producer",
                    start_time=time.time() - 10,
                    end_time=time.time() - 9.5,
                    tags={"user_id": "alice", "experiment_id": "feed_algorithm_v2"}
                ),
                Span(
                    trace_id=str(uuid.uuid4()),
                    span_id=str(uuid.uuid4()),
                    operation_name="process_recommendation",
                    service_name="recommendation-consumer",
                    start_time=time.time() - 8,
                    end_time=time.time() - 7.2,
                    tags={"user_id": "alice", "features": "enhanced_recommendations"}
                ),
                Span(
                    trace_id=str(uuid.uuid4()),
                    span_id=str(uuid.uuid4()),
                    operation_name="send_notification",
                    service_name="notification-consumer",
                    start_time=time.time() - 6,
                    end_time=time.time() - 5.8,
                    tags={"user_id": "alice", "notification_type": "post_liked"}
                ),
                Span(
                    trace_id=str(uuid.uuid4()),
                    span_id=str(uuid.uuid4()),
                    operation_name="publish_post",
                    service_name="post-producer",
                    start_time=time.time() - 4,
                    end_time=time.time() - 3.5,
                    tags={"user_id": "bob", "experiment_id": None}
                ),
                Span(
                    trace_id=str(uuid.uuid4()),
                    span_id=str(uuid.uuid4()),
                    operation_name="process_recommendation",
                    service_name="recommendation-consumer",
                    start_time=time.time() - 2,
                    end_time=time.time() - 1.8,
                    tags={"user_id": "bob", "features": "new_feed_algorithm"}
                )
            ]
            
            for trace in sample_traces:
                global_tracer.record_span(trace)
        
        while True:
            # Send periodic updates
            traces = global_tracer.get_traces()
            
            # Calculate metrics
            total_traces = len(traces)
            active_services = len(set(trace.service_name for trace in traces))
            avg_duration = sum(trace.duration_ms() for trace in traces) / max(len(traces), 1)
            
            # Get recent traces (last 10)
            recent_traces = sorted(traces, key=lambda x: x.start_time, reverse=True)[:10]
            
            update_data = {
                "total_traces": total_traces,
                "active_services": active_services,
                "avg_duration_ms": avg_duration,
                "recent_traces": [
                    {
                        "trace_id": trace.trace_id,
                        "service_name": trace.service_name,
                        "operation_name": trace.operation_name,
                        "duration_ms": trace.duration_ms(),
                        "tags": trace.tags
                    }
                    for trace in recent_traces
                ]
            }
            
            await manager.broadcast(update_data)
            await asyncio.sleep(2)  # Update every 2 seconds
            
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@app.get("/api/traces")
async def get_traces():
    traces = global_tracer.get_traces()
    return {
        "traces": [
            {
                "trace_id": trace.trace_id,
                "span_id": trace.span_id,
                "service_name": trace.service_name,
                "operation_name": trace.operation_name,
                "duration_ms": trace.duration_ms(),
                "start_time": trace.start_time,
                "tags": trace.tags
            }
            for trace in traces
        ]
    }

@app.get("/api/clear")
async def clear_traces():
    global_tracer.clear()
    return {"message": "Traces cleared"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
