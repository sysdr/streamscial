from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import json
import asyncio
from kafka import KafkaConsumer
from datetime import datetime
import threading
from collections import deque, defaultdict
import time
import os

# Get the project root directory (parent of web directory)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TEMPLATE_PATH = os.path.join(PROJECT_ROOT, 'web', 'templates', 'index.html')

app = FastAPI()

# Store recent data for dashboard
recent_profiles = deque(maxlen=50)
recent_actions = deque(maxlen=50)
recent_enriched = deque(maxlen=50)
metrics = {
    'total_profiles': 0,
    'total_actions': 0,
    'total_enriched': 0,
    'join_rate': 0,
    'avg_latency': 0,
    'latency_histogram': defaultdict(int)
}
metrics_lock = threading.Lock()

def consume_profiles():
    consumer = KafkaConsumer(
        'user-profiles',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8'),
        auto_offset_reset='latest',
        group_id='dashboard-profiles'
    )
    
    for message in consumer:
        profile_data = {
            'user_id': message.key,
            'data': message.value,
            'timestamp': datetime.now().isoformat()
        }
        recent_profiles.append(profile_data)
        
        with metrics_lock:
            metrics['total_profiles'] += 1

def consume_actions():
    consumer = KafkaConsumer(
        'user-actions',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8'),
        auto_offset_reset='latest',
        group_id='dashboard-actions'
    )
    
    for message in consumer:
        action_data = {
            'user_id': message.key,
            'data': message.value,
            'timestamp': datetime.now().isoformat()
        }
        recent_actions.append(action_data)
        
        with metrics_lock:
            metrics['total_actions'] += 1

def consume_enriched():
    consumer = KafkaConsumer(
        'enriched-actions',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8'),
        auto_offset_reset='latest',
        group_id='dashboard-enriched'
    )
    
    latency_sum = 0
    count = 0
    
    for message in consumer:
        enriched_data = {
            'user_id': message.key,
            'data': message.value,
            'timestamp': datetime.now().isoformat()
        }
        recent_enriched.append(enriched_data)
        
        latency = message.value.get('enrichment_latency_ms', 0)
        latency_sum += latency
        count += 1
        
        # Update histogram
        latency_bucket = int(latency)
        
        with metrics_lock:
            metrics['total_enriched'] += 1
            metrics['avg_latency'] = round(latency_sum / count, 2)
            metrics['latency_histogram'][latency_bucket] += 1
            
            if count > 10:
                metrics['join_rate'] = round(count / 10, 1)

# Start consumer threads
threading.Thread(target=consume_profiles, daemon=True).start()
threading.Thread(target=consume_actions, daemon=True).start()
threading.Thread(target=consume_enriched, daemon=True).start()

@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    with open(TEMPLATE_PATH, 'r') as f:
        return f.read()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            with metrics_lock:
                data = {
                    'profiles': list(recent_profiles)[-10:],
                    'actions': list(recent_actions)[-10:],
                    'enriched': list(recent_enriched)[-10:],
                    'metrics': metrics.copy()
                }
            
            await websocket.send_json(data)
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        pass

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)
