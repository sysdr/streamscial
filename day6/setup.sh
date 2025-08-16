#!/bin/bash

# StreamSocial Day 6: Consumer Groups & Scalability Implementation
# Auto-setup script for Kafka consumer group scaling demonstration

set -e

PROJECT_NAME="streamsocial-day6"
PYTHON_VERSION="3.11"
KAFKA_VERSION="2.13-3.7.0"

echo "🚀 StreamSocial Day 6: Consumer Groups & Scalability Setup"
echo "=================================================="

# Create project structure
echo "📁 Creating project structure..."
mkdir -p $PROJECT_NAME/{src/{consumers,monitoring,producers,utils,config},tests,docker,scripts}
cd $PROJECT_NAME

# Create Python virtual environment
echo "🐍 Setting up Python $PYTHON_VERSION virtual environment..."
python3.11 -m venv venv
source venv/bin/activate

# Create requirements.txt
cat > requirements.txt << 'EOF'
kafka-python==2.0.2
confluent-kafka==2.3.0
fastapi==0.104.1
uvicorn==0.24.0
prometheus-client==0.19.0
redis==5.0.1
psutil==5.9.6
aiofiles==23.2.1
pydantic==2.5.0
python-multipart==0.0.6
jinja2==3.1.2
websockets==12.0
asyncio-mqtt==0.16.1
structlog==23.2.0
typer==0.9.0
rich==13.7.0
pytest==7.4.3
pytest-asyncio==0.21.1
docker==6.1.3
EOF

# Install dependencies
echo "📦 Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Create Docker Compose for Kafka
cat > docker/docker-compose.yml << 'EOF'
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    hostname: broker
    container_name: broker
    depends_on:
      - zookeeper
    ports:
      - "29092:29092"
      - "9092:9092"
      - "9101:9101"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_JMX_PORT: 9101
      KAFKA_JMX_HOSTNAME: localhost
      KAFKA_NUM_PARTITIONS: 12

  redis:
    image: redis:7.2-alpine
    hostname: redis
    container_name: redis
    ports:
      - "6379:6379"
    command: redis-server --appendonly yes

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    depends_on:
      - kafka
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: broker:29092
EOF

# Configuration files
cat > src/config/kafka_config.py << 'EOF'
import os
from dataclasses import dataclass
from typing import Dict, List

@dataclass
class KafkaConfig:
    bootstrap_servers: str = "localhost:9092"
    topics: Dict[str, int] = None
    consumer_groups: Dict[str, str] = None
    
    def __post_init__(self):
        if self.topics is None:
            self.topics = {
                "user-activities": 12,
                "generated-feeds": 8,
                "consumer-metrics": 4
            }
        
        if self.consumer_groups is None:
            self.consumer_groups = {
                "feed-generation-workers": "feed-generation-workers",
                "feed-cache-builders": "feed-cache-builders",
                "metrics-collectors": "metrics-collectors"
            }

@dataclass 
class ConsumerConfig:
    group_id: str
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    max_poll_records: int = 100
    session_timeout_ms: int = 30000
    heartbeat_interval_ms: int = 10000
    max_poll_interval_ms: int = 300000
    
config = KafkaConfig()
EOF

# User activity producer
cat > src/producers/activity_producer.py << 'EOF'
import asyncio
import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from typing import Dict, Any
import structlog

logger = structlog.get_logger()

class ActivityProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: str(x).encode('utf-8') if x else None,
            acks='all',
            retries=3,
            batch_size=16384,
            linger_ms=10
        )
        self.user_count = 10000
        self.activity_types = ['like', 'share', 'comment', 'follow', 'post']
        
    def generate_activity(self, user_id: int) -> Dict[str, Any]:
        return {
            "user_id": user_id,
            "activity_type": random.choice(self.activity_types),
            "target_user_id": random.randint(1, self.user_count),
            "content_id": random.randint(1, 100000),
            "timestamp": datetime.utcnow().isoformat(),
            "metadata": {
                "device": random.choice(["mobile", "web", "tablet"]),
                "location": random.choice(["US", "EU", "ASIA"]),
                "session_id": f"session_{random.randint(1, 1000)}"
            }
        }
    
    async def produce_activities(self, rate_per_second: int = 1000, duration_seconds: int = 300):
        """Produce user activities at specified rate"""
        logger.info(f"Starting activity production: {rate_per_second}/sec for {duration_seconds}s")
        
        start_time = time.time()
        total_sent = 0
        
        while time.time() - start_time < duration_seconds:
            batch_start = time.time()
            
            # Send batch of activities
            for _ in range(rate_per_second):
                user_id = random.randint(1, self.user_count)
                activity = self.generate_activity(user_id)
                
                # Partition by user_id for consistent assignment
                self.producer.send(
                    'user-activities',
                    key=user_id,
                    value=activity
                )
                total_sent += 1
            
            # Rate limiting
            batch_duration = time.time() - batch_start
            if batch_duration < 1.0:
                await asyncio.sleep(1.0 - batch_duration)
                
            if total_sent % 5000 == 0:
                logger.info(f"Produced {total_sent} activities")
        
        self.producer.flush()
        logger.info(f"Production complete: {total_sent} total activities")
    
    def close(self):
        self.producer.close()

if __name__ == "__main__":
    import sys
    rate = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    duration = int(sys.argv[2]) if len(sys.argv) > 2 else 60
    
    producer = ActivityProducer()
    asyncio.run(producer.produce_activities(rate, duration))
    producer.close()
EOF

# Feed generation consumer
cat > src/consumers/feed_consumer.py << 'EOF'
import asyncio
import json
import time
import random
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from typing import Dict, List, Set
import structlog
import redis
import psutil
import os
from dataclasses import dataclass

logger = structlog.get_logger()

@dataclass
class ConsumerMetrics:
    consumer_id: str
    assigned_partitions: List[int]
    messages_processed: int = 0
    processing_rate: float = 0.0
    lag_total: int = 0
    last_update: float = 0.0

class FeedGenerationConsumer:
    def __init__(self, consumer_id: str, group_id: str = "feed-generation-workers"):
        self.consumer_id = consumer_id
        self.group_id = group_id
        self.bootstrap_servers = "localhost:9092"
        
        # Redis for caching social graph
        self.redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
        
        # Kafka consumer
        self.consumer = KafkaConsumer(
            'user-activities',
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            max_poll_records=100,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda m: int(m.decode('utf-8')) if m else None,
            consumer_timeout_ms=1000
        )
        
        # Producer for generated feeds
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: str(x).encode('utf-8'),
            acks='all'
        )
        
        # Metrics tracking
        self.metrics = ConsumerMetrics(consumer_id, [])
        self.start_time = time.time()
        self.message_count = 0
        self.last_metrics_time = time.time()
        
    def get_social_graph(self, user_id: int) -> Set[int]:
        """Get user's social connections from cache"""
        cached = self.redis_client.smembers(f"friends:{user_id}")
        if cached:
            return {int(uid) for uid in cached}
        
        # Simulate social graph for demo
        friends = set(random.sample(range(1, 10000), random.randint(50, 200)))
        self.redis_client.sadd(f"friends:{user_id}", *friends)
        self.redis_client.expire(f"friends:{user_id}", 3600)
        return friends
    
    def generate_personalized_feed(self, user_id: int, activity: Dict) -> Dict:
        """Generate personalized feed based on user activity"""
        friends = self.get_social_graph(user_id)
        
        # Simple feed generation logic
        feed_items = []
        if activity['activity_type'] == 'post':
            # Propagate to all friends
            for friend_id in friends:
                feed_items.append({
                    "feed_user_id": friend_id,
                    "content_type": "friend_post",
                    "content_id": activity['content_id'],
                    "author_id": user_id,
                    "score": random.uniform(0.5, 1.0),
                    "timestamp": activity['timestamp']
                })
        
        elif activity['activity_type'] in ['like', 'share', 'comment']:
            # Show engagement to subset of friends
            sample_size = min(len(friends), 20)
            selected_friends = random.sample(list(friends), sample_size)
            
            for friend_id in selected_friends:
                feed_items.append({
                    "feed_user_id": friend_id,
                    "content_type": f"friend_{activity['activity_type']}",
                    "content_id": activity['content_id'],
                    "author_id": user_id,
                    "score": random.uniform(0.3, 0.8),
                    "timestamp": activity['timestamp']
                })
        
        return {
            "generated_feeds": feed_items,
            "processing_time_ms": random.randint(10, 50),
            "consumer_id": self.consumer_id,
            "source_activity": activity
        }
    
    def publish_metrics(self):
        """Publish consumer metrics"""
        current_time = time.time()
        duration = current_time - self.last_metrics_time
        
        if duration >= 5.0:  # Every 5 seconds
            self.metrics.processing_rate = self.message_count / duration
            self.metrics.messages_processed = self.message_count
            self.metrics.last_update = current_time
            
            # Get system metrics
            cpu_percent = psutil.cpu_percent()
            memory_percent = psutil.virtual_memory().percent
            
            metrics_data = {
                "consumer_id": self.consumer_id,
                "group_id": self.group_id,
                "assigned_partitions": self.metrics.assigned_partitions,
                "messages_processed": self.metrics.messages_processed,
                "processing_rate": self.metrics.processing_rate,
                "cpu_percent": cpu_percent,
                "memory_percent": memory_percent,
                "uptime_seconds": current_time - self.start_time,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            self.producer.send('consumer-metrics', value=metrics_data)
            
            logger.info(
                "Consumer metrics",
                consumer_id=self.consumer_id,
                rate=self.metrics.processing_rate,
                total=self.metrics.messages_processed,
                partitions=self.metrics.assigned_partitions
            )
            
            self.last_metrics_time = current_time
            self.message_count = 0
    
    async def process_messages(self):
        """Main processing loop"""
        logger.info(f"Starting consumer {self.consumer_id} in group {self.group_id}")
        
        try:
            for message in self.consumer:
                # Update partition assignment
                current_partitions = list(self.consumer.assignment())
                self.metrics.assigned_partitions = [tp.partition for tp in current_partitions]
                
                # Process activity
                user_id = message.key
                activity = message.value
                
                # Generate personalized feeds
                feed_result = self.generate_personalized_feed(user_id, activity)
                
                # Publish generated feeds
                for feed_item in feed_result['generated_feeds']:
                    self.producer.send(
                        'generated-feeds',
                        key=feed_item['feed_user_id'],
                        value=feed_item
                    )
                
                # Commit offset
                self.consumer.commit()
                
                self.message_count += 1
                self.publish_metrics()
                
        except KeyboardInterrupt:
            logger.info(f"Shutting down consumer {self.consumer_id}")
        finally:
            self.consumer.close()
            self.producer.close()

if __name__ == "__main__":
    import sys
    import uuid
    
    consumer_id = sys.argv[1] if len(sys.argv) > 1 else f"consumer-{uuid.uuid4().hex[:8]}"
    group_id = sys.argv[2] if len(sys.argv) > 2 else "feed-generation-workers"
    
    consumer = FeedGenerationConsumer(consumer_id, group_id)
    asyncio.run(consumer.process_messages())
EOF

# Consumer group manager
cat > src/utils/consumer_manager.py << 'EOF'
import asyncio
import subprocess
import time
import signal
import os
from typing import List, Dict
import structlog
import psutil

logger = structlog.get_logger()

class ConsumerGroupManager:
    def __init__(self, group_id: str = "feed-generation-workers"):
        self.group_id = group_id
        self.consumers: Dict[str, subprocess.Popen] = {}
        self.target_count = 1
        
    async def scale_to(self, target_count: int):
        """Scale consumer group to target instance count"""
        logger.info(f"Scaling consumer group to {target_count} instances")
        
        current_count = len(self.consumers)
        
        if target_count > current_count:
            # Scale up
            for i in range(current_count, target_count):
                await self.start_consumer(f"consumer-{i:03d}")
                await asyncio.sleep(1)  # Stagger startup
                
        elif target_count < current_count:
            # Scale down
            consumers_to_stop = list(self.consumers.keys())[target_count:]
            for consumer_id in consumers_to_stop:
                await self.stop_consumer(consumer_id)
                
        self.target_count = target_count
        logger.info(f"Scaling complete: {len(self.consumers)} active consumers")
    
    async def start_consumer(self, consumer_id: str):
        """Start a single consumer instance"""
        cmd = [
            "python", "-m", "src.consumers.feed_consumer",
            consumer_id, self.group_id
        ]
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid
        )
        
        self.consumers[consumer_id] = process
        logger.info(f"Started consumer {consumer_id} (PID: {process.pid})")
    
    async def stop_consumer(self, consumer_id: str):
        """Stop a single consumer instance"""
        if consumer_id in self.consumers:
            process = self.consumers[consumer_id]
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                process.wait(timeout=10)
            except (subprocess.TimeoutExpired, ProcessLookupError):
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)
            
            del self.consumers[consumer_id]
            logger.info(f"Stopped consumer {consumer_id}")
    
    async def progressive_scaling_demo(self):
        """Demonstrate progressive scaling from 1 to 100 consumers"""
        scaling_schedule = [1, 2, 5, 10, 25, 50, 100]
        
        for target in scaling_schedule:
            logger.info(f"=== Scaling to {target} consumers ===")
            await self.scale_to(target)
            
            # Let it run for observation
            await asyncio.sleep(30)
            
            # Show current state
            alive_consumers = sum(1 for p in self.consumers.values() if p.poll() is None)
            logger.info(f"Active consumers: {alive_consumers}/{target}")
    
    async def shutdown_all(self):
        """Shutdown all consumer instances"""
        logger.info("Shutting down all consumers...")
        
        for consumer_id in list(self.consumers.keys()):
            await self.stop_consumer(consumer_id)
        
        logger.info("All consumers stopped")

if __name__ == "__main__":
    manager = ConsumerGroupManager()
    
    try:
        asyncio.run(manager.progressive_scaling_demo())
    except KeyboardInterrupt:
        logger.info("Demo interrupted")
    finally:
        asyncio.run(manager.shutdown_all())
EOF

# Web dashboard
cat > src/monitoring/dashboard.py << 'EOF'
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
EOF

# Create dashboard template
mkdir -p templates
cat > templates/dashboard.html << 'EOF'
<!DOCTYPE html>
<html>
<head>
    <title>StreamSocial Consumer Group Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { background: #2c3e50; color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
        .metrics-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin-bottom: 20px; }
        .metric-card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .metric-value { font-size: 2em; font-weight: bold; color: #3498db; }
        .consumer-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 15px; }
        .consumer-card { background: white; padding: 15px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); border-left: 4px solid #27ae60; }
        .chart-container { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 20px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>StreamSocial Consumer Group Dashboard</h1>
            <p>Real-time monitoring of Kafka consumer group scaling</p>
        </div>
        
        <div class="metrics-grid">
            <div class="metric-card">
                <h3>Total Consumers</h3>
                <div class="metric-value" id="total-consumers">0</div>
            </div>
            <div class="metric-card">
                <h3>Messages Processed</h3>
                <div class="metric-value" id="total-messages">0</div>
            </div>
            <div class="metric-card">
                <h3>Processing Rate</h3>
                <div class="metric-value" id="total-rate">0/sec</div>
            </div>
            <div class="metric-card">
                <h3>Avg CPU Usage</h3>
                <div class="metric-value" id="avg-cpu">0%</div>
            </div>
        </div>
        
        <div class="chart-container">
            <canvas id="throughputChart" width="400" height="200"></canvas>
        </div>
        
        <h2>Consumer Instances</h2>
        <div class="consumer-grid" id="consumer-grid">
        </div>
    </div>

    <script>
        const ctx = document.getElementById('throughputChart').getContext('2d');
        const chart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: 'Messages/sec',
                    data: [],
                    borderColor: '#3498db',
                    backgroundColor: 'rgba(52, 152, 219, 0.1)',
                    fill: true
                }]
            },
            options: {
                responsive: true,
                scales: {
                    y: { beginAtZero: true }
                },
                plugins: {
                    title: {
                        display: true,
                        text: 'Consumer Group Throughput'
                    }
                }
            }
        });

        const ws = new WebSocket('ws://localhost:8000/ws');
        
        ws.onmessage = function(event) {
            const data = JSON.parse(event.data);
            updateMetrics(data.summary);
            updateConsumers(data.consumers);
            updateChart(data.summary.total_rate);
        };
        
        function updateMetrics(summary) {
            document.getElementById('total-consumers').textContent = summary.total_consumers;
            document.getElementById('total-messages').textContent = summary.total_messages.toLocaleString();
            document.getElementById('total-rate').textContent = summary.total_rate.toFixed(1) + '/sec';
            document.getElementById('avg-cpu').textContent = summary.avg_cpu.toFixed(1) + '%';
        }
        
        function updateConsumers(consumers) {
            const grid = document.getElementById('consumer-grid');
            grid.innerHTML = '';
            
            Object.values(consumers).forEach(consumer => {
                const card = document.createElement('div');
                card.className = 'consumer-card';
                card.innerHTML = `
                    <h4>${consumer.consumer_id}</h4>
                    <p><strong>Partitions:</strong> ${consumer.assigned_partitions.join(', ')}</p>
                    <p><strong>Messages:</strong> ${consumer.messages_processed.toLocaleString()}</p>
                    <p><strong>Rate:</strong> ${consumer.processing_rate.toFixed(1)}/sec</p>
                    <p><strong>CPU:</strong> ${consumer.cpu_percent.toFixed(1)}%</p>
                `;
                grid.appendChild(card);
            });
        }
        
        function updateChart(rate) {
            const now = new Date().toLocaleTimeString();
            chart.data.labels.push(now);
            chart.data.datasets[0].data.push(rate);
            
            if (chart.data.labels.length > 20) {
                chart.data.labels.shift();
                chart.data.datasets[0].data.shift();
            }
            
            chart.update();
        }
    </script>
</body>
</html>
EOF

# Test files
cat > tests/test_consumer_scaling.py << 'EOF'
import pytest
import asyncio
import time
from src.utils.consumer_manager import ConsumerGroupManager
from src.producers.activity_producer import ActivityProducer

@pytest.mark.asyncio
async def test_consumer_scaling():
    """Test consumer group scaling functionality"""
    manager = ConsumerGroupManager("test-scaling-group")
    
    try:
        # Test scaling up
        await manager.scale_to(3)
        assert len(manager.consumers) == 3
        
        # Wait for stabilization
        await asyncio.sleep(10)
        
        # Test scaling down
        await manager.scale_to(1)
        assert len(manager.consumers) == 1
        
    finally:
        await manager.shutdown_all()

@pytest.mark.asyncio
async def test_activity_production():
    """Test activity producer functionality"""
    producer = ActivityProducer()
    
    try:
        # Produce small batch
        await producer.produce_activities(rate_per_second=10, duration_seconds=5)
        
    finally:
        producer.close()

def test_consumer_metrics():
    """Test consumer metrics collection"""
    from src.consumers.feed_consumer import ConsumerMetrics
    
    metrics = ConsumerMetrics("test-consumer", [0, 1, 2])
    assert metrics.consumer_id == "test-consumer"
    assert metrics.assigned_partitions == [0, 1, 2]
    assert metrics.messages_processed == 0
EOF

# Docker setup
cat > docker/Dockerfile << 'EOF'
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY templates/ ./templates/

EXPOSE 8000

CMD ["python", "-m", "src.monitoring.dashboard"]
EOF

# Main start script
cat > start.sh << 'EOF'
#!/bin/bash

echo "🚀 Starting StreamSocial Consumer Groups Demo"

# Start infrastructure
echo "📦 Starting Kafka infrastructure..."
cd docker && docker-compose up -d
cd ..

# Wait for services
echo "⏳ Waiting for services to be ready..."
sleep 30

# Activate virtual environment
source venv/bin/activate

# Create Kafka topics
echo "📝 Creating Kafka topics..."
docker exec broker kafka-topics --create --topic user-activities --partitions 12 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists
docker exec broker kafka-topics --create --topic generated-feeds --partitions 8 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists
docker exec broker kafka-topics --create --topic consumer-metrics --partitions 4 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists

# Start monitoring dashboard
echo "📊 Starting monitoring dashboard..."
python -m src.monitoring.dashboard &
DASHBOARD_PID=$!

# Wait for dashboard
sleep 5

# Run tests
echo "🧪 Running tests..."
pytest tests/ -v

# Start activity producer
echo "📤 Starting activity producer..."
python -m src.producers.activity_producer 500 300 &
PRODUCER_PID=$!

# Start consumer scaling demo
echo "🔄 Starting consumer scaling demonstration..."
python -m src.utils.consumer_manager &
MANAGER_PID=$!

echo ""
echo "✅ Demo is running!"
echo "🌐 Dashboard: http://localhost:8000"
echo "🔧 Kafka UI: http://localhost:8080"
echo ""
echo "Press Ctrl+C to stop all services"

# Store PIDs for cleanup
echo $DASHBOARD_PID > .dashboard.pid
echo $PRODUCER_PID > .producer.pid
echo $MANAGER_PID > .manager.pid

wait
EOF

chmod +x start.sh

# Stop script
cat > stop.sh << 'EOF'
#!/bin/bash

echo "🛑 Stopping StreamSocial Consumer Groups Demo"

# Kill background processes
if [ -f .dashboard.pid ]; then
    kill $(cat .dashboard.pid) 2>/dev/null
    rm .dashboard.pid
fi

if [ -f .producer.pid ]; then
    kill $(cat .producer.pid) 2>/dev/null
    rm .producer.pid
fi

if [ -f .manager.pid ]; then
    kill $(cat .manager.pid) 2>/dev/null
    rm .manager.pid
fi

# Stop all consumer processes
pkill -f "src.consumers.feed_consumer"

# Stop Docker services
cd docker && docker-compose down
cd ..

echo "✅ All services stopped"
EOF

chmod +x stop.sh

# Build and test script
cat > build_test.sh << 'EOF'
#!/bin/bash

echo "🔨 Building and testing StreamSocial Consumer Groups"

# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run syntax checks
echo "📝 Checking Python syntax..."
python -m py_compile src/consumers/feed_consumer.py
python -m py_compile src/producers/activity_producer.py
python -m py_compile src/utils/consumer_manager.py
python -m py_compile src/monitoring/dashboard.py

# Start minimal infrastructure for testing
cd docker && docker-compose up -d zookeeper kafka redis
cd ..

sleep 20

# Create test topics
docker exec broker kafka-topics --create --topic user-activities --partitions 12 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists
docker exec broker kafka-topics --create --topic generated-feeds --partitions 8 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists
docker exec broker kafka-topics --create --topic consumer-metrics --partitions 4 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists

# Run tests
echo "🧪 Running unit tests..."
pytest tests/ -v

# Quick functional test
echo "🔍 Running functional test..."
timeout 30 python -m src.producers.activity_producer 10 5 &
sleep 2
timeout 30 python -m src.consumers.feed_consumer test-consumer &
sleep 10

echo "✅ Build and test complete"

# Cleanup
cd docker && docker-compose down
cd ..
EOF

chmod +x build_test.sh

echo "✅ StreamSocial Day 6 setup complete!"
echo ""
echo "📁 Project structure created:"
find . -type f -name "*.py" -o -name "*.sh" -o -name "*.yml" -o -name "*.html" | head -20
echo ""
echo "🚀 To start the demo:"
echo "   ./start.sh"
echo ""
echo "🔨 To build and test:"
echo "   ./build_test.sh"
echo ""
echo "🛑 To stop:"
echo "   ./stop.sh"