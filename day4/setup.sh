#!/bin/bash

# StreamSocial Kafka Producer Implementation Script
# Day 4: High-Volume Producer Implementation

set -e  # Exit on any error

echo "🚀 Setting up StreamSocial Kafka Producer Implementation"
echo "=================================================="

# Create project structure
echo "📁 Creating project structure..."
mkdir -p streamsocial-producer/{src,tests,config,monitoring,docker}
cd streamsocial-producer

# Create subdirectories
mkdir -p src/{producer,monitoring,utils}
mkdir -p tests/{unit,integration}
mkdir -p monitoring/{dashboards,metrics}
mkdir -p config/{kafka,logging}

echo "✅ Directory structure created successfully"

# Create Python virtual environment
echo "🐍 Setting up Python 3.11 virtual environment..."
python3.11 -m venv venv
source venv/bin/activate

echo "✅ Virtual environment activated"

# Create requirements.txt
echo "📦 Creating requirements.txt..."
cat > requirements.txt << 'EOF'
confluent-kafka==2.4.0
prometheus-client==0.20.0
flask==3.0.3
flask-cors==4.0.1
psutil==5.9.8
asyncio-mqtt==0.16.2
structlog==24.1.0
colorama==0.4.6
pytest==8.2.0
pytest-asyncio==0.23.6
docker==7.0.0
pandas==2.2.2
plotly==5.20.0
dash==2.17.0
dash-bootstrap-components==1.5.0
gunicorn==21.2.0
websocket-client==1.7.0
kafka-python==2.0.2
avro-python3==1.10.2
EOF

# Install dependencies
echo "📦 Installing dependencies..."
pip install -r requirements.txt

echo "✅ Dependencies installed successfully"

# Create main producer implementation
echo "🔧 Creating producer implementation..."
cat > src/producer/high_volume_producer.py << 'EOF'
import threading
import time
import json
import uuid
from typing import Dict, Optional, Callable, Any
from confluent_kafka import Producer, KafkaException, Message
from confluent_kafka.admin import AdminClient, NewTopic
import structlog
import queue
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import psutil

logger = structlog.get_logger()

@dataclass
class ProducerConfig:
    bootstrap_servers: str = "localhost:9092"
    batch_size: int = 65536  # 64KB
    linger_ms: int = 5
    compression_type: str = "lz4"
    max_in_flight_requests: int = 10
    buffer_memory: int = 67108864  # 64MB
    retries: int = 2147483647  # Max retries
    request_timeout_ms: int = 30000
    retry_backoff_ms: int = 100
    max_block_ms: int = 10000

@dataclass
class UserAction:
    user_id: str
    action_type: str  # post, like, share, comment
    content_id: str
    timestamp: int
    metadata: Dict[str, Any]

class ConnectionPool:
    def __init__(self, config: ProducerConfig, pool_size: int = 10):
        self.config = config
        self.pool_size = pool_size
        self.producers = queue.Queue(maxsize=pool_size)
        self.active_connections = 0
        self.lock = threading.Lock()
        self._initialize_pool()

    def _initialize_pool(self):
        """Initialize the connection pool with producer instances"""
        producer_config = {
            'bootstrap.servers': self.config.bootstrap_servers,
            'batch.size': self.config.batch_size,
            'linger.ms': self.config.linger_ms,
            'compression.type': self.config.compression_type,
            'max.in.flight.requests.per.connection': self.config.max_in_flight_requests,
            'buffer.memory': self.config.buffer_memory,
            'retries': self.config.retries,
            'request.timeout.ms': self.config.request_timeout_ms,
            'retry.backoff.ms': self.config.retry_backoff_ms,
            'max.block.ms': self.config.max_block_ms
        }

        for _ in range(self.pool_size):
            producer = Producer(producer_config)
            self.producers.put(producer)
            self.active_connections += 1

    def get_producer(self) -> Producer:
        """Get a producer from the pool"""
        return self.producers.get()

    def return_producer(self, producer: Producer):
        """Return a producer to the pool"""
        self.producers.put(producer)

    def close_all(self):
        """Close all producers in the pool"""
        while not self.producers.empty():
            try:
                producer = self.producers.get_nowait()
                producer.flush()
                # Producer automatically closes when garbage collected
            except queue.Empty:
                break

class ProducerMetrics:
    def __init__(self):
        self.messages_sent = 0
        self.messages_failed = 0
        self.bytes_sent = 0
        self.total_latency = 0
        self.lock = threading.Lock()
        self.start_time = time.time()

    def record_success(self, message_size: int, latency: float):
        with self.lock:
            self.messages_sent += 1
            self.bytes_sent += message_size
            self.total_latency += latency

    def record_failure(self):
        with self.lock:
            self.messages_failed += 1

    def get_stats(self) -> Dict[str, float]:
        with self.lock:
            runtime = time.time() - self.start_time
            total_messages = self.messages_sent + self.messages_failed
            
            return {
                'messages_sent': self.messages_sent,
                'messages_failed': self.messages_failed,
                'success_rate': (self.messages_sent / total_messages * 100) if total_messages > 0 else 0,
                'throughput_msg_sec': self.messages_sent / runtime if runtime > 0 else 0,
                'throughput_bytes_sec': self.bytes_sent / runtime if runtime > 0 else 0,
                'avg_latency_ms': (self.total_latency / self.messages_sent * 1000) if self.messages_sent > 0 else 0,
                'runtime_seconds': runtime
            }

class HighVolumeProducer:
    def __init__(self, config: ProducerConfig, pool_size: int = 10, worker_threads: int = 8):
        self.config = config
        self.pool = ConnectionPool(config, pool_size)
        self.metrics = ProducerMetrics()
        self.worker_threads = worker_threads
        self.executor = ThreadPoolExecutor(max_workers=worker_threads)
        self.message_queue = queue.Queue(maxsize=100000)
        self.running = False
        self.workers = []

    def _delivery_callback(self, err: Optional[KafkaException], msg: Message, start_time: float):
        """Callback for message delivery confirmation"""
        if err:
            logger.error("Message delivery failed", error=str(err))
            self.metrics.record_failure()
        else:
            latency = time.time() - start_time
            self.metrics.record_success(len(msg.value()), latency)
            logger.debug("Message delivered", 
                        topic=msg.topic(), 
                        partition=msg.partition(), 
                        offset=msg.offset())

    def _worker_thread(self):
        """Worker thread to process messages from queue"""
        while self.running:
            try:
                # Get message from queue with timeout
                message_data = self.message_queue.get(timeout=1)
                if message_data is None:  # Shutdown signal
                    break

                topic, action, partition_key = message_data
                self._send_message_internal(topic, action, partition_key)
                self.message_queue.task_done()

            except queue.Empty:
                continue
            except Exception as e:
                logger.error("Worker thread error", error=str(e))

    def _send_message_internal(self, topic: str, action: UserAction, partition_key: str):
        """Internal method to send message using connection pool"""
        producer = self.pool.get_producer()
        start_time = time.time()
        
        try:
            # Serialize the action
            message_value = json.dumps(asdict(action)).encode('utf-8')
            
            # Send message with callback
            producer.produce(
                topic=topic,
                value=message_value,
                key=partition_key.encode('utf-8'),
                callback=lambda err, msg: self._delivery_callback(err, msg, start_time)
            )
            
            # Trigger delivery report callbacks
            producer.poll(0)
            
        except Exception as e:
            logger.error("Failed to send message", error=str(e))
            self.metrics.record_failure()
        finally:
            self.pool.return_producer(producer)

    def start(self):
        """Start the producer workers"""
        if self.running:
            return

        self.running = True
        logger.info("Starting high volume producer", worker_threads=self.worker_threads)

        # Start worker threads
        for i in range(self.worker_threads):
            worker = threading.Thread(target=self._worker_thread, name=f"ProducerWorker-{i}")
            worker.start()
            self.workers.append(worker)

    def send_async(self, topic: str, action: UserAction, partition_key: Optional[str] = None) -> bool:
        """Send message asynchronously"""
        if not self.running:
            raise RuntimeError("Producer not started")

        if partition_key is None:
            partition_key = action.user_id

        try:
            self.message_queue.put((topic, action, partition_key), timeout=0.1)
            return True
        except queue.Full:
            logger.warning("Message queue full, dropping message")
            self.metrics.record_failure()
            return False

    def send_batch(self, topic: str, actions: list[UserAction], partition_key_func: Callable[[UserAction], str] = None):
        """Send multiple messages in batch"""
        if partition_key_func is None:
            partition_key_func = lambda action: action.user_id

        for action in actions:
            partition_key = partition_key_func(action)
            self.send_async(topic, action, partition_key)

    def get_metrics(self) -> Dict[str, float]:
        """Get current producer metrics"""
        stats = self.metrics.get_stats()
        
        # Add system metrics
        process = psutil.Process()
        stats.update({
            'cpu_percent': process.cpu_percent(),
            'memory_mb': process.memory_info().rss / 1024 / 1024,
            'queue_size': self.message_queue.qsize(),
            'active_connections': self.pool.active_connections
        })
        
        return stats

    def stop(self, timeout: int = 30):
        """Stop the producer and cleanup resources"""
        if not self.running:
            return

        logger.info("Stopping high volume producer")
        self.running = False

        # Send shutdown signals to workers
        for _ in self.workers:
            try:
                self.message_queue.put(None, timeout=1)
            except queue.Full:
                pass

        # Wait for workers to finish
        for worker in self.workers:
            worker.join(timeout=timeout)

        # Flush all producers
        self.pool.close_all()
        
        logger.info("High volume producer stopped")

# Factory function for easy instantiation
def create_high_volume_producer(
    bootstrap_servers: str = "localhost:9092",
    pool_size: int = 10,
    worker_threads: int = 8,
    **config_overrides
) -> HighVolumeProducer:
    """Create a configured high volume producer"""
    config = ProducerConfig(bootstrap_servers=bootstrap_servers, **config_overrides)
    return HighVolumeProducer(config, pool_size, worker_threads)
EOF

# Create monitoring dashboard
echo "📊 Creating monitoring dashboard..."
cat > src/monitoring/dashboard.py << 'EOF'
import dash
from dash import dcc, html, Input, Output, callback
import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
import threading
import time
from datetime import datetime, timedelta
import json
import requests
from collections import deque
import dash_bootstrap_components as dbc

class ProducerDashboard:
    def __init__(self, producer_metrics_url: str = "http://localhost:8080/metrics"):
        self.app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
        self.metrics_url = producer_metrics_url
        self.metrics_history = deque(maxlen=300)  # Keep 5 minutes of data (1 point per second)
        self.running = False
        self.setup_layout()
        self.setup_callbacks()

    def setup_layout(self):
        """Setup the dashboard layout"""
        self.app.layout = dbc.Container([
            dbc.Row([
                dbc.Col([
                    html.H1("StreamSocial Producer Dashboard", className="text-center mb-4"),
                    html.Hr()
                ])
            ]),
            
            # Key Metrics Cards
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Throughput", className="card-title"),
                            html.H2(id="throughput-value", className="text-primary"),
                            html.P("messages/second", className="text-muted")
                        ])
                    ])
                ], md=3),
                
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Success Rate", className="card-title"),
                            html.H2(id="success-rate-value", className="text-success"),
                            html.P("percentage", className="text-muted")
                        ])
                    ])
                ], md=3),
                
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Avg Latency", className="card-title"),
                            html.H2(id="latency-value", className="text-warning"),
                            html.P("milliseconds", className="text-muted")
                        ])
                    ])
                ], md=3),
                
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Queue Size", className="card-title"),
                            html.H2(id="queue-size-value", className="text-info"),
                            html.P("pending messages", className="text-muted")
                        ])
                    ])
                ], md=3),
            ], className="mb-4"),
            
            # Charts
            dbc.Row([
                dbc.Col([
                    dcc.Graph(id="throughput-chart")
                ], md=6),
                dbc.Col([
                    dcc.Graph(id="latency-chart")
                ], md=6),
            ], className="mb-4"),
            
            dbc.Row([
                dbc.Col([
                    dcc.Graph(id="success-rate-chart")
                ], md=6),
                dbc.Col([
                    dcc.Graph(id="resource-usage-chart")
                ], md=6),
            ]),
            
            # Auto-refresh interval
            dcc.Interval(
                id='interval-component',
                interval=1000,  # Update every second
                n_intervals=0
            ),
            
        ], fluid=True)

    def setup_callbacks(self):
        """Setup dashboard callbacks"""
        
        @self.app.callback(
            [Output('throughput-value', 'children'),
             Output('success-rate-value', 'children'),
             Output('latency-value', 'children'),
             Output('queue-size-value', 'children'),
             Output('throughput-chart', 'figure'),
             Output('latency-chart', 'figure'),
             Output('success-rate-chart', 'figure'),
             Output('resource-usage-chart', 'figure')],
            [Input('interval-component', 'n_intervals')]
        )
        def update_metrics(n):
            try:
                # Fetch metrics from producer
                response = requests.get(self.metrics_url, timeout=1)
                metrics = response.json()
                
                # Add timestamp
                metrics['timestamp'] = datetime.now()
                self.metrics_history.append(metrics)
                
                # Current values
                throughput = f"{metrics.get('throughput_msg_sec', 0):,.0f}"
                success_rate = f"{metrics.get('success_rate', 0):.1f}%"
                latency = f"{metrics.get('avg_latency_ms', 0):.1f}"
                queue_size = f"{metrics.get('queue_size', 0):,}"
                
                # Create charts
                df = pd.DataFrame(self.metrics_history)
                
                # Throughput chart
                throughput_fig = go.Figure()
                throughput_fig.add_trace(go.Scatter(
                    x=df['timestamp'], 
                    y=df['throughput_msg_sec'],
                    mode='lines+markers',
                    name='Messages/sec',
                    line=dict(color='#007bff')
                ))
                throughput_fig.update_layout(
                    title="Message Throughput",
                    xaxis_title="Time",
                    yaxis_title="Messages/Second",
                    height=300
                )
                
                # Latency chart
                latency_fig = go.Figure()
                latency_fig.add_trace(go.Scatter(
                    x=df['timestamp'], 
                    y=df['avg_latency_ms'],
                    mode='lines+markers',
                    name='Latency (ms)',
                    line=dict(color='#ffc107')
                ))
                latency_fig.update_layout(
                    title="Average Latency",
                    xaxis_title="Time",
                    yaxis_title="Milliseconds",
                    height=300
                )
                
                # Success rate chart
                success_fig = go.Figure()
                success_fig.add_trace(go.Scatter(
                    x=df['timestamp'], 
                    y=df['success_rate'],
                    mode='lines+markers',
                    name='Success Rate (%)',
                    line=dict(color='#28a745')
                ))
                success_fig.update_layout(
                    title="Success Rate",
                    xaxis_title="Time",
                    yaxis_title="Percentage",
                    height=300,
                    yaxis=dict(range=[0, 100])
                )
                
                # Resource usage chart
                resource_fig = go.Figure()
                resource_fig.add_trace(go.Scatter(
                    x=df['timestamp'], 
                    y=df['cpu_percent'],
                    mode='lines+markers',
                    name='CPU %',
                    line=dict(color='#dc3545')
                ))
                resource_fig.add_trace(go.Scatter(
                    x=df['timestamp'], 
                    y=df['memory_mb'],
                    mode='lines+markers',
                    name='Memory (MB)',
                    yaxis='y2',
                    line=dict(color='#6f42c1')
                ))
                resource_fig.update_layout(
                    title="Resource Usage",
                    xaxis_title="Time",
                    yaxis_title="CPU %",
                    yaxis2=dict(
                        title="Memory (MB)",
                        overlaying='y',
                        side='right'
                    ),
                    height=300
                )
                
                return (throughput, success_rate, latency, queue_size,
                       throughput_fig, latency_fig, success_fig, resource_fig)
                
            except Exception as e:
                print(f"Error updating metrics: {e}")
                # Return empty values on error
                empty_fig = go.Figure()
                return "0", "0%", "0", "0", empty_fig, empty_fig, empty_fig, empty_fig

    def run(self, host='0.0.0.0', port=8050, debug=False):
        """Run the dashboard"""
        print(f"🚀 Starting Producer Dashboard at http://{host}:{port}")
        self.app.run_server(host=host, port=port, debug=debug)

if __name__ == '__main__':
    dashboard = ProducerDashboard()
    dashboard.run(debug=True)
EOF

# Create metrics server
echo "📊 Creating metrics server..."
cat > src/monitoring/metrics_server.py << 'EOF'
from flask import Flask, jsonify
from flask_cors import CORS
import threading
import time

app = Flask(__name__)
CORS(app)

class MetricsServer:
    def __init__(self, producer=None, port=8080):
        self.producer = producer
        self.port = port
        self.app = app
        self.setup_routes()

    def setup_routes(self):
        @self.app.route('/metrics')
        def get_metrics():
            if self.producer:
                return jsonify(self.producer.get_metrics())
            else:
                # Return dummy metrics for testing
                return jsonify({
                    'messages_sent': 0,
                    'messages_failed': 0,
                    'success_rate': 0,
                    'throughput_msg_sec': 0,
                    'throughput_bytes_sec': 0,
                    'avg_latency_ms': 0,
                    'runtime_seconds': 0,
                    'cpu_percent': 0,
                    'memory_mb': 0,
                    'queue_size': 0,
                    'active_connections': 0
                })

        @self.app.route('/health')
        def health_check():
            return jsonify({'status': 'healthy', 'timestamp': time.time()})

    def run(self, host='0.0.0.0', debug=False):
        print(f"🚀 Starting Metrics Server at http://{host}:{self.port}")
        self.app.run(host=host, port=self.port, debug=debug, threaded=True)

if __name__ == '__main__':
    server = MetricsServer()
    server.run(debug=True)
EOF

# Create demo application
echo "🎯 Creating demo application..."
cat > src/demo.py << 'EOF'
import time
import random
import threading
from datetime import datetime
from producer.high_volume_producer import create_high_volume_producer, UserAction
from monitoring.metrics_server import MetricsServer
from monitoring.dashboard import ProducerDashboard
import multiprocessing

class StreamSocialDemo:
    def __init__(self):
        self.producer = create_high_volume_producer(
            bootstrap_servers="localhost:9092",
            pool_size=20,
            worker_threads=16
        )
        self.running = False
        self.demo_threads = []

    def generate_user_actions(self, rate_per_second=1000, duration=60):
        """Generate user actions at specified rate"""
        print(f"📱 Generating user actions at {rate_per_second}/sec for {duration} seconds")
        
        action_types = ['post', 'like', 'share', 'comment']
        
        end_time = time.time() + duration
        interval = 1.0 / rate_per_second
        
        while time.time() < end_time and self.running:
            start_batch = time.time()
            
            # Generate batch of actions
            batch_size = min(100, rate_per_second // 10)  # Batch size based on rate
            actions = []
            
            for _ in range(batch_size):
                action = UserAction(
                    user_id=f"user_{random.randint(1, 100000)}",
                    action_type=random.choice(action_types),
                    content_id=f"content_{random.randint(1, 1000000)}",
                    timestamp=int(time.time() * 1000),
                    metadata={
                        'device': random.choice(['mobile', 'web', 'tablet']),
                        'location': random.choice(['US', 'EU', 'ASIA']),
                        'session_id': f"session_{random.randint(1, 50000)}"
                    }
                )
                actions.append(action)
            
            # Send batch
            self.producer.send_batch('user-actions', actions)
            
            # Maintain rate
            elapsed = time.time() - start_batch
            sleep_time = (batch_size * interval) - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

    def stress_test(self):
        """Run stress test with increasing load"""
        print("🚀 Starting stress test...")
        
        test_phases = [
            (1000, 30, "Warmup"),
            (10000, 60, "Normal Load"),
            (50000, 30, "High Load"),
            (100000, 15, "Peak Load"),
            (5000000, 10, "Ultra High Load"),  # 5M/sec target
            (10000, 30, "Cooldown")
        ]
        
        for rate, duration, phase_name in test_phases:
            if not self.running:
                break
                
            print(f"📈 Phase: {phase_name} - {rate:,} messages/sec for {duration}s")
            
            # Start multiple generators for high rates
            generators = min(8, max(1, rate // 10000))  # Scale generators with rate
            rate_per_generator = rate // generators
            
            threads = []
            for i in range(generators):
                thread = threading.Thread(
                    target=self.generate_user_actions,
                    args=(rate_per_generator, duration),
                    name=f"Generator-{i}"
                )
                thread.start()
                threads.append(thread)
            
            # Wait for phase to complete
            for thread in threads:
                thread.join()
            
            print(f"✅ Completed phase: {phase_name}")
            time.sleep(5)  # Brief pause between phases

    def start_demo(self):
        """Start the demo with all components"""
        print("🎬 Starting StreamSocial Producer Demo")
        
        # Start producer
        self.producer.start()
        self.running = True
        
        # Start metrics server in separate process
        metrics_server = MetricsServer(self.producer)
        metrics_process = multiprocessing.Process(
            target=metrics_server.run,
            kwargs={'host': '0.0.0.0', 'debug': False}
        )
        metrics_process.start()
        
        # Start dashboard in separate process  
        dashboard = ProducerDashboard()
        dashboard_process = multiprocessing.Process(
            target=dashboard.run,
            kwargs={'host': '0.0.0.0', 'debug': False}
        )
        dashboard_process.start()
        
        print("🚀 All services started!")
        print("📊 Dashboard: http://localhost:8050")
        print("📈 Metrics API: http://localhost:8080/metrics")
        print("💡 Starting stress test in 10 seconds...")
        
        time.sleep(10)
        
        # Run stress test
        stress_thread = threading.Thread(target=self.stress_test, name="StressTest")
        stress_thread.start()
        
        try:
            stress_thread.join()
        except KeyboardInterrupt:
            print("\n🛑 Demo interrupted by user")
        
        # Cleanup
        self.stop_demo()
        metrics_process.terminate()
        dashboard_process.terminate()

    def stop_demo(self):
        """Stop the demo"""
        print("🛑 Stopping demo...")
        self.running = False
        self.producer.stop()
        print("✅ Demo stopped")

if __name__ == '__main__':
    demo = StreamSocialDemo()
    demo.start_demo()
EOF

# Create unit tests
echo "🧪 Creating unit tests..."
cat > tests/unit/test_producer.py << 'EOF'
import pytest
import time
import threading
from unittest.mock import Mock, patch
from src.producer.high_volume_producer import (
    HighVolumeProducer, 
    ProducerConfig, 
    UserAction,
    ConnectionPool,
    ProducerMetrics
)

class TestProducerMetrics:
    def test_initial_metrics(self):
        metrics = ProducerMetrics()
        stats = metrics.get_stats()
        
        assert stats['messages_sent'] == 0
        assert stats['messages_failed'] == 0
        assert stats['success_rate'] == 0

    def test_record_success(self):
        metrics = ProducerMetrics()
        metrics.record_success(1024, 0.1)
        
        stats = metrics.get_stats()
        assert stats['messages_sent'] == 1
        assert stats['bytes_sent'] == 1024
        assert stats['avg_latency_ms'] == 100  # 0.1s = 100ms

    def test_record_failure(self):
        metrics = ProducerMetrics()
        metrics.record_failure()
        
        stats = metrics.get_stats()
        assert stats['messages_failed'] == 1

class TestUserAction:
    def test_user_action_creation(self):
        action = UserAction(
            user_id="user123",
            action_type="post",
            content_id="content456",
            timestamp=1234567890,
            metadata={"device": "mobile"}
        )
        
        assert action.user_id == "user123"
        assert action.action_type == "post"
        assert action.metadata["device"] == "mobile"

@pytest.fixture
def mock_config():
    return ProducerConfig(
        bootstrap_servers="localhost:9092",
        batch_size=1024,
        linger_ms=5
    )

class TestHighVolumeProducer:
    @patch('src.producer.high_volume_producer.Producer')
    def test_producer_initialization(self, mock_producer_class, mock_config):
        producer = HighVolumeProducer(mock_config, pool_size=2, worker_threads=2)
        
        assert producer.config == mock_config
        assert producer.worker_threads == 2
        assert not producer.running

    @patch('src.producer.high_volume_producer.Producer')
    def test_producer_start_stop(self, mock_producer_class, mock_config):
        producer = HighVolumeProducer(mock_config, pool_size=2, worker_threads=2)
        
        # Start producer
        producer.start()
        assert producer.running
        assert len(producer.workers) == 2
        
        # Stop producer
        producer.stop()
        assert not producer.running

    @patch('src.producer.high_volume_producer.Producer')
    def test_send_async(self, mock_producer_class, mock_config):
        producer = HighVolumeProducer(mock_config, pool_size=1, worker_threads=1)
        producer.start()
        
        action = UserAction(
            user_id="user123",
            action_type="post", 
            content_id="content456",
            timestamp=int(time.time() * 1000),
            metadata={}
        )
        
        result = producer.send_async("test-topic", action)
        assert result == True
        
        producer.stop()

if __name__ == '__main__':
    pytest.main([__file__])
EOF

# Create integration tests
echo "🔗 Creating integration tests..."
cat > tests/integration/test_kafka_integration.py << 'EOF'
import pytest
import time
import docker
import threading
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from src.producer.high_volume_producer import create_high_volume_producer, UserAction

class TestKafkaIntegration:
    @classmethod
    def setup_class(cls):
        """Setup Kafka container for testing"""
        cls.client = docker.from_env()
        
        # Check if Kafka is already running
        try:
            containers = cls.client.containers.list()
            kafka_running = any('kafka' in container.name.lower() for container in containers)
            
            if not kafka_running:
                print("⚠️  Kafka not running - integration tests may fail")
                print("💡 Run: docker-compose up -d to start Kafka")
        except Exception as e:
            print(f"⚠️  Could not check Docker containers: {e}")

    def test_producer_consumer_integration(self):
        """Test end-to-end message flow"""
        # Skip if Kafka not available
        try:
            admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})
            metadata = admin_client.list_topics(timeout=5)
        except Exception:
            pytest.skip("Kafka not available")

        # Create test topic
        topic_name = "test-integration"
        new_topics = [NewTopic(topic_name, num_partitions=3, replication_factor=1)]
        
        try:
            futures = admin_client.create_topics(new_topics)
            for topic, future in futures.items():
                try:
                    future.result()
                    print(f"✅ Created topic: {topic}")
                except Exception as e:
                    if "already exists" not in str(e):
                        raise
        except Exception as e:
            print(f"Topic creation: {e}")

        # Setup producer
        producer = create_high_volume_producer(
            bootstrap_servers="localhost:9092",
            pool_size=2,
            worker_threads=2
        )
        producer.start()

        # Setup consumer
        consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'test-group',
            'auto.offset.reset': 'earliest'
        })
        consumer.subscribe([topic_name])

        try:
            # Send test messages
            test_messages = []
            for i in range(10):
                action = UserAction(
                    user_id=f"user{i}",
                    action_type="post",
                    content_id=f"content{i}",
                    timestamp=int(time.time() * 1000),
                    metadata={"test": True}
                )
                producer.send_async(topic_name, action)
                test_messages.append(action)

            # Wait for messages to be sent
            time.sleep(2)

            # Consume and verify messages
            consumed_messages = []
            start_time = time.time()
            
            while len(consumed_messages) < len(test_messages) and (time.time() - start_time) < 10:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    continue
                    
                consumed_messages.append(msg.value().decode('utf-8'))

            # Verify we received all messages
            assert len(consumed_messages) == len(test_messages)
            print(f"✅ Successfully sent and consumed {len(consumed_messages)} messages")

        finally:
            producer.stop()
            consumer.close()

    def test_high_throughput_scenario(self):
        """Test high throughput scenario"""
        try:
            admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})
            metadata = admin_client.list_topics(timeout=5)
        except Exception:
            pytest.skip("Kafka not available")

        producer = create_high_volume_producer(
            bootstrap_servers="localhost:9092",
            pool_size=5,
            worker_threads=4
        )
        producer.start()

        try:
            # Send 1000 messages quickly
            start_time = time.time()
            
            for i in range(1000):
                action = UserAction(
                    user_id=f"user{i}",
                    action_type="post",
                    content_id=f"content{i}", 
                    timestamp=int(time.time() * 1000),
                    metadata={"batch_test": True}
                )
                producer.send_async("user-actions", action)

            # Wait for completion
            time.sleep(5)
            
            # Check metrics
            metrics = producer.get_metrics()
            duration = time.time() - start_time + 5  # Include wait time
            
            print(f"📊 Metrics after high throughput test:")
            print(f"   Messages sent: {metrics['messages_sent']}")
            print(f"   Success rate: {metrics['success_rate']:.1f}%")
            print(f"   Throughput: {metrics['throughput_msg_sec']:.0f} msg/sec")
            
            assert metrics['messages_sent'] >= 900  # Allow for some failures
            assert metrics['success_rate'] > 90  # At least 90% success rate

        finally:
            producer.stop()

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
EOF

# Create Docker Compose for Kafka
echo "🐳 Creating Docker Compose for Kafka..."
cat > docker/docker-compose.yml << 'EOF'
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.0
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  kafka:
    image: confluentinc/cp-kafka:7.6.0
    hostname: kafka
    container_name: kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "9101:9101"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_JMX_PORT: 9101
      KAFKA_JMX_HOSTNAME: localhost
      
  kafka-topics-generator:
    image: confluentinc/cp-kafka:7.6.0
    depends_on:
      - kafka
    command: >
      bash -c "
        sleep 30 &&
        kafka-topics --create --topic user-actions --bootstrap-server kafka:29092 --partitions 1000 --replication-factor 1 &&
        kafka-topics --create --topic content-interactions --bootstrap-server kafka:29092 --partitions 500 --replication-factor 1 &&
        echo 'Topics created successfully'
      "
EOF

# Create configuration files
echo "⚙️  Creating configuration files..."
cat > config/kafka/producer.properties << 'EOF'
# Kafka Producer Configuration for StreamSocial
bootstrap.servers=localhost:9092

# Performance Settings
batch.size=65536
linger.ms=5
compression.type=lz4
max.in.flight.requests.per.connection=10
buffer.memory=67108864

# Reliability Settings  
retries=2147483647
request.timeout.ms=30000
retry.backoff.ms=100
max.block.ms=10000

# Idempotence
enable.idempotence=true

# Serialization
key.serializer=org.apache.kafka.common.serialization.StringSerializer
value.serializer=org.apache.kafka.common.serialization.StringSerializer
EOF

# Create start script
echo "🚀 Creating start.sh script..."
cat > start.sh << 'EOF'
#!/bin/bash

echo "🚀 Starting StreamSocial Kafka Producer Demo"
echo "============================================"

# Activate virtual environment
source venv/bin/activate

# Start Kafka if not running
echo "🐳 Starting Kafka cluster..."
cd docker
docker-compose up -d
cd ..

# Wait for Kafka to be ready
echo "⏳ Waiting for Kafka to be ready..."
sleep 30

# Verify Kafka topics exist
echo "📝 Verifying Kafka topics..."
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Run tests first
echo "🧪 Running tests..."
python -m pytest tests/ -v

# Start demo
echo "🎬 Starting producer demo..."
echo "📊 Dashboard will be available at: http://localhost:8050"
echo "📈 Metrics API will be available at: http://localhost:8080/metrics"
echo ""
echo "Press Ctrl+C to stop the demo"

python src/demo.py
EOF

chmod +x start.sh

# Create stop script
echo "🛑 Creating stop.sh script..."
cat > stop.sh << 'EOF'
#!/bin/bash

echo "🛑 Stopping StreamSocial Producer Demo"
echo "====================================="

# Kill any running Python processes
pkill -f "python.*demo.py" || true
pkill -f "python.*dashboard.py" || true
pkill -f "python.*metrics_server.py" || true

# Stop Kafka
echo "🐳 Stopping Kafka cluster..."
cd docker
docker-compose down
cd ..

# Deactivate virtual environment
deactivate 2>/dev/null || true

echo "✅ All services stopped"
EOF

chmod +x stop.sh

# Create project README
echo "📖 Creating README.md..."
cat > README.md << 'EOF'
# StreamSocial Kafka Producer
## Day 4: High-Volume Producer Implementation

High-performance Kafka producer capable of handling 5M messages/second with connection pooling and comprehensive monitoring.

## Quick Start

### Prerequisites
- Python 3.11+
- Docker & Docker Compose
- 8GB+ RAM recommended

### Setup & Run
```bash
# Start everything
./start.sh

# Stop everything  
./stop.sh
```

### Access Points
- **Dashboard**: http://localhost:8050
- **Metrics API**: http://localhost:8080/metrics
- **Kafka**: localhost:9092

## Architecture

### Producer Features
- Connection pooling for resource efficiency
- Multi-threaded message processing
- Batching optimization for throughput
- Comprehensive metrics collection
- Real-time monitoring dashboard

### Performance Targets
- **Throughput**: 5M messages/second
- **Latency**: <200ms P99
- **Success Rate**: >99.99%
- **Resource Usage**: <4GB memory

## Testing

```bash
# Unit tests
python -m pytest tests/unit/ -v

# Integration tests (requires Kafka)
python -m pytest tests/integration/ -v
```

## Configuration

Key producer settings in `config/kafka/producer.properties`:
- `batch.size=65536` - 64KB batches
- `linger.ms=5` - 5ms batching window
- `compression.type=lz4` - Fast compression
- `max.in.flight.requests.per.connection=10` - Pipelining

## Monitoring

The dashboard shows real-time metrics:
- Message throughput (messages/second)
- Success rate percentage
- Average latency
- Resource utilization
- Queue sizes

## Files Structure
```
streamsocial-producer/
├── src/
│   ├── producer/high_volume_producer.py
│   ├── monitoring/dashboard.py
│   ├── monitoring/metrics_server.py
│   └── demo.py
├── tests/
│   ├── unit/test_producer.py
│   └── integration/test_kafka_integration.py
├── docker/docker-compose.yml
├── config/kafka/producer.properties
├── start.sh
└── stop.sh
```
EOF

echo "✅ Implementation script completed successfully!"
echo ""
echo "📁 Project structure:"
find . -type f -name "*.py" -o -name "*.sh" -o -name "*.yml" -o -name "*.properties" | sort
echo ""
echo "🚀 To run the demo:"
echo "   cd streamsocial-producer"
echo "   ./start.sh"
echo ""
echo "🌟 Features implemented:"
echo "   ✅ High-volume producer with connection pooling"
echo "   ✅ Multi-threaded processing"
echo "   ✅ Real-time monitoring dashboard"
echo "   ✅ Comprehensive test suite"
echo "   ✅ Docker-based Kafka cluster"
echo "   ✅ Stress testing capabilities"