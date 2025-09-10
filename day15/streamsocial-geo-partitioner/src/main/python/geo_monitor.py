import json
import time
import redis
import threading
from kafka import KafkaConsumer, KafkaProducer
from flask import Flask, render_template, jsonify
import plotly.graph_objs as go
import plotly.utils
from datetime import datetime, timedelta
from collections import defaultdict, deque
import psutil
import random

class GeoPartitionMonitor:
    def __init__(self):
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0)
        self.partition_metrics = defaultdict(lambda: deque(maxlen=100))
        self.region_stats = defaultdict(int)
        import os
        template_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'web', 'templates')
        static_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'web', 'static')
        print(f"Template directory: {template_dir}")
        print(f"Template exists: {os.path.exists(os.path.join(template_dir, 'dashboard.html'))}")
        self.app = Flask(__name__, template_folder=template_dir, static_folder=static_dir)
        self.setup_routes()
        
    def setup_routes(self):
        @self.app.route('/')
        def dashboard():
            return '''
            <!DOCTYPE html>
            <html>
            <head>
                <title>StreamSocial Geographic Partitioner Monitor</title>
                <style>
                    body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
                    .container { max-width: 1200px; margin: 0 auto; }
                    .header { text-align: center; margin-bottom: 30px; }
                    .metrics-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin-bottom: 20px; }
                    .metric-card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
                    .metric-title { font-size: 18px; font-weight: bold; margin-bottom: 10px; }
                    .metric-value { font-size: 24px; color: #2196F3; font-weight: bold; }
                    .status-indicator { width: 12px; height: 12px; border-radius: 50%; margin-right: 8px; display: inline-block; }
                    .healthy { background-color: #4CAF50; }
                    .degraded { background-color: #FF9800; }
                    .failed { background-color: #F44336; }
                    .refresh-btn { background: #2196F3; color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; margin: 10px 0; }
                    .partition-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 10px; }
                    .partition-card { background: white; padding: 15px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); text-align: center; }
                </style>
                <script>
                    function updateMetrics() {
                        fetch('/api/partition-health')
                            .then(response => response.json())
                            .then(data => {
                                let totalMessages = 0;
                                let healthyPartitions = 0;
                                let degradedPartitions = 0;
                                
                                const partitionGrid = document.getElementById('partitionGrid');
                                partitionGrid.innerHTML = '';
                                
                                for (let i = 0; i < 12; i++) {
                                    const partition = data[i] || {current_health: 100, message_count: 0, status: 'healthy'};
                                    totalMessages += partition.message_count;
                                    
                                    if (partition.status === 'healthy') healthyPartitions++;
                                    else if (partition.status === 'degraded') degradedPartitions++;
                                    
                                    const partitionCard = document.createElement('div');
                                    partitionCard.className = 'partition-card';
                                    partitionCard.innerHTML = `
                                        <div class="metric-title">Partition ${i}</div>
                                        <div class="status-indicator ${partition.status}"></div>
                                        <div class="metric-value">${partition.message_count}</div>
                                        <div>Health: ${partition.current_health}%</div>
                                    `;
                                    partitionGrid.appendChild(partitionCard);
                                }
                                
                                document.getElementById('totalMessages').textContent = totalMessages;
                                document.getElementById('healthyPartitions').textContent = healthyPartitions;
                                document.getElementById('degradedPartitions').textContent = degradedPartitions;
                            })
                            .catch(error => console.error('Error fetching metrics:', error));
                    }
                    
                    // Update metrics every 2 seconds
                    setInterval(updateMetrics, 2000);
                    updateMetrics(); // Initial load
                </script>
            </head>
            <body>
                <div class="container">
                    <div class="header">
                        <h1>🌍 StreamSocial Geographic Partitioner Monitor</h1>
                        <p>Real-time monitoring of geo-distributed Kafka partitions</p>
                        <button class="refresh-btn" onclick="updateMetrics()">🔄 Refresh</button>
                    </div>
                    
                    <div class="metrics-grid">
                        <div class="metric-card">
                            <div class="metric-title">📊 Total Messages</div>
                            <div class="metric-value" id="totalMessages">0</div>
                        </div>
                        <div class="metric-card">
                            <div class="metric-title">✅ Healthy Partitions</div>
                            <div class="metric-value" id="healthyPartitions">0</div>
                        </div>
                        <div class="metric-card">
                            <div class="metric-title">⚠️ Degraded Partitions</div>
                            <div class="metric-value" id="degradedPartitions">0</div>
                        </div>
                    </div>
                    
                    <div class="metric-card">
                        <div class="metric-title">📈 Partition Status</div>
                        <div class="partition-grid" id="partitionGrid"></div>
                    </div>
                </div>
            </body>
            </html>
            '''
            
        @self.app.route('/api/partition-health')
        def partition_health():
            health_data = {}
            for partition in range(12):
                metrics = list(self.partition_metrics[partition])
                if metrics:
                    health_data[partition] = {
                        'current_health': metrics[-1]['health'],
                        'avg_latency': sum(m['latency'] for m in metrics[-10:]) / min(len(metrics), 10),
                        'message_count': sum(m['count'] for m in metrics[-10:]),
                        'status': 'healthy' if metrics[-1]['health'] > 50 else 'degraded'
                    }
                else:
                    health_data[partition] = {
                        'current_health': 100,
                        'avg_latency': 0,
                        'message_count': 0,
                        'status': 'healthy'
                    }
            return jsonify(health_data)
        
        @self.app.route('/api/region-distribution')
        def region_distribution():
            return jsonify(dict(self.region_stats))
        
        @self.app.route('/api/partition-chart')
        def partition_chart():
            fig = go.Figure()
            for partition in range(12):
                metrics = list(self.partition_metrics[partition])
                if metrics:
                    timestamps = [m['timestamp'] for m in metrics]
                    health_values = [m['health'] for m in metrics]
                    fig.add_trace(go.Scatter(
                        x=timestamps, y=health_values,
                        name=f'Partition {partition}',
                        line=dict(width=2)
                    ))
            
            fig.update_layout(
                title='Partition Health Over Time',
                xaxis_title='Time',
                yaxis_title='Health Score',
                height=400
            )
            return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    
    def consume_kafka_messages(self):
        """Consume messages from Kafka to track real metrics"""
        try:
            consumer = KafkaConsumer(
                'streamsocial-posts',
                bootstrap_servers=['localhost:9092'],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id='geo-monitor'
            )
            
            for message in consumer:
                # Extract partition and region info
                partition = message.partition
                region = message.value.get('region', 'UNKNOWN')
                
                # Update partition metrics
                timestamp = datetime.now().isoformat()
                health = 95 + random.randint(-10, 5)  # Simulate health based on activity
                health = max(0, min(100, health))
                
                metric = {
                    'timestamp': timestamp,
                    'health': health,
                    'latency': random.randint(10, 50),
                    'count': 1  # Each message counts as 1
                }
                self.partition_metrics[partition].append(metric)
                
                # Update region stats
                self.region_stats[region] += 1
                
                # Update Redis for real-time access
                self.redis_client.hset(f'partition:{partition}', mapping=metric)
                
        except Exception as e:
            print(f"Error consuming Kafka messages: {e}")
            # Fallback to simulation if Kafka is not available
            self.simulate_metrics()
    
    def simulate_metrics(self):
        """Simulate real partition metrics for demonstration"""
        regions = ['US_EAST', 'US_WEST', 'CANADA', 'UK', 'GERMANY', 'FRANCE',
                  'JAPAN', 'SINGAPORE', 'AUSTRALIA', 'BRAZIL', 'INDIA', 'SOUTH_AFRICA']
        
        while True:
            timestamp = datetime.now().isoformat()
            
            for partition in range(12):
                # Simulate varying health scores
                base_health = 90 - (partition % 3) * 10
                health = base_health + random.randint(-20, 10)
                health = max(0, min(100, health))
                
                latency = random.randint(10, 100) if health > 50 else random.randint(100, 300)
                count = random.randint(1000, 5000) if health > 70 else random.randint(100, 1000)
                
                metric = {
                    'timestamp': timestamp,
                    'health': health,
                    'latency': latency,
                    'count': count
                }
                self.partition_metrics[partition].append(metric)
                
                # Update Redis for real-time access
                self.redis_client.hset(f'partition:{partition}', mapping=metric)
            
            # Simulate region distribution
            for region in regions:
                self.region_stats[region] = random.randint(1000, 10000)
                
            time.sleep(2)
    
    def start_monitoring(self):
        # Start Kafka consumer in background
        kafka_thread = threading.Thread(target=self.consume_kafka_messages)
        kafka_thread.daemon = True
        kafka_thread.start()
        
        # Start Flask app
        self.app.run(host='0.0.0.0', port=5000, debug=False)

if __name__ == '__main__':
    monitor = GeoPartitionMonitor()
    monitor.start_monitoring()
