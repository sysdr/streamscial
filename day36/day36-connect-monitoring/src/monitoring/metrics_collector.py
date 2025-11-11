import time
import psutil
import requests
from kafka import KafkaConsumer, KafkaAdminClient
from kafka.errors import KafkaError
from prometheus_client import Gauge, Counter, Histogram, generate_latest, CollectorRegistry
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ConnectMetricsCollector:
    def __init__(self, connect_urls, registry=None):
        self.connect_urls = connect_urls
        self.registry = registry or CollectorRegistry()
        self.setup_metrics()
        
    def setup_metrics(self):
        # Connector metrics
        self.connector_status = Gauge('connect_connector_status', 
                                      'Connector status (1=running, 0=failed)', 
                                      ['connector', 'worker'],
                                      registry=self.registry)
        self.task_status = Gauge('connect_task_status',
                                 'Task status (1=running, 0=failed)',
                                 ['connector', 'task_id', 'worker'],
                                 registry=self.registry)
        self.throughput = Gauge('connect_throughput_records_per_sec',
                               'Records processed per second',
                               ['connector', 'source'],
                               registry=self.registry)
        self.error_rate = Gauge('connect_error_rate',
                               'Error rate percentage',
                               ['connector', 'error_type'],
                               registry=self.registry)
        self.offset_lag = Gauge('connect_offset_lag_seconds',
                               'Offset lag in seconds',
                               ['connector', 'topic'],
                               registry=self.registry)
        self.health_score = Gauge('connect_health_score',
                                 'Overall connector health (0-100)',
                                 ['connector'],
                                 registry=self.registry)
        
        # System metrics
        self.cpu_usage = Gauge('system_cpu_percent', 'CPU usage percentage', registry=self.registry)
        self.memory_usage = Gauge('system_memory_percent', 'Memory usage percentage', registry=self.registry)
        self.disk_io = Counter('system_disk_io_bytes', 'Disk I/O bytes', ['operation'], registry=self.registry)
        
    def collect_connector_status(self):
        """Collect status from all Connect workers"""
        for url in self.connect_urls:
            try:
                response = requests.get(f"{url}/connectors", timeout=5)
                connectors = response.json()
                
                for connector_name in connectors:
                    status_resp = requests.get(
                        f"{url}/connectors/{connector_name}/status",
                        timeout=5
                    )
                    status = status_resp.json()
                    
                    # Connector status
                    connector_state = 1 if status['connector']['state'] == 'RUNNING' else 0
                    self.connector_status.labels(
                        connector=connector_name,
                        worker=status['connector'].get('worker_id', 'unknown')
                    ).set(connector_state)
                    
                    # Task status
                    for task in status['tasks']:
                        task_state = 1 if task['state'] == 'RUNNING' else 0
                        self.task_status.labels(
                            connector=connector_name,
                            task_id=str(task['id']),
                            worker=task.get('worker_id', 'unknown')
                        ).set(task_state)
                        
            except Exception as e:
                logger.error(f"Failed to collect from {url}: {e}")
                
    def collect_system_metrics(self):
        """Collect system-level metrics"""
        self.cpu_usage.set(psutil.cpu_percent(interval=1))
        self.memory_usage.set(psutil.virtual_memory().percent)
        
        io_counters = psutil.disk_io_counters()
        if io_counters:
            self.disk_io.labels(operation='read').inc(io_counters.read_bytes)
            self.disk_io.labels(operation='write').inc(io_counters.write_bytes)
            
    def simulate_connector_metrics(self):
        """Generate simulated metrics for demo"""
        import random
        
        sources = ['instagram', 'twitter', 'linkedin']
        connectors = [f'{source}-source' for source in sources]
        
        for connector in connectors:
            source = connector.split('-')[0]
            
            # Simulate throughput (varying by source)
            base_throughput = {
                'instagram': 150,
                'twitter': 300,
                'linkedin': 80
            }
            throughput = base_throughput[source] + random.randint(-20, 40)
            self.throughput.labels(connector=connector, source=source).set(throughput)
            
            # Simulate error rates
            error_types = ['authentication', 'rate_limit', 'network', 'parse']
            for error_type in error_types:
                error_pct = random.uniform(0, 0.02)  # 0-2% errors
                self.error_rate.labels(connector=connector, error_type=error_type).set(error_pct)
            
            # Simulate lag
            topic = f'{source}-posts'
            lag = random.randint(5, 60)  # 5-60 seconds
            self.offset_lag.labels(connector=connector, topic=topic).set(lag)
            
            # Calculate health score
            total_error_rate = sum(random.uniform(0, 0.02) for _ in error_types)
            tasks_running = random.randint(2, 3)
            tasks_total = 3
            lag_minutes = lag / 60
            
            score = 100 * (1 - total_error_rate) * (tasks_running / tasks_total) * (1 / (1 + lag_minutes/30))
            self.health_score.labels(connector=connector).set(score)
            
    def collect_all(self):
        """Collect all metrics"""
        self.collect_system_metrics()
        self.simulate_connector_metrics()
        # Uncomment when actual Connect cluster is running
        # self.collect_connector_status()
