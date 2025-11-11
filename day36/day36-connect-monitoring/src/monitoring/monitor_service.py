import time
import yaml
import logging
from metrics_collector import ConnectMetricsCollector
from health_aggregator import HealthAggregator
from alert_manager import AlertManager
from prometheus_client import start_http_server
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Configuration
    connect_urls = [
        'http://localhost:8083',
        'http://localhost:8084'
    ]
    
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'monitoring',
        'user': 'admin',
        'password': 'admin123'
    }
    
    # Initialize components
    collector = ConnectMetricsCollector(connect_urls)
    aggregator = HealthAggregator('config/monitoring.yaml', db_config)
    alert_manager = AlertManager()
    
    # Start Prometheus metrics server
    start_http_server(8000)
    logger.info("Prometheus metrics server started on :8000")
    
    logger.info("Starting monitoring service...")
    
    while True:
        try:
            # Collect all metrics
            collector.collect_all()
            
            # Process each connector
            connectors = ['instagram-source', 'twitter-source', 'linkedin-source']
            
            for connector in connectors:
                # Simulate metrics (in production, fetch from Prometheus)
                metrics = {
                    'error_rate': random.uniform(0, 0.03),
                    'throughput': random.randint(50, 300),
                    'lag_seconds': random.randint(10, 120),
                    'tasks_running': random.randint(2, 3),
                    'tasks_total': 3
                }
                
                # Calculate health
                health_score = aggregator.calculate_health_score(metrics)
                status = aggregator.determine_status(health_score, metrics)
                
                # Store data
                aggregator.store_health_data(connector, metrics, health_score, status)
                
                # Check alerts
                alerts = aggregator.check_alerts(connector, metrics, health_score)
                if alerts:
                    aggregator.store_alerts(connector, alerts)
                    for alert in alerts:
                        alert['connector'] = connector
                        alert_manager.process_alert(alert)
                        
                logger.info(f"{connector}: Score={health_score:.1f}, Status={status}")
            
            time.sleep(15)
            
        except KeyboardInterrupt:
            logger.info("Monitoring service stopped")
            break
        except Exception as e:
            logger.error(f"Error in monitoring loop: {e}")
            time.sleep(5)

if __name__ == '__main__':
    main()
