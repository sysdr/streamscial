"""
Main entry point - runs connector with tasks
"""
import time
import threading
import logging
from src.connector.social_stream_connector import load_connector_from_config
from src.connector.source_task import SocialStreamSourceTask
from src.monitoring.metrics_collector import MetricsCollector
from dashboards.connector_dashboard import start_dashboard, update_task_metrics

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_task(task: SocialStreamSourceTask, poll_interval_ms: int):
    """Run task poll loop"""
    while task.running:
        # Poll for new records
        records = task.poll()
        
        # Commit records to Kafka
        for record in records:
            task.commit_record(record)
            
        # Update dashboard
        update_task_metrics(task.task_id, task.get_metrics())
        
        # Wait before next poll
        time.sleep(poll_interval_ms / 1000.0)


def main():
    logger.info("Starting SocialStream Connector...")
    
    # Load connector
    connector = load_connector_from_config('config/connector_config.yaml')
    
    # Get task configurations
    task_configs = connector.task_configs(max_tasks=4)
    logger.info(f"Created {len(task_configs)} task configurations")
    
    # Start metrics collector
    metrics = MetricsCollector(port=8080)
    metrics.start_server()
    
    # Start dashboard
    dashboard_thread = threading.Thread(
        target=start_dashboard,
        args=(5000,),
        daemon=True
    )
    dashboard_thread.start()
    logger.info("Dashboard started at http://localhost:5000")
    
    # Create and start tasks
    tasks = []
    task_threads = []
    
    for i, task_config in enumerate(task_configs):
        task = SocialStreamSourceTask(task_id=i)
        task.start(task_config)
        tasks.append(task)
        
        # Start task thread
        thread = threading.Thread(
            target=run_task,
            args=(task, task_config['poll_interval_ms']),
            daemon=True
        )
        thread.start()
        task_threads.append(thread)
        
    logger.info(f"Started {len(tasks)} tasks")
    logger.info("Connector running. Press Ctrl+C to stop.")
    
    try:
        # Keep main thread alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        
        # Stop all tasks
        for task in tasks:
            task.stop()
            
        # Stop connector
        connector.stop()
        
        logger.info("Shutdown complete")


if __name__ == '__main__':
    main()
