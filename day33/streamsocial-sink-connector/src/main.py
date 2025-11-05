"""
StreamSocial Sink Connector - Main Entry Point
"""
import os
import sys
import signal
import logging
from threading import Thread

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from streamsocial.connectors.hashtag_sink_connector import HashtagSinkConnector, ConnectorConfig
from streamsocial.utils.config import SinkConnectorConfig
from web.app import app

def setup_logging():
    """Setup application logging"""
    handlers = [logging.StreamHandler(sys.stdout)]
    
    # Try to add file handler, but continue if permission denied
    try:
        handlers.append(logging.FileHandler('logs/sink_connector.log'))
    except (PermissionError, OSError) as e:
        print(f"Warning: Could not create log file: {e}. Continuing with console logging only.")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers
    )

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    print("\nReceived shutdown signal. Stopping connector...")
    connector.stop()
    sys.exit(0)

if __name__ == '__main__':
    # Create logs directory
    os.makedirs('logs', exist_ok=True)
    
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # Load configuration
    config = SinkConnectorConfig.from_env()
    config.validate()
    
    # Create connector config
    connector_config = ConnectorConfig(
        kafka_bootstrap_servers=config.kafka_bootstrap_servers,
        kafka_topic=config.kafka_topic,
        kafka_group_id=config.kafka_group_id,
        database_url=config.database_url,
        batch_size=config.batch_size,
        flush_timeout_ms=config.flush_timeout_ms,
        retry_backoff_ms=config.retry_backoff_ms,
        max_retries=config.max_retries
    )
    
    # Initialize connector
    connector = HashtagSinkConnector(connector_config)
    
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Start web dashboard in background thread
        dashboard_thread = Thread(target=lambda: app.run(host='0.0.0.0', port=5000, debug=False))
        dashboard_thread.daemon = True
        dashboard_thread.start()
        
        logger.info("Starting StreamSocial Sink Connector...")
        logger.info(f"Dashboard available at http://localhost:5000")
        
        # Start connector (blocking)
        connector.start()
        
    except Exception as e:
        logger.error(f"Error starting connector: {e}")
        sys.exit(1)
