import yaml
import logging
import threading
from .processor.stream_processor import InteractionStreamProcessor
from .web.dashboard import create_app, socketio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_config():
    with open('config/kafka_config.yaml', 'r') as f:
        return yaml.safe_load(f)

def main():
    logger.info("Starting StreamSocial KStream Processor...")
    
    # Load configuration
    config = load_config()
    
    # Create processor
    processor = InteractionStreamProcessor(config)
    
    # Start processor in separate thread
    processor_thread = threading.Thread(target=processor.process_stream, daemon=True)
    processor_thread.start()
    
    # Start web dashboard
    app = create_app(processor)
    logger.info("Dashboard available at http://localhost:8080")
    socketio.run(app, host='0.0.0.0', port=8080, debug=False, allow_unsafe_werkzeug=True)

if __name__ == '__main__':
    main()
