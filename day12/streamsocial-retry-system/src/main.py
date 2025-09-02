import json
import time
import logging
import threading
from typing import Dict, Any

from config.kafka_config import KAFKA_CONFIG, RETRY_CONFIG
from producers.retry_producer import StreamSocialRetryProducer
from web.dashboard import app, set_producer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StreamSocialRetrySystem:
    """Main application class for StreamSocial retry system"""
    
    def __init__(self):
        self.producer = None
        self.web_thread = None
        self.demo_thread = None
        self.running = False
        
    def initialize(self):
        """Initialize the retry system"""
        logger.info("🚀 Initializing StreamSocial Retry System...")
        
        # Initialize producer
        self.producer = StreamSocialRetryProducer(KAFKA_CONFIG, RETRY_CONFIG)
        
        # Set producer reference for web dashboard
        set_producer(self.producer)
        
        logger.info("✅ StreamSocial Retry System initialized successfully")
    
    def start_web_server(self):
        """Start the web dashboard"""
        logger.info("🌐 Starting web dashboard on http://localhost:5000")
        app.run(debug=False, host='0.0.0.0', port=5000, use_reloader=False)
    
    def start_demo_traffic(self):
        """Generate demo traffic to show retry behavior"""
        logger.info("📊 Starting demo traffic generation...")
        
        messages = [
            {'type': 'post', 'topic': 'streamsocial-posts'},
            {'type': 'story', 'topic': 'streamsocial-stories'},
            {'type': 'reaction', 'topic': 'streamsocial-reactions'},
            {'type': 'comment', 'topic': 'streamsocial-comments'}
        ]
        
        while self.running:
            for msg_config in messages:
                if not self.running:
                    break
                    
                message = {
                    'user_id': f'demo_user_{int(time.time()) % 100}',
                    'type': msg_config['type'],
                    'content': f'Demo {msg_config["type"]} at {time.strftime("%H:%M:%S")}',
                    'demo_mode': True
                }
                
                try:
                    success = self.producer.send_with_retry(msg_config['topic'], message)
                    logger.info(f"Demo message sent: {success}")
                except Exception as e:
                    logger.error(f"Demo message failed: {e}")
                
                time.sleep(5)  # Send message every 5 seconds
    
    def run(self):
        """Run the complete system"""
        self.initialize()
        self.running = True
        
        # Start web server in separate thread
        self.web_thread = threading.Thread(target=self.start_web_server, daemon=True)
        self.web_thread.start()
        
        # Start demo traffic in separate thread
        self.demo_thread = threading.Thread(target=self.start_demo_traffic, daemon=True)
        self.demo_thread.start()
        
        logger.info("🎯 StreamSocial Retry System is running!")
        logger.info("📊 Dashboard: http://localhost:5000")
        logger.info("⏹️  Press Ctrl+C to stop")
        
        try:
            # Keep main thread alive
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("🛑 Shutting down StreamSocial Retry System...")
            self.stop()
    
    def stop(self):
        """Stop the system"""
        self.running = False
        if self.producer:
            self.producer.close()
        logger.info("✅ StreamSocial Retry System stopped")

if __name__ == '__main__':
    system = StreamSocialRetrySystem()
    system.run()
