import logging
import threading
import time
import signal
import sys
from concurrent.futures import ThreadPoolExecutor
import redis
from prometheus_client import start_http_server

from consumers.feed_consumer import FeedGeneratorConsumer
from producers.interaction_producer import InteractionProducer
from monitoring.lag_monitor import LagMonitor
from monitoring.dashboard import RebalancingDashboard
from config.kafka_config import *

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StreamSocialRebalancingDemo:
    def __init__(self):
        self.consumers = []
        self.producer = None
        self.lag_monitor = None
        self.dashboard = None
        self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
        self.running = False
        
        # Initialize consumer count
        with open('/tmp/consumer_count.txt', 'w') as f:
            f.write(str(MIN_CONSUMERS))
            
    def start(self):
        """Start the complete system"""
        logger.info("🚀 Starting StreamSocial Rebalancing Demo...")
        
        self.running = True
        
        # Start initial consumers
        self._start_consumers(MIN_CONSUMERS)
        
        # Start event producer
        self.producer = InteractionProducer()
        self.producer.start_generating(events_per_second=100)
        
        # Start lag monitor
        self.lag_monitor = LagMonitor()
        self.lag_monitor.start_monitoring()
        
        # Start dashboard in separate thread
        self.dashboard = RebalancingDashboard()
        dashboard_thread = threading.Thread(
            target=self.dashboard.run, 
            kwargs={'debug': False}, 
            daemon=True
        )
        dashboard_thread.start()
        
        # Start consumer auto-scaling
        scaling_thread = threading.Thread(target=self._auto_scaling_loop, daemon=True)
        scaling_thread.start()
        
        logger.info("✅ System started successfully!")
        logger.info("📊 Dashboard available at: http://localhost:8050")
        logger.info("📈 Prometheus metrics at: http://localhost:8000")
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def _start_consumers(self, count: int):
        """Start specified number of consumers"""
        current_count = len(self.consumers)
        
        if count > current_count:
            # Start new consumers
            for i in range(current_count, count):
                consumer = FeedGeneratorConsumer(f"consumer-{i}")
                consumer_thread = threading.Thread(target=consumer.start, daemon=True)
                consumer_thread.start()
                self.consumers.append(consumer)
                logger.info(f"Started consumer-{i}")
                
        elif count < current_count:
            # Stop excess consumers
            for i in range(count, current_count):
                if i < len(self.consumers):
                    self.consumers[i].stop()
                    logger.info(f"Stopped consumer-{i}")
            self.consumers = self.consumers[:count]
            
    def _auto_scaling_loop(self):
        """Auto-scaling loop based on consumer count file"""
        last_count = MIN_CONSUMERS
        
        while self.running:
            try:
                with open('/tmp/consumer_count.txt', 'r') as f:
                    target_count = int(f.read().strip())
                    
                if target_count != last_count:
                    logger.info(f"Scaling consumers: {last_count} -> {target_count}")
                    self._start_consumers(target_count)
                    last_count = target_count
                    
            except Exception as e:
                logger.error(f"Error in auto-scaling loop: {e}")
                
            time.sleep(5)
            
    def create_traffic_spike(self, duration: int = 60):
        """Create traffic spike for demonstration"""
        if self.producer:
            logger.info("🔥 Creating traffic spike...")
            spike_thread = threading.Thread(
                target=self.producer.create_traffic_spike,
                args=(duration, 10),
                daemon=True
            )
            spike_thread.start()
            
    def get_system_stats(self):
        """Get overall system statistics"""
        stats = {
            'active_consumers': len(self.consumers),
            'consumer_stats': [consumer.get_stats() for consumer in self.consumers],
            'current_lag': self.lag_monitor.get_current_lag() if self.lag_monitor else {},
            'events_per_second': self.producer.events_per_second if self.producer else 0
        }
        return stats
        
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info("Received shutdown signal, stopping system...")
        self.stop()
        sys.exit(0)
        
    def stop(self):
        """Stop the complete system"""
        logger.info("🛑 Stopping StreamSocial Rebalancing Demo...")
        
        self.running = False
        
        # Stop all consumers
        for consumer in self.consumers:
            consumer.stop()
            
        # Stop producer
        if self.producer:
            self.producer.stop()
            
        # Stop lag monitor
        if self.lag_monitor:
            self.lag_monitor.stop()
            
        logger.info("✅ System stopped successfully!")

def main():
    # Start Prometheus metrics server
    start_http_server(8000)
    logger.info("📊 Prometheus metrics server started on port 8000")
    
    demo = StreamSocialRebalancingDemo()
    
    try:
        demo.start()
        
        # Keep main thread alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        demo.stop()

if __name__ == "__main__":
    main()
