#!/usr/bin/env python3

import os
import sys
import signal
import threading
import time
import json
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import redis
from prometheus_client import Counter, Histogram, Gauge, start_http_server
from flask import Flask, jsonify
import psutil

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Metrics
messages_processed = Counter('streamsocial_messages_processed_total', 'Total processed messages')
processing_time = Histogram('streamsocial_processing_seconds', 'Processing time per message')
active_consumers = Gauge('streamsocial_active_consumers', 'Number of active consumers')
shutdown_duration = Histogram('streamsocial_shutdown_duration_seconds', 'Time taken for graceful shutdown')

class GracefulShutdownManager:
    def __init__(self):
        self.shutdown_requested = threading.Event()
        self.processing_complete = threading.Event()
        self.cleanup_complete = threading.Event()
        self.shutdown_start_time = None
        
    def request_shutdown(self):
        if not self.shutdown_requested.is_set():
            self.shutdown_start_time = time.time()
            logger.info("🛑 Graceful shutdown requested")
            self.shutdown_requested.set()
    
    def mark_processing_complete(self):
        self.processing_complete.set()
        logger.info("✅ Processing phase complete")
    
    def mark_cleanup_complete(self):
        self.cleanup_complete.set()
        duration = time.time() - self.shutdown_start_time if self.shutdown_start_time else 0
        shutdown_duration.observe(duration)
        logger.info(f"✅ Cleanup complete in {duration:.2f}s")

class RecommendationProcessor:
    def __init__(self, redis_client):
        self.redis_client = redis_client
        self.user_cache = {}
        
    def process_recommendation_event(self, message):
        """Process recommendation events with realistic business logic"""
        start_time = time.time()
        
        try:
            event_data = json.loads(message.value.decode('utf-8'))
            user_id = event_data.get('user_id')
            event_type = event_data.get('event_type', 'view')
            content_id = event_data.get('content_id')
            
            # Simulate recommendation processing
            if user_id not in self.user_cache:
                # Load user preferences from Redis
                preferences = self.redis_client.get(f"user:{user_id}:preferences")
                self.user_cache[user_id] = json.loads(preferences) if preferences else {"categories": [], "engagement_score": 0.5}
            
            # Update user engagement
            user_prefs = self.user_cache[user_id]
            if event_type == 'like':
                user_prefs['engagement_score'] = min(1.0, user_prefs['engagement_score'] + 0.1)
            elif event_type == 'share':
                user_prefs['engagement_score'] = min(1.0, user_prefs['engagement_score'] + 0.2)
            
            # Generate recommendations (simulate ML inference)
            recommendations = self._generate_recommendations(user_prefs, content_id)
            
            # Store updated preferences
            self.redis_client.setex(f"user:{user_id}:preferences", 3600, json.dumps(user_prefs))
            self.redis_client.setex(f"user:{user_id}:recommendations", 1800, json.dumps(recommendations))
            
            messages_processed.inc()
            processing_time.observe(time.time() - start_time)
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return False
    
    def _generate_recommendations(self, user_prefs, content_id):
        """Generate personalized recommendations"""
        base_score = user_prefs['engagement_score']
        return {
            "recommended_content": [f"content_{i}" for i in range(5)],
            "confidence_scores": [base_score * 0.9, base_score * 0.8, base_score * 0.7, base_score * 0.6, base_score * 0.5],
            "generated_at": datetime.now().isoformat()
        }
    
    def persist_cache(self):
        """Persist in-memory cache to Redis during shutdown"""
        logger.info(f"💾 Persisting {len(self.user_cache)} user preferences")
        for user_id, prefs in self.user_cache.items():
            self.redis_client.setex(f"user:{user_id}:preferences", 3600, json.dumps(prefs))

class StreamSocialConsumer:
    def __init__(self, topic_name="recommendation-events", group_id="recommendation-processors"):
        self.topic_name = topic_name
        self.group_id = group_id
        self.shutdown_manager = GracefulShutdownManager()
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        self.processor = RecommendationProcessor(self.redis_client)
        self.consumer = None
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.health_status = {"status": "healthy", "last_processed": None, "shutdown_phase": "running"}
        
    def setup_shutdown_hooks(self):
        """Register shutdown handlers for graceful termination"""
        def signal_handler(signum, frame):
            signal_name = signal.Signals(signum).name
            logger.info(f"📡 Received {signal_name} signal")
            self.shutdown_manager.request_shutdown()
            
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
    
    def create_producer_for_demo(self):
        """Create sample events for demonstration"""
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        # Generate sample recommendation events
        sample_events = [
            {"user_id": f"user_{i}", "event_type": "view", "content_id": f"content_{i%10}", "timestamp": time.time()}
            for i in range(100)
        ]
        
        for event in sample_events:
            producer.send(self.topic_name, event)
            time.sleep(0.1)  # Simulate realistic event rate
            
        producer.flush()
        producer.close()
        logger.info("📤 Generated 100 sample events")
    
    def start_health_server(self):
        """Start health check HTTP server"""
        app = Flask(__name__)
        
        @app.route('/health')
        def health():
            return jsonify(self.health_status)
        
        @app.route('/metrics')
        def metrics():
            from prometheus_client import generate_latest
            return generate_latest()
        
        # Start server in background thread
        threading.Thread(target=lambda: app.run(host='0.0.0.0', port=8080), daemon=True).start()
    
    def run(self):
        """Main consumer loop with graceful shutdown"""
        self.setup_shutdown_hooks()
        self.start_health_server()
        start_http_server(9090)  # Prometheus metrics
        
        # Create Kafka consumer
        self.consumer = KafkaConsumer(
            self.topic_name,
            bootstrap_servers=['localhost:9092'],
            group_id=self.group_id,
            enable_auto_commit=False,  # Manual offset management
            auto_offset_reset='earliest',
            consumer_timeout_ms=1000,
            max_poll_records=10
        )
        
        active_consumers.inc()
        logger.info(f"🎯 Consumer started for topic: {self.topic_name}")
        
        # Generate demo data
        threading.Thread(target=self.create_producer_for_demo, daemon=True).start()
        
        try:
            while not self.shutdown_manager.shutdown_requested.is_set():
                self.health_status["shutdown_phase"] = "running"
                
                try:
                    message_batch = self.consumer.poll(timeout_ms=1000)
                    
                    if message_batch:
                        futures = []
                        
                        for topic_partition, messages in message_batch.items():
                            for message in messages:
                                future = self.executor.submit(self.processor.process_recommendation_event, message)
                                futures.append((future, message))
                        
                        # Process all messages in batch
                        processed_messages = []
                        for future, message in futures:
                            try:
                                if future.result(timeout=5):  # 5 second timeout per message
                                    processed_messages.append(message)
                                    self.health_status["last_processed"] = datetime.now().isoformat()
                            except Exception as e:
                                logger.error(f"Message processing failed: {e}")
                        
                        # Commit offsets for successfully processed messages
                        if processed_messages:
                            self.consumer.commit()
                            logger.debug(f"✅ Committed {len(processed_messages)} messages")
                
                except Exception as e:
                    logger.error(f"Poll error: {e}")
                    
        except KeyboardInterrupt:
            logger.info("🔄 Interrupted by user")
            
        finally:
            self.graceful_shutdown()
    
    def graceful_shutdown(self):
        """Execute graceful shutdown sequence"""
        logger.info("🔄 Starting graceful shutdown sequence")
        self.health_status["shutdown_phase"] = "shutdown_requested"
        
        # Phase 1: Stop accepting new work
        logger.info("Phase 1: Stopping message consumption")
        if self.consumer:
            self.consumer.close()
        
        # Phase 2: Complete in-flight processing
        logger.info("Phase 2: Completing in-flight processing")
        self.health_status["shutdown_phase"] = "draining"
        self.executor.shutdown(wait=True, timeout=30)
        self.shutdown_manager.mark_processing_complete()
        
        # Phase 3: Persist state
        logger.info("Phase 3: Persisting state")
        self.health_status["shutdown_phase"] = "committing"
        self.processor.persist_cache()
        
        # Phase 4: Cleanup resources
        logger.info("Phase 4: Cleaning up resources")
        self.health_status["shutdown_phase"] = "cleanup"
        self.redis_client.close()
        active_consumers.dec()
        
        self.health_status["shutdown_phase"] = "terminated"
        self.shutdown_manager.mark_cleanup_complete()
        logger.info("✅ Graceful shutdown completed")

if __name__ == "__main__":
    consumer = StreamSocialConsumer()
    consumer.run()
