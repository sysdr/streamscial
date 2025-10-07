import json
import time
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from typing import Dict, Any, Optional
import sys
sys.path.append('..')

from common.headers import HeaderManager, HeaderKeys
from common.tracing import TracingContext, global_tracer, Span

class PostConsumer:
    def __init__(self, group_id: str, bootstrap_servers='localhost:9092'):
        self.consumer = KafkaConsumer(
            'posts',
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            key_deserializer=lambda x: x.decode('utf-8') if x else None,
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        self.service_name = f"post-consumer-{group_id}"
        self.processed_count = 0
        
    def process_posts(self, max_messages: int = None):
        print(f"🔄 {self.service_name} started consuming posts...")
        
        try:
            for message_count, message in enumerate(self.consumer):
                if max_messages and message_count >= max_messages:
                    break
                    
                self._process_single_message(message)
                self.processed_count += 1
                
        except KeyboardInterrupt:
            print(f"⏹️ {self.service_name} stopping...")
        except Exception as e:
            print(f"❌ Error in consumer: {e}")
        finally:
            self.consumer.close()
    
    def _process_single_message(self, message):
        # Extract headers
        headers = {}
        if message.headers:
            headers = {k: v.decode('utf-8') for k, v in message.headers}
        
        # Extract trace context from headers
        trace_context = HeaderManager.extract_trace_context(headers)
        feature_flags = HeaderManager.extract_feature_flags(headers)
        
        # Create new span linked to incoming trace
        parent_span_id = trace_context.span_id if trace_context else None
        trace_id = trace_context.trace_id if trace_context else None
        
        with TracingContext.span("process_post", self.service_name, 
                                trace_id=trace_id, parent_span_id=parent_span_id) as span:
            
            post_data = message.value
            user_id = headers.get(HeaderKeys.USER_ID, "unknown")
            
            span.add_tag("kafka.partition", message.partition)
            span.add_tag("kafka.offset", message.offset)
            span.add_tag("user_id", user_id)
            span.add_tag("post_id", post_data.get("post_id"))
            
            if feature_flags:
                span.add_tag("experiment_id", feature_flags.experiment_id)
                span.add_tag("user_segment", feature_flags.user_segment)
            
            # Simulate processing based on feature flags
            processing_time = self._get_processing_time(feature_flags)
            time.sleep(processing_time / 1000)  # Convert to seconds
            
            span.add_tag("processing_time_ms", processing_time)
            span.log(f"Processed post from user {user_id}")
            
            # Log processing details
            self._log_processing_details(post_data, headers, feature_flags)
            
            global_tracer.record_span(span)
    
    def _get_processing_time(self, feature_flags: Optional[object]) -> float:
        """Simulate different processing times based on feature flags"""
        base_time = 50  # 50ms base processing
        
        if feature_flags:
            if feature_flags.enabled_features.get("enhanced_recommendations", False):
                base_time += 30  # Enhanced processing takes longer
            if feature_flags.enabled_features.get("new_feed_algorithm", False):
                base_time += 25  # New algorithm overhead
        
        return base_time
    
    def _log_processing_details(self, post_data: Dict, headers: Dict, feature_flags):
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] 📝 {self.service_name}")
        print(f"   Post ID: {post_data.get('post_id')}")
        print(f"   User: {post_data.get('user_id')}")
        print(f"   Trace ID: {headers.get(HeaderKeys.TRACE_ID, 'N/A')}")
        
        if feature_flags:
            print(f"   Experiment: {feature_flags.experiment_id or 'N/A'}")
            print(f"   Segment: {feature_flags.user_segment}")
            enabled = [k for k, v in feature_flags.enabled_features.items() if v]
            print(f"   Enabled Features: {enabled}")
        
        print(f"   Total Processed: {self.processed_count + 1}")
        print()

class RecommendationConsumer(PostConsumer):
    def __init__(self, bootstrap_servers='localhost:9092'):
        super().__init__("recommendation-service", bootstrap_servers)
        self.service_name = "recommendation-consumer"

class NotificationConsumer(PostConsumer):
    def __init__(self, bootstrap_servers='localhost:9092'):
        super().__init__("notification-service", bootstrap_servers)
        self.service_name = "notification-consumer"
