import json
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
from typing import Dict, Any
import sys
sys.path.append('..')

from common.headers import HeaderManager, TraceContext, FeatureFlags, HeaderKeys
from common.tracing import TracingContext, global_tracer

class PostProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None
        )
        self.service_name = "post-producer"
    
    def publish_post(self, user_id: str, content: str, feature_flags: Dict[str, bool], 
                    user_segment: str, experiment_id: str = None) -> bool:
        
        with TracingContext.span("publish_post", self.service_name) as span:
            try:
                # Create trace context
                trace_context = TracingContext.get_current_span()
                headers = trace_context.to_dict() if trace_context else {}
                
                # Add feature flags
                flags = FeatureFlags(
                    enabled_features=feature_flags,
                    user_segment=user_segment,
                    experiment_id=experiment_id
                )
                headers.update(flags.to_headers())
                headers[HeaderKeys.USER_ID] = user_id
                
                # Convert headers to bytes for Kafka
                kafka_headers = [(k, str(v).encode('utf-8')) for k, v in headers.items()]
                
                # Create post message
                post_data = {
                    "user_id": user_id,
                    "content": content,
                    "timestamp": int(time.time() * 1000),
                    "post_id": f"post_{int(time.time() * 1000000)}"
                }
                
                span.add_tag("user_id", user_id)
                span.add_tag("content_length", len(content))
                span.add_tag("experiment_id", experiment_id)
                
                # Send to Kafka with headers
                future = self.producer.send(
                    'posts',
                    key=user_id,
                    value=post_data,
                    headers=kafka_headers
                )
                
                # Wait for send to complete
                result = future.get(timeout=10)
                
                span.add_tag("kafka.topic", "posts")
                span.add_tag("kafka.partition", result.partition)
                span.add_tag("kafka.offset", result.offset)
                span.log(f"Post published successfully to partition {result.partition}")
                
                global_tracer.record_span(span)
                return True
                
            except KafkaError as e:
                span.add_tag("error", True)
                span.log(f"Kafka error: {str(e)}")
                global_tracer.record_span(span)
                return False
            except Exception as e:
                span.add_tag("error", True)
                span.log(f"Unexpected error: {str(e)}")
                global_tracer.record_span(span)
                return False
    
    def close(self):
        self.producer.close()

# Feature flag service simulation
class FeatureFlagService:
    def __init__(self):
        self.flags = {
            "new_feed_algorithm": {"enabled": True, "rollout": 0.1},
            "enhanced_recommendations": {"enabled": True, "rollout": 0.2},
            "real_time_notifications": {"enabled": True, "rollout": 0.5},
            "advanced_analytics": {"enabled": False, "rollout": 0.0}
        }
        self.experiments = {
            "feed_algorithm_v2": {"active": True, "treatment_rate": 0.1},
            "recommendation_ml": {"active": True, "treatment_rate": 0.2}
        }
    
    def get_user_flags(self, user_id: str, user_segment: str) -> tuple:
        import hashlib
        
        # Determine user's hash for consistent assignment
        user_hash = int(hashlib.md5(user_id.encode()).hexdigest(), 16) / (16**32)
        
        enabled_flags = {}
        experiment_id = None
        
        # Check feature flags
        for flag, config in self.flags.items():
            if config["enabled"] and user_hash < config["rollout"]:
                enabled_flags[flag] = True
            else:
                enabled_flags[flag] = False
        
        # Check experiments
        for exp_id, config in self.experiments.items():
            if config["active"] and user_hash < config["treatment_rate"]:
                experiment_id = exp_id
                break
        
        return enabled_flags, experiment_id
