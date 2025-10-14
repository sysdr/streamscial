import json
import requests
import time
from kafka import KafkaProducer
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class SchemaValidatingProducer:
    def __init__(self, bootstrap_servers: str, schema_registry_url: str = "http://localhost:8001"):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3
        )
        self.schema_registry_url = schema_registry_url
        self.validation_cache = {}
    
    def _validate_with_registry(self, schema_name: str, event: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """Validate event with schema registry"""
        try:
            response = requests.post(
                f"{self.schema_registry_url}/validate",
                json={"schema_name": schema_name, "event": event},
                timeout=5
            )
            
            if response.status_code == 200:
                result = response.json()
                return result["valid"], result.get("error")
            else:
                return False, f"Registry error: {response.status_code}"
                
        except Exception as e:
            logger.error(f"Schema validation failed: {e}")
            return False, str(e)
    
    def send_validated(self, topic: str, schema_name: str, event: Dict[str, Any], 
                      key: Optional[str] = None) -> bool:
        """Send event after schema validation"""
        start_time = time.time()
        
        # Validate against schema
        valid, error = self._validate_with_registry(schema_name, event)
        
        validation_time = time.time() - start_time
        logger.info(f"Schema validation took {validation_time*1000:.2f}ms")
        
        if not valid:
            # Send to DLQ
            dlq_topic = f"{topic}.dlq"
            dlq_event = {
                "original_event": event,
                "validation_error": error,
                "schema_name": schema_name,
                "timestamp": time.time(),
                "topic": topic
            }
            
            self.producer.send(dlq_topic, value=dlq_event, key=key)
            logger.error(f"Event failed validation: {error}")
            return False
        
        # Send valid event
        future = self.producer.send(topic, value=event, key=key)
        future.get(timeout=10)  # Wait for confirmation
        logger.info(f"Event sent successfully to {topic}")
        return True
    
    def close(self):
        self.producer.close()
