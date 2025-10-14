import json
import requests
from kafka import KafkaConsumer
from typing import Callable, Dict, Any
import logging

logger = logging.getLogger(__name__)

class SchemaValidatingConsumer:
    def __init__(self, topics: list, bootstrap_servers: str, group_id: str,
                 schema_registry_url: str = "http://localhost:8001"):
        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        self.schema_registry_url = schema_registry_url
        self.message_handlers: Dict[str, Callable] = {}
    
    def register_handler(self, topic: str, schema_name: str, handler: Callable):
        """Register message handler for topic"""
        self.message_handlers[topic] = {
            "schema_name": schema_name,
            "handler": handler
        }
    
    def _validate_message(self, schema_name: str, message: Dict[str, Any]) -> tuple[bool, str]:
        """Validate message against schema"""
        try:
            response = requests.post(
                f"{self.schema_registry_url}/validate",
                json={"schema_name": schema_name, "event": message},
                timeout=5
            )
            
            if response.status_code == 200:
                result = response.json()
                return result["valid"], result.get("error", "")
            return False, f"Registry error: {response.status_code}"
            
        except Exception as e:
            return False, str(e)
    
    def start_consuming(self):
        """Start consuming messages with schema validation"""
        logger.info("Starting schema-validating consumer...")
        
        for message in self.consumer:
            topic = message.topic
            
            if topic not in self.message_handlers:
                logger.warning(f"No handler registered for topic: {topic}")
                continue
            
            handler_config = self.message_handlers[topic]
            schema_name = handler_config["schema_name"]
            handler = handler_config["handler"]
            
            # Validate message
            valid, error = self._validate_message(schema_name, message.value)
            
            if not valid:
                logger.error(f"Invalid message in {topic}: {error}")
                continue
            
            try:
                # Process valid message
                handler(message.value)
                logger.info(f"Processed message from {topic}")
                
            except Exception as e:
                logger.error(f"Handler error for {topic}: {e}")
    
    def close(self):
        self.consumer.close()
