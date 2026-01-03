"""
Schema Adaptive Consumer - Handles multiple schema versions gracefully
Demonstrates forward and backward compatibility in practice
"""

import json
import time
from typing import Dict, Any, Optional
from io import BytesIO
import fastavro
from src.registry.schema_manager import registry_client


class SchemaAdaptiveConsumer:
    """
    Consumer that adapts to multiple schema versions
    Implements compatibility patterns used by LinkedIn and Netflix
    """
    
    def __init__(self, consumer_name: str = "adaptive-consumer"):
        self.consumer_name = consumer_name
        self.metrics = {
            'consumed_by_version': {'v1': 0, 'v2': 0, 'v3': 0, 'unknown': 0},
            'total_consumed': 0,
            'compatibility_errors': 0,
            'processing_times': []
        }
        self.schema_cache = {}
    
    def deserialize_message(self, message_bytes: bytes) -> Optional[Dict[str, Any]]:
        """
        Deserialize message using schema registry
        Handles schema ID extraction and schema lookup
        """
        start_time = time.time()
        
        try:
            # Extract schema ID (first 4 bytes - Schema Registry wire format)
            schema_id = int.from_bytes(message_bytes[:4], byteorder='big')
            avro_bytes = message_bytes[4:]
            
            # Get schema from registry (with caching)
            if schema_id not in self.schema_cache:
                schema_str = registry_client.get_schema_by_id(schema_id)
                if schema_str:
                    self.schema_cache[schema_id] = json.loads(schema_str)
                else:
                    print(f"✗ Schema {schema_id} not found in registry")
                    return None
            
            schema_obj = self.schema_cache[schema_id]
            
            # Deserialize Avro
            bytes_io = BytesIO(avro_bytes)
            record = fastavro.schemaless_reader(bytes_io, schema_obj)
            
            duration = (time.time() - start_time) * 1000
            self.metrics['processing_times'].append(duration)
            
            return {
                'schema_id': schema_id,
                'schema_version': self._detect_version(record),
                'data': record,
                'deserialization_time_ms': duration
            }
            
        except Exception as e:
            print(f"✗ Deserialization error: {e}")
            self.metrics['compatibility_errors'] += 1
            return None
    
    def _detect_version(self, record: Dict[str, Any]) -> str:
        """Detect schema version from record structure"""
        has_poll = 'poll_data' in record
        has_media = 'media' in record
        
        if has_media:
            return 'v3'
        elif has_poll:
            return 'v2'
        else:
            return 'v1'
    
    def process_post(self, deserialized: Dict[str, Any]):
        """
        Process post with version-aware logic
        Demonstrates graceful handling of missing/extra fields
        """
        data = deserialized['data']
        version = deserialized['schema_version']
        
        # Core processing (works for all versions)
        post_id = data['post_id']
        user_id = data['user_id']
        content = data['content']
        
        # Version-specific processing
        extras = []
        
        # v2+ features
        if 'poll_data' in data and data['poll_data']:
            poll = data['poll_data']
            extras.append(f"Poll({poll['question']}, {len(poll['options'])} options)")
        
        # v3+ features
        if 'media' in data and data['media']:
            media = data['media']
            extras.append(f"Media({media['media_type']}, {len(media['urls'])} items)")
        
        extra_info = ', '.join(extras) if extras else 'none'
        
        print(f"← Consumed {version} post {post_id} from {user_id} "
              f"[extras: {extra_info}] "
              f"(deser: {deserialized['deserialization_time_ms']:.1f}ms)")
        
        self.metrics['consumed_by_version'][version] += 1
        self.metrics['total_consumed'] += 1
    
    def consume_messages(self, messages: list):
        """Consume batch of messages with different schema versions"""
        print(f"\n=== Consumer '{self.consumer_name}' Processing Messages ===\n")
        
        for msg_bytes in messages:
            deserialized = self.deserialize_message(msg_bytes)
            if deserialized:
                self.process_post(deserialized)
                time.sleep(0.02)  # Simulate processing time
        
        # Print summary
        print(f"\n=== Consumption Complete ===")
        print(f"Total consumed: {self.metrics['total_consumed']}")
        print(f"By version: {self.metrics['consumed_by_version']}")
        print(f"Compatibility errors: {self.metrics['compatibility_errors']}")
        if self.metrics['processing_times']:
            avg_time = sum(self.metrics['processing_times']) / len(self.metrics['processing_times'])
            print(f"Avg deserialization: {avg_time:.2f}ms")
    
    def get_metrics(self) -> dict:
        """Get consumer metrics"""
        return self.metrics


# Create consumer instance
adaptive_consumer = SchemaAdaptiveConsumer()
