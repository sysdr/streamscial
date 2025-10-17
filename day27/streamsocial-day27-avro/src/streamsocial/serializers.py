import json
import avro.schema
import avro.io
import io
from typing import Dict, Any, Union
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer

class StreamSocialAvroSerializer:
    def __init__(self, schema_registry_client, schema_str: str):
        self.serializer = AvroSerializer(schema_registry_client, schema_str)
    
    def serialize(self, obj: Dict[str, Any], ctx: SerializationContext) -> bytes:
        return self.serializer(obj, ctx)

class StreamSocialAvroDeserializer:
    def __init__(self, schema_registry_client):
        self.deserializer = AvroDeserializer(schema_registry_client)
    
    def deserialize(self, data: bytes, ctx: SerializationContext) -> Dict[str, Any]:
        return self.deserializer(data, ctx)

class PerformanceSerializer:
    """Measure serialization performance"""
    
    def __init__(self, avro_serializer, json_serializer=None):
        self.avro_serializer = avro_serializer
        self.json_serializer = json_serializer or json.dumps
        
    def compare_sizes(self, data: Dict[str, Any], ctx: SerializationContext) -> Dict[str, int]:
        # Avro size
        avro_bytes = self.avro_serializer.serialize(data, ctx)
        avro_size = len(avro_bytes)
        
        # JSON size  
        json_bytes = self.json_serializer(data).encode('utf-8')
        json_size = len(json_bytes)
        
        return {
            'avro_size': avro_size,
            'json_size': json_size,
            'compression_ratio': round((json_size - avro_size) / json_size * 100, 2)
        }
