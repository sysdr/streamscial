import json
import os
from typing import Dict, Optional
from schema_registry.client import SchemaRegistryClient
from schema_registry.serializers import MessageSerializer
import avro.schema

class StreamSocialSchemaManager:
    def __init__(self, registry_url: str = "http://localhost:8081"):
        self.client = SchemaRegistryClient(url=registry_url)
        self.serializers: Dict[str, MessageSerializer] = {}
        self.schemas_dir = os.path.join(os.path.dirname(__file__), '..', 'schemas')
        
    def load_schema(self, schema_name: str) -> avro.schema.Schema:
        """Load Avro schema from file"""
        schema_path = os.path.join(self.schemas_dir, f"{schema_name}.avsc")
        with open(schema_path, 'r') as f:
            schema_dict = json.load(f)
        return avro.schema.parse(json.dumps(schema_dict))
    
    def register_schema(self, subject: str, schema_name: str) -> int:
        """Register schema with Schema Registry"""
        try:
            schema = self.load_schema(schema_name)
            schema_id = self.client.register(subject, schema)
            print(f"✅ Registered schema '{schema_name}' for subject '{subject}' with ID: {schema_id}")
            return schema_id
        except Exception as e:
            print(f"❌ Failed to register schema: {e}")
            return -1
    
    def get_serializer(self, subject: str) -> MessageSerializer:
        """Get or create serializer for subject"""
        if subject not in self.serializers:
            try:
                self.serializers[subject] = MessageSerializer(self.client, subject)
            except Exception as e:
                print(f"❌ Failed to create serializer: {e}")
                return None
        return self.serializers[subject]
    
    def check_compatibility(self, subject: str, schema_name: str) -> bool:
        """Check schema compatibility"""
        try:
            schema = self.load_schema(schema_name)
            result = self.client.check_compatibility(subject, schema)
            print(f"🔍 Schema compatibility for '{subject}': {result}")
            return result
        except Exception as e:
            print(f"❌ Compatibility check failed: {e}")
            return False
