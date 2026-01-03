"""
Test Suite for Schema Governance Framework
Validates compatibility, evolution, and monitoring
"""

import sys
import os
import json
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.registry.schema_manager import SchemaRegistryClient, CompatibilityLevel, SchemaState


class TestSchemaRegistry:
    """Test schema registry functionality"""
    
    def test_schema_registration(self):
        """Test basic schema registration"""
        registry = SchemaRegistryClient()
        
        schema_v1 = json.dumps({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "value", "type": "int"}
            ]
        })
        
        schema_id, is_compatible = registry.register_schema("test-subject", schema_v1)
        
        assert schema_id > 0
        assert is_compatible == True
        assert len(registry.schemas["test-subject"]) == 1
    
    def test_backward_compatibility(self):
        """Test backward compatible schema evolution"""
        registry = SchemaRegistryClient()
        
        # Register v1
        schema_v1 = json.dumps({
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "name", "type": "string"}
            ]
        })
        registry.register_schema("users", schema_v1, CompatibilityLevel.BACKWARD)
        
        # Add optional field (backward compatible)
        schema_v2 = json.dumps({
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": "string", "default": ""}
            ]
        })
        
        schema_id, is_compatible = registry.register_schema("users", schema_v2, CompatibilityLevel.BACKWARD)
        
        assert is_compatible == True
        assert len(registry.schemas["users"]) == 2
    
    def test_backward_incompatibility(self):
        """Test detection of backward incompatible changes"""
        registry = SchemaRegistryClient()
        
        # Register v1
        schema_v1 = json.dumps({
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "name", "type": "string"}
            ]
        })
        registry.register_schema("users", schema_v1, CompatibilityLevel.BACKWARD)
        
        # Remove required field (backward incompatible)
        schema_v2 = json.dumps({
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "string"}
            ]
        })
        
        schema_id, is_compatible = registry.register_schema("users", schema_v2, CompatibilityLevel.BACKWARD)
        
        assert is_compatible == False
        assert len(registry.schemas["users"]) == 1  # v2 not registered
    
    def test_schema_retrieval(self):
        """Test schema retrieval by ID"""
        registry = SchemaRegistryClient()
        
        schema_str = json.dumps({
            "type": "record",
            "name": "Test",
            "fields": [{"name": "id", "type": "string"}]
        })
        
        schema_id, _ = registry.register_schema("test", schema_str)
        retrieved = registry.get_schema_by_id(schema_id)
        
        assert retrieved == schema_str
    
    def test_metrics_tracking(self):
        """Test metrics collection"""
        registry = SchemaRegistryClient()
        
        schema1 = json.dumps({"type": "record", "name": "A", "fields": [{"name": "x", "type": "int"}]})
        schema2 = json.dumps({"type": "record", "name": "B", "fields": [{"name": "y", "type": "int"}]})
        
        registry.register_schema("subject1", schema1)
        registry.register_schema("subject2", schema2)
        
        metrics = registry.get_metrics()
        
        assert metrics['registrations'] == 2
        assert metrics['total_subjects'] == 2
        assert metrics['total_schemas'] == 2


class TestSchemaEvolution:
    """Test schema evolution patterns"""
    
    def test_adding_optional_field(self):
        """Test adding optional field (backward compatible)"""
        registry = SchemaRegistryClient()
        
        old_schema = {
            "type": "record",
            "name": "Post",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "content", "type": "string"}
            ]
        }
        
        new_schema = {
            "type": "record",
            "name": "Post",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "content", "type": "string"},
                {"name": "media_url", "type": "string", "default": ""}
            ]
        }
        
        assert registry._is_backward_compatible(old_schema, new_schema) == True
    
    def test_removing_optional_field(self):
        """Test removing optional field"""
        registry = SchemaRegistryClient()
        
        old_schema = {
            "type": "record",
            "name": "Post",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "content", "type": "string"},
                {"name": "temp_field", "type": "string", "default": ""}
            ]
        }
        
        new_schema = {
            "type": "record",
            "name": "Post",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "content", "type": "string"}
            ]
        }
        
        assert registry._is_backward_compatible(old_schema, new_schema) == True


def run_tests():
    """Run all tests"""
    print("\n" + "="*60)
    print("Running Schema Governance Tests")
    print("="*60 + "\n")
    
    pytest.main([__file__, '-v', '--tb=short'])


if __name__ == '__main__':
    run_tests()
