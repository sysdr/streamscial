"""
Schema Registry Manager - Central schema governance
Handles registration, versioning, and compatibility validation
"""

import json
import os
import time
from typing import Dict, List, Optional, Tuple
import requests
from avro import schema as avro_schema
import fastavro


class CompatibilityLevel:
    """Schema compatibility modes"""
    BACKWARD = "BACKWARD"
    FORWARD = "FORWARD"
    FULL = "FULL"
    NONE = "NONE"


class SchemaState:
    """Schema lifecycle states"""
    DRAFT = "DRAFT"
    TESTING = "TESTING"
    APPROVED = "APPROVED"
    ACTIVE = "ACTIVE"
    DEPRECATED = "DEPRECATED"


class SchemaRegistryClient:
    """
    Schema Registry client with compatibility management
    Simulates Confluent Schema Registry behavior
    """
    
    def __init__(self, base_url: str = "http://localhost:8081"):
        self.base_url = base_url
        self.schemas: Dict[str, List[Dict]] = {}  # subject -> [versions]
        self.compatibility_config: Dict[str, str] = {}
        self.schema_states: Dict[str, str] = {}
        self.metrics = {
            'registrations': 0,
            'validations': 0,
            'compatibility_checks': 0,
            'failures': 0
        }
        
    def register_schema(self, subject: str, schema_str: str, 
                       compatibility_level: str = CompatibilityLevel.BACKWARD) -> Tuple[int, bool]:
        """
        Register new schema version with compatibility validation
        
        Returns: (schema_id, is_compatible)
        """
        start_time = time.time()
        
        try:
            # Parse schema
            schema_obj = json.loads(schema_str)
            
            # Initialize subject if new
            if subject not in self.schemas:
                self.schemas[subject] = []
                self.compatibility_config[subject] = compatibility_level
                self.schema_states[subject] = SchemaState.DRAFT
            
            # Check compatibility
            is_compatible = self._check_compatibility(subject, schema_str, 
                                                     self.compatibility_config.get(subject, compatibility_level))
            
            if not is_compatible:
                self.metrics['failures'] += 1
                return -1, False
            
            # Assign version
            version = len(self.schemas[subject]) + 1
            schema_id = hash(f"{subject}-{version}") % 100000
            
            self.schemas[subject].append({
                'id': schema_id,
                'version': version,
                'schema': schema_str,
                'schema_obj': schema_obj,
                'registered_at': time.time(),
                'state': SchemaState.TESTING
            })
            
            self.metrics['registrations'] += 1
            self.metrics['validations'] += 1
            
            duration = (time.time() - start_time) * 1000
            print(f"✓ Registered {subject} v{version} (schema_id={schema_id}) in {duration:.1f}ms")
            
            return schema_id, True
            
        except Exception as e:
            print(f"✗ Schema registration failed: {e}")
            self.metrics['failures'] += 1
            return -1, False
    
    def _check_compatibility(self, subject: str, new_schema_str: str, 
                           compatibility_level: str) -> bool:
        """
        Check schema compatibility against existing versions
        """
        self.metrics['compatibility_checks'] += 1
        
        if subject not in self.schemas or len(self.schemas[subject]) == 0:
            return True  # First schema always compatible
        
        new_schema = json.loads(new_schema_str)
        latest_schema = self.schemas[subject][-1]['schema_obj']
        
        if compatibility_level == CompatibilityLevel.BACKWARD:
            return self._is_backward_compatible(latest_schema, new_schema)
        elif compatibility_level == CompatibilityLevel.FORWARD:
            return self._is_forward_compatible(latest_schema, new_schema)
        elif compatibility_level == CompatibilityLevel.FULL:
            return (self._is_backward_compatible(latest_schema, new_schema) and 
                   self._is_forward_compatible(latest_schema, new_schema))
        else:
            return True
    
    def _is_backward_compatible(self, old_schema: dict, new_schema: dict) -> bool:
        """
        Check if new schema can read data written with old schema
        New schema can add optional fields or remove optional fields
        """
        old_fields = {f['name']: f for f in old_schema.get('fields', [])}
        new_fields = {f['name']: f for f in new_schema.get('fields', [])}
        
        # All old required fields must exist in new schema
        for field_name, field in old_fields.items():
            if field_name not in new_fields:
                # Field removed - only OK if it was optional
                if 'default' not in field and field.get('type') != ['null', 'string']:
                    print(f"✗ Backward incompatible: Required field '{field_name}' removed")
                    return False
        
        # New required fields must have defaults
        for field_name, field in new_fields.items():
            if field_name not in old_fields:
                # New field - must be optional
                if 'default' not in field:
                    print(f"✗ Backward incompatible: New required field '{field_name}' without default")
                    return False
        
        return True
    
    def _is_forward_compatible(self, old_schema: dict, new_schema: dict) -> bool:
        """
        Check if old schema can read data written with new schema
        New schema can only add optional fields
        """
        old_fields = {f['name']: f for f in old_schema.get('fields', [])}
        new_fields = {f['name']: f for f in new_schema.get('fields', [])}
        
        # All new required fields must exist in old schema
        for field_name, field in new_fields.items():
            if field_name not in old_fields and 'default' not in field:
                print(f"✗ Forward incompatible: New required field '{field_name}'")
                return False
        
        return True
    
    def get_schema_by_id(self, schema_id: int) -> Optional[str]:
        """Retrieve schema by ID"""
        for subject_schemas in self.schemas.values():
            for schema_data in subject_schemas:
                if schema_data['id'] == schema_id:
                    return schema_data['schema']
        return None
    
    def get_latest_schema(self, subject: str) -> Optional[Tuple[int, str]]:
        """Get latest schema version for subject"""
        if subject not in self.schemas or len(self.schemas[subject]) == 0:
            return None
        latest = self.schemas[subject][-1]
        return latest['id'], latest['schema']
    
    def get_all_versions(self, subject: str) -> List[Dict]:
        """Get all schema versions for subject"""
        return self.schemas.get(subject, [])
    
    def update_schema_state(self, subject: str, version: int, state: str):
        """Update schema lifecycle state"""
        if subject in self.schemas and version <= len(self.schemas[subject]):
            self.schemas[subject][version - 1]['state'] = state
            print(f"✓ Updated {subject} v{version} state to {state}")
    
    def get_metrics(self) -> dict:
        """Get registry metrics"""
        total_schemas = sum(len(versions) for versions in self.schemas.values())
        return {
            **self.metrics,
            'total_subjects': len(self.schemas),
            'total_schemas': total_schemas,
            'avg_versions_per_subject': total_schemas / max(len(self.schemas), 1)
        }


# Global registry instance
registry_client = SchemaRegistryClient()
