import pytest
import json
import requests
from pathlib import Path
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from registry.schema_registry import SchemaRegistry

class TestSchemaValidation:
    @pytest.fixture
    def schema_registry(self):
        schema_dir = Path(__file__).parent.parent / "src" / "schemas"
        return SchemaRegistry(str(schema_dir))
    
    def test_valid_user_profile(self, schema_registry):
        """Test valid user profile event"""
        valid_event = {
            "event_id": "550e8400-e29b-41d4-a716-446655440000",
            "timestamp": "2025-01-15T10:30:00Z",
            "event_type": "profile_created",
            "user_id": "testuser123",
            "profile_data": {
                "username": "testuser",
                "email": "test@example.com",
                "bio": "Test user bio",
                "privacy_settings": {
                    "profile_visibility": "public",
                    "allow_messages": True
                }
            },
            "metadata": {
                "source": "test_system",
                "version": "1.0.0"
            }
        }
        
        valid, error = schema_registry.validate_event("user.profile.v1", valid_event)
        assert valid is True
        assert error is None
    
    def test_invalid_user_profile_missing_field(self, schema_registry):
        """Test invalid user profile - missing required field"""
        invalid_event = {
            "event_id": "550e8400-e29b-41d4-a716-446655440000",
            "timestamp": "2025-01-15T10:30:00Z",
            "event_type": "profile_created",
            # Missing user_id
            "profile_data": {
                "username": "testuser",
                "email": "test@example.com"
            },
            "metadata": {
                "source": "test_system",
                "version": "1.0.0"
            }
        }
        
        valid, error = schema_registry.validate_event("user.profile.v1", invalid_event)
        assert valid is False
        assert "user_id" in error
    
    def test_invalid_email_format(self, schema_registry):
        """Test invalid email format"""
        invalid_event = {
            "event_id": "550e8400-e29b-41d4-a716-446655440000",
            "timestamp": "2025-01-15T10:30:00Z",
            "event_type": "profile_created",
            "user_id": "testuser123",
            "profile_data": {
                "username": "testuser",
                "email": "invalid-email",  # Invalid email
                "privacy_settings": {
                    "profile_visibility": "public",
                    "allow_messages": True
                }
            },
            "metadata": {
                "source": "test_system",
                "version": "1.0.0"
            }
        }
        
        valid, error = schema_registry.validate_event("user.profile.v1", invalid_event)
        assert valid is False
        assert "email" in error.lower()
    
    def test_valid_interaction_event(self, schema_registry):
        """Test valid interaction event"""
        valid_event = {
            "event_id": "550e8400-e29b-41d4-a716-446655440001",
            "timestamp": "2025-01-15T10:30:00Z",
            "interaction_type": "like",
            "actor_id": "user123",
            "target_id": "post456",
            "interaction_data": {
                "reaction_type": "like"
            },
            "metadata": {
                "source": "mobile_app",
                "version": "1.0.0",
                "client_type": "mobile"
            }
        }
        
        valid, error = schema_registry.validate_event("interaction.v1", valid_event)
        assert valid is True
        assert error is None

@pytest.mark.asyncio
async def test_schema_registry_api():
    """Test schema registry API endpoints"""
    import time
    import subprocess
    
    # Start schema registry server
    try:
        # Test health endpoint
        response = requests.get("http://localhost:8001/health", timeout=5)
        assert response.status_code == 200
        
        # Test schemas endpoint
        response = requests.get("http://localhost:8001/schemas", timeout=5)
        assert response.status_code == 200
        schemas = response.json()
        assert len(schemas) > 0
        
    except requests.exceptions.ConnectionError:
        pytest.skip("Schema registry server not running")
