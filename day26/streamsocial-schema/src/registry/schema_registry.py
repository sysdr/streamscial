import json
import os
import hashlib
import re
from typing import Dict, Optional, Any
from datetime import datetime
import redis
from jsonschema import Draft7Validator, ValidationError
from pathlib import Path

class SchemaRegistry:
    def __init__(self, schema_dir: str, redis_client: Optional[redis.Redis] = None):
        self.schema_dir = Path(schema_dir)
        redis_host = os.getenv('REDIS_HOST', 'localhost')
        redis_port = int(os.getenv('REDIS_PORT', '6379'))
        self.redis = redis_client or redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.schemas: Dict[str, Dict] = {}
        self.validators: Dict[str, Draft7Validator] = {}
        self._load_schemas()
    
    def _load_schemas(self):
        """Load all schemas from directory"""
        config_path = self.schema_dir / "registry_config.json"
        with open(config_path) as f:
            config = json.load(f)
        
        for schema_name, schema_config in config["schemas"].items():
            schema_path = self.schema_dir / schema_config["file"]
            with open(schema_path) as f:
                schema = json.load(f)
            
            self.schemas[schema_name] = {
                "schema": schema,
                "config": schema_config,
                "version": self._calculate_version(schema),
                "loaded_at": datetime.utcnow().isoformat()
            }
            
            self.validators[schema_name] = Draft7Validator(schema)
            
            # Cache in Redis
            self.redis.hset(f"schema:{schema_name}", mapping={
                "schema": json.dumps(schema),
                "version": self.schemas[schema_name]["version"],
                "loaded_at": self.schemas[schema_name]["loaded_at"]
            })
    
    def _calculate_version(self, schema: Dict) -> str:
        """Calculate schema version hash"""
        schema_str = json.dumps(schema, sort_keys=True)
        return hashlib.sha256(schema_str.encode()).hexdigest()[:8]
    
    def _validate_email_format(self, email: str) -> bool:
        """Validate email format using regex"""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
    
    def _validate_formats(self, data: Any, schema: Dict, path: str = "") -> Optional[str]:
        """Custom format validation"""
        if isinstance(data, dict):
            for key, value in data.items():
                current_path = f"{path}.{key}" if path else key
                error = self._validate_formats(value, schema.get("properties", {}).get(key, {}), current_path)
                if error:
                    return error
        elif isinstance(data, list):
            for i, item in enumerate(data):
                current_path = f"{path}[{i}]"
                error = self._validate_formats(item, schema.get("items", {}), current_path)
                if error:
                    return error
        elif isinstance(data, str) and "format" in schema:
            if schema["format"] == "email" and not self._validate_email_format(data):
                return f"Invalid email format at path: {path}"
            elif schema["format"] == "uri" and not data.startswith(("http://", "https://", "ftp://")):
                return f"Invalid URI format at path: {path}"
            elif schema["format"] == "date-time":
                # Basic ISO 8601 validation
                try:
                    from datetime import datetime
                    datetime.fromisoformat(data.replace('Z', '+00:00'))
                except ValueError:
                    return f"Invalid date-time format at path: {path}"
            elif schema["format"] == "uuid":
                uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                if not re.match(uuid_pattern, data, re.IGNORECASE):
                    return f"Invalid UUID format at path: {path}"
        
        return None

    def validate_event(self, schema_name: str, event: Dict) -> tuple[bool, Optional[str]]:
        """Validate event against schema"""
        try:
            if schema_name not in self.validators:
                return False, f"Schema {schema_name} not found"
            
            # First validate with JSON Schema
            self.validators[schema_name].validate(event)
            
            # Then validate formats
            schema = self.schemas[schema_name]["schema"]
            format_error = self._validate_formats(event, schema)
            if format_error:
                self.redis.incr(f"metrics:validation_error:{schema_name}")
                return False, format_error
            
            # Update metrics
            self.redis.incr(f"metrics:validation_success:{schema_name}")
            return True, None
            
        except ValidationError as e:
            error_msg = f"Validation failed: {e.message} at path: {'.'.join(map(str, e.absolute_path))}"
            self.redis.incr(f"metrics:validation_error:{schema_name}")
            return False, error_msg
    
    def get_schema(self, schema_name: str) -> Optional[Dict]:
        """Get schema by name"""
        return self.schemas.get(schema_name, {}).get("schema")
    
    def list_schemas(self) -> Dict[str, Dict]:
        """List all registered schemas"""
        return {name: {
            "version": info["version"],
            "loaded_at": info["loaded_at"],
            "compatibility": info["config"]["compatibility"]
        } for name, info in self.schemas.items()}
    
    def get_validation_metrics(self) -> Dict[str, int]:
        """Get validation metrics from Redis"""
        metrics = {}
        for key in self.redis.scan_iter(match="metrics:*"):
            metrics[key] = int(self.redis.get(key) or 0)
        return metrics
