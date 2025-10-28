import json
import requests
import time
from typing import Dict, List, Optional
from dataclasses import dataclass

@dataclass
class SchemaInfo:
    id: int
    version: int
    schema: str
    subject: str
    compatibility: str

class SchemaRegistryClient:
    def __init__(self, base_url: str = "http://localhost:8081"):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/vnd.schemaregistry.v1+json'
        })
    
    def wait_for_registry(self, timeout: int = 60):
        """Wait for Schema Registry to be ready"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = self.session.get(f"{self.base_url}/subjects")
                if response.status_code == 200:
                    print("✅ Schema Registry is ready")
                    return True
            except requests.exceptions.ConnectionError:
                pass
            time.sleep(2)
        raise Exception("Schema Registry not ready within timeout")
    
    def register_schema(self, subject: str, schema_str: str) -> int:
        """Register new schema version"""
        payload = {"schema": json.dumps(json.loads(schema_str))}
        response = self.session.post(
            f"{self.base_url}/subjects/{subject}/versions",
            json=payload
        )
        response.raise_for_status()
        return response.json()["id"]
    
    def get_schema_by_id(self, schema_id: int) -> str:
        """Retrieve schema by ID"""
        response = self.session.get(f"{self.base_url}/schemas/ids/{schema_id}")
        response.raise_for_status()
        return response.json()["schema"]
    
    def get_latest_schema(self, subject: str) -> SchemaInfo:
        """Get latest schema version for subject"""
        response = self.session.get(f"{self.base_url}/subjects/{subject}/versions/latest")
        response.raise_for_status()
        data = response.json()
        return SchemaInfo(
            id=data["id"],
            version=data["version"],
            schema=data["schema"],
            subject=data["subject"],
            compatibility=self.get_compatibility(subject)
        )
    
    def set_compatibility(self, subject: str, compatibility: str) -> bool:
        """Set compatibility mode for subject"""
        payload = {"compatibility": compatibility}
        response = self.session.put(
            f"{self.base_url}/config/{subject}",
            json=payload
        )
        response.raise_for_status()
        return True
    
    def get_compatibility(self, subject: str) -> str:
        """Get compatibility mode for subject"""
        try:
            response = self.session.get(f"{self.base_url}/config/{subject}")
            response.raise_for_status()
            return response.json()["compatibilityLevel"]
        except:
            return "BACKWARD"  # Default
    
    def check_compatibility(self, subject: str, schema_str: str, version: str = "latest") -> bool:
        """Check if schema is compatible"""
        payload = {"schema": json.dumps(json.loads(schema_str))}
        response = self.session.post(
            f"{self.base_url}/compatibility/subjects/{subject}/versions/{version}",
            json=payload
        )
        response.raise_for_status()
        return response.json()["is_compatible"]
    
    def list_subjects(self) -> List[str]:
        """List all subjects"""
        response = self.session.get(f"{self.base_url}/subjects")
        response.raise_for_status()
        return response.json()
    
    def get_versions(self, subject: str) -> List[int]:
        """Get all versions for subject"""
        response = self.session.get(f"{self.base_url}/subjects/{subject}/versions")
        response.raise_for_status()
        return response.json()
