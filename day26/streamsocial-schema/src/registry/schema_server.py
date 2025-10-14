from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, Optional
from .schema_registry import SchemaRegistry
import uvicorn
import os

app = FastAPI(title="StreamSocial Schema Registry", version="1.0.0")

# Initialize schema registry
schema_dir = os.path.join(os.path.dirname(__file__), "..", "schemas")
registry = SchemaRegistry(schema_dir)

class ValidationRequest(BaseModel):
    schema_name: str
    event: Dict[str, Any]

class ValidationResponse(BaseModel):
    valid: bool
    error: Optional[str] = None
    schema_version: Optional[str] = None

@app.get("/health")
async def health_check():
    return {"status": "healthy", "schemas_loaded": len(registry.schemas)}

@app.post("/validate", response_model=ValidationResponse)
async def validate_event(request: ValidationRequest):
    valid, error = registry.validate_event(request.schema_name, request.event)
    schema_info = registry.schemas.get(request.schema_name, {})
    
    return ValidationResponse(
        valid=valid,
        error=error,
        schema_version=schema_info.get("version")
    )

@app.get("/schemas")
async def list_schemas():
    return registry.list_schemas()

@app.get("/schemas/{schema_name}")
async def get_schema(schema_name: str):
    schema = registry.get_schema(schema_name)
    if not schema:
        raise HTTPException(status_code=404, detail="Schema not found")
    return schema

@app.get("/metrics")
async def get_metrics():
    return registry.get_validation_metrics()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
