import time
import json
import avro.schema
import avro.io
import io
from typing import Dict, Any, Tuple
import sys
sys.path.append('src/generated/schemas')
import social_events_pb2

class SerializationService:
    def __init__(self):
        # Load Avro schema
        with open('schemas/social_events.avsc', 'r') as f:
            self.avro_schema = avro.schema.parse(f.read())
        
    def create_sample_event(self) -> Dict[str, Any]:
        return {
            "event_id": f"evt_{int(time.time() * 1000)}",
            "user_id": "user_12345",
            "event_type": "LIKE",
            "target_id": "post_67890",
            "timestamp": int(time.time() * 1000),
            "location": {
                "latitude": 37.7749,
                "longitude": -122.4194,
                "country_code": "US"
            },
            "device": {
                "device_type": "mobile",
                "os_version": "iOS 17.0",
                "app_version": "2.1.0",
                "connection": "WIFI"
            },
            "metadata": {
                "session_id": "sess_abc123",
                "experiment_variant": "A"
            }
        }
    
    def serialize_protobuf(self, event_data: Dict[str, Any]) -> Tuple[bytes, int]:
        start_time = time.time_ns()
        
        # Create Protobuf message
        pb_event = social_events_pb2.UserInteraction()
        pb_event.event_id = event_data["event_id"]
        pb_event.user_id = event_data["user_id"]
        pb_event.event_type = getattr(social_events_pb2.EventType, event_data["event_type"])
        pb_event.target_id = event_data["target_id"]
        pb_event.timestamp = event_data["timestamp"]
        
        # Location
        pb_event.location.latitude = event_data["location"]["latitude"]
        pb_event.location.longitude = event_data["location"]["longitude"]
        pb_event.location.country_code = event_data["location"]["country_code"]
        
        # Device
        pb_event.device.device_type = event_data["device"]["device_type"]
        pb_event.device.os_version = event_data["device"]["os_version"]
        pb_event.device.app_version = event_data["device"]["app_version"]
        pb_event.device.connection = getattr(social_events_pb2.ConnectionType, event_data["device"]["connection"])
        
        # Metadata
        for key, value in event_data["metadata"].items():
            pb_event.metadata[key] = value
        
        serialized = pb_event.SerializeToString()
        serialization_time = time.time_ns() - start_time
        
        return serialized, serialization_time
    
    def deserialize_protobuf(self, serialized_data: bytes) -> Tuple[Dict[str, Any], int]:
        start_time = time.time_ns()
        
        pb_event = social_events_pb2.UserInteraction()
        pb_event.ParseFromString(serialized_data)
        
        # Convert back to dict
        event_data = {
            "event_id": pb_event.event_id,
            "user_id": pb_event.user_id,
            "event_type": social_events_pb2.EventType.Name(pb_event.event_type),
            "target_id": pb_event.target_id,
            "timestamp": pb_event.timestamp,
            "location": {
                "latitude": pb_event.location.latitude,
                "longitude": pb_event.location.longitude,
                "country_code": pb_event.location.country_code
            },
            "device": {
                "device_type": pb_event.device.device_type,
                "os_version": pb_event.device.os_version,
                "app_version": pb_event.device.app_version,
                "connection": social_events_pb2.ConnectionType.Name(pb_event.device.connection)
            },
            "metadata": dict(pb_event.metadata)
        }
        
        deserialization_time = time.time_ns() - start_time
        return event_data, deserialization_time
    
    def serialize_avro(self, event_data: Dict[str, Any]) -> Tuple[bytes, int]:
        start_time = time.time_ns()
        
        writer = avro.io.DatumWriter(self.avro_schema)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write(event_data, encoder)
        
        serialized = bytes_writer.getvalue()
        serialization_time = time.time_ns() - start_time
        
        return serialized, serialization_time
    
    def deserialize_avro(self, serialized_data: bytes) -> Tuple[Dict[str, Any], int]:
        start_time = time.time_ns()
        
        reader = avro.io.DatumReader(self.avro_schema)
        bytes_reader = io.BytesIO(serialized_data)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        event_data = reader.read(decoder)
        
        deserialization_time = time.time_ns() - start_time
        return event_data, deserialization_time
    
    def serialize_json(self, event_data: Dict[str, Any]) -> Tuple[bytes, int]:
        start_time = time.time_ns()
        serialized = json.dumps(event_data).encode('utf-8')
        serialization_time = time.time_ns() - start_time
        return serialized, serialization_time
    
    def deserialize_json(self, serialized_data: bytes) -> Tuple[Dict[str, Any], int]:
        start_time = time.time_ns()
        event_data = json.loads(serialized_data.decode('utf-8'))
        deserialization_time = time.time_ns() - start_time
        return event_data, deserialization_time
