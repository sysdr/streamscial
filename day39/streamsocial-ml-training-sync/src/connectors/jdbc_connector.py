import json
import requests
from typing import Dict, Any, List, Optional
import structlog
import time

logger = structlog.get_logger()

class JDBCConnectorManager:
    """Manages JDBC Source Connectors for ML training data"""
    
    def __init__(self, connect_url: str, db_config: Dict[str, str]):
        self.connect_url = connect_url.rstrip('/')
        self.db_config = db_config
    
    def create_ml_training_connector(self, 
                                     name: str,
                                     tables: List[str],
                                     topic_prefix: str = 'ml-training-',
                                     poll_interval_ms: int = 5000,
                                     batch_max_rows: int = 1000,
                                     tasks_max: int = 3) -> Dict[str, Any]:
        """Create JDBC source connector for ML training data sync"""
        
        config = {
            "name": name,
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                "connection.url": self.db_config['url'],
                "connection.user": self.db_config['user'],
                "connection.password": self.db_config['password'],
                
                "table.whitelist": ",".join(tables),
                "mode": "timestamp+incrementing",
                "timestamp.column.name": "updated_at",
                "incrementing.column.name": "id",
                
                "topic.prefix": topic_prefix,
                "poll.interval.ms": str(poll_interval_ms),
                "batch.max.rows": str(batch_max_rows),
                "tasks.max": str(tasks_max),
                
                "db.fetch.size": "1000",
                "query.retry.attempts": "3",
                
                "transforms": "InsertSource,AddTimestamp,ConvertTimestamp",
                "transforms.InsertSource.type": "org.apache.kafka.connect.transforms.InsertField$Value",
                "transforms.InsertSource.static.field": "source_system",
                "transforms.InsertSource.static.value": "postgresql",
                "transforms.AddTimestamp.type": "org.apache.kafka.connect.transforms.InsertField$Value",
                "transforms.AddTimestamp.timestamp.field": "kafka_ingest_ts",
                "transforms.ConvertTimestamp.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
                "transforms.ConvertTimestamp.field": "updated_at",
                "transforms.ConvertTimestamp.target.type": "string",
                "transforms.ConvertTimestamp.format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
                
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "true"
            }
        }
        
        return self._deploy_connector(config)
    
    def create_bulk_sync_connector(self,
                                   name: str,
                                   table: str,
                                   query: str,
                                   topic: str,
                                   tasks_max: int = 4) -> Dict[str, Any]:
        """Create connector for bulk initial sync with custom query"""
        
        config = {
            "name": name,
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                "connection.url": self.db_config['url'],
                "connection.user": self.db_config['user'],
                "connection.password": self.db_config['password'],
                
                "query": query,
                "mode": "bulk",
                "topic.prefix": "",
                "topic": topic,
                
                "poll.interval.ms": "86400000",  # Once per day
                "batch.max.rows": "5000",
                "tasks.max": str(tasks_max),
                
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter"
            }
        }
        
        return self._deploy_connector(config)
    
    def _deploy_connector(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Deploy connector to Kafka Connect"""
        try:
            # Check if connector exists
            response = requests.get(
                f"{self.connect_url}/connectors/{config['name']}"
            )
            
            if response.status_code == 200:
                # Update existing
                response = requests.put(
                    f"{self.connect_url}/connectors/{config['name']}/config",
                    json=config['config'],
                    headers={'Content-Type': 'application/json'}
                )
            else:
                # Create new
                response = requests.post(
                    f"{self.connect_url}/connectors",
                    json=config,
                    headers={'Content-Type': 'application/json'}
                )
            
            response.raise_for_status()
            logger.info("connector_deployed", name=config['name'])
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error("connector_deploy_failed", name=config['name'], error=str(e))
            raise
    
    def get_connector_status(self, name: str) -> Dict[str, Any]:
        """Get connector status and task states"""
        response = requests.get(f"{self.connect_url}/connectors/{name}/status")
        response.raise_for_status()
        return response.json()
    
    def get_connector_offsets(self, name: str) -> Dict[str, Any]:
        """Get connector offset tracking info"""
        response = requests.get(f"{self.connect_url}/connectors/{name}/offsets")
        if response.status_code == 200:
            return response.json()
        return {}
    
    def pause_connector(self, name: str) -> bool:
        """Pause a running connector"""
        response = requests.put(f"{self.connect_url}/connectors/{name}/pause")
        return response.status_code == 202
    
    def resume_connector(self, name: str) -> bool:
        """Resume a paused connector"""
        response = requests.put(f"{self.connect_url}/connectors/{name}/resume")
        return response.status_code == 202
    
    def restart_connector(self, name: str) -> bool:
        """Restart connector and all tasks"""
        response = requests.post(f"{self.connect_url}/connectors/{name}/restart")
        return response.status_code == 204
    
    def delete_connector(self, name: str) -> bool:
        """Delete a connector"""
        response = requests.delete(f"{self.connect_url}/connectors/{name}")
        return response.status_code == 204
    
    def list_connectors(self) -> List[str]:
        """List all connectors"""
        response = requests.get(f"{self.connect_url}/connectors")
        response.raise_for_status()
        return response.json()
    
    def wait_for_connector_ready(self, name: str, timeout: int = 60) -> bool:
        """Wait for connector to be in RUNNING state"""
        start = time.time()
        while time.time() - start < timeout:
            try:
                status = self.get_connector_status(name)
                if status['connector']['state'] == 'RUNNING':
                    all_tasks_running = all(
                        task['state'] == 'RUNNING' 
                        for task in status.get('tasks', [])
                    )
                    if all_tasks_running:
                        return True
            except Exception:
                pass
            time.sleep(2)
        return False
