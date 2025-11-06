"""
Kafka Connect Connector Management
Handles connector deployment and lifecycle operations
"""
import requests
import json
import time
from typing import Dict, Any, Optional


class ConnectorManager:
    def __init__(self, connect_url: str = "http://localhost:8083"):
        self.connect_url = connect_url
    
    def create_connector(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new connector"""
        try:
            response = requests.post(
                f"{self.connect_url}/connectors",
                json=config,
                headers={'Content-Type': 'application/json'},
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return {'error': str(e)}
    
    def delete_connector(self, connector_name: str) -> bool:
        """Delete a connector"""
        try:
            response = requests.delete(
                f"{self.connect_url}/connectors/{connector_name}",
                timeout=10
            )
            return response.status_code == 204
        except requests.exceptions.RequestException:
            return False
    
    def pause_connector(self, connector_name: str) -> bool:
        """Pause a connector"""
        try:
            response = requests.put(
                f"{self.connect_url}/connectors/{connector_name}/pause",
                timeout=10
            )
            return response.status_code == 202
        except requests.exceptions.RequestException:
            return False
    
    def resume_connector(self, connector_name: str) -> bool:
        """Resume a connector"""
        try:
            response = requests.put(
                f"{self.connect_url}/connectors/{connector_name}/resume",
                timeout=10
            )
            return response.status_code == 202
        except requests.exceptions.RequestException:
            return False
    
    def restart_connector(self, connector_name: str) -> bool:
        """Restart a connector"""
        try:
            response = requests.post(
                f"{self.connect_url}/connectors/{connector_name}/restart",
                timeout=10
            )
            return response.status_code == 204
        except requests.exceptions.RequestException:
            return False
    
    def get_jdbc_source_config(self, 
                               connector_name: str,
                               db_url: str,
                               table: str,
                               topic_prefix: str,
                               tasks_max: int = 6) -> Dict[str, Any]:
        """Generate JDBC source connector config"""
        return {
            "name": connector_name,
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                "tasks.max": str(tasks_max),
                "connection.url": db_url,
                "connection.user": "admin",
                "connection.password": "admin123",
                "mode": "incrementing",
                "incrementing.column.name": "id",
                "table.whitelist": table,
                "topic.prefix": topic_prefix,
                "poll.interval.ms": "1000",
                "batch.max.rows": "100",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter.schemas.enable": "false"
            }
        }


def deploy_streamsocial_connectors(manager: ConnectorManager):
    """Deploy StreamSocial content ingestion connectors"""
    
    connectors = [
        manager.get_jdbc_source_config(
            connector_name="streamsocial-posts-source",
            db_url="jdbc:postgresql://postgres:5432/streamsocial",
            table="user_posts",
            topic_prefix="streamsocial-",
            tasks_max=3
        ),
        manager.get_jdbc_source_config(
            connector_name="streamsocial-comments-source",
            db_url="jdbc:postgresql://postgres:5432/streamsocial",
            table="user_comments",
            topic_prefix="streamsocial-",
            tasks_max=2
        ),
        manager.get_jdbc_source_config(
            connector_name="streamsocial-media-source",
            db_url="jdbc:postgresql://postgres:5432/streamsocial",
            table="media_uploads",
            topic_prefix="streamsocial-",
            tasks_max=1
        )
    ]
    
    results = []
    for config in connectors:
        print(f"Deploying connector: {config['name']}")
        result = manager.create_connector(config)
        results.append(result)
        time.sleep(2)  # Brief pause between deployments
    
    return results
