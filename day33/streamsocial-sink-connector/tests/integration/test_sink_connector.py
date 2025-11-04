"""
Integration tests for sink connector
"""
import pytest
import json
import time
from datetime import datetime
from unittest.mock import Mock, patch
from src.streamsocial.connectors.hashtag_sink_connector import HashtagSinkConnector, ConnectorConfig

class TestSinkConnectorIntegration:
    @pytest.fixture
    def connector_config(self):
        return ConnectorConfig(
            kafka_bootstrap_servers="localhost:9092",
            kafka_topic="test-hashtags",
            kafka_group_id="test-group",
            database_url="postgresql://postgres:password@localhost:5432/test_db",
            batch_size=10,
            flush_timeout_ms=5000
        )
        
    def test_connector_initialization(self, connector_config):
        connector = HashtagSinkConnector(connector_config)
        assert connector.config == connector_config
        assert not connector.running
        
    def test_message_processing(self, connector_config):
        connector = HashtagSinkConnector(connector_config)
        
        # Mock Kafka message
        mock_msg = Mock()
        mock_msg.value.return_value = json.dumps({
            "hashtag": "test",
            "count": 50,
            "window_start": datetime.now().isoformat(),
            "window_end": datetime.now().isoformat()
        }).encode()
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 100
        
        # Process message
        connector._process_single_message(mock_msg)
        
        assert len(connector.current_batch.records) == 1
        assert connector.current_batch.records[0].hashtag == "test"
        assert connector.current_batch.records[0].count == 50
