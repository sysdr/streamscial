import pytest
import tempfile
import os
import json
from unittest.mock import Mock, patch
import sys

sys.path.append("src/main/python")

from file_source_connector import FileSourceConnector, OffsetStorage


class TestFileSourceConnector:
    def test_config_loading(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(
                """
connector_id: "test-connector"
source_directory: "./test_uploads"
kafka:
  bootstrap_servers: "localhost:9092"
  topic: "test-topic"
            """
            )
            f.flush()

            connector = FileSourceConnector(f.name)
            assert connector.config["connector_id"] == "test-connector"
            assert connector.config["source_directory"] == "./test_uploads"

        os.unlink(f.name)

    def test_metadata_extraction(self):
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("test content")
            f.flush()

            connector = FileSourceConnector("src/main/resources/connector.yaml")
            metadata = connector._extract_metadata(f.name)

            assert "file_id" in metadata
            assert "filename" in metadata
            assert "size_bytes" in metadata
            assert metadata["size_bytes"] > 0

        os.unlink(f.name)


class TestOffsetStorage:
    def test_offset_update_and_retrieval(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            offset_storage = OffsetStorage(f.name)

            offset_storage.update("/test/file.txt", 1234567890)
            result = offset_storage.get_last_processed()

            assert result["last_processed_file"] == "/test/file.txt"
            assert result["timestamp"] == 1234567890

        os.unlink(f.name)

    def test_offset_file_not_exists(self):
        offset_storage = OffsetStorage("/nonexistent/file.json")
        result = offset_storage.get_last_processed()
        assert result == {}
