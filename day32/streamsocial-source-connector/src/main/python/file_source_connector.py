import json
import time
import hashlib
import os
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import threading
import logging
from prometheus_client import start_http_server

from kafka import KafkaProducer
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from prometheus_client import Counter, Histogram, Gauge
import yaml

# Metrics
files_processed_total = Counter("files_processed_total", "Total files processed")
processing_duration = Histogram(
    "file_processing_duration_seconds", "File processing duration"
)
current_lag = Gauge("connector_lag_seconds", "Current processing lag")
error_count = Counter(
    "connector_errors_total", "Total connector errors", ["error_type"]
)


class FileSourceConnector:
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.producer = None
        self.offset_storage = OffsetStorage(self.config["offset_file"])
        self.running = False
        self.logger = self._setup_logging()

    def _load_config(self, config_path: str) -> Dict:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        
        # Allow environment variable override for Kafka bootstrap servers (useful for Docker)
        if "KAFKA_BOOTSTRAP_SERVERS" in os.environ:
            config["kafka"]["bootstrap_servers"] = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
        
        return config

    def _setup_logging(self) -> logging.Logger:
        logger = logging.getLogger("FileSourceConnector")
        logger.setLevel(logging.INFO)
        handler = logging.FileHandler("logs/connector.log")
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def _wait_for_kafka(self, max_retries=30, delay=2):
        """Wait for Kafka to be available with retries"""
        import socket
        bootstrap_servers = self.config["kafka"]["bootstrap_servers"]
        
        # Parse bootstrap servers (handles both "host:port" and ["host:port"])
        if isinstance(bootstrap_servers, str):
            servers = [bootstrap_servers]
        else:
            servers = bootstrap_servers
        
        # First check if we can reach the Kafka broker via socket
        for attempt in range(max_retries):
            try:
                for server in servers:
                    if ':' in server:
                        host, port = server.split(':')
                        port = int(port)
                        # Try to connect to the socket
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.settimeout(2)
                        result = sock.connect_ex((host, port))
                        sock.close()
                        if result != 0:
                            raise ConnectionError(f"Cannot connect to {host}:{port}")
                
                # Socket connection works, now verify Kafka protocol
                test_producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers,
                    request_timeout_ms=3000
                )
                # Give it a moment to initialize and connect
                time.sleep(0.5)
                test_producer.close()
                self.logger.info(f"Successfully connected to Kafka at {bootstrap_servers}")
                return True
            except Exception as e:
                if attempt < max_retries - 1:
                    self.logger.warning(f"Kafka not available yet (attempt {attempt + 1}/{max_retries}): {str(e)}. Retrying in {delay}s...")
                    time.sleep(delay)
                else:
                    self.logger.error(f"Failed to connect to Kafka after {max_retries} attempts: {str(e)}")
                    raise
        
        return False

    def start(self):
        self.logger.info("Starting File Source Connector")

        # Start Prometheus metrics server
        metrics_port = self.config.get("monitoring", {}).get("metrics_port", 8080)
        start_http_server(metrics_port)
        self.logger.info(f"Prometheus metrics server started on port {metrics_port}")

        # Wait for Kafka to be available
        self.logger.info("Waiting for Kafka to be available...")
        self._wait_for_kafka()

        self.producer = KafkaProducer(
            bootstrap_servers=self.config["kafka"]["bootstrap_servers"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
        )

        # Start file watcher
        observer = Observer()
        handler = FileEventHandler(self)
        observer.schedule(handler, self.config["source_directory"], recursive=True)
        observer.start()

        self.running = True
        self.logger.info(f"Monitoring directory: {self.config['source_directory']}")

        try:
            while self.running:
                time.sleep(1)
                self._update_lag_metric()
        except KeyboardInterrupt:
            self.logger.info("Shutting down connector...")
        finally:
            observer.stop()
            observer.join()
            if self.producer:
                self.producer.close()

    def stop(self):
        self.running = False

    def process_file(self, file_path: str):
        try:
            with processing_duration.time():
                if self._already_processed(file_path):
                    return

                metadata = self._extract_metadata(file_path)
                event = self._create_event(file_path, metadata)

                # Publish to Kafka
                future = self.producer.send(
                    self.config["kafka"]["topic"], key=metadata["file_id"], value=event
                )

                # Wait for confirmation
                future.get(timeout=10)

                # Update offset
                self.offset_storage.update(file_path, metadata["timestamp"])
                files_processed_total.inc()

                self.logger.info(f"Processed file: {file_path}")

        except Exception as e:
            error_count.labels(error_type=type(e).__name__).inc()
            self.logger.error(f"Error processing file {file_path}: {str(e)}")

    def _already_processed(self, file_path: str) -> bool:
        last_processed = self.offset_storage.get_last_processed()
        file_stat = os.stat(file_path)
        return file_stat.st_mtime <= last_processed.get("timestamp", 0)

    def _extract_metadata(self, file_path: str) -> Dict:
        file_stat = os.stat(file_path)

        with open(file_path, "rb") as f:
            content_hash = hashlib.sha256(f.read()).hexdigest()

        return {
            "file_id": content_hash[:16],
            "filename": os.path.basename(file_path),
            "file_path": file_path,
            "size_bytes": file_stat.st_size,
            "timestamp": file_stat.st_mtime,
            "checksum": content_hash,
            "mime_type": self._get_mime_type(file_path),
            "created_at": datetime.fromtimestamp(file_stat.st_ctime).isoformat(),
        }

    def _get_mime_type(self, file_path: str) -> str:
        extension = Path(file_path).suffix.lower()
        mime_types = {
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".png": "image/png",
            ".gif": "image/gif",
            ".txt": "text/plain",
            ".pdf": "application/pdf",
            ".mp4": "video/mp4",
            ".avi": "video/avi",
        }
        return mime_types.get(extension, "application/octet-stream")

    def _create_event(self, file_path: str, metadata: Dict) -> Dict:
        return {
            "event_type": "file_uploaded",
            "source": "file_source_connector",
            "timestamp": datetime.now().isoformat(),
            "data": {
                "file_metadata": metadata,
                "processing_info": {
                    "connector_id": self.config.get("connector_id", "file-source-001"),
                    "version": "1.0.0",
                },
            },
        }

    def _update_lag_metric(self):
        last_processed = self.offset_storage.get_last_processed()
        if last_processed:
            lag = time.time() - last_processed.get("timestamp", time.time())
            current_lag.set(lag)


class OffsetStorage:
    def __init__(self, offset_file: str):
        self.offset_file = offset_file
        self.lock = threading.Lock()

    def update(self, file_path: str, timestamp: float):
        with self.lock:
            offset_data = {
                "last_processed_file": file_path,
                "timestamp": timestamp,
                "updated_at": datetime.now().isoformat(),
            }

            os.makedirs(os.path.dirname(self.offset_file), exist_ok=True)
            with open(self.offset_file, "w") as f:
                json.dump(offset_data, f, indent=2)

    def get_last_processed(self) -> Dict:
        try:
            with open(self.offset_file, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            return {}


class FileEventHandler(FileSystemEventHandler):
    def __init__(self, connector):
        self.connector = connector

    def on_created(self, event):
        if not event.is_directory:
            # Wait a bit for file to be completely written
            time.sleep(0.5)
            self.connector.process_file(event.src_path)

    def on_modified(self, event):
        if not event.is_directory:
            time.sleep(0.5)
            self.connector.process_file(event.src_path)


if __name__ == "__main__":
    connector = FileSourceConnector("src/main/resources/connector.yaml")
    try:
        connector.start()
    except KeyboardInterrupt:
        connector.stop()
