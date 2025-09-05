# StreamSocial Timeline Ordering - Deployment Guide

## Production Deployment

### Infrastructure Requirements

- **CPU**: 4+ cores per Kafka broker
- **Memory**: 8GB+ per Kafka broker, 4GB+ for application
- **Storage**: SSD recommended, 100GB+ per broker
- **Network**: Low latency between brokers and applications

### Kafka Production Configuration

```yaml
# docker-compose.production.yml
version: '3.8'

services:
  kafka-1:
    image: confluentinc/cp-kafka:7.4.0
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181'
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-1:9092
      KAFKA_REPLICATION_FACTOR: 3
      KAFKA_MIN_IN_SYNC_REPLICAS: 2
      KAFKA_LOG_RETENTION_HOURS: 168
      KAFKA_LOG_SEGMENT_BYTES: 1073741824
      KAFKA_NUM_PARTITIONS: 12
```

### Application Production Settings

```python
# config/production_config.py
class ProductionSettings(BaseSettings):
    kafka_bootstrap_servers: str = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
    kafka_topic: str = "user-timeline"
    partition_count: int = 12
    replication_factor: int = 3
    
    # Producer settings
    producer_acks: str = "all"
    producer_retries: int = 10
    producer_retry_backoff_ms: int = 100
    
    # Consumer settings  
    consumer_session_timeout_ms: int = 30000
    consumer_heartbeat_interval_ms: int = 3000
    consumer_max_poll_records: int = 500
```

### Monitoring Setup

```yaml
# docker-compose.monitoring.yml
services:
  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    ports:
      - "9090:9090"
      
  grafana:
    image: grafana/grafana:latest
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
    ports:
      - "3000:3000"
      
  kafka-exporter:
    image: danielqsj/kafka-exporter:latest
    command:
      - --kafka.server=kafka-1:9092
      - --kafka.server=kafka-2:9092
      - --kafka.server=kafka-3:9092
    ports:
      - "9308:9308"
```

### Health Checks

```bash
#!/bin/bash
# health_check.sh

# Check Kafka brokers
kafka-broker-api-versions --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092

# Check topic health
kafka-topics --describe --topic user-timeline --bootstrap-server kafka-1:9092

# Check consumer group lag
kafka-consumer-groups --bootstrap-server kafka-1:9092 \
  --describe --group timeline-consumer-group
```

### Disaster Recovery

```bash
# Backup Kafka topics
kafka-mirror-maker --consumer.config source.properties \
  --producer.config target.properties \
  --whitelist="user-timeline"

# Restore from backup
kafka-console-producer --topic user-timeline \
  --bootstrap-server kafka-1:9092 \
  < backup/user-timeline.backup
```

## Security Configuration

### SSL/TLS Setup

```yaml
# SSL configuration
environment:
  KAFKA_SECURITY_INTER_BROKER_PROTOCOL: SSL
  KAFKA_SSL_KEYSTORE_LOCATION: /etc/kafka/secrets/kafka.keystore.jks
  KAFKA_SSL_KEYSTORE_PASSWORD: kafka-password
  KAFKA_SSL_KEY_PASSWORD: kafka-password
  KAFKA_SSL_TRUSTSTORE_LOCATION: /etc/kafka/secrets/kafka.truststore.jks
  KAFKA_SSL_TRUSTSTORE_PASSWORD: kafka-password
```

### SASL Authentication

```yaml
environment:
  KAFKA_SECURITY_INTER_BROKER_PROTOCOL: SASL_PLAINTEXT
  KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL: PLAIN
  KAFKA_SASL_ENABLED_MECHANISMS: PLAIN
  KAFKA_SASL_PLAIN_USERNAME: kafka
  KAFKA_SASL_PLAIN_PASSWORD: kafka-secret
```

## Performance Tuning

### Producer Optimization

```python
producer_config = {
    'acks': 'all',
    'retries': 10,
    'batch_size': 16384,
    'linger_ms': 5,
    'compression_type': 'snappy',
    'max_in_flight_requests_per_connection': 5,
    'enable_idempotence': True
}
```

### Consumer Optimization

```python
consumer_config = {
    'fetch_min_bytes': 1,
    'fetch_max_wait_ms': 500,
    'max_partition_fetch_bytes': 1048576,
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': False  # Manual commit for exactly-once
}
```

### JVM Tuning

```bash
# Kafka broker JVM settings
export KAFKA_HEAP_OPTS="-Xmx6G -Xms6G"
export KAFKA_JVM_PERFORMANCE_OPTS="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35"
```
