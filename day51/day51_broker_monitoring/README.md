# Day 51: StreamSocial Broker Monitoring System

Production-grade Kafka broker monitoring with real-time metrics collection and alerting.

## Features

- Real-time JMX metrics collection from Kafka brokers
- WebSocket-powered live dashboard
- Comprehensive alerting system
- Cluster-wide metric aggregation
- Historical trend analysis

## Quick Start

```bash
# Build
./build.sh

# Start Kafka cluster
docker-compose up -d
sleep 30

# Run tests
./test.sh

# Start demo
./demo.sh

# In another terminal, generate test traffic
source venv/bin/activate
python src/test_producer.py
```

## Architecture

- **JMX Exporters**: Prometheus format metrics from each broker
- **Metrics Collector**: Python service polling exporters
- **Aggregator**: Cluster-wide metric calculation
- **Alert Manager**: Threshold-based alerting
- **Dashboard**: Real-time visualization

## Metrics Tracked

- Messages/sec per broker and cluster-wide
- Bytes in/out throughput
- Request handler idle %
- Network processor idle %
- Under-replicated partitions
- Offline partitions
- ISR shrink/expand rates

## Access Points

- Dashboard: http://localhost:5000
- Broker 1 Metrics: http://localhost:7071/metrics
- Broker 2 Metrics: http://localhost:7072/metrics
- Broker 3 Metrics: http://localhost:7073/metrics

## Testing

```bash
./test.sh
```

## Cleanup

```bash
./stop.sh
docker-compose down -v
```
