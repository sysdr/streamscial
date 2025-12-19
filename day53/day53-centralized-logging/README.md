# Day 53: Centralized Logging Strategy with ELK Stack

## Overview
This implementation demonstrates production-grade centralized logging for StreamSocial using the ELK stack (Elasticsearch, Logstash, Kibana) to aggregate and analyze logs from distributed Kafka producers, brokers, and consumers.

## Architecture
- **Elasticsearch**: Distributed search and analytics engine for log storage
- **Logstash**: Log processing pipeline for parsing and enriching logs
- **Kibana**: Visualization and analysis interface
- **Kafka**: Message broker with structured logging
- **Python Apps**: Producers and consumers with structured JSON logging
- **Dashboard**: Real-time monitoring UI with metrics and trace search

## Quick Start

### Build
```bash
./build.sh
```

### Start Infrastructure
```bash
./start.sh
```

### Run Demo
```bash
./demo.sh
```

### Stop Everything
```bash
./stop.sh
```

## Features

### Structured Logging
- JSON format for all logs
- Trace ID for request correlation
- Automatic latency calculation
- Component-level tagging

### Log Aggregation
- Real-time log shipping via Logstash
- Centralized storage in Elasticsearch
- Daily index rotation
- Automatic field parsing

### Monitoring Dashboard
- Real-time error rate tracking
- Latency distribution (p50, p95, p99)
- Top error messages
- Trace ID search
- Component-level metrics

### Kibana Analysis
- Full-text log search
- Custom dashboards
- Alert configuration
- Visualization builder

## Testing
```bash
source venv/bin/activate
pytest tests/ -v
```

## Project Structure
```
day53-centralized-logging/
├── src/
│   ├── producers/          # Kafka producers with logging
│   ├── consumers/          # Kafka consumers with logging
│   ├── shared/            # Shared utilities
│   │   ├── structured_logger.py
│   │   └── log_analyzer.py
│   └── dashboard_app.py   # Monitoring dashboard
├── config/
│   └── logstash.conf      # Logstash pipeline config
├── tests/                 # Test suite
├── docker-compose.yml     # Infrastructure setup
└── logs/                  # Log files

```

## Access Points
- Dashboard: http://localhost:5000
- Kibana: http://localhost:5601
- Elasticsearch: http://localhost:9200

## Key Concepts Demonstrated
1. Structured logging with trace IDs
2. Log aggregation pipeline
3. Real-time log analysis
4. Error rate monitoring
5. Latency tracking
6. Distributed request tracing

## Real-World Applications
- Debugging production issues across distributed systems
- Performance monitoring and optimization
- Compliance and audit trails
- Incident response and postmortems
- Capacity planning and trend analysis
