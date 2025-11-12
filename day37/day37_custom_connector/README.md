# Day 37: Custom Connector Development

Production-grade Kafka Connect custom connector for social media integration.

## Architecture

```
SocialStreamConnector (Coordinator)
    ├── Configuration Validation
    ├── Task Distribution
    └── Health Monitoring
    
SocialStreamSourceTask (Workers)
    ├── API Polling
    ├── Rate Limiting
    ├── Offset Management
    └── Kafka Production
```

## Features

- Multi-platform support (Twitter, LinkedIn)
- Token bucket rate limiting
- Exactly-once offset semantics
- Real-time monitoring dashboard
- Prometheus metrics export
- Graceful shutdown handling

## Quick Start

```bash
# Build and test
bash scripts/build.sh

# Start connector
bash scripts/start.sh

# Or run demo
bash scripts/demo.sh
```

## Endpoints

- Dashboard: http://localhost:5000
- Metrics: http://localhost:8080/metrics

## Configuration

Edit `config/connector_config.yaml`:
- Add platforms and accounts
- Configure rate limits
- Set polling intervals
- Kafka connection settings

## Testing

```bash
# Unit tests
pytest tests/test_connector.py -v

# Integration tests  
pytest tests/test_integration.py -v

# All tests
pytest tests/ -v
```

## Metrics

- `social_connector_api_calls_total` - API calls per platform
- `social_connector_records_produced_total` - Records to Kafka
- `social_connector_rate_limit_waits_total` - Rate limit hits
- `social_connector_available_tokens` - Current token count
- `social_connector_task_status` - Task health

## Production Deployment

1. Package connector as JAR (Java) or plugin
2. Deploy to Kafka Connect cluster
3. Configure via REST API
4. Monitor via Prometheus + Grafana
5. Set up alerting for failures

