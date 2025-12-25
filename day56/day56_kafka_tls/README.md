# Day 56: Encryption & TLS

## Quick Start

```bash
# Build
./build.sh

# Start infrastructure
./start.sh

# Run demo
./demo.sh

# Run tests
source venv/bin/activate
pytest tests/ -v

# Stop
./stop.sh
```

## Certificate Management

- CA Certificate: `certs/ca-cert.pem`
- Broker Certificate: `certs/broker-cert.pem`
- Client Certificate: `certs/client-cert.pem`

## Monitoring

Security dashboard: http://localhost:5000

## Architecture

- Kafka Broker: localhost:9093 (SSL), localhost:9092 (Plaintext)
- TLS 1.3 with mutual authentication
- AES-256-GCM encryption
