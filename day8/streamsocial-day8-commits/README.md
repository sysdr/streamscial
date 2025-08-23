# StreamSocial Day 8: Commit Strategies & Reliability

## Overview
This lesson implements reliable engagement processing with manual commit strategies for the StreamSocial platform. Learn how to handle consumer failures gracefully while ensuring no data loss.

## Features
- Manual commit strategy with at-least-once processing
- Reliable engagement processing pipeline
- Consumer failure recovery
- Real-time monitoring dashboard
- Comprehensive testing suite

## Quick Start
```bash
./start.sh
```

Open your browser to http://localhost:8000 to see the monitoring dashboard.

## Architecture
- **Kafka Topic**: `user-engagements` (3 partitions)
- **Consumer Group**: `engagement-processors`
- **Database**: PostgreSQL for processed engagements
- **Monitoring**: Real-time web dashboard

## Key Learning Points
1. Manual commits prevent data loss during failures
2. At-least-once processing with idempotency
3. Transaction-based storage with commit coordination
4. Consumer group rebalancing during failures

## Testing
Run unit tests: `python -m pytest tests/ -v`

## Stop Demo
```bash
./stop.sh
```
