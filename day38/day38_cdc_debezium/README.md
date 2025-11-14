# Day 38: Change Data Capture with Debezium

StreamSocial user profile CDC implementation using Debezium and PostgreSQL.

## Quick Start

### Build and Setup
```bash
./build.sh
```

### Run Demo
```bash
./demo.sh
```

### Manual Testing
Terminal 1 - Start Dashboard:
```bash
./start.sh
```

Terminal 2 - Start Consumer:
```bash
source venv/bin/activate
python src/consumer/cdc_consumer.py
```

Terminal 3 - Start Producer:
```bash
source venv/bin/activate
python src/producer/profile_updater.py
```

### Run Tests
```bash
./test.sh
```

### Stop Services
```bash
./stop.sh
```

## Components

- **PostgreSQL**: Source database with logical replication
- **Debezium**: CDC connector capturing database changes
- **Kafka**: Event streaming platform
- **Schema Registry**: Schema management
- **Monitoring Dashboard**: Real-time CDC metrics at http://localhost:5001

## Architecture

```
PostgreSQL (WAL) → Debezium Connector → Kafka Topics → Consumers
                                              ↓
                                      Schema Registry
```

## Key Features

- Real-time change data capture from PostgreSQL
- Automatic schema evolution handling
- Tombstone events for deletions
- Replication slot monitoring
- Zero application code changes
- Sub-second latency

## Metrics Monitored

- Replication slot lag
- WAL position difference
- Connector health status
- Table statistics
- Event processing rates

## Troubleshooting

**Connector not starting**: Check PostgreSQL is in logical replication mode
**High lag**: Check network connectivity between Connect and PostgreSQL
**Missing events**: Verify replication slot is active and not full
