# StreamSocial Timeline Ordering - Day 14

## Kafka Message Ordering & Partition Keys Implementation

This project demonstrates Kafka message ordering guarantees using partition keys to maintain chronological user timelines in a social media platform (StreamSocial).

## 🎯 Learning Objectives

- Understand Kafka partition key mechanics and hash-based routing
- Implement user-specific message ordering for timeline consistency
- Build real-time monitoring for partition distribution and ordering verification
- Create production-ready message ordering patterns

## 🏗️ Architecture

- **Timeline Producer**: Sends user posts with partition keys for consistent routing
- **Timeline Consumer**: Reconstructs chronological timelines maintaining user-specific ordering
- **Partition Analyzer**: Analyzes key distribution and provides optimization insights
- **Web Dashboard**: Real-time visualization of message flows and ordering verification

## 🚀 Quick Start

### Prerequisites
- Python 3.11+
- Docker & Docker Compose
- 8GB RAM minimum

### Installation

1. **Build the project:**
   ```bash
   ./build.sh
   ```

2. **Run tests:**
   ```bash
   ./test.sh
   ```

3. **Start the system:**
   ```bash
   ./start.sh
   ```

4. **Run demo:**
   ```bash
   ./demo.sh
   ```

### Access Points

- **Web Dashboard**: http://localhost:8000
- **Kafka UI**: http://localhost:8080

## 📊 Key Features

### Message Ordering Guarantees
- User-specific partition keys ensure chronological ordering
- Hash-based partition selection for deterministic routing  
- Real-time ordering violation detection

### Partition Analysis
- Load balancing visualization
- Hot partition detection
- Distribution optimization suggestions

### Real-time Monitoring
- Live message flow tracking
- WebSocket-based dashboard updates
- Timeline consistency verification

## 🧪 Demo Scenarios

### 1. Basic Timeline Ordering
```bash
# Send messages for user 'alice'
curl -X POST http://localhost:8000/api/send-message \
  -H "Content-Type: application/json" \
  -d '{"user_id": "alice", "content": "First post!"}'
```

### 2. Multi-User Timeline Analysis
- Send messages from multiple users
- Observe partition distribution
- Verify per-user chronological ordering

### 3. Load Testing
- Generate concurrent messages
- Monitor partition balance
- Analyze ordering consistency under load

## 📁 Project Structure

```
streamsocial-timeline-ordering/
├── src/
│   ├── producers/          # Timeline message producers
│   ├── consumers/          # Timeline message consumers  
│   ├── utils/             # Partition analysis utilities
│   └── web/               # Dashboard and API
├── tests/
│   ├── unit/              # Unit tests
│   └── integration/       # Integration tests
├── config/                # Configuration files
├── docker/                # Docker setup
└── scripts/               # Build and demo scripts
```

## 🔧 Configuration

### Kafka Configuration
- Topic: `user-timeline`
- Partitions: 6 (configurable)
- Replication Factor: 1

### Application Settings
```python
# Key settings in config/app_config.py
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "user-timeline"  
MAX_TIMELINE_POSTS = 100
```

## 📈 Monitoring & Metrics

### Key Metrics Tracked
- Messages sent/consumed per second
- Partition distribution balance score
- Timeline ordering violation count
- Consumer lag per partition

### Dashboard Features
- Real-time message flow visualization
- Partition load distribution charts
- User timeline reconstruction
- Ordering consistency verification

## 🚀 Production Considerations

### Scaling Guidelines
- Plan partition count carefully (cannot be decreased)
- Monitor key distribution for hot partitions
- Consider composite keys for better balance

### Operational Best Practices
- Monitor consumer lag across partitions
- Implement circuit breakers for failed producers
- Use async processing for high throughput scenarios

## 🔍 Testing

### Unit Tests
```bash
python -m pytest tests/unit/ -v
```

### Integration Tests
```bash
python -m pytest tests/integration/ -v
```

### Load Testing
```bash
# Generate 1000 messages across 10 users
python scripts/load_test.py --users 10 --messages 1000
```

## 🛑 Stopping the System

```bash
./stop.sh
```

## 📚 Next Steps

- **Day 15**: Custom Partitioning Logic for Geographic Distribution
- **Day 16**: Consumer Groups and Parallel Processing
- **Day 17**: Stream Processing with Kafka Streams

## 🤝 Troubleshooting

### Common Issues

1. **Kafka not starting**: Check Docker resources and port availability
2. **Ordering violations**: Verify partition key consistency and consumer implementation  
3. **Dashboard not loading**: Ensure all dependencies are installed and virtual environment is activated

### Debugging Commands
```bash
# Check Kafka topics
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

# Monitor consumer groups
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group timeline-consumer-group

# View application logs
docker-compose -f docker/docker-compose.yml logs streamsocial-app
```

## 📄 License

This project is part of the "Kafka Mastery: Building StreamSocial" course series.
