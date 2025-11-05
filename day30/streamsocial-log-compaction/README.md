# StreamSocial Kafka Log Compaction Demo
## Day 30: User Preference State Management

### Overview
This demo showcases Kafka log compaction for managing user preferences in the StreamSocial system. It demonstrates how log compaction maintains only the latest value for each key, making it perfect for state management scenarios.

### Features
- **Real-time Dashboard**: Monitor compaction behavior and user preferences
- **User Simulation**: Generate realistic user activity patterns
- **State Rebuilding**: Demonstrate consumer state reconstruction
- **Tombstone Deletion**: Show user deletion with cleanup markers
- **Compaction Metrics**: Visualize storage optimization effects

### Quick Start
```bash
# Build the project
./build.sh

# Run tests
./test.sh

# Start demo
./start.sh

# Open http://localhost:5000 in browser
```

### Architecture
- **Producer**: Publishes user preference changes
- **Compacted Topic**: Retains latest preferences per user
- **Consumer**: Maintains real-time user state
- **Dashboard**: Visualizes compaction behavior

### Key Learning Points
1. Log compaction retains only latest message per key
2. Perfect for state management use cases
3. Automatic cleanup of obsolete data
4. Tombstone messages for entity deletion
5. State rebuilding from compacted logs

### Compaction Configuration
```properties
cleanup.policy=compact
min.cleanable.dirty.ratio=0.5
segment.ms=7200000
delete.retention.ms=86400000
```

### Testing Scenarios
- Multiple preference updates per user
- Out-of-order message handling
- User deletion with tombstones
- Consumer restart and state rebuilding
- Performance under load

### Cleanup
```bash
./stop.sh
docker-compose down -v
```
