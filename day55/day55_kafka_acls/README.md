# Day 55: Authorization with ACLs

## StreamSocial Access Control Implementation

### Overview
Complete implementation of Kafka ACLs for StreamSocial, demonstrating:
- Service-level topic access control
- Least-privilege permission model
- Real-time ACL monitoring
- Authorization enforcement

### Quick Start

```bash
# Build project
./build.sh

# Start services
./start.sh

# Run demo
source venv/bin/activate
python src/demo.py

# In another terminal, start dashboard
python src/monitoring/acl_dashboard.py

# Run tests
pytest tests/ -v

# Stop everything
./stop.sh
```

### Architecture

**Services:**
1. **Post Service**: Write to posts topics
2. **Analytics Service**: Read from posts, write to analytics
3. **Moderation Service**: Read/delete posts, write moderation flags

**ACL Model:**
- Deny-by-default security
- Service-specific principals via SASL
- Operation-level granularity (READ, WRITE, DELETE, DESCRIBE)
- Pattern matching for topic prefixes

### Dashboard

Access at `http://localhost:5055` to view:
- Real-time access grants/denials
- Service-level statistics
- ACL configuration
- Access violations

### Testing

```bash
pytest tests/test_acl_enforcement.py -v
```

Tests cover:
- Authorized access (positive tests)
- Unauthorized access (negative tests)
- Service isolation
- ACL management

### Manual Testing

```bash
# List all ACLs
kafka-acls --bootstrap-server localhost:9093 \
  --command-config config/admin.properties --list

# Test post service
python -c "from src.services.post_service import PostService; \
  svc = PostService('localhost:9093', 'post_service', 'post-secret'); \
  print(svc.create_post('user1', 'Test post'))"
```

### Key Features

- ✅ SASL authentication integration (Day 54)
- ✅ Least-privilege ACL model
- ✅ Service isolation
- ✅ Real-time monitoring
- ✅ Comprehensive testing
- ✅ Pattern-based permissions
- ✅ Violation tracking

### Production Patterns

1. **Deny-by-default**: Nothing permitted unless explicitly granted
2. **Pattern matching**: `posts.*` for topic families
3. **Consumer group isolation**: Service-specific groups
4. **Audit logging**: Track all access decisions
5. **Graceful degradation**: Services fail safely on denial

### Performance

- ACL check latency: ~0.1-0.5ms per request
- Cached in broker memory
- No ZooKeeper dependency (with KRaft)
- Handles 50M+ events/second

### Next Steps

Day 56: Encryption & TLS - Protect data in transit
