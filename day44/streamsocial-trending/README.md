# StreamSocial Trending Hashtag Detection

Day 44: Stateful Aggregations with Kafka Streams

## Architecture

This system implements real-time trending hashtag detection using:
- Sliding window aggregations (30-minute windows, 5-minute advances)
- Velocity-based scoring algorithm
- Top-K ranking with regional breakouts

## Quick Start

### 1. Start Kafka
```bash
docker-compose up -d
```

### 2. Build Project
```bash
./build.sh
```

### 3. Start System
```bash
./start.sh
```

### 4. View Dashboard
Open http://localhost:5000

## Testing
```bash
source venv/bin/activate
pytest tests/ -v --cov=src
```

## Components

- **Hashtag Extractor**: Parses posts and extracts hashtags
- **Window Manager**: Manages overlapping time windows
- **Trending Scorer**: Calculates velocity-based scores
- **Dashboard**: Real-time visualization

## Monitoring

- Dashboard: http://localhost:5000
- Kafka topics: streamsocial.posts, streamsocial.trending

## Stopping
```bash
./stop.sh
```
