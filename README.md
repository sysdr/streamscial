# Kafka Mastery: Building StreamSocial


## [A 60-Day Intensive Course in Event-Driven Systems](https://handsonkafka.substack.com/)

## Course Overview

Build StreamSocial, a production-ready social media analytics platform handling 50M requests/second. Master Apache Kafka through hands-on development of real-time trend analysis, personalized feeds, fraud detection, and global-scale event processing.

## Learning Objectives

- Design and implement high-throughput event-driven architectures
- Master Kafka's distributed systems concepts (partitioning, replication, fault tolerance)
- Build scalable producers and consumers with reliability guarantees
- Implement real-time stream processing with Kafka Streams
- Deploy production-ready Kafka clusters with monitoring and security
- Architect microservices using event-driven patterns

## Prerequisites

- Java 17+ (intermediate proficiency)
- Docker & Docker Compose
- Maven/Gradle build tools
- IDE (IntelliJ IDEA recommended)
- Basic networking concepts
- Command line familiarity

## StreamSocial System Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   User Actions  │───▶│     Kafka       │───▶│  Feed Engine    │
│   (50M req/s)   │    │   Cluster       │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                    ┌─────────┼─────────┐
                    ▼         ▼         ▼
            ┌─────────────┐ ┌──────────┐ ┌─────────────┐
            │ Trend       │ │Analytics │ │Notification │
            │ Analysis    │ │Dashboard │ │  Service    │
            └─────────────┘ └──────────┘ └─────────────┘
```

## Course Structure

**Format:** 60 daily lessons (12 weeks)  
**Duration:** 2-3 hours per lesson  
**Approach:** Theory + Hands-on coding + Production insights

> Thanks for reading Hands On Kafka! Subscribe for free to receive new posts and support my work.

### Each lesson includes:

- **Concept Deep Dive** (30 min)
- **StreamSocial Implementation** (90 min)
- **Production Insights** (15 min)
- **Daily Challenge** (coding task)
