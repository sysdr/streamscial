#!/bin/bash
# Kafka KRaft setup script
KAFKA_CLUSTER_ID="$(kafka-storage random-uuid)"
kafka-storage format -t $KAFKA_CLUSTER_ID -c /etc/kafka/kafka.properties
