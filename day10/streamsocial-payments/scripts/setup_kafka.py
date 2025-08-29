#!/usr/bin/env python3
"""
Setup Kafka topics for StreamSocial payment processing
"""

from confluent_kafka.admin import AdminClient, NewTopic

def create_topics():
    admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})
    
    topics = [
        NewTopic("payment-events", num_partitions=3, replication_factor=1),
        NewTopic("payment-analytics", num_partitions=3, replication_factor=1),
        NewTopic("user-updates", num_partitions=3, replication_factor=1),
        NewTopic("payment-dlq", num_partitions=1, replication_factor=1)
    ]
    
    fs = admin_client.create_topics(topics)
    
    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic '{topic}' created successfully")
        except Exception as e:
            if "TopicExistsException" in str(e):
                print(f"Topic '{topic}' already exists")
            else:
                print(f"Failed to create topic '{topic}': {e}")

if __name__ == "__main__":
    create_topics()
