from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import time

class StreamSocialAvroConsumer:
    def __init__(self, topics, group_id="streamsocial-consumers", 
                 bootstrap_servers="localhost:9092", schema_registry_url="http://localhost:8081"):
        
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        })
        
        self.sr_client = SchemaRegistryClient({'url': schema_registry_url})
        self.deserializer = AvroDeserializer(self.sr_client)
        self.topics = topics
        self.message_count = 0
        
    def consume_messages(self, max_messages=100):
        """Consume and process Avro messages"""
        self.consumer.subscribe(self.topics)
        
        try:
            while self.message_count < max_messages:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    print(f"❌ Consumer error: {msg.error()}")
                    continue
                
                try:
                    # Deserialize Avro message
                    ctx = SerializationContext(msg.topic(), MessageField.VALUE)
                    data = self.deserializer(msg.value(), ctx)
                    
                    self._process_message(msg.topic(), msg.key().decode('utf-8'), data)
                    self.message_count += 1
                    
                except Exception as e:
                    print(f"❌ Failed to process message: {e}")
                    
        except KeyboardInterrupt:
            print("\n⏹️ Consumer stopped by user")
        finally:
            self.consumer.close()
    
    def _process_message(self, topic, key, data):
        """Process different message types"""
        if topic == "user-profiles":
            self._process_user_profile(key, data)
        elif topic == "user-interactions": 
            self._process_user_interaction(key, data)
        elif topic == "post-events":
            self._process_post_event(key, data)
        else:
            print(f"📨 Unknown topic: {topic}")
    
    def _process_user_profile(self, key, data):
        print(f"👤 User Profile - ID: {key}, Username: {data.get('username')}, Email: {data.get('email')}")
    
    def _process_user_interaction(self, key, data):
        print(f"💫 Interaction - User: {key}, Type: {data.get('interaction_type')}, Target: {data.get('target_id')}")
    
    def _process_post_event(self, key, data):
        content_preview = data.get('content', '')[:50] + "..." if len(data.get('content', '')) > 50 else data.get('content', '')
        print(f"📝 Post - User: {key}, Content: {content_preview}, Hashtags: {data.get('hashtags', [])}")
