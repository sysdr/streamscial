import time
import uuid
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from ..streamsocial.models import UserProfile, UserInteraction, PostEvent
from ..registry.schema_manager import StreamSocialSchemaManager
import json

class StreamSocialAvroProducer:
    def __init__(self, bootstrap_servers="localhost:9092", schema_registry_url="http://localhost:8081"):
        self.producer = Producer({'bootstrap.servers': bootstrap_servers})
        self.schema_manager = StreamSocialSchemaManager(schema_registry_url)
        self.sr_client = SchemaRegistryClient({'url': schema_registry_url})
        
        # Setup serializers
        self._setup_serializers()
        
    def _setup_serializers(self):
        """Setup Avro serializers for different event types"""
        self.serializers = {}
        
        # User Profile serializer
        user_schema = self.schema_manager.load_schema('user_profile')
        self.serializers['user_profile'] = AvroSerializer(self.sr_client, str(user_schema))
        
        # User Interaction serializer
        interaction_schema = self.schema_manager.load_schema('user_interaction')
        self.serializers['user_interaction'] = AvroSerializer(self.sr_client, str(interaction_schema))
        
        # Post Event serializer
        post_schema = self.schema_manager.load_schema('post_event')
        self.serializers['post_event'] = AvroSerializer(self.sr_client, str(post_schema))
    
    def produce_user_profile(self, profile: UserProfile, topic="user-profiles"):
        """Produce user profile event"""
        try:
            ctx = SerializationContext(topic, MessageField.VALUE)
            serialized_data = self.serializers['user_profile'](profile.to_dict(), ctx)
            
            self.producer.produce(
                topic=topic,
                key=profile.user_id,
                value=serialized_data,
                callback=self._delivery_callback
            )
            self.producer.flush()
            print(f"✅ Produced user profile: {profile.user_id}")
        except Exception as e:
            print(f"❌ Failed to produce user profile: {e}")
    
    def produce_interaction(self, interaction: UserInteraction, topic="user-interactions"):
        """Produce user interaction event"""
        try:
            ctx = SerializationContext(topic, MessageField.VALUE)
            serialized_data = self.serializers['user_interaction'](interaction.to_dict(), ctx)
            
            self.producer.produce(
                topic=topic,
                key=interaction.user_id,
                value=serialized_data,
                callback=self._delivery_callback
            )
            self.producer.flush()
            print(f"✅ Produced interaction: {interaction.interaction_type} by {interaction.user_id}")
        except Exception as e:
            print(f"❌ Failed to produce interaction: {e}")
    
    def produce_post(self, post: PostEvent, topic="post-events"):
        """Produce post event"""
        try:
            ctx = SerializationContext(topic, MessageField.VALUE)
            serialized_data = self.serializers['post_event'](post.to_dict(), ctx)
            
            self.producer.produce(
                topic=topic,
                key=post.user_id,
                value=serialized_data,
                callback=self._delivery_callback
            )
            self.producer.flush()
            print(f"✅ Produced post: {post.post_id} by {post.user_id}")
        except Exception as e:
            print(f"❌ Failed to produce post: {e}")
    
    def _delivery_callback(self, err, msg):
        """Delivery callback for producer"""
        if err:
            print(f"❌ Message delivery failed: {err}")
        else:
            print(f"📤 Message delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")
