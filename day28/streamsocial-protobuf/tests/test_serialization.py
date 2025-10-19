import unittest
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from serialization_service import SerializationService

class TestSerialization(unittest.TestCase):
    def setUp(self):
        self.service = SerializationService()
        self.sample_event = self.service.create_sample_event()
    
    def test_protobuf_serialization_roundtrip(self):
        # Serialize
        serialized, ser_time = self.service.serialize_protobuf(self.sample_event)
        self.assertIsInstance(serialized, bytes)
        self.assertGreater(len(serialized), 0)
        self.assertGreater(ser_time, 0)
        
        # Deserialize
        deserialized, deser_time = self.service.deserialize_protobuf(serialized)
        self.assertGreater(deser_time, 0)
        
        # Verify data integrity
        self.assertEqual(deserialized['event_id'], self.sample_event['event_id'])
        self.assertEqual(deserialized['user_id'], self.sample_event['user_id'])
        self.assertEqual(deserialized['event_type'], self.sample_event['event_type'])
    
    def test_avro_serialization_roundtrip(self):
        # Serialize
        serialized, ser_time = self.service.serialize_avro(self.sample_event)
        self.assertIsInstance(serialized, bytes)
        self.assertGreater(len(serialized), 0)
        self.assertGreater(ser_time, 0)
        
        # Deserialize
        deserialized, deser_time = self.service.deserialize_avro(serialized)
        self.assertGreater(deser_time, 0)
        
        # Verify data integrity
        self.assertEqual(deserialized['event_id'], self.sample_event['event_id'])
        self.assertEqual(deserialized['user_id'], self.sample_event['user_id'])
    
    def test_json_serialization_roundtrip(self):
        # Serialize
        serialized, ser_time = self.service.serialize_json(self.sample_event)
        self.assertIsInstance(serialized, bytes)
        self.assertGreater(len(serialized), 0)
        self.assertGreater(ser_time, 0)
        
        # Deserialize
        deserialized, deser_time = self.service.deserialize_json(serialized)
        self.assertGreater(deser_time, 0)
        
        # Verify data integrity
        self.assertEqual(deserialized, self.sample_event)
    
    def test_protobuf_smaller_than_json(self):
        pb_data, _ = self.service.serialize_protobuf(self.sample_event)
        json_data, _ = self.service.serialize_json(self.sample_event)
        
        self.assertLess(len(pb_data), len(json_data), 
                       "Protobuf should be smaller than JSON")
    
    def test_avro_smaller_than_json(self):
        avro_data, _ = self.service.serialize_avro(self.sample_event)
        json_data, _ = self.service.serialize_json(self.sample_event)
        
        self.assertLess(len(avro_data), len(json_data), 
                       "Avro should be smaller than JSON")

if __name__ == '__main__':
    unittest.main()
