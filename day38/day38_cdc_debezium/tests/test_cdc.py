"""Test suite for CDC implementation"""

import json
import os
import time

import psycopg2
import pytest
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5433"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "streamsocial")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin123")


class TestCDC:
    @pytest.fixture(scope="class")
    def db_connection(self):
        """Create database connection"""
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        yield conn
        conn.close()
    
    @pytest.fixture(scope="class")
    def kafka_consumer(self):
        """Create Kafka consumer"""
        consumer = KafkaConsumer(
            'cdc.user_profiles',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',
            consumer_timeout_ms=10000,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        yield consumer
        consumer.close()
    
    def test_database_connection(self, db_connection):
        """Test PostgreSQL connection"""
        cursor = db_connection.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1
        cursor.close()
    
    def test_user_profiles_table_exists(self, db_connection):
        """Test user_profiles table exists"""
        cursor = db_connection.cursor()
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'user_profiles'
            )
        """)
        exists = cursor.fetchone()[0]
        assert exists is True
        cursor.close()
    
    def test_wal_level_logical(self, db_connection):
        """Test PostgreSQL WAL level is set to logical"""
        cursor = db_connection.cursor()
        cursor.execute("SHOW wal_level")
        wal_level = cursor.fetchone()[0]
        assert wal_level == 'logical'
        cursor.close()
    
    def test_insert_generates_cdc_event(self, db_connection, kafka_consumer):
        """Test that INSERT generates CDC event"""
        cursor = db_connection.cursor()
        
        # Insert test user
        test_username = f"test_user_{int(time.time())}"
        cursor.execute("""
            INSERT INTO user_profiles (username, email, full_name, bio)
            VALUES (%s, %s, %s, %s)
            RETURNING user_id
        """, (test_username, f"{test_username}@test.com", "Test User", "Test bio"))
        
        db_connection.commit()
        user_id = cursor.fetchone()[0]
        
        # Wait for CDC event
        time.sleep(3)
        
        # Check for event in Kafka
        found_event = False
        for message in kafka_consumer:
            value = message.value
            if value and 'payload' in value:
                payload = value['payload']
                if payload.get('op') in ['c', 'r']:  # Create or Read
                    after = payload.get('after', {})
                    if after.get('username') == test_username:
                        found_event = True
                        break
        
        # Cleanup
        cursor.execute("DELETE FROM user_profiles WHERE user_id = %s", (user_id,))
        db_connection.commit()
        cursor.close()
        
        assert found_event, "CDC event not found in Kafka"
    
    def test_update_generates_cdc_event(self, db_connection, kafka_consumer):
        """Test that UPDATE generates CDC event"""
        cursor = db_connection.cursor()
        
        # Create test user
        test_username = f"update_test_{int(time.time())}"
        cursor.execute("""
            INSERT INTO user_profiles (username, email, full_name)
            VALUES (%s, %s, %s)
            RETURNING user_id
        """, (test_username, f"{test_username}@test.com", "Update Test"))
        
        db_connection.commit()
        user_id = cursor.fetchone()[0]
        time.sleep(2)
        
        # Update user
        new_bio = "Updated bio at " + str(time.time())
        cursor.execute("""
            UPDATE user_profiles SET bio = %s WHERE user_id = %s
        """, (new_bio, user_id))
        db_connection.commit()
        
        # Wait for CDC event
        time.sleep(3)
        
        # Check for update event
        found_update = False
        for message in kafka_consumer:
            value = message.value
            if value and 'payload' in value:
                payload = value['payload']
                if payload.get('op') == 'u':  # Update
                    after = payload.get('after', {})
                    if after.get('user_id') == user_id and after.get('bio') == new_bio:
                        found_update = True
                        break
        
        # Cleanup
        cursor.execute("DELETE FROM user_profiles WHERE user_id = %s", (user_id,))
        db_connection.commit()
        cursor.close()
        
        assert found_update, "Update CDC event not found"

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
