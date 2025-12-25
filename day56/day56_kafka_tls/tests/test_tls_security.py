"""Test TLS encryption functionality"""
import pytest
import ssl
import socket
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError

def test_certificate_files_exist():
    """Verify all certificate files exist"""
    import os
    cert_files = [
        'certs/ca-cert.pem',
        'certs/broker-cert.pem',
        'certs/client-cert.pem',
        'certs/broker.keystore.jks',
        'certs/client.truststore.jks'
    ]
    for cert_file in cert_files:
        assert os.path.exists(cert_file), f"Certificate file missing: {cert_file}"

def test_certificate_validity():
    """Verify certificates are valid"""
    import subprocess
    result = subprocess.run(
        ['openssl', 'verify', '-CAfile', 'certs/ca-cert.pem', 'certs/broker-cert.pem'],
        capture_output=True, text=True
    )
    assert 'OK' in result.stdout, "Certificate verification failed"

def test_tls_connection():
    """Test TLS connection to broker"""
    try:
        context = ssl.create_default_context(cafile='certs/ca-cert.pem')
        context.load_cert_chain(certfile='certs/client-cert.pem', keyfile='certs/client-key.pem')
        
        with socket.create_connection(('localhost', 9093), timeout=10) as sock:
            with context.wrap_socket(sock, server_hostname='localhost') as ssock:
                assert ssock.version() in ['TLSv1.2', 'TLSv1.3'], "Incorrect TLS version"
        
        print("✓ TLS connection successful")
    except Exception as e:
        pytest.fail(f"TLS connection failed: {e}")

def test_admin_client_with_tls():
    """Test Kafka admin operations with TLS"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers='localhost:9093',
            security_protocol='SSL',
            ssl_check_hostname=True,
            ssl_cafile='certs/ca-cert.pem',
            ssl_certfile='certs/client-cert.pem',
            ssl_keyfile='certs/client-key.pem'
        )
        
        # Create topic
        topic = NewTopic(name='test-tls-topic', num_partitions=3, replication_factor=1)
        admin_client.create_topics([topic], timeout_ms=10000)
        
        # List topics
        topics = admin_client.list_topics()
        assert 'test-tls-topic' in topics, "Topic not created"
        
        admin_client.close()
        print("✓ Admin client TLS operations successful")
    except Exception as e:
        pytest.fail(f"Admin client failed: {e}")

def test_encryption_overhead():
    """Measure encryption performance impact"""
    import time
    from src.producer.secure_producer import SecureMessageProducer
    
    producer = SecureMessageProducer('localhost:9093', 'certs')
    
    start_time = time.time()
    for i in range(100):
        producer.send_message('performance-test', f'user_{i}', 'test', 'data')
    
    elapsed = time.time() - start_time
    throughput = 100 / elapsed
    
    producer.close()
    
    assert throughput > 50, f"Throughput too low: {throughput:.2f} msg/s"
    print(f"✓ Throughput: {throughput:.2f} messages/second")

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
