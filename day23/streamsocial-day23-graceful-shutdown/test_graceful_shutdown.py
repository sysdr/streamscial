#!/usr/bin/env python3

import unittest
import time
import threading
import signal
import os
import sys
import requests
import json
from unittest.mock import patch, MagicMock

sys.path.append('src')
from main import StreamSocialConsumer, GracefulShutdownManager, RecommendationProcessor

class TestGracefulShutdown(unittest.TestCase):
    
    def test_shutdown_manager_lifecycle(self):
        """Test shutdown manager state transitions"""
        manager = GracefulShutdownManager()
        
        # Initial state
        self.assertFalse(manager.shutdown_requested.is_set())
        self.assertFalse(manager.processing_complete.is_set())
        
        # Request shutdown
        manager.request_shutdown()
        self.assertTrue(manager.shutdown_requested.is_set())
        
        # Complete phases
        manager.mark_processing_complete()
        self.assertTrue(manager.processing_complete.is_set())
        
        manager.mark_cleanup_complete()
        self.assertTrue(manager.cleanup_complete.is_set())
    
    def test_recommendation_processor(self):
        """Test recommendation processing logic"""
        redis_mock = MagicMock()
        redis_mock.get.return_value = None
        
        processor = RecommendationProcessor(redis_mock)
        
        # Mock message
        message = MagicMock()
        message.value.decode.return_value = json.dumps({
            "user_id": "test_user",
            "event_type": "like",
            "content_id": "content_123"
        })
        
        result = processor.process_recommendation_event(message)
        self.assertTrue(result)
        
        # Verify cache update
        self.assertIn("test_user", processor.user_cache)
        
    def test_health_endpoint(self):
        """Test health endpoint availability"""
        # This test assumes the service is running
        try:
            response = requests.get('http://localhost:8080/health', timeout=2)
            self.assertEqual(response.status_code, 200)
            
            health_data = response.json()
            self.assertIn('status', health_data)
            self.assertIn('shutdown_phase', health_data)
            
        except requests.exceptions.ConnectionError:
            self.skipTest("Service not running - cannot test health endpoint")
    
    def test_metrics_endpoint(self):
        """Test metrics endpoint"""
        try:
            response = requests.get('http://localhost:8080/metrics', timeout=2)
            self.assertEqual(response.status_code, 200)
            
            # Should contain Prometheus metrics
            self.assertIn('streamsocial_', response.text)
            
        except requests.exceptions.ConnectionError:
            self.skipTest("Service not running - cannot test metrics endpoint")

class TestIntegration(unittest.TestCase):
    
    def test_dashboard_accessibility(self):
        """Test dashboard web interface"""
        try:
            response = requests.get('http://localhost:5000', timeout=5)
            self.assertEqual(response.status_code, 200)
            self.assertIn('StreamSocial', response.text)
            
        except requests.exceptions.ConnectionError:
            self.skipTest("Dashboard not running")
    
    def test_api_endpoints(self):
        """Test dashboard API endpoints"""
        try:
            # Test status API
            response = requests.get('http://localhost:5000/api/status', timeout=2)
            self.assertEqual(response.status_code, 200)
            
            status_data = response.json()
            self.assertIn('health_status', status_data)
            
        except requests.exceptions.ConnectionError:
            self.skipTest("Dashboard API not available")

if __name__ == '__main__':
    # Run basic tests
    unittest.main(verbosity=2)
