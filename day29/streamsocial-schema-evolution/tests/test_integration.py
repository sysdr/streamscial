import unittest
import requests
import time
import threading
from datetime import datetime

class TestIntegration(unittest.TestCase):
    
    def setUp(self):
        self.registry_url = "http://localhost:8081"
        self.dashboard_url = "http://localhost:8050"
        
    def test_schema_registry_health(self):
        """Test Schema Registry is accessible"""
        print("\n🔍 Testing Schema Registry health...")
        
        # Wait for registry to be ready
        max_attempts = 30
        for attempt in range(max_attempts):
            try:
                response = requests.get(f"{self.registry_url}/subjects", timeout=5)
                if response.status_code == 200:
                    print("✅ Schema Registry is healthy")
                    return
            except requests.exceptions.ConnectionError:
                pass
            
            if attempt < max_attempts - 1:
                time.sleep(2)
        
        self.fail("Schema Registry not accessible")
    
    def test_dashboard_accessibility(self):
        """Test dashboard is accessible"""
        print("\n🔍 Testing dashboard accessibility...")
        
        # Wait for dashboard
        max_attempts = 15
        for attempt in range(max_attempts):
            try:
                response = requests.get(self.dashboard_url, timeout=10)
                if response.status_code == 200:
                    print("✅ Dashboard is accessible")
                    return
            except requests.exceptions.ConnectionError:
                pass
            
            if attempt < max_attempts - 1:
                time.sleep(2)
        
        self.fail("Dashboard not accessible")
    
    def test_schema_operations(self):
        """Test basic schema operations"""
        print("\n🔍 Testing schema operations...")
        
        # List subjects
        response = requests.get(f"{self.registry_url}/subjects")
        self.assertEqual(response.status_code, 200)
        subjects = response.json()
        print(f"✅ Found {len(subjects)} subjects")
        
        # If user-profile exists, check its versions
        if "user-profile" in subjects:
            response = requests.get(f"{self.registry_url}/subjects/user-profile/versions")
            self.assertEqual(response.status_code, 200)
            versions = response.json()
            print(f"✅ User-profile has {len(versions)} versions: {versions}")

if __name__ == "__main__":
    unittest.main(verbosity=2)
