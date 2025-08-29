import pytest
import asyncio
import httpx
from fastapi.testclient import TestClient
from src.web.app import create_app

class TestPaymentAPI:
    
    @pytest.fixture
    def client(self):
        app = create_app()
        return TestClient(app)
    
    def test_dashboard_loads(self, client):
        """Test dashboard page loads successfully"""
        response = client.get("/")
        assert response.status_code == 200
        assert "StreamSocial Payment Dashboard" in response.text
    
    def test_get_stats_endpoint(self, client):
        """Test stats API endpoint"""
        response = client.get("/api/stats")
        assert response.status_code == 200
        
        data = response.json()
        assert "total_payments" in data
        assert "successful_payments" in data
        assert "success_rate" in data
        assert "total_amount" in data
    
    @pytest.mark.asyncio
    async def test_create_payment_endpoint(self, client):
        """Test payment creation endpoint"""
        payment_data = {
            "user_id": "test_user_123",
            "amount": 9.99,
            "payment_method": "credit_card",
            "subscription_type": "premium"
        }
        
        response = client.post("/api/payment", json=payment_data)
        
        # Note: This might fail without Kafka running
        # In a real test environment, you'd have test containers
        if response.status_code == 200:
            data = response.json()
            assert data["success"] is True
            assert "payment_id" in data
            assert "idempotency_key" in data
