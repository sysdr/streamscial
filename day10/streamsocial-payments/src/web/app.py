from fastapi import FastAPI, Request, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import json
import redis
from typing import Dict, Any
from src.models.payment import PaymentEvent, PaymentStatus
from src.producers.payment_producer import PaymentProducer
from src.models.user import UserUpdate
from src.models.analytics import AnalyticsEvent
import structlog
from datetime import datetime
import uuid

logger = structlog.get_logger(__name__)

def create_app():
    app = FastAPI(title="StreamSocial Payment Dashboard", version="1.0.0")
    
    templates = Jinja2Templates(directory="src/web/templates")
    app.mount("/static", StaticFiles(directory="src/web/static"), name="static")
    
    # Initialize services with error handling
    try:
        producer = PaymentProducer("localhost:9092")
        redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)
        # Test Redis connection
        redis_client.ping()
        # Test producer health
        if producer.is_healthy():
            logger.info("Services initialized successfully")
        else:
            logger.warning("Producer health check failed, but continuing")
    except Exception as e:
        logger.error("Failed to initialize services", error=str(e))
        # Create dummy producer and redis client for graceful degradation
        producer = None
        redis_client = None
    
    @app.get("/", response_class=HTMLResponse)
    async def dashboard(request: Request):
        """Main dashboard showing payment metrics"""
        
        # Check if services are available
        if redis_client is None:
            return templates.TemplateResponse("dashboard.html", {
                "request": request,
                "stats": {"total_payments": 0, "successful_payments": 0, "failed_payments": 0, "success_rate": 0, "total_amount": 0},
                "recent_payments": [],
                "error": "Payment services are temporarily unavailable"
            })
        
        # Get payment statistics
        stats = get_payment_stats(redis_client)
        recent_payments = get_recent_payments(redis_client)
        
        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "stats": stats,
            "recent_payments": recent_payments
        })
    
    @app.post("/api/payment")
    async def create_payment(payment_data: Dict[str, Any]):
        """Create a new payment with exactly-once guarantees"""
        
        # Check if services are available
        if producer is None or redis_client is None:
            raise HTTPException(status_code=503, detail="Payment services are temporarily unavailable")
        
        try:
            # Generate idempotency key
            idempotency_key = f"{payment_data['user_id']}-{payment_data['subscription_type']}-{int(datetime.utcnow().timestamp())}"
            
            # Create payment event
            payment_event = PaymentEvent(
                user_id=payment_data['user_id'],
                amount=payment_data['amount'],
                payment_method=payment_data['payment_method'],
                subscription_type=payment_data.get('subscription_type'),
                idempotency_key=idempotency_key
            )
            
            # Create user update
            user_update = UserUpdate(
                user_id=payment_data['user_id'],
                subscription_type=payment_data.get('subscription_type'),
                subscription_status="active"
            )
            
            # Create analytics event
            analytics_event = AnalyticsEvent(
                event_type="payment_initiated",
                user_id=payment_data['user_id'],
                amount=payment_data['amount'],
                payment_method=payment_data['payment_method'],
                success=True,
                processing_time_ms=0
            )
            
            # Send transaction
            success = producer.send_payment_transaction(payment_event, user_update, analytics_event)
            
            if success:
                # Store payment for dashboard
                store_payment_for_dashboard(redis_client, payment_event)
                return {"success": True, "payment_id": payment_event.id, "idempotency_key": idempotency_key}
            else:
                raise HTTPException(status_code=500, detail="Failed to process payment")
                
        except Exception as e:
            logger.error("Payment creation failed", error=str(e))
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/api/stats")
    async def get_stats():
        """Get real-time payment statistics"""
        if redis_client is None:
            raise HTTPException(status_code=503, detail="Payment services are temporarily unavailable")
        return get_payment_stats(redis_client)
    
    return app

def get_payment_stats(redis_client: redis.Redis) -> Dict[str, Any]:
    """Get payment statistics from Redis"""
    try:
        total_payments = redis_client.get("stats:total_payments") or "0"
        successful_payments = redis_client.get("stats:successful_payments") or "0"
        failed_payments = redis_client.get("stats:failed_payments") or "0"
        total_amount = redis_client.get("stats:total_amount") or "0"
        
        success_rate = 0
        if int(total_payments) > 0:
            success_rate = (int(successful_payments) / int(total_payments)) * 100
        
        return {
            "total_payments": int(total_payments),
            "successful_payments": int(successful_payments),
            "failed_payments": int(failed_payments),
            "success_rate": round(success_rate, 2),
            "total_amount": float(total_amount)
        }
    except Exception as e:
        logger.error("Failed to get payment stats", error=str(e))
        return {
            "total_payments": 0,
            "successful_payments": 0,
            "failed_payments": 0,
            "success_rate": 0,
            "total_amount": 0
        }

def get_recent_payments(redis_client: redis.Redis, limit: int = 10) -> list:
    """Get recent payments for dashboard"""
    try:
        payment_keys = redis_client.keys("dashboard:payment:*")
        payments = []
        
        for key in payment_keys[-limit:]:
            payment_data = redis_client.get(key)
            if payment_data:
                payment_dict = json.loads(payment_data)
                # Ensure the payment has all required fields for the template
                payment_dict.setdefault('status', 'initiated')
                payments.append(payment_dict)
        
        return sorted(payments, key=lambda x: x.get('created_at', ''), reverse=True)
    except Exception as e:
        logger.error("Failed to get recent payments", error=str(e))
        return []

def store_payment_for_dashboard(redis_client: redis.Redis, payment: PaymentEvent):
    """Store payment data for dashboard display"""
    try:
        key = f"dashboard:payment:{payment.id}"
        payment_data = {
            "id": payment.id,
            "user_id": payment.user_id,
            "amount": payment.amount,
            "status": payment.status.value,
            "payment_method": payment.payment_method,
            "subscription_type": payment.subscription_type,
            "created_at": payment.created_at.isoformat()
        }
        
        redis_client.setex(key, 3600, json.dumps(payment_data))  # Store for 1 hour
        
        # Update stats
        redis_client.incr("stats:total_payments")
        redis_client.incrbyfloat("stats:total_amount", payment.amount)
        
    except Exception as e:
        logger.error("Failed to store payment for dashboard", payment_id=payment.id, error=str(e))
