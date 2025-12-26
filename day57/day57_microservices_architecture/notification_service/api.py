"""Notification service REST API"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from service import NotificationService
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Notification Service")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

notification_service = NotificationService()

@app.get("/notifications/{user_id}")
async def get_notifications(user_id: str, limit: int = 10):
    notifications = notification_service.get_user_notifications(user_id, limit)
    return {"notifications": notifications}

@app.get("/health")
async def health():
    return {"status": "healthy", "service": "notification-service"}

@app.get("/metrics")
async def metrics():
    return {
        "notifications_sent": notification_service.notifications_sent,
        "by_type": dict(notification_service.notifications_by_type)
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)
