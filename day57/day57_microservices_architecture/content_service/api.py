"""Content service REST API"""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from service import ContentService
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Content Service")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

content_service = ContentService()

class PostCreate(BaseModel):
    user_id: str
    content: str

class LikePost(BaseModel):
    user_id: str

@app.post("/posts")
async def create_post(post: PostCreate):
    try:
        result = content_service.create_post(post.user_id, post.content)
        return {"status": "success", "post": result}
    except Exception as e:
        logger.error(f"Post creation failed: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/posts/{post_id}/like")
async def like_post(post_id: str, like: LikePost):
    try:
        result = content_service.like_post(post_id, like.user_id)
        return {"status": "success", "like": result}
    except Exception as e:
        logger.error(f"Like failed: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/posts")
async def get_posts(limit: int = 10):
    posts = content_service.get_posts(limit)
    return {"posts": posts}

@app.get("/health")
async def health():
    return {"status": "healthy", "service": "content-service"}

@app.get("/metrics")
async def metrics():
    return {
        "events_published": content_service.producer.events_published,
        "cached_users": len(content_service.user_cache)
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)
