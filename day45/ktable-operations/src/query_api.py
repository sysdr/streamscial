import json
import rocksdict
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict
from src.models import ReputationScore
from src.reputation_calculator import ReputationCalculator
from config.settings import settings

app = FastAPI(title="Reputation Query API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Open RocksDB state store for interactive queries
state_path = Path(settings.state_dir) / "reputation-store"
state_store = None

@app.on_event("startup")
async def startup():
    global state_store
    if state_path.exists():
        state_store = rocksdict.Rdict(str(state_path), access_type=rocksdict.AccessType.read_only())
        print(f"Connected to state store: {state_path}")
    else:
        print(f"Warning: State store not found at {state_path}")

@app.on_event("shutdown")
async def shutdown():
    if state_store:
        state_store.close()

@app.get("/api/reputation/{user_id}")
async def get_user_reputation(user_id: str) -> ReputationScore:
    """Interactive query: Get current reputation score for user"""
    if not state_store:
        raise HTTPException(status_code=503, detail="State store not available")
    
    try:
        data = state_store.get(user_id)
        if data:
            score_dict = json.loads(data)
            return ReputationScore(**score_dict)
        else:
            raise HTTPException(status_code=404, detail="User not found")
    except KeyError:
        raise HTTPException(status_code=404, detail="User not found")

@app.get("/api/top-users")
async def get_top_users(limit: int = 10) -> List[Dict]:
    """Get top users by reputation score"""
    if not state_store:
        raise HTTPException(status_code=503, detail="State store not available")
    
    # Scan state store (in production, use secondary index)
    users = []
    for key in state_store.keys():
        try:
            data = state_store.get(key)
            score_dict = json.loads(data)
            score = ReputationScore(**score_dict)
            users.append({
                "user_id": score.user_id,
                "score": score.score,
                "tier": ReputationCalculator.get_tier(score.score)
            })
        except:
            continue
    
    # Sort and limit
    users.sort(key=lambda x: x['score'], reverse=True)
    return users[:limit]

@app.get("/api/stats")
async def get_statistics() -> Dict:
    """Get global statistics"""
    if not state_store:
        raise HTTPException(status_code=503, detail="State store not available")
    
    total_users = 0
    total_score = 0
    tier_counts = {"bronze": 0, "silver": 0, "gold": 0, "platinum": 0}
    
    for key in state_store.keys():
        try:
            data = state_store.get(key)
            if data:
                score_dict = json.loads(data)
                score = ReputationScore(**score_dict)
                total_users += 1
                total_score += score.score
                tier = ReputationCalculator.get_tier(score.score)
                tier_counts[tier] += 1
        except Exception as e:
            # Log error but continue processing other keys
            print(f"Error processing key {key}: {e}")
            continue
    
    return {
        "total_users": total_users,
        "total_reputation_points": total_score,
        "average_score": total_score / total_users if total_users > 0 else 0,
        "tier_distribution": tier_counts
    }

@app.get("/health")
async def health_check():
    return {"status": "healthy", "state_store_connected": state_store is not None}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=settings.query_api_host, port=settings.query_api_port)

