import pytest
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock
from src.offset_manager.offset_coordinator import OffsetCoordinator, OffsetRange

@pytest.fixture
async def offset_coordinator():
    redis_mock = AsyncMock()
    postgres_mock = AsyncMock()
    
    coordinator = OffsetCoordinator(redis_mock, postgres_mock)
    coordinator.redis = redis_mock
    coordinator.postgres = postgres_mock
    
    return coordinator

@pytest.mark.asyncio
async def test_claim_offset_range_success(offset_coordinator):
    """Test successful offset range claiming"""
    coordinator = offset_coordinator
    
    # Mock Redis to return no existing range
    coordinator.redis.get.return_value = None
    coordinator.redis.setex.return_value = True
    
    # Claim range
    range_result = await coordinator.claim_offset_range("worker_1", 0, 1000, 100)
    
    assert range_result is not None
    assert range_result.worker_id == "worker_1"
    assert range_result.partition == 0
    assert range_result.start_offset == 1000
    assert range_result.end_offset == 1099

@pytest.mark.asyncio
async def test_claim_offset_range_already_claimed(offset_coordinator):
    """Test claiming already claimed offset range"""
    coordinator = offset_coordinator
    
    # Mock Redis to return existing active range
    existing_range = {
        "worker_id": "worker_2",
        "claimed_at": asyncio.get_event_loop().time(),
        "partition": 0,
        "start_offset": 1000,
        "end_offset": 1099
    }
    coordinator.redis.get.return_value = json.dumps(existing_range)
    
    # Try to claim range
    range_result = await coordinator.claim_offset_range("worker_1", 0, 1000, 100)
    
    assert range_result is None

@pytest.mark.asyncio
async def test_commit_offset_range(offset_coordinator):
    """Test offset range commit with metrics"""
    coordinator = offset_coordinator
    
    # Setup active range
    range_key = "offset_range:0:1000"
    offset_range = OffsetRange(0, 1000, 1099, "worker_1", asyncio.get_event_loop().time())
    coordinator.active_ranges[range_key] = offset_range
    
    # Mock PostgreSQL connection
    conn_mock = AsyncMock()
    transaction_mock = AsyncMock()
    conn_mock.transaction.return_value = transaction_mock
    conn_mock.__aenter__ = AsyncMock(return_value=conn_mock)
    conn_mock.__aexit__ = AsyncMock(return_value=None)
    
    pool_mock = AsyncMock()
    pool_mock.acquire.return_value = conn_mock
    coordinator.postgres = pool_mock
    
    # Test data
    metrics_data = [
        {"user_id": "user_1", "metric_type": "view_count", "metric_value": 1},
        {"user_id": "user_1", "metric_type": "engagement_score", "metric_value": 5}
    ]
    
    # Commit range
    await coordinator.commit_offset_range(range_key, metrics_data)
    
    # Verify calls
    assert conn_mock.execute.call_count == 3  # 2 metrics + 1 offset update
    coordinator.redis.delete.assert_called_once_with(range_key)
    assert range_key not in coordinator.active_ranges
