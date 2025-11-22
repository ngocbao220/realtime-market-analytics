from fastapi import APIRouter
from db import redis_client
import json

router = APIRouter()

@router.get("/history")
def get_trade_history():
    # Lấy 50 giao dịch mới nhất
    raw_data = redis_client.lrange("trades:history", 0, 50)
    trades = [json.loads(item) for item in raw_data]
    return trades